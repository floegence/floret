package sessiontree

import (
	"context"
	"encoding/json"

	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// canonicalQueryDocumentTerms reads bounded immutable body chunks without source residency.
// Its source list and field ordering come from the canonical query projector;
// it does not create another transcript or a second search projection.
func (r *MemoryRepo) canonicalQueryDocumentTerms(ctx context.Context, tx spi.ReadTx, document spi.IndexDocument) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	collector := newQueryTermCollector()
	if metadata.Row.Object == threadquery.Threads {
		collector.beginPart("title")
		collector.write(metadata.Row.Title)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return collector.finish(), nil
	}
	writeField := func(entry Entry, part queryPart) error {
		collector.beginPart(part.field)
		field := ""
		switch part.field {
		case "body", "result":
			field = "content"
		case "arguments":
			field = "arguments"
		}
		if ref, found := entry.bodyFields[field]; field != "" && found {
			return visitCanonicalBody(ctx, tx, ref, func(chunk []byte) error { collector.write(string(chunk)); return nil })
		}
		collector.write(part.text)
		return ctx.Err()
	}
	sources, err := r.queryDocumentSources(document.ThreadID, metadata)
	if err != nil {
		return nil, err
	}
	var activities []Entry
	for sourceIndex, source := range sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal, ok := r.entryOrdinals[document.ThreadID][source]
		if !ok {
			return nil, ErrAuthorityCorrupt
		}
		entry := r.entries[document.ThreadID][ordinal]
		if metadata.Row.Object == threadquery.Messages && sourceIndex > 0 {
			if err := writeField(entry, queryPart{"body", entry.Message.Content}); err != nil {
				return nil, err
			}
		} else {
			for _, part := range queryEntryParts(entry) {
				if err := writeField(entry, part); err != nil {
					return nil, err
				}
			}
		}
		if entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
			if sourceIndex == 0 {
				if err := writeField(entry, queryPart{"tool_name", metadata.Row.ToolName}); err != nil {
					return nil, err
				}
			}
			if entry.Message.Activity != nil || entry.bodyFields["activity_json"].Bytes > 0 {
				activities = append(activities, entry)
			}
		}
	}
	if len(activities) > 0 {
		collector.beginPart("presentation")
		collector.write("[")
		for i, entry := range activities {
			if i > 0 {
				collector.write(",")
			}
			if ref, found := entry.bodyFields["activity_json"]; found {
				if err := visitCanonicalBody(ctx, tx, ref, func(chunk []byte) error { collector.write(string(chunk)); return nil }); err != nil {
					return nil, err
				}
			} else {
				raw, err := json.Marshal(entry.Message.Activity)
				if err != nil {
					return nil, err
				}
				collector.write(string(raw))
			}
		}
		collector.write("]")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return collector.finish(), nil
}
