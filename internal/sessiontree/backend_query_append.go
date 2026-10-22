package sessiontree

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func projectCanonicalQuery(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) (*queryThread, error) {
	state := newQueryThread()
	projector := &queryProjector{metadataOnly: true}
	if meta.LeafID != "" {
		leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return nil, err
		}
		for depth := int64(1); depth <= leaf.Depth; depth++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			node, err := entryAncestor(ctx, tx, meta.ID, leaf, depth)
			if err != nil {
				return nil, err
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
			if err != nil {
				return nil, err
			}
			entry, err = canonicalQueryProjectionEntry(ctx, tx, entry, false)
			if err != nil {
				return nil, err
			}
			if err := projector.observe(state, meta.ID, entry); err != nil {
				return nil, err
			}
		}
	}
	projector.finish(state, meta.ID, meta)
	return state, nil
}

func canonicalQueryProjectionEntry(ctx context.Context, tx spi.ReadTx, entry Entry, complete bool) (Entry, error) {
	if entry.Type != EntrySteeringApplied {
		return entry, nil
	}
	resolution, err := DecodeSteeringResolution(entry)
	if err != nil {
		return Entry{}, err
	}
	source, err := readCanonicalEntryMetadata(tx, entry.ThreadID, resolution.SteeringID)
	if err != nil {
		return Entry{}, err
	}
	if complete {
		source, err = completeLocatedEntry(ctx, tx, source)
		if err != nil {
			return Entry{}, err
		}
	}
	entry.Type, entry.ID, entry.Message, entry.bodyFields = EntryUserMessage, source.ID, source.Message, source.bodyFields
	return entry, nil
}

func appendCanonicalQuery(ctx context.Context, tx spi.WriteTx, before, after ThreadMeta, entry Entry, previous *queryThread) (*queryThread, error) {
	raw, err := tx.Get(queryManifestNamespace, []byte(before.ID))
	if errors.Is(err, spi.ErrNotFound) {
		// A concurrent preparation owns incomplete metadata. Canonical writes
		// may proceed, but only a complete rebuild can publish their manifest.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest queryManifest
	if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 3 || manifest.Leaf != before.LeafID {
		return nil, deleteCanonicalOptionalRecord(tx, queryManifestNamespace, []byte(before.ID))
	}
	index := queryMetadataWriter(tx)
	if entry.ParentID != before.LeafID {
		return rebuildCanonicalQuery(ctx, tx, after)
	}
	if previous == nil || previous.leaf != before.LeafID {
		var err error
		previous, err = projectCanonicalQuery(ctx, tx, before)
		if err != nil {
			return nil, err
		}
		compactQueryThread(previous)
	}
	state := cloneQueryThread(previous)
	projector := &queryProjector{metadataOnly: true, changed: map[string]*queryRecord{}, lookup: func(key string) (*queryRecord, error) {
		return canonicalQueryRecordForAppend(ctx, tx, before.ID, key)
	}}
	projectedEntry, err := canonicalQueryProjectionEntry(ctx, tx, entry, false)
	if err != nil {
		return nil, err
	}
	if err := projector.observe(state, before.ID, projectedEntry); err != nil {
		return nil, err
	}
	projector.finish(state, after.ID, after)
	for _, record := range projector.changed {
		document, err := queryDocument(record)
		if err != nil {
			return nil, err
		}

		if err := index.putDocument(document); err != nil {
			return nil, err
		}
	}
	return state, putQueryManifest(tx, after.ID, after.LeafID)
}

func canonicalQueryRecordForAppend(ctx context.Context, tx spi.ReadTx, threadID, key string) (*queryRecord, error) {
	document, found, err := readQueryDocumentByKey(queryMetadataReader(tx), threadID, key)
	if err != nil || !found {
		return nil, err
	}
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	return &queryRecord{key: key, row: metadata.Row, firstSource: metadata.FirstSource, previousSource: metadata.PreviousSource, lastSource: metadata.LastSource, lazy: true}, nil
}

func rebuildCanonicalQuery(ctx context.Context, tx spi.WriteTx, after ThreadMeta) (*queryThread, error) {
	index := queryMetadataWriter(tx)
	state, err := projectCanonicalQuery(ctx, tx, after)
	if err != nil {
		return nil, err
	}
	if err := index.deleteThread(after.ID); err != nil {
		return nil, err
	}
	for _, record := range state.records {
		document, err := queryDocument(record)
		if err != nil {
			return nil, err
		}

		if err := index.putDocument(document); err != nil {
			return nil, err
		}
	}
	return state, putQueryManifest(tx, after.ID, after.LeafID)
}
