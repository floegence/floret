package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// Search resolves only the canonical interval named by an index document.
// No journal snapshot or dependency closure is needed to read a matching fact.
func canonicalQuerySources(ctx context.Context, tx spi.ReadTx, metadata queryDocumentMetadata) ([]Entry, error) {
	var entries []Entry
	err := visitCanonicalQuerySources(ctx, tx, metadata, func(entry Entry, _ int) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// Source boundaries belong to the canonical path. Walking by depth keeps only
// one source record in memory, including arbitrarily fragmented output.
func visitCanonicalQuerySources(ctx context.Context, tx spi.ReadTx, metadata queryDocumentMetadata, visit func(Entry, int) error) error {
	threadID := metadata.Row.ThreadID.String()
	first, err := readEntryLocator(tx, threadID, metadata.FirstSource)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	last, err := readEntryLocator(tx, threadID, metadata.LastSource)
	if err != nil || last.Ordinal < first.Ordinal || last.Depth < first.Depth {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	ancestor, err := entryAncestor(ctx, tx, threadID, last, first.Depth)
	if err != nil || ancestor.ID != first.ID {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	begin, err := readLocatedEntry(tx, backendDomainV32Format, threadID, first)
	if err != nil {
		return err
	}
	index := 0
	for depth := first.Depth; depth <= last.Depth; depth++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := entryAncestor(ctx, tx, threadID, last, depth)
		if err != nil {
			return err
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, threadID, node)
		if err != nil {
			return err
		}
		matches, err := querySourceMatches(entry, begin, metadata)
		if err != nil {
			return err
		}
		if !matches {
			if depth == first.Depth || depth == last.Depth {
				return ErrAuthorityCorrupt
			}
			continue
		}
		if err := visit(entry, index); err != nil {
			return err
		}
		index++
	}
	return nil
}

func materializeCanonicalQueryDocument(ctx context.Context, tx spi.ReadTx, document queryStoredDocument) (*queryRecord, error) {
	var metadata queryDocumentMetadata
	if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
		return nil, err
	}
	if metadata.Row.ThreadID.String() != document.ThreadID || string(metadata.Row.Object) != document.Object {
		return nil, spi.ErrIndexCorrupt
	}
	record := &queryRecord{key: document.Key, row: metadata.Row, firstSource: metadata.FirstSource, previousSource: metadata.PreviousSource, lastSource: metadata.LastSource}
	if metadata.Row.Object == threadquery.Threads {
		if document.Key != document.ThreadID+"\x00threads" {
			return nil, spi.ErrIndexCorrupt
		}
		record.parts = []queryPart{{"title", metadata.Row.Title}}
		return record, nil
	}
	err := visitCanonicalQuerySources(ctx, tx, metadata, func(source Entry, index int) error {
		entry, err := completeLocatedEntry(ctx, tx, source)
		if err != nil {
			return err
		}
		if record.row.Object == threadquery.Messages && index > 0 {
			record.parts = append(record.parts, queryPart{"body", entry.Message.Content})
		} else {
			record.parts = append(record.parts, queryEntryParts(entry)...)
		}
		if entry.Type == EntryCustom && entry.Metadata["kind"] == HostedToolEntryKind {
			if len(record.parts) == 0 {
				record.parts = append(record.parts, queryPart{"tool_name", record.row.ToolName})
			}
			if entry.Message.Activity != nil {
				record.activities = append(record.activities, entry.Message.Activity)
			}
		}
		if record.estimateBytes() > querySnapshotMaxBytes {
			return threadquery.ErrSnapshotLimit
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func canonicalQueryReadParts(ctx context.Context, tx spi.ReadTx, record *queryRecord) ([]queryPart, error) {
	var source session.Message
	if record.row.Object == threadquery.Messages {
		threadID := record.row.ThreadID.String()
		entryID := strings.TrimPrefix(record.key, threadID+"\x00messages\x00")
		locator, err := readEntryLocator(tx, threadID, entryID)
		if errors.Is(err, ErrEntryNotFound) {
			return nil, threadquery.ErrRecordUnavailable
		}
		if err != nil {
			return nil, err
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, threadID, locator)
		if err != nil {
			return nil, err
		}
		source = entry.Message
	}
	return queryReadParts(record, source)
}

func (repo *BackendRepo) canonicalQuerySession(ctx context.Context, tx spi.ReadTx) *querySession {
	if repo.threadQuerySnapshots == nil {
		repo.threadQuerySnapshots = &querySnapshots{snapshots: map[string]*querySnapshot{}}
	}
	return &querySession{now: repo.now, querySnapshots: repo.threadQuerySnapshots,
		selectRecords: func(ctx context.Context, authorized map[string]bool, object threadquery.Object, filter threadquery.Filter) ([]*queryRecord, error) {
			ids := make([]string, 0, len(authorized))
			for id := range authorized {
				if len(filter.ThreadIDs) > 0 && !queryThreadSelected(filter.ThreadIDs, id) {
					continue
				}
				ids = append(ids, id)
			}
			documents, err := queryMetadataReader(tx).listDocuments(queryMetadataSelection{ThreadIDs: ids, Object: string(object)})
			if err != nil {
				return nil, err
			}
			return selectQueryCandidates(ctx, documents, authorized, object, filter, repo.threadQuerySnapshots, func(document queryStoredDocument) (*queryRecord, error) {
				return materializeCanonicalQueryDocument(ctx, tx, document)
			})
		},
		queryRecordContent: func(record *queryRecord) (*queryRecord, error) {
			if !record.lazy {
				return record, nil
			}
			document, err := queryDocument(record)
			if err != nil {
				return nil, err
			}
			return materializeCanonicalQueryDocument(ctx, tx, document)
		},
		readParts: func(record *queryRecord) ([]queryPart, error) { return canonicalQueryReadParts(ctx, tx, record) },
	}
}
