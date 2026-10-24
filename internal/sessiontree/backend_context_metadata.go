package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const contextMetadataNamespace = "floret.sessiontree.context-source-metadata.v1"

type contextProjectionWindowReader struct{ repo *BackendRepo }

func (reader contextProjectionWindowReader) PathMetadataWindow(ctx context.Context, options PathWindowOptions) (PathMetadataWindow, error) {
	return reader.repo.canonicalPathWindowMode(ctx, options, false, true)
}

// Only these final sources can survive the canonical context fold. Restore
// their complete measurements before invoking any reducer. A policy change or
// compaction may discard one; retaining that extra source is harmless.
func (repo *BackendRepo) restoreSelectedContextStatuses(ctx context.Context, thread string, pages []PathMetadataWindow) error {
	var last, confirmed, estimate *PathMetadata
	for p := range pages {
		for i := range pages[p].Entries {
			source := &pages[p].Entries[i]
			metadata := source.Entry.contextStatusMetadata
			if metadata == nil {
				continue
			}
			last = source
			if metadata.Confirmed {
				confirmed, estimate = source, nil
			} else {
				estimate = source
			}
		}
	}
	seen := map[string]bool{}
	for _, source := range []*PathMetadata{last, confirmed, estimate} {
		if source == nil || seen[source.Entry.ID] {
			continue
		}
		seen[source.Entry.ID] = true
		complete, err := repo.Entry(ctx, thread, source.Entry.ID)
		if err != nil {
			return err
		}
		status, err := contextStatusMetadataFor(complete)
		if err != nil || status == nil || *status != *source.Entry.contextStatusMetadata || complete.RawHash != source.Entry.RawHash || complete.ParentID != source.Entry.ParentID || complete.PathDepth != source.Entry.PathDepth {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		*source, err = canonicalPathMetadata(complete)
		if err != nil {
			return err
		}
	}
	return nil
}

type contextSourceMetadata struct {
	Ordinal   int                   `json:"ordinal"`
	ID        string                `json:"id"`
	ThreadID  string                `json:"thread_id"`
	TurnID    string                `json:"turn_id"`
	RunID     string                `json:"run_id"`
	ParentID  string                `json:"parent_id"`
	PathDepth int64                 `json:"path_depth"`
	CreatedAt time.Time             `json:"created_at"`
	RawHash   string                `json:"raw_hash"`
	Status    contextStatusMetadata `json:"status"`
}

func (source contextSourceMetadata) entry() Entry {
	return Entry{ID: source.ID, ThreadID: source.ThreadID, TurnID: source.TurnID, RunID: source.RunID, ParentID: source.ParentID, PathDepth: source.PathDepth, CreatedAt: source.CreatedAt, RawHash: source.RawHash, Type: EntryCustom, Metadata: map[string]string{threadContextKindKey: ThreadContextStatusEntryKind, threadContextTypeKey: ThreadContextStatusEntryKind}}
}

func contextMetadataCoverageKey(thread string) []byte { return backendDomainKey(thread, "coverage") }
func contextMetadataKey(thread string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(thread), storagecodec.TupleString("source"), storagecodec.TupleOrdinal(uint64(ordinal)))
}
func readContextMetadataCoverage(tx spi.ReadTx, thread string) (int, error) {
	raw, err := tx.Get(contextMetadataNamespace, contextMetadataCoverageKey(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var through int
	if err := storagecodec.DecodeJSON(raw, &through); err != nil || through < 0 {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return through, nil
}
func putContextMetadataCoverage(tx spi.WriteTx, thread string, through int) error {
	if through == 0 {
		return deleteCanonicalOptionalRecord(tx, contextMetadataNamespace, contextMetadataCoverageKey(thread))
	}
	raw, err := json.Marshal(through)
	if err != nil {
		return err
	}
	return tx.Put(contextMetadataNamespace, contextMetadataCoverageKey(thread), raw)
}
func putContextSourceMetadata(tx spi.WriteTx, entry Entry, ordinal int) error {
	if ThreadContextEntryKind(entry) != ThreadContextStatusEntryKind {
		return nil
	}
	status, err := contextStatusMetadataFor(entry)
	if err != nil {
		return err
	}
	// Only status records without other bodies are eligible. Complete reads
	// and unusual custom records retain their canonical source path.
	fields, err := canonicalStoredFields(entry, true)
	if err != nil {
		return err
	}
	if len(fields) != 0 || len(entry.bodyFields) != 0 {
		return nil
	}
	// Metadata is usable only for the exact standard status shape. Unusual
	// custom entries retain complete reads rather than losing other facts.
	if len(entry.Metadata) != 3 || entry.Metadata[threadContextTypeKey] != ThreadContextStatusEntryKind {
		return nil
	}
	source := contextSourceMetadata{Ordinal: ordinal, ID: entry.ID, ThreadID: entry.ThreadID, TurnID: entry.TurnID, RunID: entry.RunID, ParentID: entry.ParentID, PathDepth: entry.PathDepth, CreatedAt: entry.CreatedAt, RawHash: entry.RawHash, Status: *status}
	stub := source.entry()
	stub.Metadata[threadContextStatusKey] = entry.Metadata[threadContextStatusKey]
	stub.Raw = rawForEntry(stub)
	if stableHash(stub.Raw) != entry.RawHash {
		return nil
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return tx.Put(contextMetadataNamespace, contextMetadataKey(entry.ThreadID, ordinal), raw)
}
func appendContextSourceMetadata(tx spi.WriteTx, entry Entry, ordinal int) error {
	through, err := readContextMetadataCoverage(tx, entry.ThreadID)
	if err != nil {
		return err
	}
	if through > ordinal {
		return ErrAuthorityCorrupt
	}
	if through != ordinal {
		return nil
	}
	if err := putContextSourceMetadata(tx, entry, ordinal); err != nil {
		return err
	}
	return putContextMetadataCoverage(tx, entry.ThreadID, ordinal+1)
}
func removeContextSourceMetadata(tx spi.WriteTx, entry Entry, ordinal int) error {
	through, err := readContextMetadataCoverage(tx, entry.ThreadID)
	if err != nil {
		return err
	}
	if through > ordinal+1 {
		return ErrAuthorityCorrupt
	}
	if err := deleteCanonicalOptionalRecord(tx, contextMetadataNamespace, contextMetadataKey(entry.ThreadID, ordinal)); err != nil {
		return err
	}
	if through == ordinal+1 {
		return putContextMetadataCoverage(tx, entry.ThreadID, ordinal)
	}
	return nil
}

// Preparation adds only rebuildable numeric/source metadata. Canonical bytes
// never change. Each short transaction advances a contiguous coverage prefix;
// no reader accepts partial coverage. Append and truncation share that owner.
func (repo *BackendRepo) prepareContextSourceMetadata(ctx context.Context, thread string) error {
	for {
		complete := false
		err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
			count, err := readJournalCount(tx, thread)
			if err != nil {
				return err
			}
			through, err := readContextMetadataCoverage(tx, thread)
			if err != nil {
				return err
			}
			if through > count {
				return ErrAuthorityCorrupt
			}
			complete = through == count
			return nil
		}, func(*MemoryRepo) error { complete = true; return nil })
		if err != nil || complete {
			return err
		}
		err = func() error {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread); err != nil {
					return err
				}
				count, err := readJournalCount(tx, thread)
				if err != nil {
					return err
				}
				through, err := readContextMetadataCoverage(tx, thread)
				if err != nil {
					return err
				}
				if through > count {
					return ErrAuthorityCorrupt
				}
				end := min(count, through+128)
				for ordinal := through; ordinal < end; ordinal++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					entry, err := readCanonicalEntryOrdinal(tx, thread, ordinal)
					if err != nil {
						return err
					}
					if err := putContextSourceMetadata(tx, entry, ordinal); err != nil {
						return err
					}
				}
				return putContextMetadataCoverage(tx, thread, end)
			})
		}()
		if err != nil {
			return err
		}
	}
}

func visitContextMetadataPage(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, node entryLocator, limit int, visit func(Entry) error) (PathPage, error) {
	if node.Depth != int64(node.Ordinal)+1 {
		return visitCanonicalPage(ctx, tx, meta, node.ID, "", limit, visit)
	}
	first := max(0, node.Ordinal-limit+1)
	through, err := readContextMetadataCoverage(tx, meta.ID)
	if err != nil {
		return PathPage{}, err
	}
	count, err := readJournalCount(tx, meta.ID)
	if err != nil {
		return PathPage{}, err
	}
	if through != count {
		return PathPage{}, ErrAuthorityCorrupt
	}
	page, err := tx.Scan(spi.ScanRequest{Namespace: contextMetadataNamespace, Start: contextMetadataKey(meta.ID, first), End: contextMetadataKey(meta.ID, node.Ordinal+1), Limit: limit})
	if err != nil {
		return PathPage{}, err
	}
	if len(page.Records) == 0 {
		return visitLinearCanonicalPage(ctx, tx, meta, node, limit, visit)
	}
	if page.HasMore {
		return PathPage{}, ErrAuthorityCorrupt
	}
	entries := make([]Entry, node.Ordinal-first+1)
	for _, row := range page.Records {
		var source contextSourceMetadata
		if err := storagecodec.DecodeJSON(row.Value, &source); err != nil {
			return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		entry := source.entry()
		if source.Ordinal < first || source.Ordinal > node.Ordinal || !bytes.Equal(row.Key, contextMetadataKey(meta.ID, source.Ordinal)) || entry.ThreadID != meta.ID || entry.ID == "" || entry.RawHash == "" || entry.PathDepth != int64(source.Ordinal)+1 || ThreadContextEntryKind(entry) != ThreadContextStatusEntryKind || entry.Metadata[threadContextStatusKey] != "" {
			return PathPage{}, ErrAuthorityCorrupt
		}
		if _, err := source.Status.status(entry); err != nil {
			return PathPage{}, err
		}
		entry.contextStatusMetadata = &source.Status
		entries[source.Ordinal-first] = entry
	}
	var keys [][]byte
	var positions []int
	for i := range entries {
		if entries[i].ID == "" {
			keys = append(keys, backendDomainEntryKey(meta.ID, first+i))
			positions = append(positions, i)
		}
	}
	if len(keys) != 0 {
		rows, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: backendDomainV32Namespace, Keys: keys})
		if err != nil {
			return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		for i, raw := range rows {
			record, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
			if err != nil || record.ThreadID != meta.ID || record.Ordinal != first+positions[i] {
				return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			stored.Entry.bodyFields = stored.Fields
			entries[positions[i]] = stored.Entry
		}
	}
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return PathPage{}, err
		}
		if entry.PathDepth != int64(first+i)+1 || i > 0 && entry.ParentID != entries[i-1].ID || first+i == 0 && entry.ParentID != "" {
			return PathPage{}, ErrAuthorityCorrupt
		}
	}
	if first > 0 {
		parent, err := readEntryLocator(tx, meta.ID, entries[0].ParentID)
		if err != nil || parent.Ordinal != first-1 || parent.Depth != int64(first) {
			return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	last := entries[len(entries)-1]
	if last.ID != node.ID || last.ParentID != node.Parent {
		return PathPage{}, ErrAuthorityCorrupt
	}
	result := PathPage{Entries: []Entry{}, NewestOrdinal: node.Depth, HasMore: first > 0}
	if result.HasMore {
		result.NextEntryID = entries[0].ID
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if err := visit(entries[i]); err != nil {
			return PathPage{}, err
		}
	}
	return result, nil
}
