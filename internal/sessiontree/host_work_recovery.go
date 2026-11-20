package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// Optional sparse locators, never execution authority. Released writers do
// not maintain this record. Match both coverage and the immutable source.
type pendingWorkSources struct {
	Events, Ordinal int
	Source, Hash    string
	IDs             []string
	Checksum        string `json:",omitempty"`
}

const pendingWorkLocatorLimit = 256

func pendingWorkKey(thread string) []byte { return backendDomainKey(thread, "pending-work") }

func readPendingWorkSources(tx spi.ReadTx, thread string, coverage sparseSourceCoverage) (pendingWorkSources, bool, error) {
	var index pendingWorkSources
	if coverage.HostWork == 0 {
		return index, true, nil
	}
	raw, err := tx.Get(sparseSourcesNamespace, pendingWorkKey(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return index, false, nil
	}
	if err != nil {
		return index, false, err
	}
	if err := decodeCanonicalBodyJSON(raw, &index); err != nil || index.Ordinal < 0 || index.Events < 1 || len(index.IDs) > pendingWorkLocatorLimit {
		return index, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	checksum := index.Checksum
	index.Checksum = ""
	content, err := json.Marshal(index)
	if err != nil || checksum != StableHash(string(content)) {
		return index, false, ErrAuthorityCorrupt
	}
	if index.Events != coverage.HostWork || index.Ordinal >= coverage.Through {
		return index, false, nil
	}
	id, err := tx.Get(sparseSourcesNamespace, sparseSourceKey(thread, "host_work", index.Ordinal))
	if errors.Is(err, spi.ErrNotFound) || err == nil && string(id) != index.Source {
		return index, false, nil
	}
	if err != nil {
		return index, false, err
	}
	entry, err := readCanonicalEntryMetadata(tx, thread, index.Source)
	return index, err == nil && entry.RawHash == index.Hash && entry.PathDepth == int64(index.Ordinal)+1, err
}

func storePendingWorkSources(tx spi.WriteTx, thread string, index pendingWorkSources) error {
	if len(index.IDs) > pendingWorkLocatorLimit {
		return deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, pendingWorkKey(thread))
	}
	index.Checksum = ""
	content, err := json.Marshal(index)
	if err != nil {
		return err
	}
	index.Checksum = StableHash(string(content))
	raw, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return tx.Put(sparseSourcesNamespace, pendingWorkKey(thread), raw)
}

func appendPendingWorkSources(tx spi.WriteTx, entry Entry, ordinal int, coverage sparseSourceCoverage) error {
	if sparseSourceCategory(entry) != "host_work" || entry.PathDepth != int64(ordinal)+1 {
		return nil
	}
	index, matched, err := readPendingWorkSources(tx, entry.ThreadID, coverage)
	if err != nil || !matched {
		return err
	}
	if entry.Type == EntryHostWork {
		work, err := decodeHostWork(entry)
		if err != nil {
			return err
		}
		if !work.Inherited {
			if work.State == "bound" {
				index.IDs = append(index.IDs, work.ID)
			} else {
				position := slices.Index(index.IDs, work.ID)
				if position < 0 {
					return deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, pendingWorkKey(entry.ThreadID))
				}
				binding, err := readCanonicalEntryMetadata(tx, entry.ThreadID, hostWorkEntryID(work.ID, false))
				if err != nil {
					return err
				}
				if _, _, err := hostWorksFromPath([]Entry{binding, entry}); err != nil {
					return err
				}
				index.IDs = slices.Delete(index.IDs, position, position+1)
			}
		}
	}
	index.Events, index.Ordinal, index.Source, index.Hash = coverage.HostWork+1, ordinal, entry.ID, entry.RawHash
	return storePendingWorkSources(tx, entry.ThreadID, index)
}

func pendingHostWorks(works []HostWork) []HostWork {
	return slices.DeleteFunc(works, func(work HostWork) bool { return work.State != "bound" || work.Inherited })
}

func (repo *MemoryRepo) PendingHostWorks(ctx context.Context, thread string) ([]HostWork, error) {
	works, err := repo.HostWorks(ctx, thread)
	return pendingHostWorks(works), err
}

func (repo *BackendRepo) PendingHostWorks(ctx context.Context, thread string) ([]HostWork, error) {
	if err := repo.prepareSparseSources(ctx, thread); err != nil {
		return nil, err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.PendingHostWorks(ctx, thread)
	}
	var works []HostWork
	var rebuild bool
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		coverage, err := readSparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		if count != coverage.Through {
			return spi.ErrConflict
		}
		if coverage.HostWork == 0 {
			return nil
		}
		leaf, err := readEntryLocator(tx, thread, meta.LeafID)
		if err != nil {
			return err
		}
		if leaf.Depth == int64(count) {
			index, matched, err := readPendingWorkSources(tx, thread, coverage)
			if err != nil {
				return err
			}
			if matched {
				for _, id := range index.IDs {
					if err := ctx.Err(); err != nil {
						return err
					}
					binding, err := readCanonicalEntryMetadata(tx, thread, hostWorkEntryID(id, false))
					if err != nil {
						return requiredRecordError(err)
					}
					work, err := decodeHostWork(binding)
					if err != nil || work.ID != id || work.Inherited || work.State != "bound" {
						return ErrAuthorityCorrupt
					}
					if _, err := readEntryLocator(tx, thread, hostWorkEntryID(id, true)); err == nil {
						return ErrAuthorityCorrupt
					} else if !errors.Is(err, ErrEntryNotFound) {
						return err
					}
					works = append(works, work)
				}
				return nil
			}
		}
		rebuild = true
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		works, err = memory.PendingHostWorks(ctx, thread)
		return err
	})
	if err != nil || !rebuild {
		return works, err
	}
	for ctx.Err() == nil {
		works, err = repo.rebuildPendingWorkSources(ctx, thread)
		if !errors.Is(err, spi.ErrConflict) {
			return works, err
		}
	}
	return nil, ctx.Err()
}

func (repo *BackendRepo) rebuildPendingWorkSources(ctx context.Context, thread string) ([]HostWork, error) {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	meta, err := readPublishedThreadMeta(reader, backendDomainV32Format, thread)
	if err != nil {
		return nil, err
	}
	tx := newProjectionSourceTx(reader)
	coverage, err := readSparseCoverage(tx, thread)
	if err != nil {
		return nil, err
	}
	leaf, err := readEntryLocator(tx, thread, meta.LeafID)
	if err != nil {
		return nil, reader.validationError(err)
	}
	if leaf.Depth != int64(coverage.Through) || leaf.Ordinal+1 != coverage.Through {
		path, err := canonicalHostWorkEntries(ctx, tx, meta)
		if err != nil {
			return nil, reader.validationError(err)
		}
		works, _, err := hostWorksFromPath(path)
		if err != nil {
			return nil, reader.validationError(err)
		}
		return pendingHostWorks(works), repo.backend.View(ctx, reader.verify)
	}
	works, last, err := linearPendingWorkSources(ctx, reader, thread)
	if err != nil {
		return nil, reader.validationError(err)
	}
	if last.ID == "" || len(works) > pendingWorkLocatorLimit {
		return works, repo.backend.View(ctx, reader.verify)
	}
	index := pendingWorkSources{Events: coverage.HostWork, Ordinal: int(last.PathDepth) - 1, Source: last.ID, Hash: last.RawHash}
	for _, work := range works {
		index.IDs = append(index.IDs, work.ID)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err = repo.backend.Update(ctx, func(write spi.WriteTx) error {
		if err := reader.verify(write); err != nil {
			return err
		}
		current, err := readSparseCoverage(write, thread)
		if err != nil {
			return err
		}
		if current != coverage {
			return spi.ErrConflict
		}
		return storePendingWorkSources(write, thread, index)
	})
	return works, err
}

// On a linear journal depth proves membership for every ordinal. Read only
// sparse canonical metadata; never materialize notice, Activity or output bodies.
func linearPendingWorkSources(ctx context.Context, tx spi.ReadTx, thread string) ([]HostWork, Entry, error) {
	prefix := backendDomainKey(thread, "host_work")
	pending := map[string]Entry{}
	var last Entry
	err := visitSparseSourcePages(ctx, tx, thread, "host_work", func(rows []spi.Record, count int) error {
		keys := make([][]byte, len(rows))
		ordinals := make([]int, len(rows))
		for index, row := range rows {
			if !bytes.HasPrefix(row.Key, prefix) {
				return ErrAuthorityCorrupt
			}
			ordinal, err := historicalEntryOrdinal(row.Key, prefix)
			if err != nil || ordinal >= count || int64(ordinal) < last.PathDepth || index > 0 && ordinal <= ordinals[index-1] {
				return ErrAuthorityCorrupt
			}
			ordinals[index], keys[index] = ordinal, backendDomainEntryKey(thread, ordinal)
		}
		records, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: backendDomainV32Namespace, Keys: keys})
		if err != nil {
			return requiredRecordError(err)
		}
		for index, raw := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			record, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
			entry := stored.Entry
			entry.bodyFields = stored.Fields
			if err != nil || record.Ordinal != ordinals[index] || record.ThreadID != thread || record.ID != string(rows[index].Value) || entry.PathDepth != int64(record.Ordinal)+1 || sparseSourceCategory(entry) != "host_work" {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			last = entry
			if entry.Type != EntryHostWork {
				continue
			}
			work, err := decodeHostWork(entry)
			if err != nil {
				return err
			}
			if work.State == "bound" {
				if _, exists := pending[work.ID]; exists {
					return ErrAuthorityCorrupt
				}
				pending[work.ID] = entry
			} else {
				binding, found := pending[work.ID]
				if !found {
					return ErrAuthorityCorrupt
				}
				if _, _, err := hostWorksFromPath([]Entry{binding, entry}); err != nil {
					return err
				}
				delete(pending, work.ID)
			}
		}
		return nil
	})
	if err != nil {
		return nil, Entry{}, err
	}
	var bindings []Entry
	for _, binding := range pending {
		bindings = append(bindings, binding)
	}
	slices.SortFunc(bindings, func(first, second Entry) int { return int(first.PathDepth - second.PathDepth) })
	works, _, err := hostWorksFromPath(bindings)
	return pendingHostWorks(works), last, err
}
