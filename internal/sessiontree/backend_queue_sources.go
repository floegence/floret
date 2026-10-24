package sessiontree

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// Queue sources describe current membership, never another copy of queued input.
// Queue facts belong to the journal, including facts outside the active path.
type QueueSourceSnapshot struct {
	Pending  []Entry
	Restored []QueueRestoreSource
}
type QueueRestoreSource struct{ Added, Removed Entry }
type CurrentQueueSourceReader interface {
	CurrentQueueSources(context.Context, string) (QueueSourceSnapshot, error)
}
type queueSourceRef struct{ ID, Source string }
type queueRestoreRef struct{ Added, Removed string }
type storedQueueSources struct {
	Events   int
	Pending  []queueSourceRef
	Restored []queueRestoreRef
}

func currentQueueKey(thread string) []byte { return backendDomainKey(thread, "current-queue") }
func queueSourceID(entry Entry) (string, error) {
	var id string
	switch entry.Type {
	case EntryQueueAdded:
		var value struct{ ID string }
		if json.Unmarshal(entry.Payload, &value) != nil {
			return "", ErrAuthorityCorrupt
		}
		id = value.ID
		if id == "" {
			id = entry.ID
		}
	case EntryQueueDeleted, EntryQueuePromoted:
		if json.Unmarshal(entry.Payload, &id) != nil {
			return "", ErrAuthorityCorrupt
		}
	}
	return id, nil
}
func readQueueSourceIndex(tx spi.ReadTx, thread string) (storedQueueSources, bool, error) {
	raw, err := tx.Get(sparseSourcesNamespace, currentQueueKey(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return storedQueueSources{}, false, nil
	}
	var index storedQueueSources
	if err != nil {
		return index, false, err
	}
	if err := decodeCanonicalBodyJSON(raw, &index); err != nil || index.Events < 0 {
		return index, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return index, true, nil
}
func storeQueueSourceIndex(tx spi.WriteTx, thread string, index storedQueueSources) error {
	raw, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return tx.Put(sparseSourcesNamespace, currentQueueKey(thread), raw)
}
func observeQueueSources(index *storedQueueSources, entry Entry, latest func(string) (string, error), save func(string, string) error) error {
	id, err := queueSourceID(entry)
	if err != nil {
		return err
	}
	switch entry.Type {
	case EntryQueueAdded:
		found := false
		for _, ref := range index.Pending {
			found = found || ref.ID == id
		}
		if !found {
			index.Pending = append(index.Pending, queueSourceRef{id, entry.ID})
		}
		if err := save(id, entry.ID); err != nil {
			return err
		}
	case EntryQueueDeleted, EntryQueuePromoted:
		if entry.Type == EntryQueueDeleted && entry.Metadata["restored_from_backup"] == "true" {
			raw, err := latest(id)
			if err != nil || len(raw) == 0 {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			index.Restored = append(index.Restored, queueRestoreRef{raw, entry.ID})
		}
		for i, ref := range index.Pending {
			if ref.ID == id {
				index.Pending = append(index.Pending[:i], index.Pending[i+1:]...)
				break
			}
		}
	case EntryQueueReordered:
		var ids []string
		if json.Unmarshal(entry.Payload, &ids) != nil {
			return ErrAuthorityCorrupt
		}
		pending := make(map[string]queueSourceRef, len(index.Pending))
		for _, ref := range index.Pending {
			pending[ref.ID] = ref
		}
		next := make([]queueSourceRef, 0, len(index.Pending))
		for _, id := range ids {
			if ref, ok := pending[id]; ok {
				next = append(next, ref)
				delete(pending, id)
			}
		}
		if len(pending) != 0 {
			return ErrAuthorityCorrupt
		}
		index.Pending = next
	default:
		return ErrAuthorityCorrupt
	}
	index.Events++
	return nil
}
func appendCurrentQueueSources(tx spi.WriteTx, entry Entry, prior sparseSourceCoverage) error {
	if sparseSourceCategory(entry) != "queue" {
		return nil
	}
	index, found, err := readQueueSourceIndex(tx, entry.ThreadID)
	if err != nil {
		return err
	}
	if !found && prior.Queue != 0 {
		return nil
	}
	if found && index.Events != prior.Queue {
		return ErrAuthorityCorrupt
	}
	if err := observeStoredQueueSources(tx, entry.ThreadID, &index, entry); err != nil {
		// An invalid presentation cannot publish current membership. Opaque journal
		// append remains supported; canonical bytes remain the only authority.
		if errors.Is(err, ErrAuthorityCorrupt) {
			return deleteCanonicalOptionalRecord(tx, sparseSourcesNamespace, currentQueueKey(entry.ThreadID))
		}
		return err
	}
	return storeQueueSourceIndex(tx, entry.ThreadID, index)
}
func readCurrentQueueSources(ctx context.Context, tx spi.ReadTx, thread string) (QueueSourceSnapshot, error) {
	result := QueueSourceSnapshot{Pending: []Entry{}}
	coverage, err := readSparseCoverage(tx, thread)
	if err != nil {
		return result, err
	}
	index, found, err := readQueueSourceIndex(tx, thread)
	if err != nil {
		return result, err
	}
	if !found && coverage.Queue == 0 {
		return result, nil
	}
	if !found || index.Events != coverage.Queue {
		return result, spi.ErrConflict
	}
	read := func(id string, kind EntryType) (Entry, error) {
		if err := ctx.Err(); err != nil {
			return Entry{}, err
		}
		locator, err := readEntryLocator(tx, thread, id)
		if err != nil || locator.Ordinal >= coverage.Through {
			return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		entry, err := readLocatedEntry(tx, backendDomainV32Format, thread, locator)
		if err != nil || entry.Type != kind {
			return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		return entry, nil
	}
	seen := map[string]bool{}
	for _, ref := range index.Pending {
		entry, err := read(ref.Source, EntryQueueAdded)
		if err != nil {
			return result, err
		}
		id, err := queueSourceID(entry)
		if err != nil || id != ref.ID || seen[id] {
			return result, ErrAuthorityCorrupt
		}
		seen[id] = true
		result.Pending = append(result.Pending, entry)
	}
	for _, ref := range index.Restored {
		added, err := read(ref.Added, EntryQueueAdded)
		if err != nil {
			return result, err
		}
		removed, err := read(ref.Removed, EntryQueueDeleted)
		if err != nil {
			return result, err
		}
		a, err := queueSourceID(added)
		if err != nil {
			return result, err
		}
		b, err := queueSourceID(removed)
		if err != nil || a != b || removed.Metadata["restored_from_backup"] != "true" {
			return result, ErrAuthorityCorrupt
		}
		result.Restored = append(result.Restored, QueueRestoreSource{added, removed})
	}
	return result, nil
}
func (repo *BackendRepo) prepareCurrentQueueSources(ctx context.Context, thread string) error {
	if err := repo.prepareSparseSources(ctx, thread); err != nil {
		return err
	}
	ready := false
	if err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		coverage, err := readSparseCoverage(tx, thread)
		if err != nil {
			return err
		}
		index, found, err := readQueueSourceIndex(tx, thread)
		ready = coverage.Queue == 0 || found && index.Events == coverage.Queue
		return err
	}, func(*MemoryRepo) error { ready = true; return nil }); err != nil || ready {
		return err
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for {
		err := repo.rebuildCurrentQueueSources(work, thread)
		if !errors.Is(err, spi.ErrConflict) {
			return err
		}
		if err := work.Err(); err != nil {
			return err
		}
	}
}

// Missing metadata is scanned outside a writer transaction. The current source
// anchor publishes only after verifying the frozen canonical journal owner.
func (repo *BackendRepo) rebuildCurrentQueueSources(ctx context.Context, thread string) error {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	if _, err := readPublishedThreadMeta(reader, backendDomainV32Format, thread); err != nil {
		return err
	}
	coverage, err := readSparseCoverage(reader, thread)
	if err != nil {
		return err
	}
	index := storedQueueSources{}
	latest := map[string]string{}
	if err := visitSparseSources(ctx, reader, thread, "queue", func(entry Entry) error {
		return observeQueueSources(&index, entry, func(id string) (string, error) {
			source, ok := latest[id]
			if !ok {
				return "", ErrAuthorityCorrupt
			}
			return source, nil
		}, func(id, source string) error { latest[id] = source; return nil })
	}); err != nil {
		return reader.validationError(err)
	}
	update := func(write func(spi.WriteTx) error) error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			if err := reader.verify(tx); err != nil {
				return err
			}
			now, err := readSparseCoverage(tx, thread)
			if err != nil {
				return err
			}
			if now != coverage {
				return spi.ErrConflict
			}
			return write(tx)
		})
	}

	return update(func(tx spi.WriteTx) error { return storeQueueSourceIndex(tx, thread, index) })
}

func (repo *BackendRepo) CurrentQueueSources(ctx context.Context, thread string) (QueueSourceSnapshot, error) {
	if err := repo.prepareCurrentQueueSources(ctx, thread); err != nil {
		return QueueSourceSnapshot{}, err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.CurrentQueueSources(ctx, thread)
	}
	var result QueueSourceSnapshot
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		var err error
		result, err = readCurrentQueueSources(ctx, tx, thread)
		return err
	}, func(memory *MemoryRepo) error {
		entries, err := memory.QueueMetadata(ctx, thread)
		if err != nil {
			return err
		}
		result, err = currentQueueSourcesFromEntries(entries)
		return err
	})
	return result, err
}

func observeStoredQueueSources(tx spi.WriteTx, thread string, index *storedQueueSources, entry Entry) error {
	return observeQueueSources(index, entry, func(id string) (string, error) {
		locator, err := readEntryLocator(tx, thread, entry.ID)
		if err != nil {
			return "", err
		}
		previous := locator.QueuePrevious
		for previous != "" {
			prior, err := readEntryLocator(tx, thread, previous)
			if err != nil || prior.Ordinal >= locator.Ordinal {
				return "", errors.Join(ErrAuthorityCorrupt, err)
			}
			source, err := readLocatedEntry(tx, backendDomainV32Format, thread, prior)
			if err != nil {
				return "", err
			}
			sourceID, err := queueSourceID(source)
			if err != nil || sourceID != id {
				return "", ErrAuthorityCorrupt
			}
			if source.Type == EntryQueueAdded {
				return source.ID, nil
			}
			locator, previous = prior, prior.QueuePrevious
		}
		return "", ErrAuthorityCorrupt
	}, func(string, string) error { return nil })
}

func currentQueueSourcesFromEntries(entries []Entry) (QueueSourceSnapshot, error) {
	index := storedQueueSources{}
	latest := map[string]string{}
	byID := map[string]Entry{}
	for _, entry := range entries {
		byID[entry.ID] = entry
		if err := observeQueueSources(&index, entry, func(id string) (string, error) {
			source, ok := latest[id]
			if !ok {
				return "", ErrAuthorityCorrupt
			}
			return source, nil
		}, func(id, source string) error { latest[id] = source; return nil }); err != nil {
			return QueueSourceSnapshot{}, err
		}
	}
	result := QueueSourceSnapshot{Pending: []Entry{}}
	for _, ref := range index.Pending {
		result.Pending = append(result.Pending, byID[ref.Source])
	}
	for _, ref := range index.Restored {
		result.Restored = append(result.Restored, QueueRestoreSource{byID[ref.Added], byID[ref.Removed]})
	}
	return result, nil
}
