package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/floegence/floret/v7/storage/spi"
)

// Recover only still-unresolved interactions. Validate the source closure only
// when recovery must publish a missing receipt; current-store readiness alone
// is not evidence that every historical source has been checked this process.
// Each receipt has its released deterministic identity; a retry or restart can
// never append it twice. Publication checks mutable source authority again.
func (repo *BackendRepo) prepareParentEvents(ctx context.Context, reader *canonicalSnapshotReader, threadID string) (bool, error) {
	if !repo.parentEventsEnabled() || repo.domainFormat().version < backendDomainV32Version {
		return false, nil
	}
	meta, err := readPublishedThreadMeta(reader, repo.domainFormat(), threadID)
	if err != nil {
		return false, err
	}
	if meta.ParentThreadID == "" {
		return false, nil
	}
	var prepared bool
	var count int
	if err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		if err := reader.verify(tx); err != nil {
			return err
		}
		var err error
		count, err = readJournalCount(tx, meta.ID)
		if err != nil {
			return err
		}
		prepared, err = parentRecoveryPrepared(tx, meta.ID, count)
		return err
	}); err != nil {
		return false, err
	}
	if prepared {
		return false, nil
	}
	pending, err := canonicalPendingParentEvents(ctx, reader, meta, backendThreadEntryPageSize)
	if err != nil {
		return false, err
	}
	if len(pending) == 0 {
		return false, repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := reader.verify(tx); err != nil {
				return err
			}
			current, err := readJournalCount(tx, meta.ID)
			if err != nil {
				return err
			}
			if current != count {
				return spi.ErrConflict
			}
			return writeParentRecoverySource(tx, meta.ID, count)
		})
	}
	if _, err := validateCanonicalThreadClosure(ctx, reader, threadID); err != nil {
		return false, reader.validationError(err)
	}
	changed := false
	var observer func([]string)
	var observed []string
	err = func() error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
			if err := reader.verify(tx); err != nil {
				return err
			}
			for _, event := range pending {
				appended, err := appendCanonicalParentEvent(ctx, tx, event)
				if err != nil {
					return err
				}
				if appended {
					changed = true
				}
			}
			return nil
		})
		if err == nil && changed {
			parentID := pending[0].ParentThreadID
			delete(repo.queryThreads, parentID)
			delete(repo.verifiedThreadLeaves, parentID)
			observer, observed = repo.parentEventObserver, []string{parentID}
		}
		return err
	}()
	if err == nil && observer != nil {
		go observer(observed)
	}
	return changed, err
}

func appendCanonicalParentEvent(ctx context.Context, tx spi.WriteTx, event ParentEvent) (bool, error) {
	meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, event.ParentThreadID)
	if errors.Is(err, ErrThreadDeleted) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if locator, err := readEntryLocator(tx, meta.ID, event.ID); err == nil {
		entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, locator)
		if err != nil {
			return false, err
		}
		existing, err := decodeParentEvent(entry)
		if err != nil || entry.Type != EntryParentEvent || !reflect.DeepEqual(existing, event) {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		return false, nil
	} else if !errors.Is(err, ErrEntryNotFound) {
		return false, err
	}
	count, err := readJournalCount(tx, meta.ID)
	if err != nil || (count == 0) != (meta.LeafID == "") {
		return false, errors.Join(ErrAuthorityCorrupt, err)
	}
	depth := int64(1)
	if meta.LeafID != "" {
		leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil || leaf.Ordinal >= count {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		depth = leaf.Depth + 1
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return false, err
	}
	entry := PrepareEntry(Entry{ID: event.ID, ThreadID: meta.ID, ParentID: meta.LeafID, PathDepth: depth, Type: EntryParentEvent, Payload: payload, CreatedAt: event.ReceivedAt})
	if err := putCanonicalStoredEntry(ctx, tx, backendDomainV32Format, backendDomainEntryKey(meta.ID, count), entry.ID, meta.ID, count, entry); err != nil {
		return false, err
	}
	if record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, meta.ID), backendDomainRecordProviderState, meta.ID, "", 0); err != nil {
		return false, err
	} else if found {
		var state ProviderStateRecord
		if err := decodeBackendDomainValue(backendDomainV32Format, record, &state); err != nil || state.ThreadID != meta.ID || validateProviderStateRecord(state) != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if state.LeafEntryID == meta.LeafID {
			state.LeafEntryID, state.UpdatedAt = entry.ID, entry.CreatedAt
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, meta.ID), backendDomainRecordProviderState, meta.ID, "", 0, state); err != nil {
				return false, err
			}
		}
	}
	// Parent events do not introduce searchable documents. Advance an already
	// complete index atomically with its thread row and retain frozen pages.
	if raw, err := tx.Get(queryManifestNamespace, []byte(meta.ID)); err == nil {
		var manifest queryManifest
		if json.Unmarshal(raw, &manifest) == nil && manifest.Version == 3 && manifest.Leaf == meta.LeafID {
			if err := putQueryManifest(tx, meta.ID, entry.ID); err != nil {
				return false, err
			}
		}
	} else if !errors.Is(err, spi.ErrNotFound) {
		return false, err
	}
	meta.LeafID, meta.UpdatedAt = entry.ID, entry.CreatedAt
	if err := advanceParentRecoverySource(tx, meta.ID, count, count+1); err != nil {
		return false, err
	}
	return true, persistThreadMetadata(ctx, tx, meta)
}

// Coverage is journal-wide, including retained inactive branches. MoveLeaf
// does not change it. Disabled writers leave its count behind; enabling parent
// events checks unresolved sources again before advancing the same manifest.
type parentRecoverySource struct {
	Count int              `json:"count"`
	Last  contextSourceRef `json:"last"`
}

func parentRecoveryPrepared(tx spi.ReadTx, thread string, count int) (bool, error) {
	raw, err := tx.Get(queryManifestNamespace, []byte(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var manifest queryManifest
	if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 3 || manifest.ParentRecovery == nil {
		return false, nil
	}
	source := manifest.ParentRecovery
	if source.Count < 0 || source.Count > count || !validParentSourceRef(source.Last) || (source.Count == 0) != (source.Last.ID == "") {
		return false, ErrAuthorityCorrupt
	}
	if source.Count > 0 {
		entry, err := readCanonicalEntryOrdinal(tx, thread, source.Count-1)
		if err != nil || source.Last != (contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}) {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	return source.Count == count, nil
}

func writeParentRecoverySource(tx spi.WriteTx, thread string, count int) error {
	raw, err := tx.Get(queryManifestNamespace, []byte(thread))
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var manifest queryManifest
	if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 3 {
		return nil
	}
	source := &parentRecoverySource{Count: count}
	if count > 0 {
		entry, err := readCanonicalEntryOrdinal(tx, thread, count-1)
		if err != nil {
			return err
		}
		source.Last = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	}
	manifest.ParentRecovery = source
	raw, err = json.Marshal(manifest)
	if err != nil {
		return err
	}
	return tx.Put(queryManifestNamespace, []byte(thread), raw)
}

func advanceParentRecoverySource(tx spi.WriteTx, thread string, before, after int) error {
	ready, err := parentRecoveryPrepared(tx, thread, before)
	if err != nil || !ready {
		return err
	}
	return writeParentRecoverySource(tx, thread, after)
}
