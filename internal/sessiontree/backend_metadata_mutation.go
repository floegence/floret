package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// Metadata and side records share the canonical thread transaction without
// materializing any journal. Old-format migration owners retain their adapter.
func (repo *BackendRepo) canonicalThreadUpdate(ctx context.Context, threadID string, mutate func(spi.WriteTx, ThreadMeta) error, legacy func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalThreadUpdate(ctx, threadID, mutate, legacy)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return repo.updateThread(ctx, threadID, legacy)
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		meta, err := readPublishedThreadMeta(tx, repo.domainFormat(), threadID)
		if err != nil {
			return err
		}
		return mutate(tx, meta)
	})
	return err
}

func readPublishedThreadMeta(tx spi.ReadTx, format backendDomainFormat, threadID string) (ThreadMeta, error) {
	record, found, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordThread, threadID), backendDomainRecordThread, threadID, "", 0)
	if err != nil {
		return ThreadMeta{}, err
	}
	if !found {
		if record, deleted, err := readBackendDomainRecord(tx, format, backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0); err != nil {
			return ThreadMeta{}, err
		} else if deleted {
			var tombstone ThreadTombstone
			if err := decodeBackendDomainValue(format, record, &tombstone); err != nil || validateMemoryArtifactTombstone(tombstone, threadID) != nil {
				return ThreadMeta{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			return ThreadMeta{}, ErrThreadDeleted
		}
		return ThreadMeta{}, ErrThreadNotFound
	}
	var meta ThreadMeta
	if err := decodeBackendDomainValue(format, record, &meta); err != nil || meta.ID != threadID || ValidateThreadMetaAuthority(meta) != nil {
		return ThreadMeta{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return meta, nil
}

func (repo *BackendRepo) PutProviderState(ctx context.Context, record ProviderStateRecord) error {
	if err := validateProviderStateRecord(record); err != nil {
		return err
	}
	return repo.canonicalThreadUpdate(ctx, record.ThreadID, func(tx spi.WriteTx, meta ThreadMeta) error {
		return putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordProviderState, meta.ID), backendDomainRecordProviderState, meta.ID, "", 0, record)
	}, func(memory *MemoryRepo) error { return memory.PutProviderState(ctx, record) })
}

func (repo *BackendRepo) DeleteProviderState(ctx context.Context, threadID string) error {
	return repo.canonicalThreadUpdate(ctx, threadID, func(tx spi.WriteTx, meta ThreadMeta) error {
		err := tx.Delete(backendDomainV32Namespace, backendDomainKey(backendDomainRecordProviderState, meta.ID))
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return err
	}, func(memory *MemoryRepo) error { return memory.DeleteProviderState(ctx, threadID) })
}

func (repo *BackendRepo) CompareAndSwapAgentTodoState(ctx context.Context, state AgentTodoState, expectedVersion int64) (AgentTodoState, error) {
	if err := ValidateAgentTodoItems(state.Items); err != nil {
		return AgentTodoState{}, err
	}
	var result AgentTodoState
	err := repo.canonicalThreadUpdate(ctx, state.ThreadID, func(tx spi.WriteTx, meta ThreadMeta) error {
		if err := lifecycleRejectsWrite(meta); err != nil {
			return err
		}
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTodo, meta.ID), backendDomainRecordTodo, meta.ID, "", 0)
		if err != nil {
			return err
		}
		var current AgentTodoState
		if found {
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &current); err != nil || current.ThreadID != meta.ID || ValidateAgentTodoItems(current.Items) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if current.Version != expectedVersion {
			return ErrAgentTodoVersionConflict
		}
		state.Version = expectedVersion + 1
		if state.UpdatedAt.IsZero() {
			state.UpdatedAt = time.Now()
		}
		if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTodo, meta.ID), backendDomainRecordTodo, meta.ID, "", 0, state); err != nil {
			return err
		}
		result = cloneAgentTodoState(state)
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CompareAndSwapAgentTodoState(ctx, state, expectedVersion)
		return err
	})
	return result, err
}

func (repo *BackendRepo) UpdateThread(ctx context.Context, meta ThreadMeta) error {
	return repo.canonicalThreadUpdate(ctx, meta.ID, func(tx spi.WriteTx, current ThreadMeta) error {
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return err
		}
		if !SameThreadAuthority(current, meta) {
			return fmt.Errorf("%w: thread %q authority is immutable", ErrInvalidThreadAuthority, meta.ID)
		}
		if !SameThreadTitleState(current, meta) {
			return ErrRequestConflict
		}
		if err := lifecycleRejectsWrite(current); err != nil {
			return err
		}
		meta.UpdatedAt = nonZeroTime(meta.UpdatedAt)
		if meta.LeafID != current.LeafID {
			count, err := readJournalCount(tx, meta.ID)
			if err != nil || (count == 0) != (meta.LeafID == "") {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if meta.LeafID != "" {
				locator, err := readEntryLocator(tx, meta.ID, meta.LeafID)
				if err != nil || locator.Ordinal >= count {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			if err := tx.Delete(queryManifestNamespace, []byte(meta.ID)); err != nil && !errors.Is(err, spi.ErrNotFound) {
				return err
			}
		}
		return persistThreadMetadata(ctx, tx, meta)
	}, func(memory *MemoryRepo) error { return memory.UpdateThread(ctx, meta) })
}

func persistThreadMetadata(ctx context.Context, tx spi.WriteTx, meta ThreadMeta) error {
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, meta.ID), backendDomainRecordThread, meta.ID, "", 0, meta); err != nil {
		return err
	}
	raw, err := tx.Get(queryManifestNamespace, []byte(meta.ID))
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var manifest queryManifest
	if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 3 || manifest.Leaf != meta.LeafID {
		return nil
	}
	key := meta.ID + "\x00threads"
	document, found, err := readQueryDocumentByKey(queryMetadataReader(tx), meta.ID, key)
	if err != nil {
		return err
	}
	if !found {
		return tx.Delete(queryManifestNamespace, []byte(meta.ID))
	}
	var metadata queryDocumentMetadata
	if json.Unmarshal(document.Metadata, &metadata) != nil || metadata.Row.Object != threadquery.Threads || metadata.Row.ThreadID.String() != meta.ID {
		return spi.ErrIndexCorrupt
	}
	metadata.Row.Title, metadata.Row.Status = meta.Title, string(meta.Lifecycle)
	metadata.Row.Time, metadata.Row.CreatedAt, metadata.Row.UpdatedAt = meta.CreatedAt.UTC(), meta.CreatedAt.UTC(), meta.UpdatedAt.UTC()
	record := &queryRecord{key: key, row: metadata.Row, parts: []queryPart{{"title", meta.Title}}}
	document, err = queryDocument(record)
	if err != nil {
		return err
	}
	return queryMetadataWriter(tx).putDocument(document)
}

func (repo *BackendRepo) mutateThreadTitle(ctx context.Context, threadID string, mutate func(ThreadMeta) (ThreadMeta, bool, error)) (ThreadTitleMutationResult, error) {
	var result ThreadTitleMutationResult
	err := repo.canonicalThreadUpdate(ctx, threadID, func(tx spi.WriteTx, meta ThreadMeta) error {
		if err := lifecycleRejectsWrite(meta); err != nil {
			return err
		}
		if err := ValidateThreadTitleState(meta); err != nil {
			return err
		}
		updated, changed, err := mutate(meta)
		if err != nil {
			return err
		}
		result = ThreadTitleMutationResult{Thread: meta}
		if !changed {
			return nil
		}
		if err := ValidateThreadTitleState(updated); err != nil {
			return err
		}
		if err := persistThreadMetadata(ctx, tx, updated); err != nil {
			return err
		}
		result = ThreadTitleMutationResult{Thread: updated, Changed: true}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.mutateThreadTitle(ctx, threadID, mutate)
		return err
	})
	return result, err
}

func (repo *BackendRepo) SetThreadTitle(ctx context.Context, req SetThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateSetThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return repo.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), setThreadTitleTransition(req))
}

func (repo *BackendRepo) BeginAutomaticThreadTitle(ctx context.Context, req BeginAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateBeginAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return repo.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), beginAutomaticThreadTitleTransition(req))
}

func (repo *BackendRepo) CompleteAutomaticThreadTitle(ctx context.Context, req CompleteAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateCompleteAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return repo.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), completeAutomaticThreadTitleTransition(req))
}

func (repo *BackendRepo) FailAutomaticThreadTitle(ctx context.Context, req FailAutomaticThreadTitleRequest) (ThreadTitleMutationResult, error) {
	if err := ValidateFailAutomaticThreadTitleRequest(req); err != nil {
		return ThreadTitleMutationResult{}, err
	}
	return repo.mutateThreadTitle(ctx, strings.TrimSpace(req.ThreadID), failAutomaticThreadTitleTransition(req))
}
