package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) ThreadTombstone(ctx context.Context, threadID string) (ThreadTombstone, error) {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return ThreadTombstone{}, spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return ThreadTombstone{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ThreadTombstone(ctx, threadID)
	}
	if !repo.threadScoped {
		var result ThreadTombstone
		err := repo.view(ctx, func(memory *MemoryRepo) error {
			var err error
			result, err = memory.ThreadTombstone(ctx, threadID)
			return err
		})
		return result, err
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil && !errors.Is(err, ErrThreadDeleted) && !errors.Is(err, ErrThreadNotFound) {
		return ThreadTombstone{}, err
	}
	var result ThreadTombstone
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		record, found, err := readBackendDomainRecord(tx, repo.domainFormat(), backendDomainKey(backendDomainRecordTombstone, threadID), backendDomainRecordTombstone, threadID, "", 0)
		if err != nil {
			return err
		}
		if !found {
			return ErrThreadNotFound
		}
		if err := decodeBackendDomainValue(repo.domainFormat(), record, &result); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return validateMemoryArtifactTombstone(result, threadID)
	})
	return result, err
}

func (repo *BackendRepo) DeleteRootTree(ctx context.Context, rootThreadID string) (DeleteRootTreeResult, error) {
	return repo.deleteRootTree(ctx, rootThreadID, "", "")
}

func (repo *BackendRepo) DeleteRootTreeWithRequest(ctx context.Context, rootThreadID, requestKey, fingerprint string) (DeleteRootTreeResult, error) {
	if strings.TrimSpace(requestKey) == "" || strings.TrimSpace(fingerprint) == "" {
		return DeleteRootTreeResult{}, errors.New("delete request key and fingerprint are required")
	}
	return repo.deleteRootTree(ctx, rootThreadID, requestKey, fingerprint)
}

func (repo *BackendRepo) deleteRootTree(ctx context.Context, rootThreadID, requestKey, fingerprint string) (DeleteRootTreeResult, error) {
	return repo.DeleteRootTreeInTransaction(ctx, rootThreadID, requestKey, fingerprint, nil)
}

// DeleteRootTreeInTransaction gives the storage kernel the same canonical
// preparation and deletion path while committing prompt removal atomically.
func (repo *BackendRepo) DeleteRootTreeInTransaction(ctx context.Context, rootThreadID, requestKey, fingerprint string, settle func(spi.WriteTx, []string) error) (DeleteRootTreeResult, error) {
	if repo == nil || ctx == nil || strings.TrimSpace(rootThreadID) == "" {
		return DeleteRootTreeResult{}, spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return DeleteRootTreeResult{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.DeleteRootTreeInTransaction(ctx, rootThreadID, requestKey, fingerprint, settle)
	}
	if repo.domainFormat().version >= backendDomainV32Version {
		return repo.deleteCanonicalRootTree(ctx, rootThreadID, requestKey, fingerprint, settle)
	}
	if repo.threadScoped {
		// A root deletion owns its whole canonical tree, including children
		// still awaiting migration. Prepare that closure before deleting it.
		var ids []string
		err := repo.threadMetadataView(ctx, func(memory *MemoryRepo) error {
			if _, found := memory.threads[rootThreadID]; !found {
				return nil
			}
			var err error
			ids, err = threadTreeIDsLocked(memory.threads, rootThreadID)
			return err
		})
		if err != nil {
			return DeleteRootTreeResult{}, fmt.Errorf("read deletion tree: %w", err)
		}
		if len(ids) == 0 {
			ids = []string{rootThreadID}
		}
		for _, id := range ids {
			if err := repo.prepareThreadIfNeeded(ctx, id); err != nil && !errors.Is(err, ErrThreadDeleted) && !errors.Is(err, ErrThreadNotFound) {
				return DeleteRootTreeResult{}, err
			}
		}
	}
	var result DeleteRootTreeResult
	err := repo.UpdateDomain(ctx, func(memory *MemoryRepo, tx spi.WriteTx) error {
		var err error
		result, err = memory.deleteRootTree(rootThreadID, requestKey, fingerprint)
		if err == nil && settle != nil {
			return settle(tx, result.ThreadIDs)
		}
		return err
	})
	if err != nil {
		return result, fmt.Errorf("commit deletion tree: %w", err)
	}
	return result, err
}
