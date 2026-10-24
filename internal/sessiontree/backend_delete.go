package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) deleteCanonicalRootTree(ctx context.Context, rootID, requestKey, fingerprint string, settle func(spi.WriteTx, []string) error) (DeleteRootTreeResult, error) {
	rootID = strings.TrimSpace(rootID)
	metas, err := repo.ListThreads(ctx, ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return DeleteRootTreeResult{}, err
	}
	ids, err := ThreadAuthorityTreeIDs(metas, rootID)
	if err != nil && !errors.Is(err, ErrThreadNotFound) {
		return DeleteRootTreeResult{}, err
	}
	for _, id := range ids {
		if err := repo.prepareThreadIfNeeded(ctx, id); err != nil {
			return DeleteRootTreeResult{}, err
		}
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return DeleteRootTreeResult{}, err
	}
	defer release()
	ctx = work
	repo.mu.Lock()
	defer repo.mu.Unlock()
	var result DeleteRootTreeResult
	err = repo.backend.Update(ctx, func(tx spi.WriteTx) error {
		root, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTombstone, rootID), backendDomainRecordTombstone, rootID, "", 0)
		if err != nil {
			return err
		}
		if found {
			var value ThreadTombstone
			if err := decodeBackendDomainValue(backendDomainV32Format, root, &value); err != nil || validateMemoryArtifactTombstone(value, rootID) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if value.RootThreadID != rootID {
				return ErrThreadNotFound
			}
			if requestKey != "" && (value.DeleteRequestKey != requestKey || value.DeleteFingerprint != fingerprint) {
				return ErrRequestConflict
			}
			result.Replayed = true
			if err := visitCanonicalTombstones(ctx, tx, func(value ThreadTombstone) error {
				if value.RootThreadID == rootID {
					result.ThreadIDs = append(result.ThreadIDs, value.ThreadID)
				}
				return nil
			}); err != nil {
				return err
			}
			slices.Sort(result.ThreadIDs)
		} else {
			current := map[string]ThreadMeta{}
			if err := repo.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error { current[meta.ID] = meta; return nil }); err != nil {
				return err
			}
			ids, err := threadTreeIDsLocked(current, rootID)
			if err != nil {
				return err
			}
			now := repo.now().UTC()
			for _, id := range ids {
				meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, id)
				if err != nil {
					return err
				}
				tombstone := ThreadTombstone{ThreadID: id, RootThreadID: rootID, ParentThreadID: meta.ParentThreadID,
					OriginRequestKey: meta.OriginRequestKey, OriginFingerprint: meta.OriginFingerprint,
					DeleteRequestKey: requestKey, DeleteFingerprint: fingerprint,
					ForkedFromThreadID: meta.ForkedFromThreadID, ForkedFromEntryID: meta.ForkedFromEntryID, DeletedAt: now}
				if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0, tombstone); err != nil {
					return err
				}
				if err := deleteCanonicalThreadRecords(ctx, tx, meta); err != nil {
					return err
				}
			}
			result.ThreadIDs = ids
		}
		if settle != nil {
			return settle(tx, result.ThreadIDs)
		}
		return nil
	})
	if err != nil {
		return DeleteRootTreeResult{}, err
	}
	for _, id := range result.ThreadIDs {
		delete(repo.queryThreads, id)
		delete(repo.verifiedThreadLeaves, id)
		repo.threadQuerySnapshots.invalidateThread(id)
	}
	return result, nil
}

func visitCanonicalTombstones(ctx context.Context, tx spi.ReadTx, visit func(ThreadTombstone) error) error {
	prefix := backendDomainKey(backendDomainRecordTombstone)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			record, err := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
			if err != nil || record.Kind != backendDomainRecordTombstone || record.ThreadID != "" || record.Ordinal != 0 || !bytes.Equal(row.Key, backendDomainKey(backendDomainRecordTombstone, record.ID)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			var value ThreadTombstone
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &value); err != nil || validateMemoryArtifactTombstone(value, record.ID) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := visit(value); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

func deleteCanonicalThreadRecords(ctx context.Context, tx spi.WriteTx, meta ThreadMeta) error {
	if err := deleteItemMembership(ctx, tx, meta.ID); err != nil {
		return err
	}
	count, err := readJournalCount(tx, meta.ID)
	if err != nil {
		return err
	}
	for ordinal := count - 1; ordinal >= 0; ordinal-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := readCanonicalEntryOrdinal(tx, meta.ID, ordinal)
		if err != nil {
			return err
		}
		if result := entry.Message.ToolResult; result != nil && result.FullOutput != nil {
			key := backendDomainKey(backendDomainRecordArtifact, artifactRecordKey(meta.ID, result.FullOutput.ID))
			if err := deleteCanonicalOptionalRecord(tx, backendDomainV32Namespace, key); err != nil {
				return err
			}
		}
		if err := deleteCanonicalStoredEntry(ctx, tx, backendDomainV32Format, meta.ID, ordinal); err != nil {
			return err
		}
	}
	for _, kind := range []string{backendDomainRecordThread, backendDomainRecordRootThread, backendDomainRecordTodo, backendDomainRecordProviderState} {
		if err := deleteCanonicalOptionalRecord(tx, backendDomainV32Namespace, backendDomainKey(kind, meta.ID)); err != nil {
			return err
		}
	}
	if err := tx.Delete(backendEntryIndexNamespace, backendDomainKey("count", meta.ID)); err != nil {
		return err
	}
	if err := clearThreadMigrationFailure(tx, meta.ID); err != nil {
		return err
	}
	if err := deleteCanonicalOptionalRecord(tx, queryManifestNamespace, []byte(meta.ID)); err != nil {
		return err
	}
	return queryMetadataWriter(tx).deleteThread(meta.ID)
}

func deleteCanonicalOptionalRecord(tx spi.WriteTx, namespace string, key []byte) error {
	err := tx.Delete(namespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return nil
	}
	return err
}
