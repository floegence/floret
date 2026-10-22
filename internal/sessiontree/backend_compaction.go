package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) CommitContextCompaction(ctx context.Context, req ContextCompactionCommit) (ContextCompactionCommitResult, error) {
	if err := validateContextCompactionCommit(req); err != nil {
		return ContextCompactionCommitResult{}, err
	}
	threadID, turnID, runID := req.Summary.ThreadID, req.Summary.TurnID, req.Summary.RunID
	var result ContextCompactionCommitResult
	err := repo.canonicalJournalUpdate(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		cancelled, err := canonicalTurnAuthority(mutation.tx, threadID, turnID, runID)
		if err != nil {
			return err
		}
		started, err := readCanonicalEntryMetadata(mutation.tx, threadID, runtimeTurnStartedEntryID(turnID))
		if err != nil || started.RunID != runID || (started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction) != (req.Finish != nil) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if cancelled {
			return context.Canceled
		}
		// Compaction explicitly consumes the complete eligible canonical history.
		// Ordinary turn writes do not use this full-context operation.
		path, err := canonicalCompletePath(ctx, mutation.tx, threadID, mutation.meta.LeafID)
		if err != nil {
			return err
		}
		if _, err := projectContextCompactionCommit(path, req); err != nil {
			return err
		}
		for index, pending := range []Entry{req.Summary, req.Lifecycle, req.Estimate} {
			entry, err := mutation.append(pending, AppendOptions{})
			if err != nil {
				return err
			}
			if index == 0 {
				result.Summary = entry
			}
			result.Facts = append(result.Facts, entry)
		}
		if err := mutation.tx.Delete(backendDomainV32Namespace, backendDomainKey(backendDomainRecordProviderState, threadID)); err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if req.Finish != nil {
			var finished FinishTurnResult
			if err := mutation.finishTurn(*req.Finish, &finished); err != nil {
				return err
			}
			result.Facts = append(result.Facts, finished.Terminal)
		}
		path, err = canonicalCompletePath(ctx, mutation.tx, threadID, mutation.meta.LeafID)
		if err != nil {
			return err
		}
		result.Messages, err = BuildContextChecked(path, ContextOptions{})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CommitContextCompaction(ctx, req)
		return err
	})
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	return result, nil
}
