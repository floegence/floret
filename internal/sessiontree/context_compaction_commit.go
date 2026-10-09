package sessiontree

import (
	"context"
	"errors"
	"maps"
)

// ContextCompactionCommit installs the validated model context and its visible
// result together. A context-only turn also completes in this transaction.
type ContextCompactionCommit struct {
	Summary   Entry
	Lifecycle Entry
	Estimate  Entry
	Finish    *FinishTurnRequest
}

type ContextCompactionCommitResult struct {
	Summary Entry
	Facts   []Entry
}

type ContextCompactionRepo interface {
	CommitContextCompaction(context.Context, ContextCompactionCommit) (ContextCompactionCommitResult, error)
}

func (r *MemoryRepo) CommitContextCompaction(ctx context.Context, req ContextCompactionCommit) (ContextCompactionCommitResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID, turnID, runID := req.Summary.ThreadID, req.Summary.TurnID, req.Summary.RunID
	if threadID == "" || turnID == "" || runID == "" || req.Summary.Type != EntryCompaction {
		return ContextCompactionCommitResult{}, errors.New("compaction installation requires exact execution identity")
	}
	compact, err := DecodeThreadContextCompactionEntry(req.Lifecycle)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if compact.ThreadID != threadID || compact.TurnID != turnID || compact.RunID != runID || compact.Phase != "complete" || compact.OperationID != req.Summary.CompactionOperationID {
		return ContextCompactionCommitResult{}, ErrAuthorityCorrupt
	}
	status, err := DecodeThreadContextStatusEntry(req.Estimate)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if status.ThreadID.String() != threadID || status.TurnID.String() != turnID || status.RunID.String() != runID || status.Phase != "projected_request" {
		return ContextCompactionCommitResult{}, ErrAuthorityCorrupt
	}
	if req.Finish != nil {
		if err := ValidateFinishTurnRequest(*req.Finish); err != nil {
			return ContextCompactionCommitResult{}, err
		}
		if req.Finish.ThreadID != threadID || req.Finish.TurnID != turnID || req.Finish.RunID != runID || req.Finish.Status != TurnCompleted || !req.Finish.ClearProviderState {
			return ContextCompactionCommitResult{}, ErrAuthorityCorrupt
		}
	}
	if err := ctx.Err(); err != nil {
		return ContextCompactionCommitResult{}, err
	}
	activeTurnID, active := runtimeActiveTurn(r.entries[threadID])
	if !active || activeTurnID != turnID {
		return ContextCompactionCommitResult{}, ErrStaleAuthority
	}
	started, found := findEntry(r.entries[threadID], runtimeTurnStartedEntryID(turnID))
	if !found || started.RunID != runID || (started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction) != (req.Finish != nil) {
		return ContextCompactionCommitResult{}, ErrAuthorityCorrupt
	}
	if turnCancellationRequested(r.entries[threadID], turnID) {
		return ContextCompactionCommitResult{}, context.Canceled
	}

	previousEntries := r.entries[threadID]
	previousOrdinals := maps.Clone(r.entryOrdinals[threadID])
	previousDepths := maps.Clone(r.entryDepths[threadID])
	previousTurnOrdinals := cloneOrdinalLists(r.turnEntryOrdinals[threadID])
	previousTurnCounts := maps.Clone(r.turnEntryCounts[threadID])
	previousMeta := r.threads[threadID]
	previousState, stateFound := r.providerStates[threadID]
	previousSequence := r.seq
	committed := false
	defer func() {
		if committed {
			return
		}
		r.entries[threadID] = previousEntries
		r.entryOrdinals[threadID] = previousOrdinals
		r.entryDepths[threadID] = previousDepths
		r.turnEntryOrdinals[threadID] = previousTurnOrdinals
		r.turnEntryCounts[threadID] = previousTurnCounts
		r.threads[threadID] = previousMeta
		if stateFound {
			r.providerStates[threadID] = previousState
		} else {
			delete(r.providerStates, threadID)
		}
		r.seq = previousSequence
	}()
	out := ContextCompactionCommitResult{}
	for i, pending := range []Entry{req.Summary, req.Lifecycle, req.Estimate} {
		entry, err := r.appendLocked(ctx, pending, AppendOptions{})
		if err != nil {
			return ContextCompactionCommitResult{}, err
		}
		if i == 0 {
			out.Summary = entry
		}
		out.Facts = append(out.Facts, entry)
	}
	delete(r.providerStates, threadID)
	if req.Finish != nil {
		finish, err := r.finishTurnLocked(*req.Finish)
		if err != nil {
			return ContextCompactionCommitResult{}, err
		}
		out.Facts = append(out.Facts, finish.Terminal)
	}
	committed = true
	return out, nil
}
