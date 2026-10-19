package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/floegence/floret/v7/internal/session"
)

// ContextCompactionCommit installs the validated model context and its visible
// result together. A context-only turn also completes in this transaction.
type ContextCompactionCommit struct {
	History        []session.Message
	ActiveMessages []session.Message
	Summary        Entry
	Lifecycle      Entry
	Estimate       Entry
	Finish         *FinishTurnRequest
}

type ContextCompactionCommitResult struct {
	Messages []session.Message
	Summary  Entry
	Facts    []Entry
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
		return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction lifecycle identity conflicts", ErrAuthorityCorrupt)
	}
	status, err := DecodeThreadContextStatusEntry(req.Estimate)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if status.ThreadID.String() != threadID || status.TurnID.String() != turnID || status.RunID.String() != runID || status.Phase != "projected_request" {
		return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction estimate identity conflicts", ErrAuthorityCorrupt)
	}
	if req.Finish != nil {
		if err := ValidateFinishTurnRequest(*req.Finish); err != nil {
			return ContextCompactionCommitResult{}, err
		}
		if req.Finish.ThreadID != threadID || req.Finish.TurnID != turnID || req.Finish.RunID != runID || req.Finish.Status != TurnCompleted || !req.Finish.ClearProviderState {
			return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction completion identity conflicts", ErrAuthorityCorrupt)
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
		return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction turn admission identity conflicts", ErrAuthorityCorrupt)
	}
	if turnCancellationRequested(r.entries[threadID], turnID) {
		return ContextCompactionCommitResult{}, context.Canceled
	}

	path, err := pathLocked(r.threads, r.entries, threadID, r.threads[threadID].LeafID)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if req.History != nil && !reflect.DeepEqual(req.History, history) {
		return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction history changed", ErrStaleAuthority)
	}
	var previous Entry
	for _, entry := range path {
		if entry.Type == EntryCompaction {
			previous = entry
		}
	}
	if req.Summary.PreviousCompactionID != previous.CompactionID || req.Summary.CompactionGeneration != previous.CompactionGeneration+1 || req.Summary.CompactionID == "" || req.Summary.CompactionWindowID != req.Summary.CompactionID {
		return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction generation conflicts with canonical path", ErrAuthorityCorrupt)
	}
	projected, err := projectCheckpoint(history, req.Summary, path)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if req.ActiveMessages != nil {
		expected := session.CloneMessages(req.ActiveMessages)
		if len(expected) == 0 {
			return ContextCompactionCommitResult{}, fmt.Errorf("%w: compaction replacement context is empty", ErrAuthorityCorrupt)
		}
		expected[0].EntryID, expected[0].ParentEntryID = projected[0].EntryID, projected[0].ParentEntryID
		if !reflect.DeepEqual(expected, projected) {
			return ContextCompactionCommitResult{}, fmt.Errorf("%w: generated compaction context differs from canonical projection", ErrAuthorityCorrupt)
		}
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
		r.setThreadLocked(threadID, previousMeta)
		if stateFound {
			r.setProviderStateLocked(threadID, previousState)
		} else {
			r.deleteProviderStateLocked(threadID)
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
	r.deleteProviderStateLocked(threadID)
	if req.Finish != nil {
		finish, err := r.finishTurnLocked(*req.Finish)
		if err != nil {
			return ContextCompactionCommitResult{}, err
		}
		out.Facts = append(out.Facts, finish.Terminal)
	}
	path, err = pathLocked(r.threads, r.entries, threadID, r.threads[threadID].LeafID)
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	out.Messages, err = BuildContextChecked(path, ContextOptions{})
	if err != nil {
		return ContextCompactionCommitResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ContextCompactionCommitResult{}, err
	}
	committed = true
	return out, nil
}
