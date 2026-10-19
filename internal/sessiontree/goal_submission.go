package sessiontree

import (
	"context"
	"errors"
	"maps"
)

// GoalSubmission reuses the Goal mutation and normal user-input admission
// journal shapes. The submitted text is the user's actual execution request.
type GoalSubmission struct {
	Mutation GoalMutation
	Turn     AcceptTurnRequest
}

type GoalSubmissionResult struct {
	Mutation GoalMutationResult
	Accepted *AcceptTurnResult
}

type GoalSubmissionRepo interface {
	SubmitGoal(context.Context, GoalSubmission) (GoalSubmissionResult, error)
}

// SubmitGoal linearizes goal changes, idle admission, Stop and natural finish
// on the canonical journal. A replay never changes its original admission.
func (r *MemoryRepo) SubmitGoal(ctx context.Context, in GoalSubmission) (GoalSubmissionResult, error) {
	if err := ctx.Err(); err != nil {
		return GoalSubmissionResult{}, err
	}
	m := in.Mutation
	if m.Actor != "user" || m.Operation != "set" || m.RequestKey == "" || m.ThreadID != in.Turn.ThreadID || in.Turn.Input.Content != m.Text || in.Turn.TurnKind != "" || in.Turn.RetrySourceEntryID != "" || in.Turn.PromotedQueueID != "" {
		return GoalSubmissionResult{}, errors.New("invalid goal submission")
	}
	if err := ValidateAcceptTurnRequest(in.Turn); err != nil {
		return GoalSubmissionResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// The request key remains shared with save-only mutations; this reason
	// distinguishes a submitted command from an edit for conflict detection.
	m.Reason = "User submitted goal."
	previousMeta, found := r.threads[m.ThreadID]
	if !found {
		return GoalSubmissionResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(previousMeta); err != nil {
		return GoalSubmissionResult{}, err
	}
	previousEntries := r.entries[m.ThreadID]
	previousOrdinals := maps.Clone(r.entryOrdinals[m.ThreadID])
	previousDepths := maps.Clone(r.entryDepths[m.ThreadID])
	previousTurns := cloneOrdinalLists(r.turnEntryOrdinals[m.ThreadID])
	previousCounts := maps.Clone(r.turnEntryCounts[m.ThreadID])
	previousSequence := r.seq
	committed := false
	defer func() {
		if committed {
			return
		}
		r.entries[m.ThreadID] = previousEntries
		r.entryOrdinals[m.ThreadID] = previousOrdinals
		r.entryDepths[m.ThreadID] = previousDepths
		r.turnEntryOrdinals[m.ThreadID] = previousTurns
		r.turnEntryCounts[m.ThreadID] = previousCounts
		r.setThreadLocked(m.ThreadID, previousMeta)
		r.seq = previousSequence
	}()
	mutation, err := r.mutateGoalLocked(ctx, m)
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	result := GoalSubmissionResult{Mutation: mutation}
	if mutation.Conflict {
		committed = true
		return result, nil
	}
	if mutation.Replayed {
		for _, entry := range r.entries[m.ThreadID] {
			if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.RequestKey == in.Turn.LogicalRequestID {
				result.Accepted = &AcceptTurnResult{TurnStarted: cloneEntry(entry), Replayed: true}
				break
			}
		}
		committed = true
		return result, nil
	}
	if _, active := runtimeActiveTurn(r.entries[m.ThreadID]); active {
		committed = true
		return result, nil
	}
	accepted, err := r.acceptTurnLocked(in.Turn)
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	result.Accepted = &accepted
	committed = true
	return result, nil
}
