package sessiontree

import (
	"context"
	"errors"
)

func (repo *BackendRepo) SubmitGoal(ctx context.Context, in GoalSubmission) (GoalSubmissionResult, error) {
	m := in.Mutation
	if m.Actor != "user" || m.Operation != "set" || m.RequestKey == "" || m.ThreadID != in.Turn.ThreadID || in.Turn.Input.Content != m.Text || in.Turn.TurnKind != "" || in.Turn.RetrySourceEntryID != "" || in.Turn.PromotedQueueID != "" {
		return GoalSubmissionResult{}, errors.New("invalid goal submission")
	}
	if err := ValidateAcceptTurnRequest(in.Turn); err != nil {
		return GoalSubmissionResult{}, err
	}
	m.Reason = "User submitted goal."
	var result GoalSubmissionResult
	err := repo.canonicalJournalUpdate(ctx, m.ThreadID, func(mutation *canonicalJournalMutation) error {
		if err := lifecycleRejectsWrite(mutation.meta); err != nil {
			return err
		}
		if err := mutation.mutateGoal(m, &result.Mutation); err != nil {
			return err
		}
		if result.Mutation.Conflict {
			return nil
		}
		if result.Mutation.Replayed {
			entry, found, err := findTurnRequestEntry(mutation.tx, m.ThreadID, in.Turn.LogicalRequestID)
			if err != nil || !found {
				return err
			}
			result.Accepted = &AcceptTurnResult{TurnStarted: entry, Replayed: true}
			return nil
		}
		_, active, err := canonicalActiveTurn(mutation.tx, m.ThreadID)
		if err != nil || active {
			return err
		}
		accepted := AcceptTurnResult{}
		if err := mutation.acceptTurn(in.Turn, &accepted); err != nil {
			return err
		}
		result.Accepted = &accepted
		return nil
	}, func(memory *MemoryRepo) error { var err error; result, err = memory.SubmitGoal(ctx, in); return err })
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	return result, nil
}
