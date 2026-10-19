package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadGoalSubmitter accepts explicit user intent to pursue a goal. Unlike
// SetGoal, it starts a normal Turn when idle; an active or waiting Turn is only
// updated, never queued or resumed. Exact retries never start new execution.
type ThreadGoalSubmitter interface {
	SubmitGoal(context.Context, SetGoalInput) (GoalSubmissionResult, error)
}

type GoalExecution string

const (
	GoalExecutionStarted GoalExecution = "started"
	GoalExecutionUpdated GoalExecution = "updated"
)

type GoalSubmissionResult struct {
	GoalMutationResult
	Execution GoalExecution `json:"execution,omitempty"`
}

// GoalSubmissionRequestKey returns the ordinary Turn identity used by SubmitGoal.
// Products that persist execution authorization before admission must use this
// key; idle admission and its replay decision remain owned by Floret.
func GoalSubmissionRequestKey(key RequestKey) RequestKey {
	return RequestKey("goal-submit:" + sessiontree.StableHash(string(key)))
}

var _ ThreadGoalSubmitter = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SubmitGoal(ctx context.Context, in SetGoalInput) (GoalSubmissionResult, error) {
	if err := service.host.available(); err != nil {
		return GoalSubmissionResult{}, err
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return GoalSubmissionResult{}, err
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return GoalSubmissionResult{}, err
	}
	input, err := normalizeTurnInput(UserInput{Text: in.Text})
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	repo, ok := service.host.store.repo.(sessiontree.GoalSubmissionRepo)
	if !ok {
		return GoalSubmissionResult{}, ErrUnsupportedStoreCapability
	}
	turnID, runID, err := service.host.nextTurnRunIDs()
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	// A separate normal-input identity avoids collisions with user Send keys.
	key := string(GoalSubmissionRequestKey(in.RequestKey))
	fingerprint, err := stableFingerprint(input)
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	request := turnExecutionRequest{LogicalRequestID: identity.LogicalRequestID(key), TurnID: turnID, RunID: runID, Input: input, InputFingerprint: fingerprint}
	canonical, err := canonicalTurnRequest(in.ThreadID, request)
	if err != nil {
		return GoalSubmissionResult{}, err
	}
	actor := service.runtime(in.ThreadID)
	executionCtx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	var previous <-chan struct{}
	var accepted acceptedTurn
	var submission sessiontree.GoalSubmissionResult
	var view ThreadView
	start := false
	err = actor.apply(ctx, func() error {
		var err error
		submission, err = repo.SubmitGoal(ctx, sessiontree.GoalSubmission{Mutation: sessiontree.GoalMutation{ThreadID: in.ThreadID.String(), RequestKey: string(in.RequestKey), ExpectedVersion: in.ExpectedVersion, Actor: "user", Operation: "set", Text: in.Text}, Turn: canonical})
		if err != nil {
			return runtimeHostError(err)
		}
		if submission.Mutation.Conflict || submission.Mutation.Replayed {
			return nil
		}
		canonical, err := service.loadCanonicalThreadView(ctx, in.ThreadID)
		if err != nil {
			return err
		}
		if a := submission.Accepted; a != nil {
			// The preceding Turn is terminal: discard its stream-only drafts
			// before admitting the new canonical prefix.
			actor.state.view.Items = cloneThreadItems(canonical.Items)
			previous, err = actor.beginRun(beginThreadRunInput{turnID: turnID, runID: runID, logicalRequestID: identity.LogicalRequestID(key), cancel: cancel, executionDone: done, clearOutcome: true})
			if err != nil {
				return err
			}
			accepted = acceptedTurn{ThreadID: in.ThreadID, TurnID: turnID, RunID: runID, UserEntryID: a.UserMessage.ID, BaseLeafID: a.BaseLeafID}
			if actor.state.requestKeys == nil {
				actor.state.requestKeys = map[string]threadRuntimeRequest{}
			}
			actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
			start = true
		}
		// Goal facts may arrive while an uncommitted text segment is live.
		// Install their canonical order before the next observation can run.
		if err := actor.installCanonicalView(canonical); err != nil {
			return err
		}
		view = cloneThreadRuntimeView(actor.state.view)
		return nil
	})
	if err != nil {
		cancel(context.Canceled)
		return GoalSubmissionResult{}, err
	}
	if !submission.Mutation.Conflict && !submission.Mutation.Replayed {
		service.publish(view)
	}
	if start {
		prepared := service.prepareExecution(executionCtx, actor, AgentRequest{ThreadID: in.ThreadID, TurnID: turnID, RequestKey: key, Input: input, CanonicalTurnInput: input})
		go service.executePreparedSend(executionCtx, actor, prepared, accepted, request, previous, done)
	} else {
		cancel(context.Canceled)
	}

	result := GoalSubmissionResult{GoalMutationResult: GoalMutationResult{Goal: *publicThreadGoal(submission.Mutation.State), Conflict: submission.Mutation.Conflict, Replayed: submission.Mutation.Replayed}}
	if !result.Conflict {
		result.Execution = GoalExecutionUpdated
		if submission.Accepted != nil {
			result.Execution = GoalExecutionStarted
		}
	}
	return result, nil
}
