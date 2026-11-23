package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadResumer explicitly continues a stopped conversation from its current history.
type ThreadResumer interface {
	Resume(context.Context, ResumeThreadInput) (ThreadCommandAck, error)
}

type ResumeThreadInput struct {
	ThreadID       identity.ThreadID `json:"thread_id"`
	ExpectedTurnID identity.TurnID   `json:"expected_turn_id"`
	ExpectedRunID  identity.RunID    `json:"expected_run_id"`
}

// RequestKey is the single admission identity for this stopped execution.
func (in ResumeThreadInput) RequestKey() RequestKey {
	return RequestKey("resume:" + sessiontree.StableHash(in.ThreadID.String()+"\x00"+in.ExpectedTurnID.String()+"\x00"+in.ExpectedRunID.String()))
}

// CanResume reports eligibility from the latest execution facts, not host authorization.
func (facts ThreadRuntimeFacts) CanResume(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID) bool {
	if threadID == "" || turnID == "" || runID == "" || facts.Activity != ThreadActivityIdle || facts.LastOutcome == nil || facts.QueueCount != 0 || facts.Attention.InputCount != 0 || facts.Attention.ApprovalCount != 0 || (facts.TurnKind != "" && facts.TurnKind != TurnKindResume) {
		return false
	}
	if *facts.LastOutcome == TurnOutcomeInterrupted {
		return facts.Cancellation == nil && (facts.Failure == nil || facts.Failure.Code == ThreadTurnFailureInterrupted)
	}
	stop := facts.Cancellation
	return *facts.LastOutcome == TurnOutcomeCancelled && facts.Failure == nil && stop != nil && stop.Source == "user_stop" && stop.ThreadID == threadID && stop.TurnID == turnID && stop.RunID == runID
}

func (service *threadRuntimeService) Resume(ctx context.Context, in ResumeThreadInput) (ThreadCommandAck, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadCommandAck{}, err
	}
	if _, err := identity.ParseThreadID(in.ThreadID.String()); err != nil {
		return ThreadCommandAck{}, err
	}
	if _, err := identity.ParseTurnID(in.ExpectedTurnID.String()); err != nil {
		return ThreadCommandAck{}, err
	}
	if _, err := identity.ParseRunID(in.ExpectedRunID.String()); err != nil {
		return ThreadCommandAck{}, err
	}
	view, err := service.admitThreadExecutionResult(ctx, in.ThreadID, UserInput{}, nil, string(in.RequestKey()), TurnKindResume, threadAckResult)
	return commandAck(view, in.RequestKey(), err)
}
