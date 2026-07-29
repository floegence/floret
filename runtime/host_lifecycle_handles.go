package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v2/observation"
)

// ThreadTurnsRequest selects a canonical turn page after ThreadID is bound.
type ThreadTurnsRequest struct {
	BeforeCursor *ThreadTurnCursor
	SinceCursor  *ThreadTurnCursor
	Tail         int
	Limit        int
}

// ThreadDetailRequest selects a canonical detail-event page after ThreadID is
// bound.
type ThreadDetailRequest struct {
	AfterOrdinal int64
	Limit        int
	IncludeRaw   bool
}

// RetryRequest describes a retry after ThreadID and Agent are bound.
type RetryRequest struct {
	Reason string
	Labels RunLabels
}

// ActivePendingToolTarget identifies one pending tool on the thread already
// bound by a TurnRunner.
type ActivePendingToolTarget struct {
	TurnID          TurnID
	RunID           RunID
	ToolCallID      string
	ToolName        string
	Handle          string
	EffectAttemptID string
}

// ActivePendingToolCompletion describes a provider continuation for pending
// work on the thread already bound by a TurnRunner.
type ActivePendingToolCompletion struct {
	CompletionRequestID string
	Target              ActivePendingToolTarget
	ContinuationTurnID  TurnID
	ContinuationRunID   RunID
	Status              PendingToolCompletionStatus
	Summary             string
	Output              string
	Input               TurnInput
	Labels              RunLabels
}

// ApprovalResolutionRequest describes one approval decision after the root
// ThreadID is bound by a TurnRunner. ExpectedCurrent.ThreadID identifies the
// exact root or descendant execution that requested approval.
type ApprovalResolutionRequest struct {
	DecisionID               string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 ApprovalDecision
}

// AgentTodoUpdateRequest updates canonical Agent todos after ThreadID is bound.
type AgentTodoUpdateRequest struct {
	ExpectedVersion int64
	Items           []AgentTodo
	TurnID          TurnID
	RunID           RunID
	ToolCallID      string
}

// ActivePendingToolSettlement records a pending tool outcome on the thread
// already bound by a TurnRunner without resuming provider execution.
type ActivePendingToolSettlement struct {
	Target   ActivePendingToolTarget
	Status   PendingToolSettlementStatus
	Summary  string
	Output   string
	Activity *observation.ActivityPresentation
}

// ReadOverview returns the bound thread and its latest canonical turn.
func (reader *ThreadReader) ReadOverview(ctx context.Context) (ThreadOverview, error) {
	if reader == nil || reader.inner == nil {
		return ThreadOverview{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadOverview(ctx, reader.threadID)
}

// ListTurns returns one canonical turn page for the bound thread.
func (reader *ThreadReader) ListTurns(ctx context.Context, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnsPage{}, errors.New("thread reader is required")
	}
	return reader.inner.ListThreadTurns(ctx, ListThreadTurnsRequest{
		ThreadID: reader.threadID, BeforeCursor: request.BeforeCursor,
		SinceCursor: request.SinceCursor, Tail: request.Tail, Limit: request.Limit,
	})
}

// ListDetailEvents returns one canonical detail-event page for the bound
// thread.
func (reader *ThreadReader) ListDetailEvents(ctx context.Context, request ThreadDetailRequest) (ThreadDetailEvents, error) {
	if reader == nil || reader.inner == nil {
		return ThreadDetailEvents{}, errors.New("thread reader is required")
	}
	return reader.inner.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{
		ThreadID: reader.threadID, AfterOrdinal: request.AfterOrdinal,
		Limit: request.Limit, IncludeRaw: request.IncludeRaw,
	})
}

// ListPendingToolTargets returns unsettled host-owned work for the bound
// thread.
func (reader *ThreadReader) ListPendingToolTargets(ctx context.Context) ([]PendingToolSettlementTarget, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("thread reader is required")
	}
	return reader.inner.ListPendingToolSettlementTargets(ctx, reader.threadID)
}

// ReadAgentTodos returns the canonical Agent todo state for the bound thread.
func (reader *ThreadReader) ReadAgentTodos(ctx context.Context) (ThreadAgentTodoState, error) {
	if reader == nil || reader.inner == nil {
		return ThreadAgentTodoState{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadAgentTodos(ctx, reader.threadID)
}

// ReadContext returns canonical context usage and compaction state for the
// bound thread.
func (reader *ThreadReader) ReadContext(ctx context.Context) (ThreadContextSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadContextSnapshot{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadContext(ctx, reader.threadID)
}

// ReadApprovalQueue returns the canonical approval queue rooted at the bound
// thread.
func (reader *ThreadReader) ReadApprovalQueue(ctx context.Context) (ApprovalQueue, error) {
	if reader == nil || reader.inner == nil {
		return ApprovalQueue{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadApprovalQueue(ctx, ReadApprovalQueueRequest{ThreadID: reader.threadID})
}

// ReadProjection rebuilds one canonical turn projection from the bound thread.
func (reader *ThreadReader) ReadProjection(ctx context.Context, turnID TurnID, runID RunID) (ThreadTurnProjection, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnProjection{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadTurnProjection(ctx, ReadTurnProjectionRequest{
		ThreadID: reader.threadID, TurnID: turnID, RunID: runID,
	})
}

// ReadArtifact returns one artifact owned by the bound thread.
func (reader *ThreadReader) ReadArtifact(ctx context.Context, artifactID ArtifactID) (ArtifactContent, error) {
	if reader == nil || reader.inner == nil {
		return ArtifactContent{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: reader.threadID, ArtifactID: artifactID})
}

// Retry retries the latest eligible turn on the bound thread.
func (runner *TurnRunner) Retry(ctx context.Context, request RetryRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.RetryTurn(ctx, RetryTurnRequest{
		ThreadID: runner.threadID, Reason: request.Reason, Labels: request.Labels,
	})
}

// CompletePendingTool admits one host-owned provider continuation on the bound
// thread.
func (runner *TurnRunner) CompletePendingTool(ctx context.Context, request ActivePendingToolCompletion) (PendingToolCompletionResult, error) {
	if runner == nil || runner.inner == nil {
		return PendingToolCompletionResult{}, errors.New("turn runner is required")
	}
	return runner.inner.CompletePendingTool(ctx, PendingToolCompletionRequest{
		CompletionRequestID: request.CompletionRequestID,
		Target:              request.Target.withThreadID(runner.threadID),
		ContinuationTurnID:  request.ContinuationTurnID,
		ContinuationRunID:   request.ContinuationRunID,
		Status:              request.Status,
		Summary:             request.Summary,
		Output:              request.Output,
		Input:               request.Input,
		Labels:              request.Labels,
	})
}

// ResolveApproval submits one decision to the approval queue rooted at the
// bound thread.
func (runner *TurnRunner) ResolveApproval(ctx context.Context, request ApprovalResolutionRequest) (ResolveApprovalResult, error) {
	if runner == nil || runner.inner == nil {
		return ResolveApprovalResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ResolveApproval(ctx, ResolveApprovalRequest{
		DecisionID: request.DecisionID, ExpectedRootThreadID: runner.threadID,
		ExpectedGeneration: request.ExpectedGeneration, ExpectedRevision: request.ExpectedRevision,
		ExpectedCurrent: request.ExpectedCurrent, ExpectedApprovalRevision: request.ExpectedApprovalRevision,
		Decision: request.Decision,
	})
}

// UpdateAgentTodos atomically updates canonical Agent todos on the bound
// thread.
func (runner *TurnRunner) UpdateAgentTodos(ctx context.Context, request AgentTodoUpdateRequest) (ThreadAgentTodoState, error) {
	if runner == nil || runner.inner == nil {
		return ThreadAgentTodoState{}, errors.New("turn runner is required")
	}
	return runner.inner.UpdateThreadAgentTodos(ctx, UpdateThreadAgentTodosRequest{
		ThreadID: runner.threadID, ExpectedVersion: request.ExpectedVersion,
		Items: append([]AgentTodo(nil), request.Items...), TurnID: request.TurnID,
		RunID: request.RunID, ToolCallID: request.ToolCallID,
	})
}

// SettlePendingTool records one host-owned pending tool outcome on the bound
// thread without resuming provider execution.
func (runner *TurnRunner) SettlePendingTool(ctx context.Context, request ActivePendingToolSettlement) (PendingToolSettlementResult, error) {
	if runner == nil || runner.inner == nil {
		return PendingToolSettlementResult{}, errors.New("turn runner is required")
	}
	return runner.inner.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: request.Target.withThreadID(runner.threadID), Status: request.Status,
		Summary: request.Summary, Output: request.Output, Activity: request.Activity,
	})
}

// SettlePendingTool records one host-owned pending tool outcome for a direct
// child of the bound parent without resuming provider execution.
func (manager *SubAgentManager) SettlePendingTool(ctx context.Context, request PendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if manager == nil || manager.inner == nil {
		return PendingToolSettlementResult{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.SettlePendingTool(ctx, request)
}

// ReadTurn returns one canonical turn from a direct child of the bound parent.
func (reader *SubAgentReader) ReadTurn(ctx context.Context, childThreadID ThreadID, turnID TurnID) (ThreadTurnSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnSnapshot{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: childThreadID, TurnID: turnID})
}

// ListTurns returns one canonical turn page from a direct child of the bound
// parent.
func (reader *SubAgentReader) ListTurns(ctx context.Context, childThreadID ThreadID, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnsPage{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListThreadTurns(ctx, ListThreadTurnsRequest{
		ThreadID: childThreadID, BeforeCursor: request.BeforeCursor,
		SinceCursor: request.SinceCursor, Tail: request.Tail, Limit: request.Limit,
	})
}

// ListPendingToolTargets returns unsettled host-owned work for a direct child
// of the bound parent.
func (reader *SubAgentReader) ListPendingToolTargets(ctx context.Context, childThreadID ThreadID) ([]PendingToolSettlementTarget, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListPendingToolSettlementTargets(ctx, ListSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: reader.parentThreadID, ChildThreadID: childThreadID,
	})
}

// ReadArtifact returns one artifact from a direct child of the bound parent.
func (reader *SubAgentReader) ReadArtifact(ctx context.Context, childThreadID ThreadID, artifactID ArtifactID) (ArtifactContent, error) {
	if reader == nil || reader.inner == nil {
		return ArtifactContent{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadArtifact(ctx, ReadArtifactRequest{ThreadID: childThreadID, ArtifactID: artifactID})
}

func (target ActivePendingToolTarget) withThreadID(threadID ThreadID) PendingToolSettlementTarget {
	return PendingToolSettlementTarget{
		ThreadID: threadID, TurnID: target.TurnID, RunID: target.RunID,
		ToolCallID: target.ToolCallID, ToolName: target.ToolName, Handle: target.Handle,
		EffectAttemptID: target.EffectAttemptID,
	}
}
