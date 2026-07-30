package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/tools"
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

// boundRetryRequest describes a retry after ThreadID and Agent are bound.
type boundRetryRequest struct {
	TurnID identity.TurnID
	RunID  identity.RunID
	Reason string
	Labels RunLabels
}

// ActivePendingToolTarget identifies one pending tool on the thread already
// bound by a turnRunnerHandle.
type ActivePendingToolTarget struct {
	TurnID          identity.TurnID
	RunID           identity.RunID
	ToolCallID      string
	ToolName        string
	Handle          string
	EffectAttemptID string
}

// ActivePendingToolCompletion describes a provider continuation for pending
// work on the thread already bound by a turnRunnerHandle.
type activePendingToolCompletion struct {
	CompletionRequestID string
	Target              ActivePendingToolTarget
	ContinuationTurnID  identity.TurnID
	ContinuationRunID   identity.RunID
	Status              PendingToolCompletionStatus
	Summary             string
	Output              string
	Input               TurnInput
	Labels              RunLabels
}

// ApprovalResolutionRequest describes one approval decision after the root
// ThreadID is bound by a turnRunnerHandle. ExpectedCurrent.ThreadID identifies the
// exact root or descendant execution that requested approval.
type approvalResolutionRequest struct {
	DecisionID               string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 ApprovalDecision
}

// AgentTodoUpdateRequest updates canonical Agent todos after ThreadID is bound.
type agentTodoUpdateRequest struct {
	ExpectedVersion int64
	Items           []AgentTodo
	TurnID          identity.TurnID
	RunID           identity.RunID
	ToolCallID      string
}

// ActivePendingToolSettlement records a pending tool outcome on the thread
// already bound by a turnRunnerHandle without resuming provider execution.
type activePendingToolSettlement struct {
	Target   ActivePendingToolTarget
	Status   PendingToolSettlementStatus
	Summary  string
	Output   string
	Activity *tools.ActivityPresentation
}

// ReadOverview returns the bound thread and its latest canonical turn.
func (reader *threadReaderHandle) ReadOverview(ctx context.Context) (ThreadOverview, error) {
	if reader == nil || reader.inner == nil {
		return ThreadOverview{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadOverview(ctx, reader.threadID)
}

// ListTurns returns one canonical turn page for the bound thread.
func (reader *threadReaderHandle) ListTurns(ctx context.Context, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnsPage{}, errors.New("thread reader is required")
	}
	return reader.inner.ListThreadTurns(ctx, listThreadTurnsRequest{
		ThreadID: reader.threadID, BeforeCursor: request.BeforeCursor,
		SinceCursor: request.SinceCursor, Tail: request.Tail, Limit: request.Limit,
	})
}

// ListDetailEvents returns one canonical detail-event page for the bound
// thread.
func (reader *threadReaderHandle) ListDetailEvents(ctx context.Context, request ThreadDetailRequest) (ThreadDetailEvents, error) {
	if reader == nil || reader.inner == nil {
		return ThreadDetailEvents{}, errors.New("thread reader is required")
	}
	return reader.inner.ListThreadDetailEvents(ctx, listThreadDetailEventsRequest{
		ThreadID: reader.threadID, AfterOrdinal: request.AfterOrdinal,
		Limit: request.Limit, IncludeRaw: request.IncludeRaw,
	})
}

// ListPendingToolTargets returns unsettled host-owned work for the bound
// thread.
func (reader *threadReaderHandle) ListPendingToolTargets(ctx context.Context) ([]PendingToolSettlementTarget, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("thread reader is required")
	}
	return reader.inner.ListPendingToolSettlementTargets(ctx, reader.threadID)
}

// ReadAgentTodos returns the canonical Agent todo state for the bound thread.
func (reader *threadReaderHandle) ReadAgentTodos(ctx context.Context) (ThreadAgentTodoState, error) {
	if reader == nil || reader.inner == nil {
		return ThreadAgentTodoState{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadAgentTodos(ctx, reader.threadID)
}

// ReadContext returns canonical context usage and compaction state for the
// bound thread.
func (reader *threadReaderHandle) ReadContext(ctx context.Context) (ThreadContextSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadContextSnapshot{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadContext(ctx, reader.threadID)
}

// ReadApprovalQueue returns the canonical approval queue rooted at the bound
// thread.
func (reader *threadReaderHandle) ReadApprovalQueue(ctx context.Context) (ApprovalQueue, error) {
	if reader == nil || reader.inner == nil {
		return ApprovalQueue{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadApprovalQueue(ctx, readApprovalQueueRequest{ThreadID: reader.threadID})
}

// ReadProjection rebuilds one canonical turn projection from the bound thread.
func (reader *threadReaderHandle) ReadProjection(ctx context.Context, turnID identity.TurnID, runID identity.RunID) (ThreadTurnProjection, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnProjection{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadTurnProjection(ctx, readTurnProjectionRequest{
		ThreadID: reader.threadID, TurnID: turnID, RunID: runID,
	})
}

// ReadArtifact returns one artifact owned by the bound thread.
func (reader *threadReaderHandle) ReadArtifact(ctx context.Context, artifactID identity.ArtifactID) (ArtifactContent, error) {
	if reader == nil || reader.inner == nil {
		return ArtifactContent{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadArtifact(ctx, readArtifactRequest{ThreadID: reader.threadID, ArtifactID: artifactID})
}

// Retry retries the latest eligible turn on the bound thread.
func (runner *turnRunnerHandle) Retry(ctx context.Context, request boundRetryRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.RetryTurn(ctx, retryTurnRequest{
		ThreadID: runner.threadID, TurnID: request.TurnID, RunID: request.RunID,
		Reason: request.Reason, Labels: request.Labels,
	})
}

// CompletePendingTool admits one host-owned provider continuation on the bound
// thread.
func (runner *turnRunnerHandle) CompletePendingTool(ctx context.Context, request activePendingToolCompletion) (PendingToolCompletionResult, error) {
	if runner == nil || runner.inner == nil {
		return PendingToolCompletionResult{}, errors.New("turn runner is required")
	}
	return runner.inner.CompletePendingTool(ctx, pendingToolCompletionRequest{
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
func (runner *turnRunnerHandle) ResolveApproval(ctx context.Context, request approvalResolutionRequest) (ResolveApprovalResult, error) {
	if runner == nil || runner.inner == nil {
		return ResolveApprovalResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ResolveApproval(ctx, resolveApprovalRequest{
		DecisionID: request.DecisionID, ExpectedRootThreadID: runner.threadID,
		ExpectedGeneration: request.ExpectedGeneration, ExpectedRevision: request.ExpectedRevision,
		ExpectedCurrent: request.ExpectedCurrent, ExpectedApprovalRevision: request.ExpectedApprovalRevision,
		Decision: request.Decision,
	})
}

// UpdateAgentTodos atomically updates canonical Agent todos on the bound
// thread.
func (runner *turnRunnerHandle) UpdateAgentTodos(ctx context.Context, request agentTodoUpdateRequest) (ThreadAgentTodoState, error) {
	if runner == nil || runner.inner == nil {
		return ThreadAgentTodoState{}, errors.New("turn runner is required")
	}
	return runner.inner.UpdateThreadAgentTodos(ctx, updateThreadAgentTodosRequest{
		ThreadID: runner.threadID, ExpectedVersion: request.ExpectedVersion,
		Items: append([]AgentTodo(nil), request.Items...), TurnID: request.TurnID,
		RunID: request.RunID, ToolCallID: request.ToolCallID,
	})
}

// SettlePendingTool records one host-owned pending tool outcome on the bound
// thread without resuming provider execution.
func (runner *turnRunnerHandle) SettlePendingTool(ctx context.Context, request activePendingToolSettlement) (PendingToolSettlementResult, error) {
	if runner == nil || runner.inner == nil {
		return PendingToolSettlementResult{}, errors.New("turn runner is required")
	}
	return runner.inner.SettlePendingTool(ctx, pendingToolSettlementRequest{
		Target: request.Target.withThreadID(runner.threadID), Status: request.Status,
		Summary: request.Summary, Output: request.Output, Activity: request.Activity,
	})
}

// SettlePendingTool records one host-owned pending tool outcome for a direct
// child of the bound parent without resuming provider execution.
func (manager *subAgentManagerHandle) SettlePendingTool(ctx context.Context, request pendingToolSettlementRequest) (PendingToolSettlementResult, error) {
	if manager == nil || manager.inner == nil {
		return PendingToolSettlementResult{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.SettlePendingTool(ctx, request)
}

// ReadTurn returns one canonical turn from a direct child of the bound parent.
func (reader *subAgentReaderHandle) ReadTurn(ctx context.Context, childThreadID identity.ThreadID, turnID identity.TurnID) (ThreadTurnSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnSnapshot{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadThreadTurn(ctx, readThreadTurnRequest{ThreadID: childThreadID, TurnID: turnID})
}

// ListTurns returns one canonical turn page from a direct child of the bound
// parent.
func (reader *subAgentReaderHandle) ListTurns(ctx context.Context, childThreadID identity.ThreadID, request ThreadTurnsRequest) (ThreadTurnsPage, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnsPage{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListThreadTurns(ctx, listThreadTurnsRequest{
		ThreadID: childThreadID, BeforeCursor: request.BeforeCursor,
		SinceCursor: request.SinceCursor, Tail: request.Tail, Limit: request.Limit,
	})
}

// ListPendingToolTargets returns unsettled host-owned work for a direct child
// of the bound parent.
func (reader *subAgentReaderHandle) ListPendingToolTargets(ctx context.Context, childThreadID identity.ThreadID) ([]PendingToolSettlementTarget, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListPendingToolSettlementTargets(ctx, listSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: reader.parentThreadID, ChildThreadID: childThreadID,
	})
}

// ReadArtifact returns one artifact from a direct child of the bound parent.
func (reader *subAgentReaderHandle) ReadArtifact(ctx context.Context, childThreadID identity.ThreadID, artifactID identity.ArtifactID) (ArtifactContent, error) {
	if reader == nil || reader.inner == nil {
		return ArtifactContent{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadArtifact(ctx, readArtifactRequest{ThreadID: childThreadID, ArtifactID: artifactID})
}

func (target ActivePendingToolTarget) withThreadID(threadID identity.ThreadID) PendingToolSettlementTarget {
	return PendingToolSettlementTarget{
		ThreadID: threadID, TurnID: target.TurnID, RunID: target.RunID,
		ToolCallID: target.ToolCallID, ToolName: target.ToolName, Handle: target.Handle,
		EffectAttemptID: target.EffectAttemptID,
	}
}
