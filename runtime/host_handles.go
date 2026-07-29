package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/observation"
)

// ThreadTitleEditor binds title mutation to one root thread.
func (host *Host) ThreadTitleEditor(ctx context.Context, threadID ThreadID) (*ThreadTitleEditor, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.title.NewHost(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	return &ThreadTitleEditor{inner: inner, threadID: threadID}, nil
}

// ThreadForker binds fork source identity to one root thread.
func (host *Host) ThreadForker(ctx context.Context, threadID ThreadID) (*ThreadForker, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.fork.NewHost(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	return &ThreadForker{inner: inner, threadID: threadID}, nil
}

// ThreadDeleter binds deletion to one root thread tree.
func (host *Host) ThreadDeleter(ctx context.Context, threadID ThreadID) (*ThreadDeleter, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.delete.NewHost(ctx, threadID)
	if err != nil {
		return nil, err
	}
	return &ThreadDeleter{inner: inner, threadID: threadID}, nil
}

// ThreadCompactor binds one immutable Agent to compaction for a root thread.
func (host *Host) ThreadCompactor(ctx context.Context, threadID ThreadID, agent *Agent) (*ThreadCompactor, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("thread compactor requires an Agent")
	}
	factory, err := host.binders.compact.Bind(threadID)
	if err != nil {
		return nil, err
	}
	inner, err := factory.NewHost(ctx, agent.threadCompactionOptions())
	if err != nil {
		return nil, err
	}
	return &ThreadCompactor{inner: inner, threadID: threadID}, nil
}

// SubAgentManager binds child lifecycle to one parent and one immutable Agent.
func (host *Host) SubAgentManager(ctx context.Context, parentThreadID ThreadID, agent *Agent) (*SubAgentManager, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("SubAgent manager requires an Agent")
	}
	factory, err := host.binders.subAgent.Bind(parentThreadID)
	if err != nil {
		return nil, err
	}
	inner, err := factory.NewHost(ctx, agent.subAgentOptions())
	if err != nil {
		return nil, err
	}
	return &SubAgentManager{inner: inner, parentThreadID: parentThreadID}, nil
}

// SubAgentReader binds child reads to one exact parent.
func (host *Host) SubAgentReader(ctx context.Context, parentThreadID ThreadID) (*SubAgentReader, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.subAgentRead.NewHost(ctx, parentThreadID)
	if err != nil {
		return nil, err
	}
	return &SubAgentReader{inner: inner, parentThreadID: parentThreadID}, nil
}

// ThreadTitleEditor is title authority for one exact root thread.
type ThreadTitleEditor struct {
	inner    *ThreadTitleHost
	threadID ThreadID
}

// Set replaces the canonical title of the bound thread.
func (editor *ThreadTitleEditor) Set(ctx context.Context, title string) (ThreadSnapshot, error) {
	if editor == nil || editor.inner == nil {
		return ThreadSnapshot{}, errors.New("thread title editor is required")
	}
	return editor.inner.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: editor.threadID, Title: title})
}

// ThreadForkRequest describes a fork after the source thread is bound.
type ThreadForkRequest struct {
	OperationID         ForkOperationID
	DestinationThreadID ThreadID
}

// ThreadForker is fork authority for one exact source thread.
type ThreadForker struct {
	inner    *ThreadForkHost
	threadID ThreadID
}

// Fork creates or replays a fork from the bound source.
func (forker *ThreadForker) Fork(ctx context.Context, request ThreadForkRequest) (ForkThreadResult, error) {
	if forker == nil || forker.inner == nil {
		return ForkThreadResult{}, errors.New("thread forker is required")
	}
	return forker.inner.ForkThread(ctx, ForkThreadRequest{
		OperationID: request.OperationID, SourceThreadID: forker.threadID,
		DestinationThreadID: request.DestinationThreadID,
	})
}

// ThreadDeleter is deletion authority for one exact root thread tree.
type ThreadDeleter struct {
	inner    *ThreadDeleteHost
	threadID ThreadID
}

// Delete deletes or replays deletion of the bound root thread tree.
func (deleter *ThreadDeleter) Delete(ctx context.Context) error {
	if deleter == nil || deleter.inner == nil {
		return errors.New("thread deleter is required")
	}
	return deleter.inner.DeleteThread(ctx, deleter.threadID)
}

// ThreadCompactionRequest describes compaction after ThreadID is bound.
type ThreadCompactionRequest struct {
	RequestID string
	Source    string
	Labels    RunLabels
	Limits    TurnLimits
	Reasoning config.ReasoningSelection
}

// ThreadCompactor owns provider-backed compaction for one exact thread.
type ThreadCompactor struct {
	inner    *ThreadCompactionHost
	threadID ThreadID
}

// Compact compacts the bound thread.
func (compactor *ThreadCompactor) Compact(ctx context.Context, request ThreadCompactionRequest) (CompactThreadResult, error) {
	if compactor == nil || compactor.inner == nil {
		return CompactThreadResult{}, errors.New("thread compactor is required")
	}
	return compactor.inner.CompactThread(ctx, CompactThreadRequest{
		ThreadID: compactor.threadID, RequestID: request.RequestID, Source: request.Source,
		Labels: request.Labels, Limits: request.Limits, Reasoning: request.Reasoning,
	})
}

// SpawnSubAgent describes child creation after ParentThreadID is bound.
type SpawnSubAgent struct {
	PublicationID   string
	ParentTurnID    TurnID
	ThreadID        ThreadID
	TaskName        string
	TaskDescription string
	Message         string
	Attachments     []MessageAttachment
	References      []MessageReference
	HostProfileRef  string
	ForkMode        SubAgentForkMode
	Labels          RunLabels
}

// SendSubAgentInput describes child input after ParentThreadID is bound.
type SendSubAgentInput struct {
	InputRequestID string
	ChildThreadID  ThreadID
	Message        string
	Attachments    []MessageAttachment
	References     []MessageReference
	Interrupt      bool
	Labels         RunLabels
}

// PublishSubAgentPendingToolCompletion describes host-owned child continuation
// input after ParentThreadID is bound.
type PublishSubAgentPendingToolCompletion struct {
	InputRequestID string
	ChildThreadID  ThreadID
	Target         PendingToolSettlementTarget
	Status         PendingToolCompletionStatus
	Summary        string
	Output         string
	Input          TurnInput
	Labels         RunLabels
}

// WaitSubAgents describes a bounded wait after ParentThreadID is bound.
type WaitSubAgents struct {
	ChildThreadIDs []ThreadID
	Timeout        time.Duration
}

// CloseSubAgent describes child closure after ParentThreadID is bound.
type CloseSubAgent struct {
	CloseOperationID string
	ChildThreadID    ThreadID
	Reason           string
}

// SubAgentManager owns child lifecycle for one exact parent.
type SubAgentManager struct {
	inner          *SubAgentHost
	parentThreadID ThreadID
}

// Spawn creates or replays one child publication.
func (manager *SubAgentManager) Spawn(ctx context.Context, request SpawnSubAgent) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		PublicationID: request.PublicationID, ParentThreadID: manager.parentThreadID,
		ParentTurnID: request.ParentTurnID, ThreadID: request.ThreadID, TaskName: request.TaskName,
		TaskDescription: request.TaskDescription, Message: request.Message,
		Attachments:    append([]MessageAttachment(nil), request.Attachments...),
		References:     append([]MessageReference(nil), request.References...),
		HostProfileRef: request.HostProfileRef, ForkMode: request.ForkMode, Labels: request.Labels,
	})
}

// SendInput appends or interrupts with one child input.
func (manager *SubAgentManager) SendInput(ctx context.Context, request SendSubAgentInput) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.SendSubAgentInput(ctx, SendSubAgentInputRequest{
		InputRequestID: request.InputRequestID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Message: request.Message,
		Attachments: append([]MessageAttachment(nil), request.Attachments...),
		References:  append([]MessageReference(nil), request.References...),
		Interrupt:   request.Interrupt, Labels: request.Labels,
	})
}

// PublishPendingToolCompletion admits one host-owned child continuation.
func (manager *SubAgentManager) PublishPendingToolCompletion(ctx context.Context, request PublishSubAgentPendingToolCompletion) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.PublishPendingToolCompletion(ctx, PublishSubAgentPendingToolCompletionRequest{
		InputRequestID: request.InputRequestID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Target: request.Target, Status: request.Status,
		Summary: request.Summary, Output: request.Output, Input: request.Input, Labels: request.Labels,
	})
}

// Wait waits for selected children of the bound parent.
func (manager *SubAgentManager) Wait(ctx context.Context, request WaitSubAgents) (WaitSubAgentsResult, error) {
	if manager == nil || manager.inner == nil {
		return WaitSubAgentsResult{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: manager.parentThreadID,
		ChildThreadIDs: append([]ThreadID(nil), request.ChildThreadIDs...), Timeout: request.Timeout,
	})
}

// Close closes one child of the bound parent.
func (manager *SubAgentManager) Close(ctx context.Context, request CloseSubAgent) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.CloseSubAgent(ctx, CloseSubAgentRequest{
		CloseOperationID: request.CloseOperationID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Reason: request.Reason,
	})
}

// SubAgentDetailRequest identifies one child after ParentThreadID is bound.
type SubAgentDetailRequest struct {
	ChildThreadID ThreadID
	AfterOrdinal  int64
	Limit         int
	IncludeRaw    bool
}

// SubAgentReader reads descendants of one exact parent.
type SubAgentReader struct {
	inner          *SubAgentReadHost
	parentThreadID ThreadID
}

// List returns direct children of the bound parent.
func (reader *SubAgentReader) List(ctx context.Context) ([]SubAgentSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListSubAgents(ctx, reader.parentThreadID)
}

// ReadDetail returns canonical detail for one child of the bound parent.
func (reader *SubAgentReader) ReadDetail(ctx context.Context, request SubAgentDetailRequest) (SubAgentDetail, error) {
	if reader == nil || reader.inner == nil {
		return SubAgentDetail{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: reader.parentThreadID, ChildThreadID: request.ChildThreadID,
		AfterOrdinal: request.AfterOrdinal, Limit: request.Limit, IncludeRaw: request.IncludeRaw,
	})
}

// ActivityTimeline returns the canonical activity projection for the bound
// parent and supplied run metadata.
func (reader *SubAgentReader) ActivityTimeline(ctx context.Context, meta observation.ActivityRunMeta) (SubAgentActivityTimelineResult, error) {
	if reader == nil || reader.inner == nil {
		return SubAgentActivityTimelineResult{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{
		ParentThreadID: reader.parentThreadID, Meta: meta,
	})
}

func (agent *Agent) threadCompactionOptions() ThreadCompactionHostOptions {
	turn := agent.turnExecutionOptions()
	return ThreadCompactionHostOptions{
		config: turn.config, modelGateway: turn.modelGateway,
		modelGatewayIdentity: turn.modelGatewayIdentity, modelGatewayCapabilities: turn.modelGatewayCapabilities,
		sink: turn.sink, idGenerator: turn.idGenerator, loopLimits: turn.loopLimits, initialized: true,
	}
}

func (agent *Agent) subAgentOptions() SubAgentHostOptions {
	turn := agent.turnExecutionOptions()
	return SubAgentHostOptions{
		config: turn.config, modelGateway: turn.modelGateway,
		modelGatewayIdentity: turn.modelGatewayIdentity, modelGatewayCapabilities: turn.modelGatewayCapabilities,
		tools: turn.tools, effectAuthorizationGate: turn.effectAuthorizationGate,
		sink: turn.sink, toolSurfaceProvider: turn.toolSurfaceProvider,
		idGenerator: turn.idGenerator, loopLimits: turn.loopLimits,
		subAgentRunTimeout: agent.subAgentRunTimeout, capabilities: turn.capabilities,
		threadTitleMode: turn.threadTitleMode, initialized: true,
	}
}
