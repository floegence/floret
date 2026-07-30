package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
)

func (host *Host) threadTitleEditor(ctx context.Context, threadID identity.ThreadID) (*threadTitleEditorHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.title.NewHost(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	return &threadTitleEditorHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) threadForker(ctx context.Context, threadID identity.ThreadID) (*threadForkerHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.fork.NewHost(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	return &threadForkerHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) threadDeleter(ctx context.Context, threadID identity.ThreadID) (*threadDeleterHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.delete.NewHost(ctx, threadID)
	if err != nil {
		return nil, err
	}
	return &threadDeleterHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) threadCompactor(ctx context.Context, threadID identity.ThreadID, agent *Agent) (*threadCompactorHandle, error) {
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
	return &threadCompactorHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) subAgentManager(ctx context.Context, parentThreadID identity.ThreadID, agent *Agent) (*subAgentManagerHandle, error) {
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
	return &subAgentManagerHandle{inner: inner, parentThreadID: parentThreadID}, nil
}

func (host *Host) subAgentReader(ctx context.Context, parentThreadID identity.ThreadID) (*subAgentReaderHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.subAgentRead.NewHost(ctx, parentThreadID)
	if err != nil {
		return nil, err
	}
	return &subAgentReaderHandle{inner: inner, parentThreadID: parentThreadID}, nil
}

// threadTitleEditorHandle is title authority for one exact root thread.
type threadTitleEditorHandle struct {
	inner    *threadTitleCapability
	threadID identity.ThreadID
}

// Set replaces the canonical title of the bound thread.
func (editor *threadTitleEditorHandle) Set(ctx context.Context, title string) (ThreadSnapshot, error) {
	if editor == nil || editor.inner == nil {
		return ThreadSnapshot{}, errors.New("thread title editor is required")
	}
	return editor.inner.SetThreadTitle(ctx, setThreadTitleRequest{ThreadID: editor.threadID, Title: title})
}

// boundThreadForkRequest describes a fork after the source thread is bound.
type boundThreadForkRequest struct {
	OperationID         forkOperationID
	DestinationThreadID identity.ThreadID
}

// threadForkerHandle is fork authority for one exact source thread.
type threadForkerHandle struct {
	inner    *threadForkCapability
	threadID identity.ThreadID
}

// Fork creates or replays a fork from the bound source.
func (forker *threadForkerHandle) Fork(ctx context.Context, request boundThreadForkRequest) (forkThreadResult, error) {
	if forker == nil || forker.inner == nil {
		return forkThreadResult{}, errors.New("thread forker is required")
	}
	return forker.inner.ForkThread(ctx, forkThreadRequest{
		OperationID: request.OperationID, SourceThreadID: forker.threadID,
		DestinationThreadID: request.DestinationThreadID,
	})
}

// threadDeleterHandle is deletion authority for one exact root thread tree.
type threadDeleterHandle struct {
	inner    *threadDeleteCapability
	threadID identity.ThreadID
}

// Delete deletes or replays deletion of the bound root thread tree.
func (deleter *threadDeleterHandle) Delete(ctx context.Context) error {
	if deleter == nil || deleter.inner == nil {
		return errors.New("thread deleter is required")
	}
	return deleter.inner.DeleteThread(ctx, deleter.threadID)
}

// threadCompactionRequest describes compaction after ThreadID is bound.
type threadCompactionRequest struct {
	RequestID string
	Source    string
	Labels    RunLabels
	Limits    TurnLimits
	Reasoning config.ReasoningSelection
}

// threadCompactorHandle owns provider-backed compaction for one exact thread.
type threadCompactorHandle struct {
	inner    *threadCompactionCapability
	threadID identity.ThreadID
}

// Compact compacts the bound thread.
func (compactor *threadCompactorHandle) Compact(ctx context.Context, request threadCompactionRequest) (compactThreadResult, error) {
	if compactor == nil || compactor.inner == nil {
		return compactThreadResult{}, errors.New("thread compactor is required")
	}
	return compactor.inner.CompactThread(ctx, compactThreadRequest{
		ThreadID: compactor.threadID, RequestID: request.RequestID, Source: request.Source,
		Labels: request.Labels, Limits: request.Limits, Reasoning: request.Reasoning,
	})
}

// spawnSubAgentCommand describes child creation after ParentThreadID is bound.
type spawnSubAgentCommand struct {
	PublicationID   string
	ParentTurnID    identity.TurnID
	ThreadID        identity.ThreadID
	TaskName        string
	TaskDescription string
	Message         string
	Attachments     []MessageAttachment
	References      []MessageReference
	HostProfileRef  string
	ForkMode        SubAgentForkMode
	Labels          RunLabels
}

// sendSubAgentInputCommand describes child input after ParentThreadID is bound.
type sendSubAgentInputCommand struct {
	InputRequestID string
	ChildThreadID  identity.ThreadID
	Message        string
	Attachments    []MessageAttachment
	References     []MessageReference
	Interrupt      bool
	Labels         RunLabels
}

// publishSubAgentPendingToolCompletionCommand describes host-owned child continuation
// input after ParentThreadID is bound.
type publishSubAgentPendingToolCompletionCommand struct {
	InputRequestID string
	ChildThreadID  identity.ThreadID
	Target         PendingToolSettlementTarget
	Status         PendingToolCompletionStatus
	Summary        string
	Output         string
	Input          TurnInput
	Labels         RunLabels
}

// waitSubAgentsCommand describes a bounded wait after ParentThreadID is bound.
type waitSubAgentsCommand struct {
	ChildThreadIDs []identity.ThreadID
	Timeout        time.Duration
}

// closeSubAgentCommand describes child closure after ParentThreadID is bound.
type closeSubAgentCommand struct {
	CloseOperationID string
	ChildThreadID    identity.ThreadID
	Reason           string
}

// subAgentManagerHandle owns child lifecycle for one exact parent.
type subAgentManagerHandle struct {
	inner          *subAgentCapability
	parentThreadID identity.ThreadID
}

// Spawn creates or replays one child publication.
func (manager *subAgentManagerHandle) Spawn(ctx context.Context, request spawnSubAgentCommand) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.spawnSubAgentCommand(ctx, spawnSubAgentRequest{
		PublicationID: request.PublicationID, ParentThreadID: manager.parentThreadID,
		ParentTurnID: request.ParentTurnID, ThreadID: request.ThreadID, TaskName: request.TaskName,
		TaskDescription: request.TaskDescription, Message: request.Message,
		Attachments:    append([]MessageAttachment(nil), request.Attachments...),
		References:     append([]MessageReference(nil), request.References...),
		HostProfileRef: request.HostProfileRef, ForkMode: request.ForkMode, Labels: request.Labels,
	})
}

// SendInput appends or interrupts with one child input.
func (manager *subAgentManagerHandle) SendInput(ctx context.Context, request sendSubAgentInputCommand) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.sendSubAgentInputCommand(ctx, sendSubAgentInputRequest{
		InputRequestID: request.InputRequestID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Message: request.Message,
		Attachments: append([]MessageAttachment(nil), request.Attachments...),
		References:  append([]MessageReference(nil), request.References...),
		Interrupt:   request.Interrupt, Labels: request.Labels,
	})
}

// Activate starts and drains durable input for one direct child.
func (manager *subAgentManagerHandle) Activate(ctx context.Context, childThreadID identity.ThreadID) error {
	if manager == nil || manager.inner == nil {
		return errors.New("SubAgent manager is required")
	}
	return manager.inner.activateSubAgent(ctx, childThreadID)
}

// PublishPendingToolCompletion admits one host-owned child continuation.
func (manager *subAgentManagerHandle) PublishPendingToolCompletion(ctx context.Context, request publishSubAgentPendingToolCompletionCommand) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.PublishPendingToolCompletion(ctx, publishSubAgentPendingToolCompletionRequest{
		InputRequestID: request.InputRequestID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Target: request.Target, Status: request.Status,
		Summary: request.Summary, Output: request.Output, Input: request.Input, Labels: request.Labels,
	})
}

// Wait waits for selected children of the bound parent.
func (manager *subAgentManagerHandle) Wait(ctx context.Context, request waitSubAgentsCommand) (waitSubAgentsCommandResult, error) {
	if manager == nil || manager.inner == nil {
		return waitSubAgentsCommandResult{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.waitSubAgentsCommand(ctx, waitSubAgentsRequest{
		ParentThreadID: manager.parentThreadID,
		ChildThreadIDs: append([]identity.ThreadID(nil), request.ChildThreadIDs...), Timeout: request.Timeout,
	})
}

// Close closes one child of the bound parent.
func (manager *subAgentManagerHandle) Close(ctx context.Context, request closeSubAgentCommand) (SubAgentSnapshot, error) {
	if manager == nil || manager.inner == nil {
		return SubAgentSnapshot{}, errors.New("SubAgent manager is required")
	}
	return manager.inner.closeSubAgentCommand(ctx, closeSubAgentRequest{
		CloseOperationID: request.CloseOperationID, ParentThreadID: manager.parentThreadID,
		ChildThreadID: request.ChildThreadID, Reason: request.Reason,
	})
}

// subAgentDetailRequest identifies one child after ParentThreadID is bound.
type subAgentDetailRequest struct {
	ChildThreadID identity.ThreadID
	AfterOrdinal  int64
	Limit         int
	IncludeRaw    bool
}

// subAgentReaderHandle reads descendants of one exact parent.
type subAgentReaderHandle struct {
	inner          *subAgentReadCapability
	parentThreadID identity.ThreadID
}

// List returns direct children of the bound parent.
func (reader *subAgentReaderHandle) List(ctx context.Context) ([]SubAgentSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return nil, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListSubAgents(ctx, reader.parentThreadID)
}

// ReadDetail returns canonical detail for one child of the bound parent.
func (reader *subAgentReaderHandle) ReadDetail(ctx context.Context, request subAgentDetailRequest) (SubAgentDetail, error) {
	if reader == nil || reader.inner == nil {
		return SubAgentDetail{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ReadSubAgentDetail(ctx, readSubAgentDetailRequest{
		ParentThreadID: reader.parentThreadID, ChildThreadID: request.ChildThreadID,
		AfterOrdinal: request.AfterOrdinal, Limit: request.Limit, IncludeRaw: request.IncludeRaw,
	})
}

// ActivityTimeline returns the canonical activity projection for the bound
// parent and supplied run metadata.
func (reader *subAgentReaderHandle) ActivityTimeline(ctx context.Context, meta observation.ActivityRunMeta) (subAgentActivityTimelineResult, error) {
	if reader == nil || reader.inner == nil {
		return subAgentActivityTimelineResult{}, errors.New("SubAgent reader is required")
	}
	return reader.inner.ListSubAgentActivityTimeline(ctx, listSubAgentActivityTimelineRequest{
		ParentThreadID: reader.parentThreadID, Meta: meta,
	})
}

func (agent *Agent) threadCompactionOptions() threadCompactionOptions {
	turn := agent.turnExecutionOptions()
	return threadCompactionOptions{
		config: turn.config, modelGateway: turn.modelGateway,
		modelGatewayIdentity: turn.modelGatewayIdentity, modelGatewayCapabilities: turn.modelGatewayCapabilities,
		sink: turn.sink, idGenerator: turn.idGenerator, loopLimits: turn.loopLimits, initialized: true,
	}
}

func (agent *Agent) subAgentOptions() subAgentOptions {
	turn := agent.turnExecutionOptions()
	return subAgentOptions{
		config: turn.config, modelGateway: turn.modelGateway,
		modelGatewayIdentity: turn.modelGatewayIdentity, modelGatewayCapabilities: turn.modelGatewayCapabilities,
		tools: turn.tools, effectAuthorizationGate: turn.effectAuthorizationGate,
		sink: turn.sink, toolSurfaceProvider: turn.toolSurfaceProvider,
		idGenerator: turn.idGenerator, loopLimits: turn.loopLimits,
		subAgentRunTimeout: agent.subAgentRunTimeout, capabilities: turn.capabilities,
		threadTitleMode: turn.threadTitleMode, initialized: true,
	}
}
