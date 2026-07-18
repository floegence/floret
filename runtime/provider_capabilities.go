package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/tools"
)

// TurnExecutionHost owns provider-backed turn admission and continuation for
// one canonical thread.
type TurnExecutionHost struct {
	threadID ThreadID
	host     *providerHost
}

// ThreadCompactionHost owns provider-backed compaction for one canonical thread.
type ThreadCompactionHost struct {
	threadID ThreadID
	host     *providerHost
}

// SubAgentHost owns provider-backed child-thread lifecycle under one canonical
// parent. Child reads use a separately parent-bound SubAgentReadHost.
type SubAgentHost struct {
	parentThreadID ThreadID
	host           *providerHost
}

// TurnExecutionHostFactory issues thread-bound turn execution capabilities.
type TurnExecutionHostFactory struct {
	store *Store
}

// ThreadCompactionHostFactory issues thread-bound compaction capabilities.
type ThreadCompactionHostFactory struct {
	store *Store
}

// SubAgentHostFactory issues parent-bound interactive child capabilities.
type SubAgentHostFactory struct {
	store *Store
}

// TurnExecutionHostOptions configures one thread-bound turn capability.
type TurnExecutionHostOptions struct {
	ThreadID             ThreadID
	Config               config.Config
	ModelGateway         ModelGateway
	ModelGatewayIdentity ModelGatewayIdentity
	Tools                *tools.Registry
	Approver             tools.Approver
	Sink                 EventSink
	ToolSurfaceProvider  ToolSurfaceProvider
	IDGenerator          func(string) string
	LoopLimits           LoopLimits
	Capabilities         CapabilityOptions
	ThreadTitleMode      ThreadTitleMode
}

// ThreadCompactionHostOptions configures one thread-bound compaction capability.
type ThreadCompactionHostOptions struct {
	ThreadID             ThreadID
	Config               config.Config
	ModelGateway         ModelGateway
	ModelGatewayIdentity ModelGatewayIdentity
	Sink                 EventSink
	IDGenerator          func(string) string
	LoopLimits           LoopLimits
}

// SubAgentHostOptions configures one parent-bound interactive child capability.
type SubAgentHostOptions struct {
	ParentThreadID       ThreadID
	Config               config.Config
	ModelGateway         ModelGateway
	ModelGatewayIdentity ModelGatewayIdentity
	Tools                *tools.Registry
	Approver             tools.Approver
	Sink                 EventSink
	ToolSurfaceProvider  ToolSurfaceProvider
	IDGenerator          func(string) string
	LoopLimits           LoopLimits
	SubAgentRunTimeout   time.Duration
	Capabilities         CapabilityOptions
	ThreadTitleMode      ThreadTitleMode
}

// NewTurnExecutionHostFactory constructs the only authority that can issue
// provider-backed root-thread turn capabilities.
func NewTurnExecutionHostFactory(bootstrap *HostBootstrap) (*TurnExecutionHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &TurnExecutionHostFactory{store: store}, nil
}

// NewThreadCompactionHostFactory constructs the only authority that can issue
// provider-backed root-thread compaction capabilities.
func NewThreadCompactionHostFactory(bootstrap *HostBootstrap) (*ThreadCompactionHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadCompactionHostFactory{store: store}, nil
}

// NewSubAgentHostFactory constructs the only authority that can issue
// provider-backed capabilities under a bound parent.
func NewSubAgentHostFactory(bootstrap *HostBootstrap) (*SubAgentHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &SubAgentHostFactory{store: store}, nil
}

// NewHost constructs a provider-backed capability for one root thread.
func (f *TurnExecutionHostFactory) NewHost(opts TurnExecutionHostOptions) (*TurnExecutionHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("turn execution host factory is required")
	}
	threadID, err := normalizeBoundThreadID(opts.ThreadID, "turn execution host")
	if err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:               opts.Config,
		ModelGateway:         opts.ModelGateway,
		ModelGatewayIdentity: opts.ModelGatewayIdentity,
		Store:                f.store,
		Tools:                opts.Tools,
		Approver:             opts.Approver,
		Sink:                 opts.Sink,
		ToolSurfaceProvider:  opts.ToolSurfaceProvider,
		IDGenerator:          opts.IDGenerator,
		LoopLimits:           opts.LoopLimits,
		Capabilities:         opts.Capabilities,
		ThreadTitleMode:      opts.ThreadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	return &TurnExecutionHost{threadID: threadID, host: host}, nil
}

// NewHost constructs a provider-backed compaction capability for one root thread.
func (f *ThreadCompactionHostFactory) NewHost(opts ThreadCompactionHostOptions) (*ThreadCompactionHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("thread compaction host factory is required")
	}
	threadID, err := normalizeBoundThreadID(opts.ThreadID, "thread compaction host")
	if err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:               opts.Config,
		ModelGateway:         opts.ModelGateway,
		ModelGatewayIdentity: opts.ModelGatewayIdentity,
		Store:                f.store,
		Sink:                 opts.Sink,
		IDGenerator:          opts.IDGenerator,
		LoopLimits:           opts.LoopLimits,
	})
	if err != nil {
		return nil, err
	}
	return &ThreadCompactionHost{threadID: threadID, host: host}, nil
}

// NewHost constructs a provider-backed child lifecycle capability for one parent.
func (f *SubAgentHostFactory) NewHost(opts SubAgentHostOptions) (*SubAgentHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("subagent host factory is required")
	}
	parentThreadID, err := normalizeBoundThreadID(opts.ParentThreadID, "subagent host")
	if err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:               opts.Config,
		ModelGateway:         opts.ModelGateway,
		ModelGatewayIdentity: opts.ModelGatewayIdentity,
		Store:                f.store,
		Tools:                opts.Tools,
		Approver:             opts.Approver,
		Sink:                 opts.Sink,
		ToolSurfaceProvider:  opts.ToolSurfaceProvider,
		IDGenerator:          opts.IDGenerator,
		LoopLimits:           opts.LoopLimits,
		SubAgentRunTimeout:   opts.SubAgentRunTimeout,
		Capabilities:         opts.Capabilities,
		ThreadTitleMode:      opts.ThreadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	return &SubAgentHost{parentThreadID: parentThreadID, host: host}, nil
}

func (h *TurnExecutionHost) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return TurnResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return TurnResult{}, err
	}
	return h.host.RunTurn(ctx, req)
}

func (h *TurnExecutionHost) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return TurnResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return TurnResult{}, err
	}
	return h.host.RetryTurn(ctx, req)
}

func (h *TurnExecutionHost) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (TurnResult, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return TurnResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return TurnResult{}, err
	}
	return h.host.CompletePendingTool(ctx, req)
}

func (h *TurnExecutionHost) ListPendingApprovals(ctx context.Context, req ListPendingApprovalsRequest) (PendingApprovals, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return PendingApprovals{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return PendingApprovals{}, err
	}
	return h.host.ListPendingApprovals(ctx, req)
}

func (h *TurnExecutionHost) UpdateThreadAgentTodos(ctx context.Context, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return ThreadAgentTodoState{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return ThreadAgentTodoState{}, err
	}
	return h.host.UpdateThreadAgentTodos(ctx, req)
}

func (h *ThreadCompactionHost) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "thread compaction host"); err != nil {
		return CompactThreadResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return CompactThreadResult{}, err
	}
	return h.host.CompactThread(ctx, req)
}

func (h *SubAgentHost) SpawnSubAgent(ctx context.Context, req SpawnSubAgentRequest) (SubAgentSnapshot, error) {
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	h.host.store.threadAuthorityMu.Lock()
	defer h.host.store.threadAuthorityMu.Unlock()
	return h.host.SpawnSubAgent(ctx, req)
}

func (h *SubAgentHost) SendSubAgentInput(ctx context.Context, req SendSubAgentInputRequest) (SubAgentSnapshot, error) {
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	return h.host.SendSubAgentInput(ctx, req)
}

func (h *SubAgentHost) WaitSubAgents(ctx context.Context, req WaitSubAgentsRequest) (WaitSubAgentsResult, error) {
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return WaitSubAgentsResult{}, err
	}
	return h.host.WaitSubAgents(ctx, req)
}

func (h *SubAgentHost) CloseSubAgent(ctx context.Context, req CloseSubAgentRequest) (SubAgentSnapshot, error) {
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	return h.host.CloseSubAgent(ctx, req)
}

func normalizeBoundThreadID(threadID ThreadID, owner string) (ThreadID, error) {
	id := ThreadID(strings.TrimSpace(string(threadID)))
	if id == "" {
		return "", fmt.Errorf("%s requires thread id", owner)
	}
	return id, nil
}

func validateBoundThreadID(bound, requested ThreadID, owner string) error {
	if strings.TrimSpace(string(requested)) == "" {
		return errors.New("thread id is required")
	}
	if requested != bound {
		return fmt.Errorf("%s is bound to thread %q, got %q", owner, bound, requested)
	}
	return nil
}
