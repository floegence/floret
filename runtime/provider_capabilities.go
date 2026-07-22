package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/sessiontree"
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

// TurnExecutionHostBinder issues only thread-bound turn factories.
type TurnExecutionHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadCompactionHostBinder issues only thread-bound compaction factories.
type ThreadCompactionHostBinder struct {
	store *Store
	lease *capabilityLease
}

// SubAgentHostBinder issues only parent-bound interactive child factories.
type SubAgentHostBinder struct {
	store *Store
	lease *capabilityLease
}

// TurnExecutionHostFactory issues thread-bound turn execution capabilities.
type TurnExecutionHostFactory struct {
	store    *Store
	threadID ThreadID
}

// ThreadCompactionHostFactory issues thread-bound compaction capabilities.
type ThreadCompactionHostFactory struct {
	store    *Store
	threadID ThreadID
}

// SubAgentHostFactory issues parent-bound interactive child capabilities.
type SubAgentHostFactory struct {
	store          *Store
	parentThreadID ThreadID
}

// TurnExecutionHostOptions configures one thread-bound turn capability.
type TurnExecutionHostOptions struct {
	Config                   config.Config
	ModelGateway             ModelGateway
	ModelGatewayIdentity     ModelGatewayIdentity
	ModelGatewayCapabilities ModelGatewayCapabilities
	Tools                    *tools.Registry
	EffectAuthorizationGate  EffectAuthorizationGate
	Sink                     EventSink
	ToolSurfaceProvider      ToolSurfaceProvider
	IDGenerator              func(string) string
	LoopLimits               LoopLimits
	Capabilities             CapabilityOptions
	ThreadTitleMode          ThreadTitleMode
}

// ThreadCompactionHostOptions configures one thread-bound compaction capability.
type ThreadCompactionHostOptions struct {
	Config                   config.Config
	ModelGateway             ModelGateway
	ModelGatewayIdentity     ModelGatewayIdentity
	ModelGatewayCapabilities ModelGatewayCapabilities
	Sink                     EventSink
	IDGenerator              func(string) string
	LoopLimits               LoopLimits
}

// SubAgentHostOptions configures one parent-bound interactive child capability.
type SubAgentHostOptions struct {
	Config                   config.Config
	ModelGateway             ModelGateway
	ModelGatewayIdentity     ModelGatewayIdentity
	ModelGatewayCapabilities ModelGatewayCapabilities
	Tools                    *tools.Registry
	EffectAuthorizationGate  EffectAuthorizationGate
	Sink                     EventSink
	ToolSurfaceProvider      ToolSurfaceProvider
	IDGenerator              func(string) string
	LoopLimits               LoopLimits
	SubAgentRunTimeout       time.Duration
	Capabilities             CapabilityOptions
	ThreadTitleMode          ThreadTitleMode
}

// ModelGatewayCapabilities describes host-resolved behavior for a gateway model.
// A nil Reasoning means the host did not resolve the capability; an explicit
// Kind="none" value means the host resolved that reasoning is unsupported.
type ModelGatewayCapabilities struct {
	Reasoning *config.ReasoningCapability
}

func (c ModelGatewayCapabilities) validate(gateway ModelGateway) error {
	if gateway == nil {
		if c.Reasoning != nil {
			return errors.New("native provider host must not provide model gateway capabilities")
		}
		return nil
	}
	if c.Reasoning == nil {
		return errors.New("model gateway reasoning capability is required")
	}
	reasoning := c.Reasoning.Normalize()
	if reasoning.IsZero() {
		return errors.New("model gateway reasoning capability must be explicit; use kind none when unsupported")
	}
	if err := reasoning.Validate(); err != nil {
		return fmt.Errorf("invalid model gateway reasoning capability: %w", err)
	}
	return nil
}

// NewTurnExecutionHostBinder constructs the turn execution issuer.
func NewTurnExecutionHostBinder(bootstrap *HostBootstrap) (*TurnExecutionHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &TurnExecutionHostBinder{store: store, lease: lease}, nil
}

// Bind constructs provider-backed turn capability factory for exactly one root thread.
func (b *TurnExecutionHostBinder) Bind(threadID ThreadID) (*TurnExecutionHostFactory, error) {
	if b == nil {
		return nil, errors.New("turn execution host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "turn execution host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	threadID, err = normalizeBoundThreadID(threadID, "turn execution host factory")
	if err != nil {
		return nil, err
	}
	return &TurnExecutionHostFactory{store: b.store, threadID: threadID}, nil
}

// NewThreadCompactionHostBinder constructs the compaction issuer.
func NewThreadCompactionHostBinder(bootstrap *HostBootstrap) (*ThreadCompactionHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadCompactionHostBinder{store: store, lease: lease}, nil
}

// Bind constructs provider-backed compaction factory for exactly one root thread.
func (b *ThreadCompactionHostBinder) Bind(threadID ThreadID) (*ThreadCompactionHostFactory, error) {
	if b == nil {
		return nil, errors.New("thread compaction host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread compaction host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	threadID, err = normalizeBoundThreadID(threadID, "thread compaction host factory")
	if err != nil {
		return nil, err
	}
	return &ThreadCompactionHostFactory{store: b.store, threadID: threadID}, nil
}

// NewSubAgentHostBinder constructs the interactive child issuer.
func NewSubAgentHostBinder(bootstrap *HostBootstrap) (*SubAgentHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &SubAgentHostBinder{store: store, lease: lease}, nil
}

// Bind constructs provider-backed child capability factory for exactly one parent.
func (b *SubAgentHostBinder) Bind(parentThreadID ThreadID) (*SubAgentHostFactory, error) {
	if b == nil {
		return nil, errors.New("subagent host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "subagent host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	parentThreadID, err = normalizeBoundThreadID(parentThreadID, "subagent host factory")
	if err != nil {
		return nil, err
	}
	return &SubAgentHostFactory{store: b.store, parentThreadID: parentThreadID}, nil
}

// NewHost constructs a provider-backed capability for one existing root thread.
func (f *TurnExecutionHostFactory) NewHost(ctx context.Context, opts TurnExecutionHostOptions) (*TurnExecutionHost, error) {
	if f == nil || f.store == nil || f.threadID == "" {
		return nil, errors.New("turn execution host factory is required")
	}
	ctx, done, err := beginHostOperationContext(f.store, ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := validateRootHostConstructionAuthority(ctx, f.store, f.threadID); err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:                   opts.Config,
		ModelGateway:             opts.ModelGateway,
		ModelGatewayIdentity:     opts.ModelGatewayIdentity,
		ModelGatewayCapabilities: opts.ModelGatewayCapabilities,
		Store:                    f.store,
		Tools:                    opts.Tools,
		EffectAuthorizationGate:  opts.EffectAuthorizationGate,
		Sink:                     opts.Sink,
		ToolSurfaceProvider:      opts.ToolSurfaceProvider,
		IDGenerator:              opts.IDGenerator,
		LoopLimits:               opts.LoopLimits,
		Capabilities:             opts.Capabilities,
		ThreadTitleMode:          opts.ThreadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	return &TurnExecutionHost{threadID: f.threadID, host: host}, nil
}

// NewHost constructs a provider-backed compaction capability for one existing root thread.
func (f *ThreadCompactionHostFactory) NewHost(ctx context.Context, opts ThreadCompactionHostOptions) (*ThreadCompactionHost, error) {
	if f == nil || f.store == nil || f.threadID == "" {
		return nil, errors.New("thread compaction host factory is required")
	}
	ctx, done, err := beginHostOperationContext(f.store, ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := validateRootHostConstructionAuthority(ctx, f.store, f.threadID); err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:                   opts.Config,
		ModelGateway:             opts.ModelGateway,
		ModelGatewayIdentity:     opts.ModelGatewayIdentity,
		ModelGatewayCapabilities: opts.ModelGatewayCapabilities,
		Store:                    f.store,
		Sink:                     opts.Sink,
		IDGenerator:              opts.IDGenerator,
		LoopLimits:               opts.LoopLimits,
	})
	if err != nil {
		return nil, err
	}
	return &ThreadCompactionHost{threadID: f.threadID, host: host}, nil
}

// NewHost constructs a provider-backed child lifecycle capability for one existing parent.
func (f *SubAgentHostFactory) NewHost(ctx context.Context, opts SubAgentHostOptions) (*SubAgentHost, error) {
	if f == nil || f.store == nil || f.parentThreadID == "" {
		return nil, errors.New("subagent host factory is required")
	}
	ctx, done, err := beginHostOperationContext(f.store, ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if err := validateSubAgentParentConstructionAuthority(ctx, f.store, f.parentThreadID); err != nil {
		return nil, err
	}
	host, err := newProviderHost(providerHostOptions{
		Config:                   opts.Config,
		ModelGateway:             opts.ModelGateway,
		ModelGatewayIdentity:     opts.ModelGatewayIdentity,
		ModelGatewayCapabilities: opts.ModelGatewayCapabilities,
		Store:                    f.store,
		Tools:                    opts.Tools,
		EffectAuthorizationGate:  opts.EffectAuthorizationGate,
		Sink:                     opts.Sink,
		ToolSurfaceProvider:      opts.ToolSurfaceProvider,
		IDGenerator:              opts.IDGenerator,
		LoopLimits:               opts.LoopLimits,
		SubAgentRunTimeout:       opts.SubAgentRunTimeout,
		Capabilities:             opts.Capabilities,
		ThreadTitleMode:          opts.ThreadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	return &SubAgentHost{parentThreadID: f.parentThreadID, host: host}, nil
}

func (h *TurnExecutionHost) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return TurnResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return TurnResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return TurnResult{}, err
	}
	result, err := h.host.RunTurn(ctx, req)
	return result, requestConflictError(err, "turn", string(req.TurnID))
}

func (h *TurnExecutionHost) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return TurnResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return TurnResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return TurnResult{}, err
	}
	result, err := h.host.RetryTurn(ctx, req)
	return result, requestConflictError(err, "retry", string(req.ThreadID))
}

func (h *TurnExecutionHost) CompletePendingTool(ctx context.Context, req PendingToolCompletionRequest) (PendingToolCompletionResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return PendingToolCompletionResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.Target.ThreadID, "turn execution host"); err != nil {
		return PendingToolCompletionResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.Target.ThreadID); err != nil {
		return PendingToolCompletionResult{}, err
	}
	result, err := h.host.CompletePendingTool(ctx, req)
	return result, requestConflictError(err, "pending_tool_completion", req.CompletionRequestID)
}

func (h *TurnExecutionHost) ReadApprovalQueue(ctx context.Context, req ReadApprovalQueueRequest) (ApprovalQueue, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return ApprovalQueue{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return ApprovalQueue{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return ApprovalQueue{}, err
	}
	return h.host.ReadApprovalQueue(ctx, req)
}

func (h *TurnExecutionHost) ResolveApproval(ctx context.Context, req ResolveApprovalRequest) (ResolveApprovalResult, error) {
	if err := req.Validate(); err != nil {
		return ResolveApprovalResult{}, err
	}
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return ResolveApprovalResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ExpectedRootThreadID, "turn execution host"); err != nil {
		return ResolveApprovalResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ExpectedRootThreadID); err != nil {
		return ResolveApprovalResult{}, err
	}
	return h.host.ResolveApproval(ctx, req)
}

func (h *TurnExecutionHost) UpdateThreadAgentTodos(ctx context.Context, req UpdateThreadAgentTodosRequest) (ThreadAgentTodoState, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return ThreadAgentTodoState{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "turn execution host"); err != nil {
		return ThreadAgentTodoState{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return ThreadAgentTodoState{}, err
	}
	proof, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || proof.Purpose != sessiontree.TurnLeasePurposeTurn || proof.ThreadID != string(req.ThreadID) || proof.TurnID != string(req.TurnID) {
		return ThreadAgentTodoState{}, &AuthorityBusyError{Kind: AuthorityBusyTurn}
	}
	_, localProof, owned, err := h.host.harness.OwnedActiveThread(ctx, string(req.ThreadID), string(req.TurnID))
	if err != nil {
		return ThreadAgentTodoState{}, runtimeHostError(err)
	}
	if !owned || !sessiontree.SameTurnLease(localProof, proof) {
		return ThreadAgentTodoState{}, &AuthorityBusyError{Kind: AuthorityBusyTurn}
	}
	return h.host.UpdateThreadAgentTodos(ctx, req)
}

func (h *ThreadCompactionHost) CompactThread(ctx context.Context, req CompactThreadRequest) (CompactThreadResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return CompactThreadResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.threadID, req.ThreadID, "thread compaction host"); err != nil {
		return CompactThreadResult{}, err
	}
	if err := validateRootThreadAuthority(ctx, h.host.store, req.ThreadID); err != nil {
		return CompactThreadResult{}, err
	}
	result, err := h.host.CompactThread(ctx, req)
	return result, requestConflictError(err, "compaction", req.RequestID)
}

func (h *SubAgentHost) SpawnSubAgent(ctx context.Context, req SpawnSubAgentRequest) (SubAgentSnapshot, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	h.host.store.threadAuthorityMu.Lock()
	defer h.host.store.threadAuthorityMu.Unlock()
	result, err := h.host.SpawnSubAgent(ctx, req)
	return result, requestConflictError(err, "subagent_publication", req.PublicationID)
}

func (h *SubAgentHost) SendSubAgentInput(ctx context.Context, req SendSubAgentInputRequest) (SubAgentSnapshot, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	result, err := h.host.SendSubAgentInput(ctx, req)
	return result, requestConflictError(err, "subagent_input", req.InputRequestID)
}

func (h *SubAgentHost) PublishPendingToolCompletion(ctx context.Context, req PublishSubAgentPendingToolCompletionRequest) (SubAgentSnapshot, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	result, err := h.host.PublishSubAgentPendingToolCompletion(ctx, req)
	return result, requestConflictError(err, "subagent_pending_tool_completion", req.InputRequestID)
}

func (h *SubAgentHost) WaitSubAgents(ctx context.Context, req WaitSubAgentsRequest) (WaitSubAgentsResult, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return WaitSubAgentsResult{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return WaitSubAgentsResult{}, err
	}
	return h.host.WaitSubAgents(ctx, req)
}

func (h *SubAgentHost) CloseSubAgent(ctx context.Context, req CloseSubAgentRequest) (SubAgentSnapshot, error) {
	ctx, done, err := beginHostOperationContext(h.host.store, ctx)
	if err != nil {
		return SubAgentSnapshot{}, err
	}
	defer done()
	if err := validateBoundThreadID(h.parentThreadID, req.ParentThreadID, "subagent host parent"); err != nil {
		return SubAgentSnapshot{}, err
	}
	result, err := h.host.CloseSubAgent(ctx, req)
	return result, requestConflictError(err, "subagent_close", req.CloseOperationID)
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

func validateBoundRootThreadAuthority(ctx context.Context, store *Store, bound, requested ThreadID, owner string) error {
	if err := validateBoundThreadID(bound, requested, owner); err != nil {
		return err
	}
	return validateRootThreadAuthority(ctx, store, requested)
}

func validateSubAgentParentAuthority(ctx context.Context, store *Store, parentThreadID ThreadID) error {
	if strings.TrimSpace(string(parentThreadID)) == "" {
		return errors.New("parent thread id is required")
	}
	snapshot, err := inspectThreadAuthority(ctx, store, parentThreadID)
	if err != nil {
		return err
	}
	return validateLiveThreadLifecycle(snapshot.Thread)
}

func validateRootHostConstructionAuthority(ctx context.Context, store *Store, threadID ThreadID) error {
	if err := validateRootBoundAuthority(ctx, store, threadID); err != nil {
		return err
	}
	snapshot, err := inspectThreadAuthority(ctx, store, threadID)
	if err != nil {
		return err
	}
	if snapshot.Lease != nil || strings.TrimSpace(snapshot.ClaimOperationID) != "" {
		return authorityBusyForSnapshot(snapshot)
	}
	return nil
}

func validateRootBoundAuthority(ctx context.Context, store *Store, threadID ThreadID) error {
	snapshot, err := inspectThreadAuthority(ctx, store, threadID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(snapshot.Thread.ParentThreadID) != "" {
		return fmt.Errorf("%w: %s", ErrSubAgentParentRequired, threadID)
	}
	if err := validateLiveThreadLifecycle(snapshot.Thread); err != nil {
		return err
	}
	return nil
}

func validateSubAgentParentConstructionAuthority(ctx context.Context, store *Store, parentThreadID ThreadID) error {
	if err := validateParentBoundAuthority(ctx, store, parentThreadID); err != nil {
		return err
	}
	snapshot, err := inspectThreadAuthority(ctx, store, parentThreadID)
	if err != nil {
		return err
	}
	if snapshot.Lease != nil || strings.TrimSpace(snapshot.ClaimOperationID) != "" {
		return authorityBusyForSnapshot(snapshot)
	}
	return nil
}

func authorityBusyForSnapshot(snapshot sessiontree.ThreadAuthoritySnapshot) error {
	kind := AuthorityBusyAuthority
	if snapshot.Lease != nil && snapshot.Lease.Purpose == sessiontree.TurnLeasePurposeTurn {
		kind = AuthorityBusyTurn
	}
	return &AuthorityBusyError{Kind: kind}
}

func validateParentBoundAuthority(ctx context.Context, store *Store, parentThreadID ThreadID) error {
	snapshot, err := inspectThreadAuthority(ctx, store, parentThreadID)
	if err != nil {
		return err
	}
	if err := validateLiveThreadLifecycle(snapshot.Thread); err != nil {
		return err
	}
	return nil
}

func validateReadableParentBoundAuthority(ctx context.Context, store *Store, parentThreadID ThreadID) error {
	snapshot, err := inspectThreadAuthority(ctx, store, parentThreadID)
	if err != nil {
		return err
	}
	switch snapshot.Thread.Lifecycle {
	case "", sessiontree.ThreadLifecycleOpen, sessiontree.ThreadLifecycleClosing, sessiontree.ThreadLifecycleClosed:
		return nil
	case sessiontree.ThreadLifecycleDeleted:
		return ErrThreadDeleted
	default:
		return ErrAuthorityCorrupt
	}
}

func validateDeleteHostConstructionAuthority(ctx context.Context, store *Store, threadID ThreadID) error {
	if err := validateRootBoundAuthority(ctx, store, threadID); err == nil {
		return nil
	} else if !errors.Is(err, ErrThreadDeleted) {
		return err
	}
	tombstone, err := store.rootAuthority.ThreadTombstone(ctx, string(threadID))
	if err != nil {
		return runtimeHostError(err)
	}
	if tombstone.ThreadID != string(threadID) || tombstone.RootThreadID != string(threadID) || strings.TrimSpace(tombstone.ParentThreadID) != "" {
		return ErrAuthorityCorrupt
	}
	return nil
}

func inspectThreadAuthority(ctx context.Context, store *Store, threadID ThreadID) (sessiontree.ThreadAuthoritySnapshot, error) {
	if store == nil || store.repo == nil {
		return sessiontree.ThreadAuthoritySnapshot{}, errors.New("runtime store is required")
	}
	inspector, ok := store.repo.(sessiontree.ThreadAuthorityInspectionRepo)
	if !ok {
		return sessiontree.ThreadAuthoritySnapshot{}, ErrUnsupportedStoreCapability
	}
	snapshot, err := inspector.InspectThreadAuthority(ctx, string(threadID))
	return snapshot, runtimeHostError(err)
}

func inspectSubAgentThreadAuthority(ctx context.Context, store *Store, parentThreadID, childThreadID ThreadID) (sessiontree.SubAgentThreadAuthoritySnapshot, error) {
	if store == nil || store.repo == nil {
		return sessiontree.SubAgentThreadAuthoritySnapshot{}, errors.New("runtime store is required")
	}
	inspector, ok := store.repo.(sessiontree.SubAgentThreadAuthorityInspectionRepo)
	if !ok {
		return sessiontree.SubAgentThreadAuthoritySnapshot{}, ErrUnsupportedStoreCapability
	}
	snapshot, err := inspector.InspectSubAgentThreadAuthority(ctx, string(parentThreadID), string(childThreadID))
	return snapshot, runtimeHostError(err)
}

func validateLiveThreadLifecycle(meta sessiontree.ThreadMeta) error {
	lifecycle := meta.Lifecycle
	if lifecycle == "" {
		lifecycle = sessiontree.ThreadLifecycleOpen
	}
	switch lifecycle {
	case sessiontree.ThreadLifecycleOpen:
		return nil
	case sessiontree.ThreadLifecycleClosing:
		return ErrSubAgentClosing
	case sessiontree.ThreadLifecycleClosed:
		return ErrSubAgentClosed
	case sessiontree.ThreadLifecycleDeleted:
		return ErrThreadDeleted
	default:
		return ErrAuthorityCorrupt
	}
}
