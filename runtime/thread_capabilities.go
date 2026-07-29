package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/floegence/floret/v2/internal/agentharness"
	"github.com/floegence/floret/v2/internal/sessiontree"
)

// hostBootstrap is an active, one-time composition scope for one opened store.
// configureHostCapabilities seals it before returning to the caller.
type hostBootstrap struct {
	state *hostBootstrapState
}

type hostBootstrapState struct {
	mu     sync.Mutex
	store  *runtimeStore
	lease  *capabilityLease
	active bool
}

type capabilityLease struct {
	mu     sync.RWMutex
	active bool
}

// threadCreateBinder issues only canonical root-thread create handles.
type threadCreateBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// threadReadBinder issues only root-thread read handles.
type threadReadBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// threadInventoryCapability lists canonical root threads for composition and
// maintenance coordinators. It is not reachable from a normal run.
type threadInventoryCapability struct {
	store   *runtimeStore
	harness *agentharness.AgentHarness
	lease   *capabilityLease
}

// threadTitleBinder issues only root-thread title handles.
type threadTitleBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// threadForkBinder issues only root-thread fork handles.
type threadForkBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// threadDeleteBinder issues only root-thread delete handles.
type threadDeleteBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// subAgentReadBinder issues only parent-bound child read handles.
type subAgentReadBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// pendingToolRecoveryBinder issues only provider-free recovery settlement handles.
type pendingToolRecoveryBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// interruptedTurnRecoveryBinder issues only exact interrupted-turn recovery factories.
type interruptedTurnRecoveryBinder struct {
	store *runtimeStore
	lease *capabilityLease
}

// threadCreateCapability is the coordinator capability that creates a canonical thread.
type threadCreateCapability struct {
	store          *runtimeStore
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	createIntentID CreateIntentID
}

// threadReadCapability reads one bound top-level canonical thread without mutation.
type threadReadCapability struct {
	store    *runtimeStore
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// subAgentReadCapability reads child lifecycle and detail under one bound parent.
type subAgentReadCapability struct {
	store          *runtimeStore
	harness        *agentharness.AgentHarness
	parentThreadID ThreadID
}

// threadTitleCapability writes the canonical title for one bound root thread.
type threadTitleCapability struct {
	store    *runtimeStore
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// threadForkCapability forks one bound canonical root thread.
type threadForkCapability struct {
	store    *runtimeStore
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// threadDeleteCapability deletes one bound canonical root thread tree.
type threadDeleteCapability struct {
	store    *runtimeStore
	threadID ThreadID
}

// pendingToolRecoveryCapability settles host-owned pending tool work when no active
// provider owner exists for the bound thread or parent.
type pendingToolRecoveryCapability struct {
	store          *runtimeStore
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	parentThreadID ThreadID
}

// interruptedTurnRecoveryFactory refreshes recovery authority for one exact
// root or parent-child turn owner and generation.
type interruptedTurnRecoveryFactory struct {
	state *interruptedTurnRecoveryFactoryState
}

type interruptedTurnRecoveryFactoryState struct {
	mu             sync.Mutex
	store          *runtimeStore
	threadID       ThreadID
	parentThreadID ThreadID
	latestLease    sessiontree.TurnLease
	resolved       bool
}

// interruptedTurnRecoveryCapability finalizes one exact expired turn authority proof.
type interruptedTurnRecoveryCapability struct {
	store          *runtimeStore
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	parentThreadID ThreadID
	expectedLease  sessiontree.TurnLease
	factoryState   *interruptedTurnRecoveryFactoryState
}

// configureHostCapabilities exposes one short-lived bootstrap scope. The store
// rejects a second configuration attempt. Callers may retain only narrow binders
// created during configure; those binders become active after configure succeeds.
func configureHostCapabilities(store *runtimeStore, configure func(*hostBootstrap) error) (err error) {
	if err := validateCapabilityStore(store); err != nil {
		return err
	}
	done, err := beginHostOperation(store)
	if err != nil {
		return err
	}
	defer done()
	if configure == nil {
		return errors.New("host capability configure callback is required")
	}
	store.bootstrapMu.Lock()
	if store.bootstrapIssued {
		store.bootstrapMu.Unlock()
		return errors.New("host capabilities already configured for store")
	}
	store.bootstrapIssued = true
	store.bootstrapMu.Unlock()

	state := &hostBootstrapState{store: store, lease: &capabilityLease{}, active: true}
	bootstrap := &hostBootstrap{state: state}
	completed := false
	defer func() {
		state.seal(completed && err == nil)
	}()
	err = configure(bootstrap)
	completed = true
	return err
}

// newThreadCreateBinder constructs the canonical root-thread create issuer.
func newThreadCreateBinder(bootstrap *hostBootstrap) (*threadCreateBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &threadCreateBinder{store: store, lease: lease}, nil
}

// Bind constructs canonical root-create authority for one exact identity and
// durable create intent before it is delivered to a coordinator.
func (b *threadCreateBinder) Bind(threadID ThreadID, createIntentID CreateIntentID) (*threadCreateCapability, error) {
	if b == nil {
		return nil, errors.New("thread create host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread create host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	threadID, err = normalizeBoundThreadID(threadID, "thread create host")
	if err != nil {
		return nil, err
	}
	createIntentID = CreateIntentID(strings.TrimSpace(string(createIntentID)))
	if createIntentID == "" {
		return nil, errors.New("thread create host requires create intent id")
	}
	harness, err := newCapabilityHarness(b.store, nil)
	if err != nil {
		return nil, err
	}
	return &threadCreateCapability{store: b.store, harness: harness, threadID: threadID, createIntentID: createIntentID}, nil
}

// newThreadReadBinder constructs the root-thread read issuer.
func newThreadReadBinder(bootstrap *hostBootstrap) (*threadReadBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &threadReadBinder{store: store, lease: lease}, nil
}

// newThreadInventoryCapability constructs the store-wide canonical root inventory
// capability for a composition owner.
func newThreadInventoryCapability(bootstrap *hostBootstrap) (*threadInventoryCapability, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, nil)
	if err != nil {
		return nil, err
	}
	return &threadInventoryCapability{store: store, harness: harness, lease: lease}, nil
}

// NewHost constructs read authority for exactly one root thread.
func (b *threadReadBinder) NewHost(ctx context.Context, threadID ThreadID) (*threadReadCapability, error) {
	if b == nil {
		return nil, errors.New("thread read host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread read host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	store := b.store
	threadID, err = normalizeBoundThreadID(threadID, "thread read host")
	if err != nil {
		return nil, err
	}
	if err := validateRootBoundAuthority(ctx, store, threadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, nil)
	if err != nil {
		return nil, err
	}
	return &threadReadCapability{store: store, harness: harness, threadID: threadID}, nil
}

// newSubAgentReadBinder constructs the parent-bound child read issuer.
func newSubAgentReadBinder(bootstrap *hostBootstrap) (*subAgentReadBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &subAgentReadBinder{store: store, lease: lease}, nil
}

// NewHost constructs child reads for exactly one parent.
func (b *subAgentReadBinder) NewHost(ctx context.Context, parentThreadID ThreadID) (*subAgentReadCapability, error) {
	if b == nil {
		return nil, errors.New("subagent read host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "subagent read host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	parentThreadID, err = normalizeBoundThreadID(parentThreadID, "subagent read host")
	if err != nil {
		return nil, err
	}
	if err := validateReadableParentBoundAuthority(ctx, b.store, parentThreadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(b.store, nil)
	if err != nil {
		return nil, err
	}
	return &subAgentReadCapability{store: b.store, harness: harness, parentThreadID: parentThreadID}, nil
}

// newThreadTitleBinder constructs the root-thread title issuer.
func newThreadTitleBinder(bootstrap *hostBootstrap) (*threadTitleBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &threadTitleBinder{store: store, lease: lease}, nil
}

// NewHost constructs title authority for exactly one root thread.
func (b *threadTitleBinder) NewHost(ctx context.Context, threadID ThreadID, sink EventSink) (*threadTitleCapability, error) {
	if b == nil {
		return nil, errors.New("thread title host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread title host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	store := b.store
	threadID, err = normalizeBoundThreadID(threadID, "thread title host")
	if err != nil {
		return nil, err
	}
	if err := validateRootBoundAuthority(ctx, store, threadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &threadTitleCapability{store: store, harness: harness, threadID: threadID}, nil
}

// newThreadForkBinder constructs the root-thread fork issuer.
func newThreadForkBinder(bootstrap *hostBootstrap) (*threadForkBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &threadForkBinder{store: store, lease: lease}, nil
}

// NewHost constructs fork authority for exactly one source root thread.
func (b *threadForkBinder) NewHost(ctx context.Context, threadID ThreadID, sink EventSink) (*threadForkCapability, error) {
	if b == nil {
		return nil, errors.New("thread fork host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread fork host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	store := b.store
	threadID, err = normalizeBoundThreadID(threadID, "thread fork host")
	if err != nil {
		return nil, err
	}
	if err := validateRootBoundAuthority(ctx, store, threadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &threadForkCapability{store: store, harness: harness, threadID: threadID}, nil
}

// newThreadDeleteBinder constructs the root-thread delete issuer.
func newThreadDeleteBinder(bootstrap *hostBootstrap) (*threadDeleteBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &threadDeleteBinder{store: store, lease: lease}, nil
}

// NewHost constructs delete authority for exactly one root thread.
func (b *threadDeleteBinder) NewHost(ctx context.Context, threadID ThreadID) (*threadDeleteCapability, error) {
	if b == nil {
		return nil, errors.New("thread delete host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "thread delete host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	store := b.store
	threadID, err = normalizeBoundThreadID(threadID, "thread delete host")
	if err != nil {
		return nil, err
	}
	if err := validateDeleteHostConstructionAuthority(ctx, store, threadID); err != nil {
		return nil, err
	}
	return &threadDeleteCapability{store: store, threadID: threadID}, nil
}

// newPendingToolRecoveryBinder constructs the recovery settlement issuer.
func newPendingToolRecoveryBinder(bootstrap *hostBootstrap) (*pendingToolRecoveryBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &pendingToolRecoveryBinder{store: store, lease: lease}, nil
}

// newInterruptedTurnRecoveryBinder constructs the interrupted-turn recovery issuer.
func newInterruptedTurnRecoveryBinder(bootstrap *hostBootstrap) (*interruptedTurnRecoveryBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &interruptedTurnRecoveryBinder{store: store, lease: lease}, nil
}

// NewThreadHost constructs recovery settlement authority for one root thread.
func (b *pendingToolRecoveryBinder) NewThreadHost(ctx context.Context, threadID ThreadID, sink EventSink) (*pendingToolRecoveryCapability, error) {
	if b == nil {
		return nil, errors.New("pending tool recovery host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "pending tool recovery host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	threadID, err = normalizeBoundThreadID(threadID, "pending tool recovery host")
	if err != nil {
		return nil, err
	}
	if err := validateRootBoundAuthority(ctx, b.store, threadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(b.store, sink)
	if err != nil {
		return nil, err
	}
	return &pendingToolRecoveryCapability{
		store:    b.store,
		harness:  harness,
		threadID: threadID,
	}, nil
}

// NewSubAgentHost constructs recovery settlement authority for one SubAgent parent.
func (b *pendingToolRecoveryBinder) NewSubAgentHost(ctx context.Context, parentThreadID ThreadID, sink EventSink) (*pendingToolRecoveryCapability, error) {
	if b == nil {
		return nil, errors.New("pending tool recovery host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "pending tool recovery host binder"); err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	parentThreadID, err = normalizeBoundThreadID(parentThreadID, "pending tool recovery parent host")
	if err != nil {
		return nil, err
	}
	if err := validateParentBoundAuthority(ctx, b.store, parentThreadID); err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(b.store, sink)
	if err != nil {
		return nil, err
	}
	return &pendingToolRecoveryCapability{
		store:          b.store,
		harness:        harness,
		parentThreadID: parentThreadID,
	}, nil
}

// BindThread binds recovery to the exact current turn owner and generation of one root thread.
func (b *interruptedTurnRecoveryBinder) BindThread(ctx context.Context, threadID ThreadID) (*interruptedTurnRecoveryFactory, error) {
	return b.bindThread(ctx, threadID)
}

// BindSubAgent binds recovery to the exact current turn owner and generation of one child under one parent.
func (b *interruptedTurnRecoveryBinder) BindSubAgent(ctx context.Context, parentThreadID, childThreadID ThreadID) (*interruptedTurnRecoveryFactory, error) {
	if b == nil {
		return nil, errors.New("interrupted turn recovery host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "interrupted turn recovery host binder"); err != nil {
		return nil, err
	}
	parentThreadID, err := normalizeBoundThreadID(parentThreadID, "interrupted turn recovery parent")
	if err != nil {
		return nil, err
	}
	childThreadID, err = normalizeBoundThreadID(childThreadID, "interrupted turn recovery host")
	if err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	scoped, err := inspectSubAgentThreadAuthority(ctx, b.store, parentThreadID, childThreadID)
	if err != nil {
		return nil, err
	}
	if err := validateLiveThreadLifecycle(scoped.Parent); err != nil {
		return nil, err
	}
	return newInterruptedTurnRecoveryFactory(b.store, scoped.Child, parentThreadID)
}

func (b *interruptedTurnRecoveryBinder) bindThread(ctx context.Context, threadID ThreadID) (*interruptedTurnRecoveryFactory, error) {
	if b == nil {
		return nil, errors.New("interrupted turn recovery host binder is required")
	}
	if err := validateCapabilityBinder(b.store, b.lease, "interrupted turn recovery host binder"); err != nil {
		return nil, err
	}
	threadID, err := normalizeBoundThreadID(threadID, "interrupted turn recovery host")
	if err != nil {
		return nil, err
	}
	done, err := beginHostOperation(b.store)
	if err != nil {
		return nil, err
	}
	defer done()
	snapshot, err := inspectThreadAuthority(ctx, b.store, threadID)
	if err != nil {
		return nil, err
	}
	if err := validateInterruptedRecoveryIdentity(snapshot.Thread, ""); err != nil {
		return nil, err
	}
	return newInterruptedTurnRecoveryFactory(b.store, snapshot, "")
}

func newInterruptedTurnRecoveryFactory(store *runtimeStore, snapshot sessiontree.ThreadAuthoritySnapshot, parentThreadID ThreadID) (*interruptedTurnRecoveryFactory, error) {
	if store == nil || store.repo == nil {
		return nil, errors.New("runtime store is required")
	}
	if _, ok := store.repo.(sessiontree.InterruptedTurnResolutionValidationRepo); !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	if snapshot.ClaimOperationID != "" {
		return nil, ErrInterruptedTurnNotFound
	}
	if err := validateInterruptedRecoveryLifecycle(snapshot.Thread, parentThreadID); err != nil {
		return nil, err
	}
	if snapshot.Lease == nil || snapshot.Lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return nil, ErrInterruptedTurnNotFound
	}
	return &interruptedTurnRecoveryFactory{
		state: &interruptedTurnRecoveryFactoryState{
			store: store, threadID: ThreadID(snapshot.Thread.ID), parentThreadID: parentThreadID, latestLease: *snapshot.Lease,
		},
	}, nil
}

// NewHost binds one recovery attempt to the current complete proof for the factory's exact target.
func (f *interruptedTurnRecoveryFactory) NewHost(ctx context.Context, sink EventSink) (*interruptedTurnRecoveryCapability, error) {
	if f == nil || f.state == nil || f.state.store == nil || f.state.threadID == "" {
		return nil, errors.New("interrupted turn recovery host factory is required")
	}
	state := f.state
	done, err := beginHostOperation(state.store)
	if err != nil {
		return nil, err
	}
	defer done()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.resolved {
		return nil, ErrRecoveryTargetResolved
	}
	snapshot, err := inspectInterruptedRecoveryFactoryAuthority(ctx, state)
	if err != nil {
		return nil, err
	}
	if snapshot.Lease == nil {
		if snapshot.LeaseGeneration < state.latestLease.Generation {
			return nil, ErrAuthorityCorrupt
		}
		if err := validateInterruptedTurnResolution(ctx, state); err != nil {
			return nil, err
		}
		state.resolved = true
		return nil, ErrRecoveryTargetResolved
	}
	current := *snapshot.Lease
	if err := sessiontree.ValidateInterruptedTurnLeaseSuccessor(state.latestLease, current); err != nil {
		return nil, runtimeHostError(err)
	}
	if current.Generation > state.latestLease.Generation {
		if err := validateInterruptedTurnResolution(ctx, state); err != nil {
			return nil, err
		}
		state.resolved = true
		return nil, ErrRecoveryTargetResolved
	}
	if strings.TrimSpace(snapshot.ClaimOperationID) != "" {
		return nil, ErrAuthorityCorrupt
	}
	if err := validateInterruptedRecoveryLifecycle(snapshot.Thread, state.parentThreadID); err != nil {
		return nil, err
	}
	state.latestLease = current
	harness, err := newCapabilityHarness(state.store, sink)
	if err != nil {
		return nil, err
	}
	return &interruptedTurnRecoveryCapability{
		store: state.store, harness: harness, threadID: state.threadID, parentThreadID: state.parentThreadID, expectedLease: current,
		factoryState: state,
	}, nil
}

func validateInterruptedTurnResolution(ctx context.Context, state *interruptedTurnRecoveryFactoryState) error {
	validator, ok := state.store.repo.(sessiontree.InterruptedTurnResolutionValidationRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	err := validator.ValidateInterruptedTurnResolution(ctx, sessiontree.RecoverInterruptedTurnRequest{
		ExpectedLease: state.latestLease, ParentThreadID: string(state.parentThreadID),
	})
	if errors.Is(err, sessiontree.ErrRequestConflict) {
		return ErrAuthorityCorrupt
	}
	return runtimeHostError(err)
}

func inspectInterruptedRecoveryFactoryAuthority(ctx context.Context, state *interruptedTurnRecoveryFactoryState) (sessiontree.ThreadAuthoritySnapshot, error) {
	if strings.TrimSpace(string(state.parentThreadID)) == "" {
		snapshot, err := inspectThreadAuthority(ctx, state.store, state.threadID)
		if err != nil {
			return sessiontree.ThreadAuthoritySnapshot{}, err
		}
		if err := validateInterruptedRecoveryIdentity(snapshot.Thread, ""); err != nil {
			return sessiontree.ThreadAuthoritySnapshot{}, err
		}
		return snapshot, nil
	}
	scoped, err := inspectSubAgentThreadAuthority(ctx, state.store, state.parentThreadID, state.threadID)
	if err != nil {
		return sessiontree.ThreadAuthoritySnapshot{}, err
	}
	if err := validateLiveThreadLifecycle(scoped.Parent); err != nil {
		return sessiontree.ThreadAuthoritySnapshot{}, err
	}
	return scoped.Child, nil
}

func validateInterruptedRecoveryIdentity(meta sessiontree.ThreadMeta, parentThreadID ThreadID) error {
	actualParent := strings.TrimSpace(meta.ParentThreadID)
	expectedParent := strings.TrimSpace(string(parentThreadID))
	if expectedParent == "" {
		if actualParent != "" {
			return fmt.Errorf("%w: %s", ErrSubAgentParentRequired, meta.ID)
		}
		return nil
	}
	if actualParent != expectedParent {
		return runtimeHostError(sessiontree.ErrInvalidThreadAuthority)
	}
	return nil
}

func validateInterruptedRecoveryLifecycle(meta sessiontree.ThreadMeta, parentThreadID ThreadID) error {
	if strings.TrimSpace(string(parentThreadID)) == "" {
		return validateLiveThreadLifecycle(meta)
	}
	switch meta.Lifecycle {
	case "", sessiontree.ThreadLifecycleOpen, sessiontree.ThreadLifecycleClosing:
		return nil
	case sessiontree.ThreadLifecycleClosed:
		return ErrSubAgentClosed
	case sessiontree.ThreadLifecycleDeleted:
		return ErrThreadDeleted
	default:
		return ErrAuthorityCorrupt
	}
}

func validateCapabilityStore(store *runtimeStore) error {
	if store == nil {
		return errors.New("thread capability store is required")
	}
	return store.validate()
}

func beginHostOperation(store *runtimeStore) (func(), error) {
	if store == nil {
		return nil, errors.New("thread capability store is required")
	}
	return store.beginOperation()
}

func beginHostOperationContext(store *runtimeStore, ctx context.Context) (context.Context, func(), error) {
	if store == nil {
		return nil, nil, errors.New("thread capability store is required")
	}
	return store.beginOperationContext(ctx)
}

func capabilityScope(bootstrap *hostBootstrap) (*runtimeStore, *capabilityLease, error) {
	if bootstrap == nil || bootstrap.state == nil {
		return nil, nil, errors.New("host bootstrap is required")
	}
	state := bootstrap.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.store == nil || state.lease == nil {
		return nil, nil, errors.New("host bootstrap is no longer active")
	}
	if err := validateCapabilityStore(state.store); err != nil {
		return nil, nil, err
	}
	return state.store, state.lease, nil
}

func (state *hostBootstrapState) seal(publish bool) {
	if state == nil {
		return
	}
	state.mu.Lock()
	state.active = false
	state.store = nil
	lease := state.lease
	state.mu.Unlock()
	lease.setActive(publish)
}

func validateCapabilityBinder(store *runtimeStore, lease *capabilityLease, name string) error {
	if store == nil || lease == nil {
		return fmt.Errorf("%s is required", name)
	}
	if !lease.isActive() {
		return fmt.Errorf("%s is not active", name)
	}
	return validateCapabilityStore(store)
}

func (lease *capabilityLease) setActive(active bool) {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	lease.active = active
	lease.mu.Unlock()
}

func (lease *capabilityLease) isActive() bool {
	if lease == nil {
		return false
	}
	lease.mu.RLock()
	defer lease.mu.RUnlock()
	return lease.active
}

func newCapabilityHarness(store *runtimeStore, sink EventSink) (*agentharness.AgentHarness, error) {
	if err := validateCapabilityStore(store); err != nil {
		return nil, err
	}
	return agentharness.New(agentharness.Options{
		Repo:           store.repo,
		ForkOperations: store.forkOperations,
		PromptStore:    store.prompt,
		Sink:           newRuntimeEventSink(sink),
		SinkPolicy:     runtimeHarnessSinkPolicy(),
		TurnExecutions: store.turnExecutionRegistry(),
	}), nil
}
