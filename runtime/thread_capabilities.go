package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/sessiontree"
)

// HostBootstrap is an active, one-time composition scope for one opened Store.
// ConfigureHostCapabilities seals it before returning to the caller.
type HostBootstrap struct {
	state *hostBootstrapState
}

type hostBootstrapState struct {
	mu     sync.Mutex
	store  *Store
	lease  *capabilityLease
	active bool
}

type capabilityLease struct {
	mu     sync.RWMutex
	active bool
}

// ThreadCreateHostBinder issues only canonical root-thread create handles.
type ThreadCreateHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadReadHostBinder issues only root-thread read handles.
type ThreadReadHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadTitleHostBinder issues only root-thread title handles.
type ThreadTitleHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadForkHostBinder issues only root-thread fork handles.
type ThreadForkHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadDeleteHostBinder issues only root-thread delete handles.
type ThreadDeleteHostBinder struct {
	store *Store
	lease *capabilityLease
}

// SubAgentReadHostBinder issues only parent-bound child read handles.
type SubAgentReadHostBinder struct {
	store *Store
	lease *capabilityLease
}

// PendingToolRecoveryHostBinder issues only provider-free recovery settlement handles.
type PendingToolRecoveryHostBinder struct {
	store *Store
	lease *capabilityLease
}

// InterruptedTurnRecoveryHostBinder issues only exact interrupted-turn recovery handles.
type InterruptedTurnRecoveryHostBinder struct {
	store *Store
	lease *capabilityLease
}

// ThreadCreateHost is the coordinator capability that creates a canonical thread.
type ThreadCreateHost struct {
	store          *Store
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	createIntentID CreateIntentID
}

// ThreadReadHost reads one bound top-level canonical thread without mutation.
type ThreadReadHost struct {
	store    *Store
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// SubAgentReadHost reads child lifecycle and detail under one bound parent.
type SubAgentReadHost struct {
	store          *Store
	harness        *agentharness.AgentHarness
	parentThreadID ThreadID
}

// ThreadTitleHost writes the canonical title for one bound root thread.
type ThreadTitleHost struct {
	store    *Store
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// ThreadForkHost forks one bound canonical root thread.
type ThreadForkHost struct {
	store    *Store
	harness  *agentharness.AgentHarness
	threadID ThreadID
}

// ThreadDeleteHost deletes one bound canonical root thread tree.
type ThreadDeleteHost struct {
	store    *Store
	threadID ThreadID
}

// PendingToolRecoveryHost settles host-owned pending tool work when no active
// provider owner exists for the bound thread or parent.
type PendingToolRecoveryHost struct {
	store          *Store
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	parentThreadID ThreadID
}

// InterruptedTurnRecoveryHost finalizes one exact expired turn authority proof.
type InterruptedTurnRecoveryHost struct {
	store          *Store
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	parentThreadID ThreadID
	expectedLease  sessiontree.TurnLease
}

// ConfigureHostCapabilities exposes one short-lived bootstrap scope. The Store
// rejects a second configuration attempt. Callers may retain only narrow binders
// created during configure; those binders become active after configure succeeds.
func ConfigureHostCapabilities(store *Store, configure func(*HostBootstrap) error) (err error) {
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
	bootstrap := &HostBootstrap{state: state}
	completed := false
	defer func() {
		state.seal(completed && err == nil)
	}()
	err = configure(bootstrap)
	completed = true
	return err
}

// NewThreadCreateHostBinder constructs the canonical root-thread create issuer.
func NewThreadCreateHostBinder(bootstrap *HostBootstrap) (*ThreadCreateHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadCreateHostBinder{store: store, lease: lease}, nil
}

// Bind constructs canonical root-create authority for one exact identity and
// durable create intent before it is delivered to a coordinator.
func (b *ThreadCreateHostBinder) Bind(threadID ThreadID, createIntentID CreateIntentID) (*ThreadCreateHost, error) {
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
	return &ThreadCreateHost{store: b.store, harness: harness, threadID: threadID, createIntentID: createIntentID}, nil
}

// NewThreadReadHostBinder constructs the root-thread read issuer.
func NewThreadReadHostBinder(bootstrap *HostBootstrap) (*ThreadReadHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadReadHostBinder{store: store, lease: lease}, nil
}

// NewHost constructs read authority for exactly one root thread.
func (b *ThreadReadHostBinder) NewHost(ctx context.Context, threadID ThreadID) (*ThreadReadHost, error) {
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
	return &ThreadReadHost{store: store, harness: harness, threadID: threadID}, nil
}

// NewSubAgentReadHostBinder constructs the parent-bound child read issuer.
func NewSubAgentReadHostBinder(bootstrap *HostBootstrap) (*SubAgentReadHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &SubAgentReadHostBinder{store: store, lease: lease}, nil
}

// NewHost constructs child reads for exactly one parent.
func (b *SubAgentReadHostBinder) NewHost(ctx context.Context, parentThreadID ThreadID) (*SubAgentReadHost, error) {
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
	return &SubAgentReadHost{store: b.store, harness: harness, parentThreadID: parentThreadID}, nil
}

// NewThreadTitleHostBinder constructs the root-thread title issuer.
func NewThreadTitleHostBinder(bootstrap *HostBootstrap) (*ThreadTitleHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadTitleHostBinder{store: store, lease: lease}, nil
}

// NewHost constructs title authority for exactly one root thread.
func (b *ThreadTitleHostBinder) NewHost(ctx context.Context, threadID ThreadID, sink EventSink) (*ThreadTitleHost, error) {
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
	return &ThreadTitleHost{store: store, harness: harness, threadID: threadID}, nil
}

// NewThreadForkHostBinder constructs the root-thread fork issuer.
func NewThreadForkHostBinder(bootstrap *HostBootstrap) (*ThreadForkHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadForkHostBinder{store: store, lease: lease}, nil
}

// NewHost constructs fork authority for exactly one source root thread.
func (b *ThreadForkHostBinder) NewHost(ctx context.Context, threadID ThreadID, sink EventSink) (*ThreadForkHost, error) {
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
	return &ThreadForkHost{store: store, harness: harness, threadID: threadID}, nil
}

// NewThreadDeleteHostBinder constructs the root-thread delete issuer.
func NewThreadDeleteHostBinder(bootstrap *HostBootstrap) (*ThreadDeleteHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadDeleteHostBinder{store: store, lease: lease}, nil
}

// NewHost constructs delete authority for exactly one root thread.
func (b *ThreadDeleteHostBinder) NewHost(ctx context.Context, threadID ThreadID) (*ThreadDeleteHost, error) {
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
	return &ThreadDeleteHost{store: store, threadID: threadID}, nil
}

// NewPendingToolRecoveryHostBinder constructs the recovery settlement issuer.
func NewPendingToolRecoveryHostBinder(bootstrap *HostBootstrap) (*PendingToolRecoveryHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &PendingToolRecoveryHostBinder{store: store, lease: lease}, nil
}

// NewInterruptedTurnRecoveryHostBinder constructs the interrupted-turn recovery issuer.
func NewInterruptedTurnRecoveryHostBinder(bootstrap *HostBootstrap) (*InterruptedTurnRecoveryHostBinder, error) {
	store, lease, err := capabilityScope(bootstrap)
	if err != nil {
		return nil, err
	}
	return &InterruptedTurnRecoveryHostBinder{store: store, lease: lease}, nil
}

// NewThreadHost constructs recovery settlement authority for one root thread.
func (b *PendingToolRecoveryHostBinder) NewThreadHost(ctx context.Context, threadID ThreadID, sink EventSink) (*PendingToolRecoveryHost, error) {
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
	return &PendingToolRecoveryHost{
		store:    b.store,
		harness:  harness,
		threadID: threadID,
	}, nil
}

// NewSubAgentHost constructs recovery settlement authority for one SubAgent parent.
func (b *PendingToolRecoveryHostBinder) NewSubAgentHost(ctx context.Context, parentThreadID ThreadID, sink EventSink) (*PendingToolRecoveryHost, error) {
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
	return &PendingToolRecoveryHost{
		store:          b.store,
		harness:        harness,
		parentThreadID: parentThreadID,
	}, nil
}

// NewThreadHost binds recovery to the exact current turn proof of one root thread.
func (b *InterruptedTurnRecoveryHostBinder) NewThreadHost(ctx context.Context, threadID ThreadID, sink EventSink) (*InterruptedTurnRecoveryHost, error) {
	return b.newHost(ctx, "", threadID, sink)
}

// NewSubAgentHost binds recovery to the exact current turn proof of one child under one parent.
func (b *InterruptedTurnRecoveryHostBinder) NewSubAgentHost(ctx context.Context, parentThreadID, childThreadID ThreadID, sink EventSink) (*InterruptedTurnRecoveryHost, error) {
	parentThreadID, err := normalizeBoundThreadID(parentThreadID, "interrupted turn recovery parent")
	if err != nil {
		return nil, err
	}
	return b.newHost(ctx, parentThreadID, childThreadID, sink)
}

func (b *InterruptedTurnRecoveryHostBinder) newHost(ctx context.Context, parentThreadID, threadID ThreadID, sink EventSink) (*InterruptedTurnRecoveryHost, error) {
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
	if strings.TrimSpace(snapshot.Thread.ParentThreadID) != strings.TrimSpace(string(parentThreadID)) {
		return nil, runtimeHostError(sessiontree.ErrInvalidThreadAuthority)
	}
	if err := validateLiveThreadLifecycle(snapshot.Thread); err != nil {
		return nil, err
	}
	if snapshot.Lease == nil || snapshot.Lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return nil, ErrTurnNotFound
	}
	if snapshot.ClaimOperationID != "" {
		return nil, ErrAuthorityCorrupt
	}
	harness, err := newCapabilityHarness(b.store, sink)
	if err != nil {
		return nil, err
	}
	return &InterruptedTurnRecoveryHost{
		store: b.store, harness: harness, threadID: threadID, parentThreadID: parentThreadID, expectedLease: *snapshot.Lease,
	}, nil
}

func validateCapabilityStore(store *Store) error {
	if store == nil {
		return errors.New("thread capability store is required")
	}
	return store.validate()
}

func beginHostOperation(store *Store) (func(), error) {
	if store == nil {
		return nil, errors.New("thread capability store is required")
	}
	return store.beginOperation()
}

func beginHostOperationContext(store *Store, ctx context.Context) (context.Context, func(), error) {
	if store == nil {
		return nil, nil, errors.New("thread capability store is required")
	}
	return store.beginOperationContext(ctx)
}

func capabilityScope(bootstrap *HostBootstrap) (*Store, *capabilityLease, error) {
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

func validateCapabilityBinder(store *Store, lease *capabilityLease, name string) error {
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

func newCapabilityHarness(store *Store, sink EventSink) (*agentharness.AgentHarness, error) {
	if err := validateCapabilityStore(store); err != nil {
		return nil, err
	}
	return agentharness.New(agentharness.Options{
		Repo:           store.repo,
		ForkOperations: store.forkOperations,
		PromptStore:    store.prompt,
		Sink:           newRuntimeEventSink(sink),
		SinkPolicy:     runtimeHarnessSinkPolicy(),
	}), nil
}
