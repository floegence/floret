package runtime

import (
	"errors"
	"strings"

	"github.com/floegence/floret/internal/agentharness"
)

// HostBootstrap is the composition-root authority for one opened Store. It has
// no exported methods and must not be retained by runtime services or runs.
// Bootstrap code uses it only to issue the narrow capabilities those owners need.
type HostBootstrap struct {
	store *Store
}

// ThreadCreateHost is the coordinator capability that creates a canonical thread.
type ThreadCreateHost struct {
	store   *Store
	harness *agentharness.AgentHarness
}

// ThreadReadHost reads top-level canonical thread projections without mutation.
type ThreadReadHost struct {
	store   *Store
	harness *agentharness.AgentHarness
}

// SubAgentReadHost reads child lifecycle and detail under one bound parent.
type SubAgentReadHost struct {
	harness        *agentharness.AgentHarness
	parentThreadID ThreadID
}

// SubAgentReadHostFactory issues parent-bound child read capabilities.
type SubAgentReadHostFactory struct {
	store *Store
}

// ThreadTitleHost is the coordinator capability that writes a canonical title.
type ThreadTitleHost struct {
	store   *Store
	harness *agentharness.AgentHarness
}

// ThreadForkHost is the coordinator capability that forks a canonical thread.
type ThreadForkHost struct {
	store   *Store
	harness *agentharness.AgentHarness
}

// ThreadDeleteHost is the coordinator capability that deletes a canonical thread tree.
type ThreadDeleteHost struct {
	store *Store
}

// SubAgentMaintenanceHost closes unfinished child threads for one bound parent.
type SubAgentMaintenanceHost struct {
	harness        *agentharness.AgentHarness
	parentThreadID ThreadID
}

// SubAgentMaintenanceHostFactory issues parent-bound child maintenance capabilities.
type SubAgentMaintenanceHostFactory struct {
	store *Store
}

// PendingToolSettlementHost settles host-owned pending tool work for one bound
// thread or parent. It may share an active provider harness or use a
// provider-free recovery harness.
type PendingToolSettlementHost struct {
	store          *Store
	harness        *agentharness.AgentHarness
	threadID       ThreadID
	parentThreadID ThreadID
	mode           pendingToolSettlementMode
}

// PendingToolRecoveryHostFactory issues provider-free recovery settlement
// capabilities. Active settlement is derived from an active turn or SubAgent host.
type PendingToolRecoveryHostFactory struct {
	store *Store
}

type pendingToolSettlementMode uint8

const (
	pendingToolSettlementRecovery pendingToolSettlementMode = iota
	pendingToolSettlementActive
)

// PendingToolRecoveryHostOptions binds a recovery settlement coordinator to
// exactly one root thread or SubAgent parent.
type PendingToolRecoveryHostOptions struct {
	ThreadID       ThreadID
	ParentThreadID ThreadID
	Sink           EventSink
}

// SubAgentMaintenanceHostOptions binds bulk child closure to one parent.
type SubAgentMaintenanceHostOptions struct {
	ParentThreadID ThreadID
	Sink           EventSink
}

// SubAgentReadHostOptions binds child reads to one parent.
type SubAgentReadHostOptions struct {
	ParentThreadID ThreadID
	Sink           EventSink
}

// NewHostBootstrap converts an opened Store into an opaque composition-root authority.
func NewHostBootstrap(store *Store) (*HostBootstrap, error) {
	if err := validateCapabilityStore(store); err != nil {
		return nil, err
	}
	return &HostBootstrap{store: store}, nil
}

func NewThreadCreateHost(bootstrap *HostBootstrap, sink EventSink) (*ThreadCreateHost, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &ThreadCreateHost{store: store, harness: harness}, nil
}

func NewThreadReadHost(bootstrap *HostBootstrap, sink EventSink) (*ThreadReadHost, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &ThreadReadHost{store: store, harness: harness}, nil
}

// NewSubAgentReadHostFactory constructs the authority that issues bound child reads.
func NewSubAgentReadHostFactory(bootstrap *HostBootstrap) (*SubAgentReadHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &SubAgentReadHostFactory{store: store}, nil
}

// NewHost constructs child reads for one parent.
func (f *SubAgentReadHostFactory) NewHost(opts SubAgentReadHostOptions) (*SubAgentReadHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("subagent read host factory is required")
	}
	parentThreadID, err := normalizeBoundThreadID(opts.ParentThreadID, "subagent read host")
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(f.store, opts.Sink)
	if err != nil {
		return nil, err
	}
	return &SubAgentReadHost{harness: harness, parentThreadID: parentThreadID}, nil
}

func NewThreadTitleHost(bootstrap *HostBootstrap, sink EventSink) (*ThreadTitleHost, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &ThreadTitleHost{store: store, harness: harness}, nil
}

func NewThreadForkHost(bootstrap *HostBootstrap, sink EventSink) (*ThreadForkHost, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(store, sink)
	if err != nil {
		return nil, err
	}
	return &ThreadForkHost{store: store, harness: harness}, nil
}

func NewThreadDeleteHost(bootstrap *HostBootstrap) (*ThreadDeleteHost, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &ThreadDeleteHost{store: store}, nil
}

// NewSubAgentMaintenanceHostFactory constructs the authority that issues bound child maintenance.
func NewSubAgentMaintenanceHostFactory(bootstrap *HostBootstrap) (*SubAgentMaintenanceHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &SubAgentMaintenanceHostFactory{store: store}, nil
}

// NewHost constructs bulk child maintenance for one parent.
func (f *SubAgentMaintenanceHostFactory) NewHost(opts SubAgentMaintenanceHostOptions) (*SubAgentMaintenanceHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("subagent maintenance host factory is required")
	}
	parentThreadID, err := normalizeBoundThreadID(opts.ParentThreadID, "subagent maintenance host")
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(f.store, opts.Sink)
	if err != nil {
		return nil, err
	}
	return &SubAgentMaintenanceHost{harness: harness, parentThreadID: parentThreadID}, nil
}

// NewPendingToolRecoveryHostFactory constructs provider-free recovery authority.
func NewPendingToolRecoveryHostFactory(bootstrap *HostBootstrap) (*PendingToolRecoveryHostFactory, error) {
	store, err := capabilityStore(bootstrap)
	if err != nil {
		return nil, err
	}
	return &PendingToolRecoveryHostFactory{store: store}, nil
}

// NewHost constructs a provider-free, bound recovery settlement capability.
func (f *PendingToolRecoveryHostFactory) NewHost(opts PendingToolRecoveryHostOptions) (*PendingToolSettlementHost, error) {
	if f == nil || f.store == nil {
		return nil, errors.New("pending tool recovery host factory is required")
	}
	threadID, parentThreadID, err := normalizeSettlementAuthority(opts.ThreadID, opts.ParentThreadID)
	if err != nil {
		return nil, err
	}
	harness, err := newCapabilityHarness(f.store, opts.Sink)
	if err != nil {
		return nil, err
	}
	return &PendingToolSettlementHost{
		store:          f.store,
		harness:        harness,
		threadID:       threadID,
		parentThreadID: parentThreadID,
		mode:           pendingToolSettlementRecovery,
	}, nil
}

// NewTurnPendingToolSettlementHost derives the sole settlement capability that
// shares an active turn execution harness.
func NewTurnPendingToolSettlementHost(host *TurnExecutionHost) (*PendingToolSettlementHost, error) {
	if host == nil || host.host == nil || host.host.harness == nil {
		return nil, errors.New("turn execution host is required")
	}
	return &PendingToolSettlementHost{store: host.host.store, harness: host.host.harness, threadID: host.threadID, mode: pendingToolSettlementActive}, nil
}

// NewSubAgentPendingToolSettlementHost derives the sole settlement capability
// that shares a parent-bound SubAgent harness.
func NewSubAgentPendingToolSettlementHost(host *SubAgentHost) (*PendingToolSettlementHost, error) {
	if host == nil || host.host == nil || host.host.harness == nil {
		return nil, errors.New("subagent host is required")
	}
	return &PendingToolSettlementHost{store: host.host.store, harness: host.host.harness, parentThreadID: host.parentThreadID, mode: pendingToolSettlementActive}, nil
}

func normalizeSettlementAuthority(threadID, parentThreadID ThreadID) (ThreadID, ThreadID, error) {
	threadID = ThreadID(strings.TrimSpace(string(threadID)))
	parentThreadID = ThreadID(strings.TrimSpace(string(parentThreadID)))
	if (threadID == "") == (parentThreadID == "") {
		return "", "", errors.New("pending tool settlement host requires exactly one thread or parent thread id")
	}
	return threadID, parentThreadID, nil
}

func validateCapabilityStore(store *Store) error {
	if store == nil {
		return errors.New("thread capability store is required")
	}
	return store.validate()
}

func capabilityStore(bootstrap *HostBootstrap) (*Store, error) {
	if bootstrap == nil {
		return nil, errors.New("host bootstrap is required")
	}
	if err := validateCapabilityStore(bootstrap.store); err != nil {
		return nil, err
	}
	return bootstrap.store, nil
}

func newCapabilityHarness(store *Store, sink EventSink) (*agentharness.AgentHarness, error) {
	if err := validateCapabilityStore(store); err != nil {
		return nil, err
	}
	return agentharness.New(agentharness.Options{
		Repo:           store.repo,
		ForkOperations: store.forkOperations,
		PromptStore:    store.prompt,
		Artifacts:      store.artifacts,
		Sink:           newRuntimeEventSink(sink),
		SinkPolicy:     runtimeHarnessSinkPolicy(),
	}), nil
}
