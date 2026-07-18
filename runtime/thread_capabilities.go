package runtime

import (
	"errors"

	"github.com/floegence/floret/internal/agentharness"
)

// HostRuntime is the opaque durable runtime capability accepted by NewHost and
// capability constructors. It has no exported methods; bootstrap code should
// use it only to construct the selected narrow handles and must not pass it to
// long-lived coordinators.
type HostRuntime struct {
	store *Store
}

// ThreadCreateHost is the coordinator capability that creates a canonical thread.
type ThreadCreateHost struct {
	harness *agentharness.AgentHarness
}

// ThreadReadHost reads canonical thread and SubAgent projections without mutation.
type ThreadReadHost struct {
	store   *Store
	harness *agentharness.AgentHarness
}

// ThreadTitleHost is the coordinator capability that writes a canonical title.
type ThreadTitleHost struct {
	harness *agentharness.AgentHarness
}

// ThreadForkHost is the coordinator capability that forks a canonical thread.
type ThreadForkHost struct {
	harness *agentharness.AgentHarness
}

// ThreadDeleteHost is the coordinator capability that deletes a canonical thread tree.
type ThreadDeleteHost struct {
	store *Store
}

// SubAgentMaintenanceHost closes unfinished child threads as one maintenance operation.
type SubAgentMaintenanceHost struct {
	harness *agentharness.AgentHarness
}

// PendingToolSettlementHost is the provider-free capability for settling
// host-owned pending tool work. It is not a general maintenance facade; the
// host must bind one instance to the coordinator that owns that settlement.
type PendingToolSettlementHost struct {
	harness *agentharness.AgentHarness
}

// ThreadCapabilityOptions configures one narrow capability from an opaque runtime.
// Callers should construct these handles at bootstrap and pass only the selected
// handle to the coordinator that owns the corresponding lifecycle transition.
type ThreadCapabilityOptions struct {
	Runtime *HostRuntime
	Sink    EventSink
}

func NewHostRuntime(store *Store) (*HostRuntime, error) {
	if err := validateCapabilityStore(store); err != nil {
		return nil, err
	}
	return &HostRuntime{store: store}, nil
}

func NewThreadCreateHost(opts ThreadCapabilityOptions) (*ThreadCreateHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &ThreadCreateHost{harness: harness}, nil
}

func NewThreadReadHost(opts ThreadCapabilityOptions) (*ThreadReadHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &ThreadReadHost{store: opts.Runtime.store, harness: harness}, nil
}

func NewThreadTitleHost(opts ThreadCapabilityOptions) (*ThreadTitleHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &ThreadTitleHost{harness: harness}, nil
}

func NewThreadForkHost(opts ThreadCapabilityOptions) (*ThreadForkHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &ThreadForkHost{harness: harness}, nil
}

func NewThreadDeleteHost(opts ThreadCapabilityOptions) (*ThreadDeleteHost, error) {
	if err := validateCapabilityRuntime(opts.Runtime); err != nil {
		return nil, err
	}
	return &ThreadDeleteHost{store: opts.Runtime.store}, nil
}

func NewSubAgentMaintenanceHost(opts ThreadCapabilityOptions) (*SubAgentMaintenanceHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &SubAgentMaintenanceHost{harness: harness}, nil
}

func NewPendingToolSettlementHost(opts ThreadCapabilityOptions) (*PendingToolSettlementHost, error) {
	harness, err := newCapabilityHarness(opts)
	if err != nil {
		return nil, err
	}
	return &PendingToolSettlementHost{harness: harness}, nil
}

func validateCapabilityStore(store *Store) error {
	if store == nil {
		return errors.New("thread capability store is required")
	}
	return store.validate()
}

func validateCapabilityRuntime(runtime *HostRuntime) error {
	if runtime == nil {
		return errors.New("thread capability runtime is required")
	}
	return validateCapabilityStore(runtime.store)
}

func newCapabilityHarness(opts ThreadCapabilityOptions) (*agentharness.AgentHarness, error) {
	if err := validateCapabilityRuntime(opts.Runtime); err != nil {
		return nil, err
	}
	return agentharness.New(agentharness.Options{
		Repo:           opts.Runtime.store.repo,
		ForkOperations: opts.Runtime.store.forkOperations,
		PromptStore:    opts.Runtime.store.prompt,
		Artifacts:      opts.Runtime.store.artifacts,
		Sink:           newRuntimeEventSink(opts.Sink),
		SinkPolicy:     runtimeHarnessSinkPolicy(),
	}), nil
}
