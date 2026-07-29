package runtime

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v2/observation"
)

// PendingToolRecoveryTarget binds one exact pending tool and, for a child
// thread, its canonical parent.
type PendingToolRecoveryTarget struct {
	ParentThreadID ThreadID
	Target         PendingToolSettlementTarget
}

// Validate checks the complete recovery authority identity.
func (target PendingToolRecoveryTarget) Validate() error {
	if err := target.Target.Validate(); err != nil {
		return err
	}
	if target.ParentThreadID != ThreadID(strings.TrimSpace(string(target.ParentThreadID))) {
		return errors.New("pending tool recovery parent identity must be trim-stable")
	}
	if target.ParentThreadID == target.Target.ThreadID {
		return errors.New("pending tool recovery parent and target thread must differ")
	}
	return nil
}

// PendingToolRecoveryRequest contains only the host-owned outcome for a target
// already bound by PendingToolRecoveryTarget.
type PendingToolRecoveryRequest struct {
	Status   PendingToolSettlementStatus
	Summary  string
	Output   string
	Activity *observation.ActivityPresentation
}

// PendingToolRecovery settles one exact target without provider execution.
type PendingToolRecovery struct {
	inner  *pendingToolRecoveryCapability
	target PendingToolSettlementTarget
}

// PendingToolRecovery binds recovery to one exact root or child pending tool.
func (host *Host) PendingToolRecovery(ctx context.Context, target PendingToolRecoveryTarget, sink EventSink) (*PendingToolRecovery, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	var (
		inner *pendingToolRecoveryCapability
		err   error
	)
	if target.ParentThreadID == "" {
		inner, err = host.binders.pending.NewThreadHost(ctx, target.Target.ThreadID, sink)
	} else {
		inner, err = host.binders.pending.NewSubAgentHost(ctx, target.ParentThreadID, sink)
		if err == nil {
			err = validateSubAgentSettlementAuthority(ctx, inner.harness, target.ParentThreadID, target.Target.ThreadID)
		}
	}
	if err != nil {
		return nil, err
	}
	return &PendingToolRecovery{inner: inner, target: target.Target}, nil
}

// Settle records the bound pending tool outcome exactly once or replays the
// same durable settlement.
func (recovery *PendingToolRecovery) Settle(ctx context.Context, request PendingToolRecoveryRequest) (PendingToolSettlementResult, error) {
	if recovery == nil || recovery.inner == nil {
		return PendingToolSettlementResult{}, errors.New("pending tool recovery is required")
	}
	return recovery.inner.SettlePendingTool(ctx, PendingToolSettlementRequest{
		Target: recovery.target, Status: request.Status, Summary: request.Summary,
		Output: request.Output, Activity: request.Activity,
	})
}

// InterruptedTurnRecoveryTarget identifies a root or canonical child whose
// current interrupted lease proof will be bound during issuance.
type InterruptedTurnRecoveryTarget struct {
	ParentThreadID ThreadID
	ThreadID       ThreadID
}

// Validate checks the complete thread relationship identity.
func (target InterruptedTurnRecoveryTarget) Validate() error {
	if target.ThreadID == "" || target.ThreadID != ThreadID(strings.TrimSpace(string(target.ThreadID))) {
		return errors.New("interrupted recovery thread identity is required and must be trim-stable")
	}
	if target.ParentThreadID != ThreadID(strings.TrimSpace(string(target.ParentThreadID))) {
		return errors.New("interrupted recovery parent identity must be trim-stable")
	}
	if target.ParentThreadID == target.ThreadID {
		return errors.New("interrupted recovery parent and target thread must differ")
	}
	return nil
}

// InterruptedTurnRecovery owns one exact interrupted lease proof.
type InterruptedTurnRecovery struct {
	inner *interruptedTurnRecoveryCapability
}

// InterruptedTurnRecovery binds the current durable interrupted lease proof
// for one exact root or child thread.
func (host *Host) InterruptedTurnRecovery(ctx context.Context, target InterruptedTurnRecoveryTarget, sink EventSink) (*InterruptedTurnRecovery, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	var (
		factory *interruptedTurnRecoveryFactory
		err     error
	)
	if target.ParentThreadID == "" {
		factory, err = host.binders.interrupted.BindThread(ctx, target.ThreadID)
	} else {
		factory, err = host.binders.interrupted.BindSubAgent(ctx, target.ParentThreadID, target.ThreadID)
	}
	if err != nil {
		return nil, err
	}
	inner, err := factory.NewHost(ctx, sink)
	if err != nil {
		return nil, err
	}
	return &InterruptedTurnRecovery{inner: inner}, nil
}

// Recover atomically finalizes the exact interrupted lease proof bound during
// issuance.
func (recovery *InterruptedTurnRecovery) Recover(ctx context.Context) (RecoverInterruptedTurnResult, error) {
	if recovery == nil || recovery.inner == nil {
		return RecoverInterruptedTurnResult{}, errors.New("interrupted turn recovery is required")
	}
	return recovery.inner.RecoverInterruptedTurn(ctx)
}

// ThreadInventory is composition-owned root-thread enumeration authority.
type ThreadInventory struct {
	inner *threadInventoryCapability
}

// ThreadInventory issues canonical root-thread enumeration authority.
func (host *Host) ThreadInventory(ctx context.Context) (*ThreadInventory, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	return &ThreadInventory{inner: host.binders.inventory}, nil
}

// List returns one stable page of canonical root threads.
func (inventory *ThreadInventory) List(ctx context.Context, request ListRootThreadsRequest) (RootThreadsPage, error) {
	if inventory == nil || inventory.inner == nil {
		return RootThreadsPage{}, errors.New("thread inventory is required")
	}
	return inventory.inner.ListRootThreads(ctx, request)
}
