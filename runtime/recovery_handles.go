package runtime

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/tools"
)

// pendingToolRecoveryTarget binds one exact pending tool and, for a child
// thread, its canonical parent.
type pendingToolRecoveryTarget struct {
	ParentThreadID identity.ThreadID
	Target         PendingToolSettlementTarget
}

// Validate checks the complete recovery authority identity.
func (target pendingToolRecoveryTarget) Validate() error {
	if err := target.Target.Validate(); err != nil {
		return err
	}
	if target.ParentThreadID != identity.ThreadID(strings.TrimSpace(string(target.ParentThreadID))) {
		return errors.New("pending tool recovery parent identity must be trim-stable")
	}
	if target.ParentThreadID == target.Target.ThreadID {
		return errors.New("pending tool recovery parent and target thread must differ")
	}
	return nil
}

// pendingToolRecoveryRequest contains only the host-owned outcome for a target
// already bound by pendingToolRecoveryTarget.
type pendingToolRecoveryRequest struct {
	Status   PendingToolSettlementStatus
	Summary  string
	Output   string
	Activity *tools.ActivityPresentation
}

// pendingToolRecoveryHandle settles one exact target without provider execution.
type pendingToolRecoveryHandle struct {
	inner  *pendingToolRecoveryCapability
	target PendingToolSettlementTarget
}

// pendingToolRecoveryHandle binds recovery to one exact root or child pending tool.
func (host *Host) pendingToolRecovery(ctx context.Context, target pendingToolRecoveryTarget, sink EventSink) (*pendingToolRecoveryHandle, error) {
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
	return &pendingToolRecoveryHandle{inner: inner, target: target.Target}, nil
}

// Settle records the bound pending tool outcome exactly once or replays the
// same durable settlement.
func (recovery *pendingToolRecoveryHandle) Settle(ctx context.Context, request pendingToolRecoveryRequest) (PendingToolSettlementResult, error) {
	if recovery == nil || recovery.inner == nil {
		return PendingToolSettlementResult{}, errors.New("pending tool recovery is required")
	}
	return recovery.inner.SettlePendingTool(ctx, pendingToolSettlementRequest{
		Target: recovery.target, Status: request.Status, Summary: request.Summary,
		Output: request.Output, Activity: request.Activity,
	})
}

// interruptedTurnRecoveryTarget identifies a root or canonical child whose
// current interrupted lease proof will be bound during issuance.
type interruptedTurnRecoveryTarget struct {
	ParentThreadID identity.ThreadID
	ThreadID       identity.ThreadID
}

// Validate checks the complete thread relationship identity.
func (target interruptedTurnRecoveryTarget) Validate() error {
	if target.ThreadID == "" || target.ThreadID != identity.ThreadID(strings.TrimSpace(string(target.ThreadID))) {
		return errors.New("interrupted recovery thread identity is required and must be trim-stable")
	}
	if target.ParentThreadID != identity.ThreadID(strings.TrimSpace(string(target.ParentThreadID))) {
		return errors.New("interrupted recovery parent identity must be trim-stable")
	}
	if target.ParentThreadID == target.ThreadID {
		return errors.New("interrupted recovery parent and target thread must differ")
	}
	return nil
}

// interruptedTurnRecoveryHandle owns one exact interrupted lease proof.
type interruptedTurnRecoveryHandle struct {
	inner *interruptedTurnRecoveryCapability
}

// interruptedTurnRecoveryHandle binds the current durable interrupted lease proof
// for one exact root or child thread.
func (host *Host) interruptedTurnRecovery(ctx context.Context, target interruptedTurnRecoveryTarget, sink EventSink) (*interruptedTurnRecoveryHandle, error) {
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
	return &interruptedTurnRecoveryHandle{inner: inner}, nil
}

// Recover atomically finalizes the exact interrupted lease proof bound during
// issuance.
func (recovery *interruptedTurnRecoveryHandle) Recover(ctx context.Context) (RecoverInterruptedTurnResult, error) {
	if recovery == nil || recovery.inner == nil {
		return RecoverInterruptedTurnResult{}, errors.New("interrupted turn recovery is required")
	}
	return recovery.inner.RecoverInterruptedTurn(ctx)
}

// threadInventoryHandle is composition-owned root-thread enumeration authority.
type threadInventoryHandle struct {
	inner *threadInventoryCapability
}

// threadInventoryHandle issues canonical root-thread enumeration authority.
func (host *Host) threadInventory(ctx context.Context) (*threadInventoryHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	return &threadInventoryHandle{inner: host.binders.inventory}, nil
}

// List returns one stable page of canonical root threads.
func (inventory *threadInventoryHandle) List(ctx context.Context, request listRootThreadsRequest) (rootThreadsPage, error) {
	if inventory == nil || inventory.inner == nil {
		return rootThreadsPage{}, errors.New("thread inventory is required")
	}
	return inventory.inner.ListRootThreads(ctx, request)
}
