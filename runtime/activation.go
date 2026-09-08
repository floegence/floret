package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
)

// ErrExecutionDeferred means a maintenance Host has not been activated.
var ErrExecutionDeferred = errors.New("runtime execution is deferred")

func (host *Host) requireExecution() error {
	if host == nil {
		return ErrHostClosed
	}
	host.executionMu.RLock()
	defer host.executionMu.RUnlock()
	if host.executionDeferred {
		return ErrExecutionDeferred
	}
	return nil
}

// Activate enables execution after the host application has finished preparing
// all of its stores and current authorization. It is idempotent. Hydrated active
// threads retain the ordinary recovery policy; unopened threads recover on View.
func (host *Host) Activate(ctx context.Context) error {
	if ctx == nil {
		return errors.New("activation context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if host == nil {
		return ErrHostClosed
	}
	host.maintenanceMu.Lock()
	defer host.maintenanceMu.Unlock()
	host.closeMu.Lock()
	if host.closing || host.closed {
		host.closeMu.Unlock()
		return ErrHostClosed
	}
	if err := ctx.Err(); err != nil {
		host.closeMu.Unlock()
		return err
	}
	host.executionMu.Lock()
	if !host.executionDeferred {
		host.executionMu.Unlock()
		host.closeMu.Unlock()
		return nil
	}
	host.executionDeferred = false
	host.executionMu.Unlock()
	host.closeMu.Unlock()
	host.threadRuntimeMu.Lock()
	service := host.threadRuntime
	host.threadRuntimeMu.Unlock()
	if service == nil {
		return nil
	}
	service.runtimesMu.Lock()
	actors := make([]*threadRuntimeState, 0, len(service.runtimes))
	for _, actor := range service.runtimes {
		actors = append(actors, actor)
	}
	service.runtimesMu.Unlock()
	for _, actor := range actors {
		actor.mu.Lock()
		start := !actor.closed && actor.state.view.Activity == ThreadActivityActive && !actor.state.hydrationStarted
		if start {
			actor.state.hydrationStarted = true
		}
		actor.mu.Unlock()
		if start {
			go service.recoverHydratedThread(identity.ThreadID(actor.threadID))
		}
	}
	return nil
}
