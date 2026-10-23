package runtime

import (
	"context"
	"fmt"

	"github.com/floegence/floret/v7/identity"
)

// The existing actor owns at most one cold preparation. HTTP cancellation only
// releases a waiter; deletion and Host shutdown cancel and join the actual read.
// No partial view is published and no preparation state is persisted.
type threadReadPreparation struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error // published by closing done
}

func (service *threadRuntimeService) prepareThreadView(ctx context.Context, id identity.ThreadID, actor *threadRuntimeState) error {
	var pending *threadReadPreparation
	err := actor.apply(ctx, func() error {
		if err := service.host.available(); err != nil {
			return err
		}
		if actor.state.view.ViewVersion > 0 {
			return nil
		}
		pending = actor.preparation
		if pending != nil {
			select {
			case <-pending.done:
				pending = nil
			default:
			}
		}
		if pending == nil {
			work, cancel := context.WithCancel(context.WithoutCancel(ctx))
			pending = &threadReadPreparation{done: make(chan struct{}), cancel: cancel}
			actor.preparation = pending
			go func(preparation *threadReadPreparation) {
				defer cancel()
				defer func() {
					if recovered := recover(); recovered != nil {
						preparation.err = fmt.Errorf("cold thread preparation panicked: %v", recovered)
					}
					close(preparation.done)
				}()
				service.preparationOnce.Do(func() { service.preparationSlots = make(chan struct{}, 2) })
				select {
				case service.preparationSlots <- struct{}{}:
					defer func() { <-service.preparationSlots }()
				case <-work.Done():
					preparation.err = work.Err()
					return
				}
				preparation.err = service.hydrateThreadView(work, id, actor)
			}(pending)
		}
		return nil
	})
	if err != nil || pending == nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pending.done:
		return pending.err
	}
}
