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
	return service.prepareThreadRead(ctx, id, actor, false)
}

func (service *threadRuntimeService) prepareThreadRead(ctx context.Context, id identity.ThreadID, actor *threadRuntimeState, window bool, requested ...ThreadWindowInput) error {
	var pending *threadReadPreparation
	err := actor.apply(ctx, func() error {
		if err := service.host.available(); err != nil {
			return err
		}
		if actor.state.contextErr != nil {
			canonical, err := service.loadCanonicalThreadView(ctx, id)
			if err != nil {
				return err
			}
			if err := actor.installCanonicalView(canonical); err != nil {
				return err
			}
		}
		if actor.state.view.ViewVersion > 0 {
			if actor.sourceWindow == nil {
				return nil
			}
			if window {
				meta, err := service.ensureThread(ctx, id)
				if err != nil {
					return err
				}
				if meta.LeafID == actor.sourceWindow.leaf {
					if len(requested) == 0 || sameWindowInterval(normalizeWindowInput(requested[0]), actor.sourceWindow.input) {
						return nil
					}
					for _, frozen := range actor.sourceWindow.companions {
						if sameWindowInterval(normalizeWindowInput(requested[0]), frozen.page.input) {
							return nil
						}
					}
				}
			}
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
				if window {
					input := ThreadWindowInput{ThreadID: id, Limit: threadDisplayPageItems}
					if len(requested) > 0 {
						input = requested[0]
					}
					preparation.err = service.hydrateThreadWindow(work, input, actor)
				} else {
					preparation.err = service.hydrateThreadView(work, id, actor)
				}
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
		if pending.err != nil {
			return pending.err
		}
		// A complete reader may have joined an already running bounded read.
		// Its explicit request still requires the complete execution reducer.
		if !window {
			return service.prepareThreadRead(ctx, id, actor, false)
		}
		return nil
	}
}
