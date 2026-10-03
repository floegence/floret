package runtime

import (
	"context"
	"errors"
	"strings"
	"time"
)

// SendQueuedNow gracefully stops the current response, then admits the selected
// pending input as a new turn. Confirmed work is preserved; it never replays an
// unknown effect. The bounded transition belongs to the runtime after the stop
// is accepted, independently of the requesting connection. If the host closes
// before admission, the input remains queued and requires explicit resubmission.
func (service *threadRuntimeService) SendQueuedNow(ctx context.Context, in PromoteQueuedInput) (ThreadView, error) {
	in.QueueItemID = strings.TrimSpace(in.QueueItemID)
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	view, err := service.View(ctx, in.ThreadID)
	if err != nil {
		return ThreadView{}, err
	}
	if view.Activity != ThreadActivityActive {
		return service.PromoteQueued(ctx, in)
	}
	actor := service.runtime(in.ThreadID)
	// Check a completed replay before touching whichever response is now active.
	fingerprint, _ := stableFingerprint(struct {
		QueueItemID string `json:"queue_item_id"`
	}{in.QueueItemID})
	replayed := false
	err = actor.apply(ctx, func() error {
		if previous, ok := actor.state.requestKeys[key]; ok {
			if previous.fingerprint != fingerprint {
				return ErrRequestConflict
			}
			replayed = true
			return nil
		}
		if !queuedInputExists(actor.state.view.Queue, in.QueueItemID) {
			return ErrRequestConflict
		}
		if actor.state.view.Cancellation != nil {
			return ErrThreadBusy
		}
		return nil
	})
	if err != nil {
		return ThreadView{}, err
	}
	if replayed {
		return service.currentView(actor), nil
	}
	subscription, err := service.Subscribe(ctx)
	if err != nil {
		return ThreadView{}, err
	}
	// Subscribe before accepting Stop so a fast terminal update cannot be missed.
	stopped, err := service.requestGracefulCancellation(ctx, actor, in.ThreadID, view.TurnID, "send-queue-stop:"+key)
	if err != nil {
		subscription.Close()
		return ThreadView{}, err
	}
	if stopped.Activity == ThreadActivityActive && (stopped.TurnID != view.TurnID || stopped.Cancellation == nil) {
		subscription.Close()
		return ThreadView{}, ErrThreadBusy
	}
	if stopped.Activity == ThreadActivityActive {
		if _, err := service.host.store.repo.Entry(context.WithoutCancel(ctx), in.ThreadID.String(), "cancel:send-queue-stop:"+key); err != nil {
			subscription.Close()
			return ThreadView{}, ErrThreadBusy
		}
	}
	type result struct {
		view ThreadView
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		defer subscription.Close()
		transition, cancel := context.WithTimeout(context.Background(), 2*gracefulCancellationLimit+time.Second)
		defer cancel()
		current := stopped
		for current.Activity == ThreadActivityActive && current.TurnID == view.TurnID {
			next, readErr := subscription.Next(transition)
			if readErr != nil {
				finished <- result{err: readErr}
				return
			}
			if next.ThreadID == in.ThreadID {
				current = next
			}
		}
		if current.Failure != nil && current.Failure.Code == ThreadTurnFailureEffectOutcomeUnknown {
			finished <- result{err: ErrEffectOutcomeUnknown}
			return
		}
		promoted, promoteErr := service.PromoteQueued(transition, in)
		// A concurrent replay can win admission after both requests observe idle.
		if errors.Is(promoteErr, ErrThreadBusy) || errors.Is(promoteErr, ErrRequestConflict) {
			_ = actor.apply(transition, func() error {
				if previous, ok := actor.state.requestKeys[key]; ok && previous.fingerprint == fingerprint {
					promoted = cloneThreadRuntimeView(actor.state.view)
					promoteErr = nil
				}
				return nil
			})
		}
		finished <- result{promoted, promoteErr}
	}()
	select {
	case out := <-finished:
		return out.view, out.err
	case <-ctx.Done():
		return ThreadView{}, ctx.Err()
	}
}
