package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// The actor may own complete current facts with a bounded item interval. It
// never exposes that interval as View or execution state. Explicit full reads
// hydrate the existing actor at the same leaf/version; there is no second actor.
func (service *threadRuntimeService) hydrateThreadWindow(ctx context.Context, input ThreadWindowInput, actor *threadRuntimeState) error {
	id := input.ThreadID
	if _, ok := service.host.store.repo.(sessiontree.WindowProjectionReader); !ok {
		return service.hydrateThreadView(ctx, id, actor)
	}
	var accepted *threadSourceProjection
	if err := actor.apply(ctx, func() error {
		accepted = actor.sourceWindow
		return nil
	}); err != nil {
		return err
	}
	interests := service.sourceWindowInterests(id)
	canonical, page, err := service.loadSourceWindowWithInterests(ctx, input, "", false, interests)
	if err != nil {
		return err
	}

	// Catch the first append without restarting preparation indefinitely. Execution
	// publishes through this actor; additional append-only growth does not make a
	// validated frozen baseline corrupt. Every later explicit read checks its
	// canonical tip again, including read-only hosts with no live observations.
	meta, err := service.ensureThread(ctx, id)
	if err != nil {
		return err
	}
	if meta.LeafID != page.leaf {
		canonical, page, err = service.loadSourceWindowWithInterests(ctx, input, "", false, interests)
		if err != nil {
			return err
		}
	}
	err = actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 && (actor.sourceWindow == nil || actor.sourceWindow != accepted) {
			return nil
		}
		meta, err := service.ensureThread(ctx, id)
		if err != nil {
			return err
		}
		if meta.LeafID != page.leaf {
			// Validate ancestry, not equality with an independently sampled tip.
			// A branch replacement still rejects every source in the old path.
			reader, ok := service.host.store.repo.(sessiontree.PathWindowReader)
			if !ok || page.leaf == "" {
				return errCanonicalRefreshDiscarded
			}
			if _, err := reader.PathMetadataWindow(ctx, sessiontree.PathWindowOptions{ThreadID: id.String(), LeafID: page.leaf, Direction: sessiontree.PathWindowBefore, Limit: 1}); err != nil {
				return runtimeHostError(err)
			}
		}
		service.flushUpdateLocked(actor)
		changed := accepted == nil || accepted.leaf != page.leaf
		canonical.ViewVersion = actor.state.view.ViewVersion
		if changed {
			canonical.ViewVersion++
		}
		actor.state.view = canonical.ThreadView
		actor.state.turnID, actor.state.runID = canonical.TurnID, canonical.RunID
		page.input = normalizeWindowInput(input)
		actor.sourceWindow = &page
		actor.interactionIndex = nil
		actor.invalidateItemIndex()
		if changed {
			actor.markComponents(threadContextChanged | threadGoalChanged | threadParentEventsChanged | threadQueueChanged | threadInteractionsChanged | threadRestoredInputsChanged)
		}
		// Every interest has its own bounded interval. Publication schedules its
		// frozen source read outside the actor instead of slicing this interval.
		actor.mutations.order = changed && accepted != nil
		return nil
	})
	if err == nil && canonical.Activity == ThreadActivityActive && service.host.requireExecution() == nil {
		// Recovery needs complete executable history. Hydrating the same actor at
		// the frozen leaf preserves its accepted version and window interests.
		go service.prepareActiveThreadWindow(id, actor)
	}
	return err
}

// The accepted display interval remains intact if complete recovery fails.
// Its observers must learn that live continuity stopped instead of waiting
// forever on a successful-looking active baseline. Explicit reads may retry.
func (service *threadRuntimeService) prepareActiveThreadWindow(id identity.ThreadID, actor *threadRuntimeState) {
	err := service.prepareThreadView(context.Background(), id, actor)
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrHostClosed) || errors.Is(err, ErrThreadDeleted) {
		return
	}
	err = fmt.Errorf("prepare active thread %s: %w", id, err)
	service.host.store.reportBackgroundError(err)
	service.mu.Lock()
	defer service.mu.Unlock()
	for subscription := range service.updateSubscribers {
		subscription.mu.Lock()
		affected := !subscription.windowed || subscription.factsOnly || subscription.authorized[id.String()]
		if affected && !subscription.closed {
			subscription.finishLocked(errors.Join(ErrThreadUpdateInvalidated, err))
			delete(service.updateSubscribers, subscription)
		}
		subscription.mu.Unlock()
	}
}

// Collect only accepted finite interests. Canonical membership and source facts
// for these readers are frozen in the same transaction as the next actor view.
func (service *threadRuntimeService) sourceWindowInterests(id identity.ThreadID) []sourceWindowRequest {
	service.mu.Lock()
	defer service.mu.Unlock()
	var result []sourceWindowRequest
	for sub := range service.updateSubscribers {
		sub.mu.Lock()
		interest, ok := sub.windows[id.String()]
		if ok && !sub.closed && interest.lifetime != nil {
			input := interest.sourceInput
			input.RequestID = ""
			result = append(result, sourceWindowRequest{input: input, lifetime: interest.lifetime})
		}
		sub.mu.Unlock()
	}
	return result
}
