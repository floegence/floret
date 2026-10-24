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
func (service *threadRuntimeService) hydrateThreadWindow(ctx context.Context, id identity.ThreadID, actor *threadRuntimeState) error {
	if _, ok := service.host.store.repo.(sessiontree.WindowProjectionReader); !ok {
		return service.hydrateThreadView(ctx, id, actor)
	}
	canonical, page, err := service.loadSourceWindow(ctx, ThreadWindowInput{ThreadID: id, Limit: threadDisplayPageItems})
	if err != nil {
		return err
	}
	err = actor.apply(ctx, func() error {
		if actor.state.view.ViewVersion > 0 {
			return nil
		}
		meta, err := service.ensureThread(ctx, id)
		if err != nil {
			return err
		}
		if meta.LeafID != page.leaf {
			return errCanonicalRefreshDiscarded
		}
		canonical.ViewVersion = 1
		actor.state.view = canonical.ThreadView
		actor.state.turnID, actor.state.runID = canonical.TurnID, canonical.RunID
		actor.sourceWindow = &page
		actor.interactionIndex = nil
		actor.invalidateItemIndex()
		actor.markComponents(threadContextChanged | threadGoalChanged | threadParentEventsChanged | threadQueueChanged | threadInteractionsChanged | threadRestoredInputsChanged)
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

func (actor *threadRuntimeState) sourceWindowSnapshot(ctx context.Context, input ThreadWindowInput) (ThreadWindowSnapshot, []ThreadTurn, []ThreadInteraction, error) {
	var canonical canonicalThreadView
	page := *actor.sourceWindow
	if input.AnchorID == "" && (input.Direction == "" || input.Direction == ThreadWindowBefore) && (input.Limit == 0 || input.Limit == threadDisplayPageItems) {
		canonical.ThreadView = actor.state.view
	} else {
		var err error
		canonical, page, err = actor.service.loadSourceWindow(ctx, input)
		if err != nil {
			return ThreadWindowSnapshot{}, nil, nil, err
		}
		if page.leaf != actor.sourceWindow.leaf {
			return ThreadWindowSnapshot{}, nil, nil, errCanonicalRefreshDiscarded
		}
	}
	limit := min(threadDisplayTextBytes, threadDisplayPreviewBytes/max(1, len(canonical.Items))-3)
	items := cloneWindowItemsLimit(canonical.Items, limit)
	for i, item := range items {
		var err error
		items[i], err = actor.windowItem(ctx, item, limit)
		if err != nil {
			return ThreadWindowSnapshot{}, nil, nil, err
		}
	}
	snapshot := ThreadWindowSnapshot{ThreadID: input.ThreadID, ViewVersion: actor.state.view.ViewVersion, LeafID: page.leaf, Items: items, HasBefore: page.hasBefore, HasAfter: page.hasAfter}
	snapshot, err := boundWindowSnapshot(snapshot, input)
	if err != nil {
		return ThreadWindowSnapshot{}, nil, nil, err
	}
	selected := map[string]bool{}
	interactionIDs := map[string]bool{}
	for _, item := range snapshot.Items {
		selected[item.ID] = true
		if item.Interaction != nil {
			interactionIDs[item.Interaction.ID] = true
		}
	}
	turns := cloneThreadTurns(canonical.Turns)
	for i := range turns {
		turns[i].IntermediateItemIDs = selectedThreadIDs(turns[i].IntermediateItemIDs, selected)
		turns[i].FinalItemIDs = selectedThreadIDs(turns[i].FinalItemIDs, selected)
	}
	interactions := []ThreadInteraction{}
	for _, interaction := range canonical.Interactions {
		if !interaction.Resolved || interactionIDs[interaction.ID] {
			interactions = append(interactions, cloneThreadInteraction(interaction))
		}
	}
	return snapshot, turns, interactions, nil
}
