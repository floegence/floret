package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// threadSourceProjection is an exact item interval, not an accepted ThreadView.
// The membership reader supplies public item ordinals. Complete context, turn
// facts and pending controls are still required before installing a baseline.
type threadSourceProjection struct {
	leaf                string
	items               []ThreadItem
	interactions        []ThreadInteraction
	sources             int
	hasBefore, hasAfter bool
}

// Use the same reducer for complete and sparse item sources. The dependency
// reader verifies the frozen ancestry; source positions retain canonical order
// even though unrelated entries are not materialized.
func (service *threadRuntimeService) projectThreadSources(ctx context.Context, threadID identity.ThreadID, leaf string, seeds []string) (threadSourceProjection, error) {
	reader, ok := service.host.store.repo.(sessiontree.ProjectionSourceScanner)
	if !ok {
		return threadSourceProjection{}, ErrUnsupportedStoreCapability
	}
	projection := newThreadItemProjection()
	projection.deferActivity = true
	page, err := reader.ReadProjectionSources(ctx, threadID.String(), leaf, seeds, func(source sessiontree.PathMetadata, control bool) []string {
		return coldProjectionFields(source, false, control)
	})
	if err != nil {
		return threadSourceProjection{}, runtimeHostError(err)
	}
	var depth int64
	for _, source := range page.Entries {
		if err := ctx.Err(); err != nil {
			return threadSourceProjection{}, err
		}
		if source.Entry.PathDepth <= depth {
			return threadSourceProjection{}, ErrAuthorityCorrupt
		}
		depth = source.Entry.PathDepth
		projection.ordinal = uint64(depth - 1)
		if err := projection.append([]sessiontree.Entry{source.Entry}, nil); err != nil {
			return threadSourceProjection{}, err
		}
	}
	items, interactions := projection.snapshot()
	return threadSourceProjection{leaf: page.LeafID, items: cloneThreadItems(items), interactions: cloneThreadInteractions(interactions), sources: len(page.Entries)}, nil
}

// Exact source-backed membership supplies public ordinals before rendering.
// Dependencies may produce other items; only the selected stable members escape.
func (service *threadRuntimeService) projectThreadSourceTail(ctx context.Context, threadID identity.ThreadID, limit int) (threadSourceProjection, error) {
	return service.projectThreadMembershipWindow(ctx, sessiontree.ItemMembershipWindowOptions{ThreadID: threadID.String(), Limit: limit})
}

func (service *threadRuntimeService) projectThreadMembershipWindow(ctx context.Context, options sessiontree.ItemMembershipWindowOptions) (threadSourceProjection, error) {
	reader, ok := service.host.store.repo.(sessiontree.ItemMembershipReader)
	if !ok {
		return threadSourceProjection{}, ErrUnsupportedStoreCapability
	}
	page, err := reader.ItemMembershipWindow(ctx, options)
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return threadSourceProjection{}, threadquery.ErrRecordUnavailable
	}
	if err != nil {
		return threadSourceProjection{}, runtimeHostError(err)
	}
	result := threadSourceProjection{leaf: page.LeafID, items: []ThreadItem{}, hasBefore: page.HasBefore, hasAfter: page.HasAfter}
	if len(page.Members) == 0 {
		return result, nil
	}
	seen := map[string]bool{}
	var seeds []string
	for _, member := range page.Members {
		if !seen[member.SourceID] {
			seeds = append(seeds, member.SourceID)
			seen[member.SourceID] = true
		}
	}
	projected, err := service.projectThreadSources(ctx, identity.ThreadID(options.ThreadID), page.LeafID, seeds)
	if err != nil {
		return threadSourceProjection{}, err
	}
	type key struct {
		id      string
		ordinal uint64
	}
	byID := map[key]ThreadItem{}
	for _, item := range projected.items {
		byID[key{item.ID, item.CanonicalOrdinal}] = item
	}
	for i, member := range page.Members {
		item, found := byID[key{member.ID, member.CanonicalOrdinal}]
		if !found || string(item.Kind) != member.Kind || item.TurnID.String() != member.TurnID || item.RunID.String() != member.RunID {
			return threadSourceProjection{}, ErrAuthorityCorrupt
		}
		item.Ordinal = page.Ordinals[i]
		result.items = append(result.items, item)
	}
	result.sources = projected.sources
	result.interactions = projected.interactions
	return result, nil
}

// loadSourceWindow assembles complete non-item facts and the requested item
// interval. It remains explicitly distinct from an execution/full-view reducer.
func (service *threadRuntimeService) loadSourceWindow(ctx context.Context, input ThreadWindowInput) (canonicalThreadView, threadSourceProjection, error) {
	repo := service.host.store.repo
	reader, ok := repo.(sessiontree.WindowProjectionReader)
	if !ok {
		return canonicalThreadView{}, threadSourceProjection{}, ErrUnsupportedStoreCapability
	}
	limit := input.Limit
	if limit == 0 {
		limit = threadDisplayPageItems
	}
	window, err := reader.ReadWindowProjection(ctx, sessiontree.ItemMembershipWindowOptions{ThreadID: input.ThreadID.String(), AnchorID: input.AnchorID, Direction: sessiontree.PathWindowDirection(input.Direction), Limit: limit}, func(source sessiontree.PathMetadata, control bool) []string {
		return coldProjectionFields(source, false, control)
	})
	if err != nil {
		return canonicalThreadView{}, threadSourceProjection{}, runtimeHostError(err)
	}
	page, facts, sources := window.Membership, window.Facts, window.Entries
	selected := map[string]bool{}
	for _, member := range page.Members {
		selected[member.ID] = true
	}
	projection := newThreadCanonicalProjection()
	projection.items.deferActivity = true
	for _, source := range sources {
		entry := source.Entry
		projection.items.ordinal = uint64(entry.PathDepth - 1)
		if err := projection.items.append([]sessiontree.Entry{entry}, nil); err != nil {
			return canonicalThreadView{}, threadSourceProjection{}, err
		}
		if err := projection.turns.observe(entry, projection.items.itemForEntry); err != nil {
			return canonicalThreadView{}, threadSourceProjection{}, err
		}
	}
	items, interactions := projection.items.snapshot()
	byID := map[string]ThreadItem{}
	for _, item := range items {
		byID[item.ID] = item
	}
	selectedItems := make([]ThreadItem, 0, len(page.Members))
	for i, member := range page.Members {
		item, found := byID[member.ID]
		if !found || item.CanonicalOrdinal != member.CanonicalOrdinal || string(item.Kind) != member.Kind || item.TurnID.String() != member.TurnID || item.RunID.String() != member.RunID {
			return canonicalThreadView{}, threadSourceProjection{}, ErrAuthorityCorrupt
		}
		item.Ordinal = page.Ordinals[i]
		selectedItems = append(selectedItems, item)
	}
	view := canonicalThreadView{ThreadView: ThreadView{ThreadID: input.ThreadID, Items: selectedItems, Interactions: cloneThreadInteractions(interactions), Goal: publicThreadGoal(projection.items.goal.State)}}
	view.TurnID, view.RunID, view.Activity, view.LastOutcome, view.Failure = projection.turns.lifecycle.snapshot()
	if latest := projection.turns.lifecycle.turns[view.TurnID.String()]; latest != nil {
		view.TurnKind = latest.kind
		view.Cancellation = cloneThreadCancellation(latest.cancellation)
	}
	// Restrict membership only after the original reducer has validated complete
	// final boundaries and selected-turn waits, including off-window dependencies.
	allTurns, err := projection.turns.snapshot()
	if err != nil {
		return canonicalThreadView{}, threadSourceProjection{}, err
	}
	selectedTurns := map[identity.TurnID]bool{}
	for _, item := range selectedItems {
		selectedTurns[item.TurnID] = true
	}
	for _, turn := range allTurns {
		if selectedTurns[turn.TurnID] {
			turn.IntermediateItemIDs = selectedThreadIDs(turn.IntermediateItemIDs, selected)
			turn.FinalItemIDs = selectedThreadIDs(turn.FinalItemIDs, selected)
			view.Turns = append(view.Turns, turn)
		}
	}
	contextSnapshot, modelSources := window.Context, window.ModelContext
	model, err := service.modelContextFromHistory(ctx, input.ThreadID, modelSources.Entries, modelSources.Messages)
	if err != nil {
		return canonicalThreadView{}, threadSourceProjection{}, err
	}
	contextView := runtimeCanonicalContext(contextSnapshot, model, facts.Anchors, facts.CompactionOrdinals)
	view.Context = &contextView
	if service.host.enableParentEvents {
		if window.ParentEvents == nil {
			return canonicalThreadView{}, threadSourceProjection{}, ErrAuthorityCorrupt
		}
		parents := publicParentEventPage(input.ThreadID, *window.ParentEvents, false)
		view.ParentEvents = &parents
	}
	queueSources := make(map[string]sessiontree.Entry, len(window.Queue))
	for _, entry := range window.Queue {
		queueSources[entry.ID] = entry
	}
	view.Queue, view.RestoredInputs, err = projectThreadQueues(ctx, window.Queue, func(id string) (sessiontree.Entry, error) {
		entry, found := queueSources[id]
		if !found {
			return sessiontree.Entry{}, ErrAuthorityCorrupt
		}
		return entry, nil
	})
	if err != nil {
		return canonicalThreadView{}, threadSourceProjection{}, err
	}
	return view, threadSourceProjection{leaf: page.LeafID, items: selectedItems, interactions: interactions, sources: len(sources), hasBefore: page.HasBefore, hasAfter: page.HasAfter}, nil
}
