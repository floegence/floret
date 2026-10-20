package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// canonicalThreadView keeps disposable reducer ownership out of public views.
type canonicalThreadView struct {
	ThreadView
	projection *threadCanonicalProjection
}

// threadCanonicalProjection advances immutable canonical entries through the
// same reducers as full adapters. It is neither provider context nor durable state.
type threadCanonicalProjection struct {
	leafID                                    string
	items                                     *threadItemProjection
	turns                                     threadTurnProjection
	context                                   agentharness.ThreadContextProjection
	anchors                                   map[string]string
	compactionOrdinals                        map[string]uint64
	changedTurns                              map[string]bool
	contextChanged, goalChanged, modelChanged bool
}

func newThreadCanonicalProjection() *threadCanonicalProjection {
	return &threadCanonicalProjection{items: newThreadItemProjection(), anchors: map[string]string{}, compactionOrdinals: map[string]uint64{}, changedTurns: map[string]bool{}}
}
func (projection *threadCanonicalProjection) append(entries []sessiontree.Entry) error {
	for _, entry := range entries {
		if projection.leafID != "" && entry.ParentID != projection.leafID {
			return fmt.Errorf("%w: incremental canonical parent changed", ErrAuthorityCorrupt)
		}
		if err := projection.items.append([]sessiontree.Entry{entry}, projection.anchors); err != nil {
			return err
		}
		if err := projection.turns.observe(entry, projection.items.itemForEntry); err != nil {
			return err
		}
		if err := projection.context.Observe(entry); err != nil {
			return runtimeHostError(err)
		}
		if entry.TurnID != "" {
			projection.changedTurns[entry.TurnID] = true
		}
		if kind := sessiontree.ThreadContextEntryKind(entry); kind != "" {
			projection.contextChanged = true
			if kind == sessiontree.ThreadContextCompactionEntryKind {
				compact, err := sessiontree.DecodeThreadContextCompactionEntry(entry)
				if err != nil {
					return runtimeHostError(err)
				}
				if _, found := projection.compactionOrdinals[compact.OperationID]; !found {
					projection.compactionOrdinals[compact.OperationID] = projection.items.ordinal
				}
			}
		}
		if entry.Type == sessiontree.EntryTurnMarker {
			projection.contextChanged = true
		}
		if entry.Type == sessiontree.EntryGoal || entry.Type == sessiontree.EntryTurnMarker {
			projection.goalChanged = true
		}
		if entry.Message.ModelContextSnapshot != nil || entry.Type == sessiontree.EntryCompaction || entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != "" {
			projection.modelChanged = true
		}
		projection.leafID = entry.ID
	}
	return nil
}

// Full initialization may touch every historical row. Retaining that map's
// capacity would make later one-entry iteration scan its historical buckets.
func resetProjectionChanges[K comparable](changes *map[K]bool) {
	if len(*changes) > 256 {
		*changes = make(map[K]bool)
	} else {
		clear(*changes)
	}
}

func (projection *threadCanonicalProjection) resetChanges() {
	resetProjectionChanges(&projection.items.changedItems)
	resetProjectionChanges(&projection.items.changedInteractions)
	resetProjectionChanges(&projection.changedTurns)
	projection.contextChanged, projection.goalChanged, projection.modelChanged = false, false, false
}
func (projection *threadCanonicalProjection) snapshot(ctx context.Context, service *threadRuntimeService, threadID identity.ThreadID, path []sessiontree.Entry) (canonicalThreadView, error) {
	items, interactions := projection.items.snapshot()
	turns, err := projection.turns.snapshot()
	if err != nil {
		return canonicalThreadView{}, err
	}
	contextSnapshot, err := projection.context.Snapshot(time.Time{})
	if err != nil {
		return canonicalThreadView{}, runtimeHostError(err)
	}
	model, err := service.readModelContext(ctx, threadID, path)
	if err != nil {
		return canonicalThreadView{}, runtimeHostError(err)
	}
	context := runtimeCanonicalContext(contextSnapshot, model, projection.anchors, projection.compactionOrdinals)
	return canonicalThreadView{ThreadView: ThreadView{ThreadID: threadID, Items: items, Interactions: interactions, Turns: turns, Context: &context, Goal: publicThreadGoal(projection.items.goal.State)}, projection: projection}, nil
}

func runtimeCanonicalContext(snapshot agentharness.ThreadContextSnapshot, model *ThreadModelContext, anchors map[string]string, ordinals map[string]uint64) ThreadContextSnapshot {
	compactions := make([]ThreadContextCompaction, 0, len(snapshot.Compactions))
	for _, compact := range snapshot.Compactions {
		compactions = append(compactions, ThreadContextCompaction{
			RunID: identity.RunID(compact.RunID), ThreadID: identity.ThreadID(compact.ThreadID), TurnID: identity.TurnID(compact.TurnID), Step: compact.Step, OperationID: compact.OperationID, RequestID: compact.RequestID, AfterItemID: anchors[compact.OperationID], CanonicalOrdinal: ordinals[compact.OperationID], Phase: compact.Phase, Status: compact.Status, Trigger: compact.Trigger, Reason: compact.Reason, Source: compact.Source, TokensBefore: compact.TokensBefore, TokensAfterEstimate: compact.TokensAfterEstimate, Decision: compact.Decision.Clone(), Error: compact.Error, ObservedAt: compact.ObservedAt})
	}
	return ThreadContextSnapshot{ModelContext: model, ContextUsage: runtimeThreadContextUsage(snapshot.ContextUsage), Model: ThreadContextModel{Provider: snapshot.Model.Provider, Model: snapshot.Model.Model}, Policy: ThreadContextPolicy{ModelMaxOutputTokens: snapshot.Policy.ModelMaxOutputTokens, ContextWindowTokens: snapshot.Policy.ContextWindowTokens, MaxOutputTokens: snapshot.Policy.MaxOutputTokens, ReservedOutputTokens: snapshot.Policy.ReservedOutputTokens}, Usage: snapshot.Usage, UsageTotals: threadTokenUsageTotals(snapshot.UsageTotals), Compactions: compactions, UpdatedAt: snapshot.UpdatedAt}
}

// advanceCanonical runs within the existing actor boundary. It reads only the
// appended interval and records explicit affected items, never a full-view diff.
func (service *threadRuntimeService) advanceCanonical(ctx context.Context, actor *threadRuntimeState) (err error) {
	defer func() {
		if err != nil {
			actor.canonical = nil
			actor.state.contextErr = err
		}
	}()
	threadID := identity.ThreadID(actor.threadID)
	if actor.canonical == nil {
		canonical, err := service.loadCanonicalThreadView(ctx, threadID)
		if err != nil {
			return err
		}
		return actor.installCanonicalView(canonical)
	}
	projection := actor.canonical
	reader, ok := service.host.store.repo.(sessiontree.PathEntriesWindowReader)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	projection.resetChanges()
	leaf := ""
	for {
		page, err := reader.PathEntriesWindow(ctx, sessiontree.PathWindowOptions{ThreadID: actor.threadID, LeafID: leaf, AnchorID: projection.leafID, Direction: sessiontree.PathWindowAfter, Limit: 256})
		if err != nil {
			actor.canonical = nil
			return runtimeHostError(err)
		}
		leaf = page.LeafID
		if err := projection.append(page.Entries); err != nil {
			actor.canonical = nil
			return err
		}
		if !page.HasAfter {
			break
		}
	}
	if projection.modelChanged {
		path, err := service.host.store.repo.Path(ctx, actor.threadID, projection.leafID)
		if err != nil {
			actor.canonical = nil
			return runtimeHostError(err)
		}
		model, err := service.readModelContext(ctx, threadID, path)
		if err != nil {
			actor.canonical = nil
			return err
		}
		if actor.state.view.Context == nil {
			actor.state.view.Context = &ThreadContextSnapshot{}
		}
		actor.state.view.Context.ModelContext = model
	}
	if err := actor.installCanonicalChanges(projection); err != nil {
		actor.canonical = nil
		return err
	}
	if actor.state.view.Context != nil && actor.state.view.Context.ModelContext != nil {
		request, found, err := cache.LatestProviderRequest(ctx, service.host.store.prompt, actor.threadID)
		if err != nil {
			return err
		}
		model := actor.state.view.Context.ModelContext
		if found && request.ThreadID == actor.threadID && request.ModelContextEntryID == model.EntryID && request.ID != model.UsedByRequestID {
			model.UsedByRequestID, model.UsedByTurnID, model.UsedByRunID = request.ID, identity.TurnID(request.TurnID), identity.RunID(request.RunID)
			at := request.CreatedAt
			model.UsedAt = &at
			actor.markComponents(threadContextChanged)
		}
	}
	actor.state.contextErr = nil
	actor.state.view.ViewVersion++
	return nil
}

func (actor *threadRuntimeState) installCanonicalChanges(projection *threadCanonicalProjection) error {
	indices := make([]int, 0, len(projection.items.changedItems))
	for index := range projection.items.changedItems {
		if !projection.items.hiddenItems[index] {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	for _, index := range indices {
		item := projection.items.items[index]
		item.Ordinal = projection.items.visibleOrdinal(index)
		current := actor.itemIndexByID(item.ID)
		if current >= 0 {
			previous := actor.state.view.Items[current]
			if previous.Kind != item.Kind || previous.TurnID != item.TurnID || previous.RunID != item.RunID || previous.CanonicalOrdinal != 0 && previous.CanonicalOrdinal != item.CanonicalOrdinal {
				return fmt.Errorf("%w: canonical item identity changed", ErrAuthorityCorrupt)
			}
			if previous.Kind == ThreadItemTool && previous.Activity != nil && item.Activity != nil && (threadToolActivityTerminal(previous.Activity.Status) || previous.Live && !threadToolActivityTerminal(item.Activity.Status)) {
				item.Activity, item.Live = previous.Activity, previous.Live
			}
			item.Ordinal = previous.Ordinal
			actor.state.view.Items[current] = cloneThreadItems([]ThreadItem{item})[0]
			actor.markItem(item.ID, ThreadItemReplaced)
		} else {
			position := int(item.Ordinal - 1)
			if position > len(actor.state.view.Items) {
				return fmt.Errorf("%w: canonical item gap", ErrAuthorityCorrupt)
			}
			actor.state.view.Items = append(actor.state.view.Items, ThreadItem{})
			copy(actor.state.view.Items[position+1:], actor.state.view.Items[position:])
			actor.state.view.Items[position] = cloneThreadItems([]ThreadItem{item})[0]
			for tail := position + 1; tail < len(actor.state.view.Items); tail++ {
				actor.state.view.Items[tail].Ordinal = uint64(tail + 1)
			}
			if position < len(actor.state.view.Items)-1 {
				actor.invalidateItemIndex()
				actor.mutations.order = true
			}
			actor.markItem(item.ID, ThreadItemAdded)
		}
	}

	if len(projection.items.changedInteractions) > 0 {
		byID := make(map[string]int, len(actor.state.view.Interactions))
		for index, item := range actor.state.view.Interactions {
			byID[item.ID] = index
		}
		changed := make([]int, 0, len(projection.items.changedInteractions))
		for index := range projection.items.changedInteractions {
			changed = append(changed, index)
		}
		sort.Ints(changed)
		for _, index := range changed {
			if projection.items.hiddenInteractions[index] {
				continue
			}
			interaction := projection.items.interactions[index]
			if previous, ok := byID[interaction.ID]; ok {
				if actor.state.view.Interactions[previous].Resolved && !interaction.Resolved {
					continue
				}
				actor.state.view.Interactions[previous] = cloneThreadInteractions([]ThreadInteraction{interaction})[0]
			} else {
				position := index
				for hidden := range projection.items.hiddenInteractions {
					if hidden < index {
						position--
					}
				}
				if position > len(actor.state.view.Interactions) {
					return fmt.Errorf("%w: canonical interaction gap", ErrAuthorityCorrupt)
				}
				actor.state.view.Interactions = append(actor.state.view.Interactions, ThreadInteraction{})
				copy(actor.state.view.Interactions[position+1:], actor.state.view.Interactions[position:])
				actor.state.view.Interactions[position] = cloneThreadInteractions([]ThreadInteraction{interaction})[0]
				for tail := position; tail < len(actor.state.view.Interactions); tail++ {
					byID[actor.state.view.Interactions[tail].ID] = tail
				}
			}
		}
		actor.markComponents(threadInteractionsChanged)
	}
	if len(projection.changedTurns) > 0 {
		indices := make([]int, 0, len(projection.changedTurns))
		for id := range projection.changedTurns {
			if index, found := projection.turns.indices[id]; found {
				indices = append(indices, index)
			}
		}
		sort.Ints(indices)
		for _, index := range indices {
			turn, _, err := projection.turns.turnSnapshot(projection.turns.order[index])
			if err != nil {
				return err
			}
			if index < len(actor.state.view.Turns) {
				if actor.state.view.Turns[index].TurnID != turn.TurnID {
					return fmt.Errorf("%w: canonical turn order changed", ErrAuthorityCorrupt)
				}
				actor.state.view.Turns[index] = turn
			} else if index == len(actor.state.view.Turns) {
				actor.state.view.Turns = append(actor.state.view.Turns, turn)
			} else {
				return fmt.Errorf("%w: canonical turn gap", ErrAuthorityCorrupt)
			}
		}
		actor.markComponents(threadTurnsChanged)
	}
	if projection.contextChanged {
		snapshot, err := projection.context.Snapshot(time.Time{})
		if err != nil {
			return runtimeHostError(err)
		}
		var model *ThreadModelContext
		if actor.state.view.Context != nil {
			model = actor.state.view.Context.ModelContext
		}
		context := runtimeCanonicalContext(snapshot, model, projection.anchors, projection.compactionOrdinals)
		actor.state.view.Context = &context
		actor.markComponents(threadContextChanged)
	}
	if projection.goalChanged {
		actor.state.view.Goal = publicThreadGoal(projection.items.goal.State)
		actor.markComponents(threadGoalChanged)
	}
	for _, index := range indices {
		item := projection.items.items[index]
		if item.Activity != nil && threadToolActivityTerminal(item.Activity.Status) {
			actor.removeArgumentPreview(item.Activity.ToolID)
		}
	}
	normalizeActiveThreadRunProgress(&actor.state.view)
	return nil
}
