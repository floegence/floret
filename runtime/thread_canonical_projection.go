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
	terminalItems                             map[int]bool
	terminalCounts                            map[identity.TurnID]int
	contextChanged, goalChanged, modelChanged bool
}

func newThreadCanonicalProjection() *threadCanonicalProjection {
	return &threadCanonicalProjection{items: newThreadItemProjection(), anchors: map[string]string{}, compactionOrdinals: map[string]uint64{}, changedTurns: map[string]bool{}, terminalItems: map[int]bool{}, terminalCounts: map[identity.TurnID]int{}}
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
		if canonicalEntryChangesModel(entry) {
			projection.modelChanged = true
		}
		projection.leafID = entry.ID
	}
	for index := range projection.items.changedItems {
		item := projection.items.items[index]
		previous := projection.terminalItems[index]
		current := !projection.items.hiddenItems[index] && threadItemTerminalPresentation(item)
		if current != previous {
			if current {
				projection.terminalCounts[item.TurnID]++
				projection.terminalItems[index] = true
			} else {
				projection.terminalCounts[item.TurnID]--
				delete(projection.terminalItems, index)
			}
		}
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
	if projection.items.metadataOnly {
		return canonicalThreadView{}, fmt.Errorf("%w: complete snapshot requires complete source entries", ErrAuthorityCorrupt)
	}
	model, err := service.readModelContext(ctx, threadID, path)
	if err != nil {
		return canonicalThreadView{}, runtimeHostError(err)
	}
	return projection.retainedSnapshot(threadID, model)
}

// Retained assembly shares all canonical reducers with complete adapters; only
// the latter reconstruct omitted body fields after leaving the actor boundary.
func (projection *threadCanonicalProjection) retainedSnapshot(threadID identity.ThreadID, model *ThreadModelContext) (canonicalThreadView, error) {
	items, interactions := projection.items.snapshot()
	turns, err := projection.turns.snapshot()
	if err != nil {
		return canonicalThreadView{}, err
	}
	contextSnapshot, err := projection.context.Snapshot(time.Time{})
	if err != nil {
		return canonicalThreadView{}, runtimeHostError(err)
	}
	context := runtimeCanonicalContext(contextSnapshot, model, projection.anchors, projection.compactionOrdinals)
	return canonicalThreadView{ThreadView: ThreadView{ThreadID: threadID, Items: cloneThreadItems(items), Interactions: cloneThreadInteractions(interactions), Turns: turns, Context: &context, Goal: publicThreadGoal(projection.items.goal.State)}, projection: projection}, nil
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
	tail, err := service.readCanonicalTail(ctx, actor.threadID, projection.leafID)
	if err != nil {
		return err
	}
	projection.resetChanges()
	if err := projection.append(tail.entries); err != nil {
		return err
	}
	var model *ThreadModelContext
	if actor.state.view.Context != nil && actor.state.view.Context.ModelContext != nil {
		copy := *actor.state.view.Context.ModelContext
		model = &copy
	}
	if projection.modelChanged {
		path, err := service.host.store.repo.Path(ctx, actor.threadID, projection.leafID)
		if err != nil {
			return runtimeHostError(err)
		}
		model, err = service.readModelContext(ctx, threadID, path)
		if err != nil {
			return err
		}
	}
	return service.installCanonicalTail(ctx, actor, model, projection.modelChanged)
}

// A frozen appended interval can be read without holding the actor mutex. Its
// starting leaf is checked again before the existing reducer accepts it.
type canonicalThreadTail struct {
	after, leaf string
	entries     []sessiontree.Entry
}

func (service *threadRuntimeService) readCanonicalTail(ctx context.Context, threadID, after string) (canonicalThreadTail, error) {
	reader, ok := service.host.store.repo.(sessiontree.PathEntriesWindowReader)
	if !ok {
		return canonicalThreadTail{}, ErrUnsupportedStoreCapability
	}
	tail := canonicalThreadTail{after: after}
	anchor := after
	for {
		page, err := reader.PathEntriesWindow(ctx, sessiontree.PathWindowOptions{ThreadID: threadID, LeafID: tail.leaf, AnchorID: anchor, Direction: sessiontree.PathWindowAfter, Limit: 256})
		if err != nil {
			return canonicalThreadTail{}, runtimeHostError(err)
		}
		tail.leaf = page.LeafID
		tail.entries = append(tail.entries, page.Entries...)
		if !page.HasAfter {
			break
		}
		if len(page.Entries) == 0 {
			return canonicalThreadTail{}, fmt.Errorf("%w: canonical interval made no progress", ErrAuthorityCorrupt)
		}
		anchor = page.Entries[len(page.Entries)-1].ID
	}
	return tail, nil
}

func (service *threadRuntimeService) installCanonicalTail(ctx context.Context, actor *threadRuntimeState, model *ThreadModelContext, modelChanged bool) error {
	model, modelChanged, err := service.resolveCanonicalModel(ctx, actor.threadID, model, modelChanged)
	if err != nil {
		return err
	}
	if err := actor.installCanonicalChanges(actor.canonical); err != nil {
		return err
	}
	actor.installCanonicalModel(model, modelChanged)
	actor.state.contextErr = nil
	actor.state.view.ViewVersion++
	return nil
}

func (service *threadRuntimeService) resolveCanonicalModel(ctx context.Context, threadID string, model *ThreadModelContext, modelChanged bool) (*ThreadModelContext, bool, error) {
	if model != nil {
		request, found, err := cache.LatestProviderRequest(ctx, service.host.store.prompt, threadID)
		if err != nil {
			return nil, false, err
		}
		if found && request.ThreadID == threadID && request.ModelContextEntryID == model.EntryID && request.ID != model.UsedByRequestID {
			copy := *model
			model = &copy
			model.UsedByRequestID, model.UsedByTurnID, model.UsedByRunID = request.ID, identity.TurnID(request.TurnID), identity.RunID(request.RunID)
			at := request.CreatedAt
			model.UsedAt = &at
			modelChanged = true
		}
	}
	return model, modelChanged, nil
}

func (actor *threadRuntimeState) installCanonicalModel(model *ThreadModelContext, modelChanged bool) {
	if modelChanged {
		if actor.state.view.Context == nil {
			actor.state.view.Context = &ThreadContextSnapshot{}
		}
		actor.state.view.Context.ModelContext = model
		actor.markComponents(threadContextChanged)
	}
}

func (actor *threadRuntimeState) installCanonicalChanges(projection *threadCanonicalProjection) error {
	return actor.installCanonicalChangesMode(projection, false)
}

func (actor *threadRuntimeState) installCanonicalChangesMode(projection *threadCanonicalProjection, terminal bool) error {
	indices := make([]int, 0, len(projection.items.changedItems))
	for index := range projection.items.changedItems {
		if !projection.items.hiddenItems[index] {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	itemCount := len(actor.state.view.Items)
	prepared := make(map[int]ThreadItem, len(indices))
	for _, index := range indices {
		item := projection.items.items[index]
		if current := actor.itemIndexByID(item.ID); current >= 0 {
			previous := actor.state.view.Items[current]
			if item.textOmitted && !terminal {
				previousBytes := len(previous.Text)
				if previous.textOmitted {
					previousBytes = previous.textBytes
				}
				if previousBytes+len(item.Text) != item.textBytes {
					return fmt.Errorf("%w: canonical text extent changed", ErrAuthorityCorrupt)
				}
				if previous.textOmitted {
					item.textSummary, item.textSummaryTail = canonicalTextSummary(previous.textSummary, item.Text, previous.textSummaryTail)
					item.Text = ""
				} else {
					item.Text = previous.Text + item.Text
					item.textOmitted, item.textBytes, item.textNonSpace = false, 0, false
				}
			}
			if previous.Kind != item.Kind || previous.TurnID != item.TurnID || previous.RunID != item.RunID || previous.CanonicalOrdinal != 0 && previous.CanonicalOrdinal != item.CanonicalOrdinal {
				return fmt.Errorf("%w: canonical item identity changed", ErrAuthorityCorrupt)
			}
		} else {
			position := int(projection.items.visibleOrdinal(index) - 1)
			if position < 0 || position > itemCount {
				return fmt.Errorf("%w: canonical item gap", ErrAuthorityCorrupt)
			}
			itemCount++
		}
		prepared[index] = actor.retainItem(item, projection.leafID)
	}
	turnChanges := make(map[int]ThreadTurn, len(projection.changedTurns))
	turnIndices := make([]int, 0, len(projection.changedTurns))
	for id := range projection.changedTurns {
		if index, found := projection.turns.indices[id]; found {
			turn, _, err := projection.turns.turnValue(id)
			if err != nil {
				return err
			}
			turnChanges[index] = turn
			turnIndices = append(turnIndices, index)
		}
	}
	sort.Ints(turnIndices)
	turnCount := len(actor.state.view.Turns)
	for _, index := range turnIndices {
		if index < turnCount {
			if actor.state.view.Turns[index].TurnID != turnChanges[index].TurnID {
				return fmt.Errorf("%w: canonical turn order changed", ErrAuthorityCorrupt)
			}
		} else if index == turnCount {
			turnCount++
		} else {
			return fmt.Errorf("%w: canonical turn gap", ErrAuthorityCorrupt)
		}
	}
	var context *ThreadContextSnapshot
	if projection.contextChanged {
		snapshot, err := projection.context.Snapshot(time.Time{})
		if err != nil {
			return runtimeHostError(err)
		}
		var model *ThreadModelContext
		if actor.state.view.Context != nil {
			model = actor.state.view.Context.ModelContext
		}
		value := runtimeCanonicalContext(snapshot, model, projection.anchors, projection.compactionOrdinals)
		context = &value
	}
	interactionIndices := make([]int, 0, len(projection.items.changedInteractions))
	interactionCount := len(actor.state.view.Interactions)
	for index := range projection.items.changedInteractions {
		if !projection.items.hiddenInteractions[index] {
			interactionIndices = append(interactionIndices, index)
		}
	}
	sort.Ints(interactionIndices)
	for _, index := range interactionIndices {
		position := index
		for hidden := range projection.items.hiddenInteractions {
			if hidden < index {
				position--
			}
		}
		if position > interactionCount {
			return fmt.Errorf("%w: canonical interaction gap", ErrAuthorityCorrupt)
		}
		if position == interactionCount {
			interactionCount++
		}
	}
	// All storage reads and fallible projection checks precede installation.
	for _, index := range indices {
		item := prepared[index]
		item.Ordinal = projection.items.visibleOrdinal(index)
		current := actor.itemIndexByID(item.ID)
		if current >= 0 {
			previous := actor.state.view.Items[current]
			preservedObservation := false
			if !terminal && actor.observedItemIDs[item.ID] && previous.Kind == ThreadItemTool && previous.Activity != nil && item.Activity != nil && (threadToolActivityTerminal(previous.Activity.Status) || previous.Live && !threadToolActivityTerminal(item.Activity.Status)) {
				item.Activity, item.Live = previous.Activity, previous.Live
				item.activityOmitted = previous.activityOmitted
				item.activityEntryIDs = previous.activityEntryIDs
				item.activitySummary = previous.activitySummary
				preservedObservation = true
			}
			if !preservedObservation {
				delete(actor.observedItemIDs, item.ID)
			}
			item.Ordinal = previous.Ordinal
			if previous.Interaction == nil && item.Interaction != nil && actor.interactionItems != nil {
				actor.interactionItems[item.Interaction.ID] = append(actor.interactionItems[item.Interaction.ID], item.ID)
			}
			actor.state.view.Items[current] = cloneThreadItems([]ThreadItem{item})[0]
			actor.markItem(item.ID, ThreadItemReplaced)
		} else {
			position := int(item.Ordinal - 1)
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

	for _, index := range interactionIndices {
		position := index
		for hidden := range projection.items.hiddenInteractions {
			if hidden < index {
				position--
			}
		}
		actor.installInteraction(projection.items.interactions[index], position)
	}

	for _, index := range turnIndices {
		turn := turnChanges[index]
		if index < len(actor.state.view.Turns) {
			actor.state.view.Turns[index] = turn
		} else {
			actor.state.view.Turns = append(actor.state.view.Turns, turn)
		}
	}
	if len(turnIndices) > 0 {
		actor.markComponents(threadTurnsChanged)
	}
	if context != nil {
		actor.state.view.Context = context
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
	actor.normalizeRunProgress()
	projection.items.releaseText()
	return nil
}
