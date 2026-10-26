package runtime

import (
	"context"
	"fmt"
	"sort"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Preparation reads a frozen appended interval while the accepted actor view
// remains available. Installation checks that the same reducer still owns it.
type terminalCanonicalRead struct {
	projection   *threadCanonicalProjection
	tail         canonicalThreadTail
	version      uint64
	source       *threadSourceProjection
	full         *canonicalThreadView
	model        *ThreadModelContext
	modelChanged bool
	parentEvents *ParentEventPage
}

func (service *threadRuntimeService) readTerminalCanonical(ctx context.Context, actor *threadRuntimeState) (terminalCanonicalRead, error) {
	read := terminalCanonicalRead{}
	if err := actor.apply(ctx, func() error {
		read.source = actor.sourceWindow
		read.projection = actor.canonical
		read.version = actor.state.view.ViewVersion
		if read.projection != nil {
			read.tail.after = read.projection.leafID
		}
		if actor.state.view.Context != nil && actor.state.view.Context.ModelContext != nil {
			copy := *actor.state.view.Context.ModelContext
			read.model = &copy
		}
		return nil
	}); err != nil {
		return read, err
	}
	if read.source != nil {
		full, page, err := service.loadSourceWindow(ctx, ThreadWindowInput{ThreadID: identity.ThreadID(actor.threadID), Limit: 1, ActivityFields: true})
		page.input = ThreadWindowInput{ThreadID: identity.ThreadID(actor.threadID), Limit: 1, ActivityFields: true}
		read.full, read.source = &full, &page
		return read, err
	}
	_, bodyReader := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	_, sourceReader := service.host.store.repo.(sessiontree.SourceBodyPageReader)
	if read.projection == nil || !bodyReader || !sourceReader {
		full, err := service.loadCanonicalThreadView(ctx, identity.ThreadID(actor.threadID))
		read.full = &full
		return read, err
	}
	tail, err := service.readCanonicalTail(ctx, actor.threadID, read.tail.after)
	if err != nil {
		return read, err
	}
	read.tail = tail
	for _, entry := range tail.entries {
		if canonicalEntryChangesModel(entry) {
			read.modelChanged = true
		}
	}
	if read.modelChanged {
		path, err := service.host.store.repo.Path(ctx, actor.threadID, tail.leaf)
		if err != nil {
			return read, runtimeHostError(err)
		}
		read.model, err = service.readModelContext(ctx, identity.ThreadID(actor.threadID), path)
		if err != nil {
			return read, err
		}
	}
	read.parentEvents, err = service.parentEventView(ctx, identity.ThreadID(actor.threadID))
	if err != nil {
		return read, err
	}
	read.model, read.modelChanged, err = service.resolveCanonicalModel(ctx, actor.threadID, read.model, read.modelChanged)
	return read, err
}

func (read terminalCanonicalRead) current(actor *threadRuntimeState) bool {
	if read.projection == nil {
		return actor.canonical == nil && actor.state.view.ViewVersion == read.version
	}
	return actor.canonical == read.projection && actor.canonical.leafID == read.tail.after
}

func (service *threadRuntimeService) installTerminalCanonical(ctx context.Context, actor *threadRuntimeState, read terminalCanonicalRead) (canonicalThreadView, error) {
	if read.full != nil {
		canonical := *read.full
		if read.source != nil {
			service.installSourceTail(actor, canonical, *read.source)
			return canonical, nil
		}
		actor.sourceWindow = nil
		actor.canonical = canonical.projection
		actor.state.view.Turns = canonical.Turns
		actor.replaceItems(canonical.Items)
		actor.state.view.ParentEvents = canonical.ParentEvents
		actor.state.view.Goal = canonical.Goal
		actor.state.view.Context = canonical.Context
		actor.markComponents(threadContextChanged | threadGoalChanged | threadParentEventsChanged | threadTurnsChanged | threadInteractionsChanged | threadPreviewsChanged)
		actor.replaceInteractions(mergeThreadInteractions(actor.state.view.Interactions, canonical.Interactions))
		applyThreadInteractionsToItems(actor.state.view.Items, actor.state.view.Interactions)
		actor.retainItems()
		actor.observedItemIDs = nil
		return canonical, nil
	}
	projection := actor.canonical
	projection.resetChanges()
	if err := projection.append(read.tail.entries); err != nil {
		actor.canonical = nil
		return canonicalThreadView{}, err
	}
	// Restore only observations that could differ from their accepted canonical
	// fact. Unchanged historical items are neither scanned nor republished.
	removed := []string{}
	for id := range actor.observedItemIDs {
		index, found := projection.items.presentationIndex[id]
		if found && !projection.items.hiddenItems[index] {
			projection.items.changedItems[index] = true
		} else if current := actor.itemIndexByID(id); current >= 0 {
			if actor.state.view.Items[current].CanonicalOrdinal != 0 {
				actor.canonical = nil
				return canonicalThreadView{}, fmt.Errorf("%w: accepted item lost canonical identity", ErrAuthorityCorrupt)
			}
			removed = append(removed, id)
		}
	}
	// Reduce pending source deltas to exact extents before terminal replacement.
	projection.items.releaseText()
	if err := actor.installCanonicalChangesMode(projection, true); err != nil {
		actor.canonical = nil
		return canonicalThreadView{}, err
	}
	positions := make([]int, 0, len(removed))
	for _, id := range removed {
		if index := actor.itemIndexByID(id); index >= 0 {
			positions = append(positions, index)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(positions)))
	for _, index := range positions {
		id := actor.state.view.Items[index].ID
		copy(actor.state.view.Items[index:], actor.state.view.Items[index+1:])
		actor.state.view.Items[len(actor.state.view.Items)-1] = ThreadItem{}
		actor.state.view.Items = actor.state.view.Items[:len(actor.state.view.Items)-1]
		actor.markItem(id, ThreadItemRemoved)
		for next := index; next < len(actor.state.view.Items); next++ {
			actor.state.view.Items[next].Ordinal = uint64(next + 1)
		}
	}
	if len(positions) > 0 {
		actor.invalidateItemIndex()
		actor.mutations.order = true
	}
	actor.installCanonicalModel(read.model, read.modelChanged)
	actor.state.view.ParentEvents = read.parentEvents
	actor.markComponents(threadParentEventsChanged)
	actor.observedItemIDs = nil
	actor.state.attemptDraftIDs = nil
	canonical := canonicalThreadView{}
	canonical.TurnID, canonical.RunID, canonical.Activity, canonical.LastOutcome, canonical.Failure = projection.turns.lifecycle.snapshot()
	if facts := projection.turns.lifecycle.turns[canonical.TurnID.String()]; facts != nil {
		canonical.Cancellation = cloneThreadCancellation(facts.cancellation)
		canonical.TurnKind = facts.kind
	}
	return canonical, nil
}

func canonicalEntryChangesModel(entry sessiontree.Entry) bool {
	return entry.Message.ModelContextSnapshot != nil || entry.Type == sessiontree.EntryCompaction || entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != ""
}
