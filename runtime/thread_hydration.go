package runtime

import (
	"context"
	"sort"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Cold initialization is cancellable between frozen canonical pages. Only the
// existing reducers survive each page; accumulated source bodies do not. This
// still visits historical metadata once and is not a bounded cold item index.
func (service *threadRuntimeService) loadRetainedThreadView(ctx context.Context, threadID identity.ThreadID, meta sessiontree.ThreadMeta) (canonicalThreadView, error) {
	reader, paged := service.host.store.repo.(sessiontree.PathEntriesWindowReader)
	_, body := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	_, sources := service.host.store.repo.(sessiontree.SourceBodyPageReader)
	_, activities := service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
	if !paged || !body || !sources || !activities {
		return service.loadCanonicalThreadView(ctx, threadID)
	}
	projection := newThreadCanonicalProjection()
	projection.items.restoreActivity = func(item ThreadItem) (ThreadItem, error) {
		return service.completeItemActivity(ctx, threadID.String(), item)
	}
	options := sessiontree.PathWindowOptions{ThreadID: threadID.String(), LeafID: meta.LeafID, Direction: sessiontree.PathWindowAfter, Limit: 256}
	completeContext := false
	for {
		page, err := reader.PathEntriesWindow(ctx, options)
		if err != nil {
			return canonicalThreadView{}, runtimeHostError(err)
		}
		projection.resetChanges()
		if err := projection.append(page.Entries); err != nil {
			return canonicalThreadView{}, err
		}
		for _, entry := range page.Entries {
			completeContext = completeContext || canonicalContextNeedsCompleteValidation(entry)
		}
		projection.items.releaseText()
		if !page.HasAfter {
			break
		}
		if len(page.Entries) == 0 {
			return canonicalThreadView{}, ErrAuthorityCorrupt
		}
		options.LeafID, options.AnchorID = page.LeafID, page.LastEntryID
	}
	// Context-dependent paths retain the exact existing projection validation.
	// Ordinary messages without these dependencies cannot alter managed facts.
	var model *ThreadModelContext
	if completeContext {
		path, err := service.host.store.repo.Path(ctx, threadID.String(), projection.leafID)
		if err != nil {
			return canonicalThreadView{}, runtimeHostError(err)
		}
		model, err = service.readModelContext(ctx, threadID, path)
		if err != nil {
			return canonicalThreadView{}, err
		}
	}
	canonical, err := projection.retainedSnapshot(threadID, model)
	if err != nil {
		return canonicalThreadView{}, err
	}
	for index, item := range canonical.Items {
		canonical.Items[index] = retainCanonicalItem(item, projection.leafID)
	}
	canonical.TurnID, canonical.RunID, canonical.Activity, canonical.LastOutcome, canonical.Failure = projection.turns.lifecycle.snapshot()
	if facts := projection.turns.lifecycle.turns[canonical.TurnID.String()]; facts != nil {
		canonical.TurnKind = facts.kind
		canonical.Cancellation = cloneThreadCancellation(facts.cancellation)
	}
	canonical.ParentEvents, err = service.parentEventView(ctx, threadID)
	return canonical, err
}

func canonicalContextNeedsCompleteValidation(entry sessiontree.Entry) bool {
	if canonicalEntryChangesModel(entry) || entry.Message.ControlSignal != nil {
		return true
	}
	if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && (entry.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] != "" || entry.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != "") {
		return true
	}
	switch entry.Type {
	case sessiontree.EntrySteeringRequested, sessiontree.EntrySteeringApplied, sessiontree.EntrySteeringCancelled, sessiontree.EntryInteractionDone:
		return true
	}
	return false
}

func (projection *threadCanonicalProjection) pendingSteering(turnID identity.TurnID) []string {
	indices := make([]int, 0, len(projection.items.steering.pending[turnID.String()]))
	for id := range projection.items.steering.pending[turnID.String()] {
		if index, found := projection.items.steeringItems[id]; found {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	ids := make([]string, 0, len(indices))
	for _, index := range indices {
		ids = append(ids, projection.items.items[index].ID)
	}
	return ids
}
