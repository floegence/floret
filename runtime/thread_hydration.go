package runtime

import (
	"context"
	"sort"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Cold initialization selects context identities from body-free metadata, then
// shares complete source pages between the timeline and checked context reducers.
// Compacted context bodies are released; timeline bodies do not accumulate.
// This still traverses history and is not a bounded cold item index.
func (service *threadRuntimeService) loadRetainedThreadView(ctx context.Context, threadID identity.ThreadID, meta sessiontree.ThreadMeta) (canonicalThreadView, error) {
	reader, paged := service.host.store.repo.(sessiontree.PathEntriesWindowReader)
	metadataReader, metadataPaged := service.host.store.repo.(sessiontree.PathWindowReader)
	_, body := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	_, sources := service.host.store.repo.(sessiontree.SourceBodyPageReader)
	_, activities := service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
	if !paged || !metadataPaged || !body || !sources || !activities {
		return service.loadCanonicalThreadView(ctx, threadID)
	}
	projection := newThreadCanonicalProjection()
	projection.items.restoreActivity = func(item ThreadItem) (ThreadItem, error) {
		return service.completeItemActivity(ctx, threadID.String(), item)
	}
	options := sessiontree.PathWindowOptions{ThreadID: threadID.String(), LeafID: meta.LeafID, Direction: sessiontree.PathWindowAfter, Limit: 256}
	selected, completeContext, err := service.coldContextSelection(ctx, metadataReader, threadID, meta)
	if err != nil {
		return canonicalThreadView{}, err
	}
	var contextProjection sessiontree.ContextReducer
	var contextSources []sessiontree.Entry
	nextContext := 0
	for {
		page, err := reader.PathEntriesWindow(ctx, options)
		if err != nil {
			return canonicalThreadView{}, runtimeHostError(err)
		}
		projection.resetChanges()
		if err := projection.append(page.Entries); err != nil {
			return canonicalThreadView{}, err
		}
		if completeContext {
			for _, entry := range page.Entries {
				if nextContext >= len(selected) || selected[nextContext] != entry.ID {
					continue
				}
				nextContext++
				if err := contextProjection.Append([]sessiontree.Entry{entry}); err != nil {
					return canonicalThreadView{}, runtimeHostError(err)
				}
				if entry.Message.ModelContextSnapshot != nil || entry.ModelContextBaseline != nil {
					contextSources = append(contextSources, sessiontree.Entry{ID: entry.ID, PathDepth: entry.PathDepth, TurnID: entry.TurnID, RunID: entry.RunID, CreatedAt: entry.CreatedAt})
				}
			}
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
	var model *ThreadModelContext
	if completeContext {
		if nextContext != len(selected) {
			return canonicalThreadView{}, ErrAuthorityCorrupt
		}
		model, err = service.modelContextFromHistory(ctx, threadID, contextSources, contextProjection.Messages())
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

// Select Retry identities with body-free metadata, then feed the already-read
// complete timeline pages to the same context reducer. Neither Retry nor a late
// context dependency causes a second complete historical body traversal.
func (service *threadRuntimeService) coldContextSelection(ctx context.Context, reader sessiontree.PathWindowReader, threadID identity.ThreadID, meta sessiontree.ThreadMeta) ([]string, bool, error) {
	options := sessiontree.PathWindowOptions{ThreadID: threadID.String(), LeafID: meta.LeafID, Direction: sessiontree.PathWindowAfter, Limit: 256}
	var metadata []sessiontree.PathMetadata
	required := false
	for {
		page, err := reader.PathMetadataWindow(ctx, options)
		if err != nil {
			return nil, false, runtimeHostError(err)
		}
		metadata = append(metadata, page.Entries...)
		for _, entry := range page.Entries {
			required = required || canonicalContextNeedsCompleteValidation(entry.Entry)
		}
		if !page.HasAfter {
			break
		}
		if len(page.Entries) == 0 || page.LastEntryID == options.AnchorID {
			return nil, false, ErrAuthorityCorrupt
		}
		options.LeafID, options.AnchorID = page.LeafID, page.LastEntryID
	}
	if !required {
		return nil, false, nil
	}
	ids, err := sessiontree.ContextEntrySelection(metadata)
	return ids, true, runtimeHostError(err)
}
