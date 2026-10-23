package runtime

import (
	"context"
	"sort"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// Cold initialization selects context identities from body-free metadata, then
// shares selected source fields between the timeline and checked context reducers.
// Ordinary tool activity bodies load only for selected windows or explicit full
// reads. Context bodies remain complete; compacted bodies are released.
// This still traverses history and is not a bounded cold item index.
func (service *threadRuntimeService) loadRetainedThreadView(ctx context.Context, threadID identity.ThreadID, meta sessiontree.ThreadMeta) (canonicalThreadView, error) {
	scanner, sharedScan := service.host.store.repo.(sessiontree.PathProjectionScanner)
	_, body := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	_, sources := service.host.store.repo.(sessiontree.SourceBodyPageReader)
	_, activities := service.host.store.repo.(sessiontree.CanonicalBodyMetadataReader)
	if !sharedScan || !body || !sources || !activities {
		// Custom JournalRepo implementations retain their complete adapter.
		return service.loadCanonicalThreadView(ctx, threadID)
	}
	projection := newThreadCanonicalProjection()
	projection.items.restoreActivity = func(item ThreadItem) (ThreadItem, error) {
		return service.completeItemActivity(ctx, threadID.String(), item)
	}
	projection.items.deferActivity = true
	var selected []string
	var completeContext bool
	var err error
	var contextProjection sessiontree.ContextReducer
	var contextSources []sessiontree.Entry
	nextContext := 0
	var selectedIDs map[string]bool
	controlResults := make(map[string]bool)
	selectContext := func(metadata []sessiontree.PathMetadata) error {
		selected, completeContext, err = coldContextSelection(metadata)
		selectedIDs = make(map[string]bool, len(selected))
		for _, id := range selected {
			selectedIDs[id] = true
		}
		for _, source := range metadata {
			if signal := source.Entry.Message.ControlSignal; signal != nil {
				controlResults[threadToolKey(identity.TurnID(source.Entry.TurnID), identity.RunID(source.Entry.RunID), signal.CallID)] = true
			}
		}
		return err
	}
	// RetainAppliedSteering can place a request after an intervening model
	// context. Preserve the selected order while reading canonical pages once.
	pendingContext := make(map[string]sessiontree.Entry)
	consume := func(entries []sessiontree.Entry) error {
		projection.resetChanges()
		if err := projection.append(entries); err != nil {
			return err
		}
		if completeContext {
			for _, entry := range entries {
				if !selectedIDs[entry.ID] {
					continue
				}
				pendingContext[entry.ID] = entry
				for nextContext < len(selected) {
					next, found := pendingContext[selected[nextContext]]
					if !found {
						break
					}
					delete(pendingContext, next.ID)
					nextContext++
					if err := contextProjection.Append([]sessiontree.Entry{next}); err != nil {
						return runtimeHostError(err)
					}
					if next.Message.ModelContextSnapshot != nil || next.ModelContextBaseline != nil {
						contextSources = append(contextSources, sessiontree.Entry{ID: next.ID, PathDepth: next.PathDepth, TurnID: next.TurnID, RunID: next.RunID, CreatedAt: next.CreatedAt})
					}
				}
			}
		}
		projection.items.releaseText()
		return nil
	}
	fields := func(source sessiontree.PathMetadata) []string {
		entry := source.Entry
		control := controlResults[threadToolKey(identity.TurnID(entry.TurnID), identity.RunID(entry.RunID), entry.Message.ToolCallID)]
		return coldProjectionFields(source, selectedIDs[entry.ID], control)
	}
	err = scanner.ScanPathProjection(ctx, threadID.String(), meta.LeafID, selectContext, fields, func(page sessiontree.PathProjectionPage) error {
		entries := make([]sessiontree.Entry, len(page.Entries))
		for i, source := range page.Entries {
			entries[i] = source.Entry
		}
		return consume(entries)
	})
	if err != nil {
		return canonicalThreadView{}, runtimeHostError(err)
	}
	projection.items.deferActivity = false
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

func coldProjectionFields(source sessiontree.PathMetadata, contextSelected, controlResult bool) []string {
	fields := make([]string, 0, len(source.Bodies))
	localTool := source.Entry.Type == sessiontree.EntryToolCall || source.Entry.Type == sessiontree.EntryToolResult
	for _, body := range source.Bodies {
		// Only ordinary local activity has an exact call/result reconstruction.
		// Hosted tools and Goal keep their existing complete semantic reducer.
		if body.Field == "activity_json" && localTool && source.Entry.Message.ToolName != "goal" {
			continue
		}
		// Local tool parameters and results have no timeline text. The checked
		// context still receives every selected body; Goal and control-pair
		// validation retain their semantic inputs even outside that selection.
		if localTool && !contextSelected && !controlResult && source.Entry.Message.ToolName != "goal" && (body.Field == "arguments" || body.Field == "content") {
			continue
		}
		fields = append(fields, body.Field)
	}
	return fields
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

func coldContextSelection(metadata []sessiontree.PathMetadata) ([]string, bool, error) {
	required := false
	for _, entry := range metadata {
		required = required || canonicalContextNeedsCompleteValidation(entry.Entry)
	}
	if !required {
		return nil, false, nil
	}
	ids, err := sessiontree.ContextEntrySelection(metadata)
	return ids, true, runtimeHostError(err)
}
