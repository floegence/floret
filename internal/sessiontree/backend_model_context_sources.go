package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// ModelContextSources contains the last two managed facts in canonical context
// order. Two sources preserve changed/removed keys without retaining messages.
type ModelContextSources struct {
	LeafID   string
	Entries  []Entry
	Messages []session.Message
}
type ModelContextSourceReader interface {
	ReadModelContextSources(context.Context, string, string) (ModelContextSources, error)
}
type storedModelContextSources struct {
	Leaf    contextSourceRef   `json:"leaf"`
	Sources []contextSourceRef `json:"sources,omitempty"`
}

func modelContextSourcesKey(thread string) []byte { return backendDomainKey(thread, "model-context") }

// The public identity of applied steering is its application, while the facts
// remain on the immutable request. Resolve that exact dependency through the
// steering owner; copying the request's identity would change UsedBy matching.
func modelContextSourceSnapshot(entry Entry, read func(string) (Entry, error)) (*session.ModelContextSnapshot, error) {
	if entry.Type == EntryCompaction {
		return session.CloneModelContext(entry.ModelContextBaseline), nil
	}
	if entry.Type == EntrySteeringApplied {
		resolution, err := DecodeSteeringResolution(entry)
		if err != nil {
			return nil, err
		}
		request, err := read(resolution.SteeringID)
		if err != nil || request.Type != EntrySteeringRequested || request.PathDepth >= entry.PathDepth {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		var projection steeringProjection
		if _, err := projection.observe(request); err != nil {
			return nil, err
		}
		entry, err = projection.observe(entry)
		if err != nil {
			return nil, err
		}
	}
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryModelContext, EntryParentEventsAdmitted, EntryGoalContext:
		if entry.Message.Role != "" {
			return session.CloneModelContext(entry.Message.ModelContextSnapshot), nil
		}
	}
	return nil, nil
}
func modelContextSourceRefs(path []Entry) ([]contextSourceRef, error) {
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		return nil, err
	}
	byID := make(map[string]Entry, len(path))
	for _, entry := range path {
		byID[entry.ID] = entry
	}
	var refs []contextSourceRef
	for _, message := range history {
		if message.ModelContextSnapshot == nil {
			continue
		}
		entry, found := byID[message.EntryID]
		if !found {
			return nil, ErrAuthorityCorrupt
		}
		refs = append(refs, contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth})
		if len(refs) > 2 {
			refs = refs[len(refs)-2:]
		}
	}
	return refs, nil
}

// Boundary reselection validates the original context reducer while omitting
// ordinary tool bodies and activity payloads. These bytes cannot affect managed
// facts; controls, retained users, summaries and steering remain complete.
func canonicalModelContextSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) ([]contextSourceRef, contextSourceRef, error) {
	var metadata []PathMetadata
	if err := visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, func(entry Entry) error {
		source, err := canonicalPathMetadata(entry)
		if err != nil {
			return err
		}
		metadata = append(metadata, source)
		return nil
	}); err != nil {
		return nil, contextSourceRef{}, err
	}
	return modelContextSourcesFromMetadata(ctx, tx, meta, metadata)
}

func modelContextSourcesFromMetadata(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, metadata []PathMetadata) ([]contextSourceRef, contextSourceRef, error) {
	controls := map[controlCallIdentity]bool{}
	for _, source := range metadata {
		entry := source.Entry
		if signal := entry.Message.ControlSignal; signal != nil {
			controls[controlCallIdentity{entry.ThreadID, entry.TurnID, entry.RunID, signal.CallID}] = true
		}
	}
	selected, err := ContextEntrySelection(metadata)
	if err != nil {
		return nil, contextSourceRef{}, err
	}
	byID := make(map[string]PathMetadata, len(metadata))
	var leaf contextSourceRef
	for _, source := range metadata {
		byID[source.Entry.ID] = source
		leaf = contextSourceRef{ID: source.Entry.ID, Hash: source.Entry.RawHash, Depth: source.Entry.PathDepth}
	}
	bodies := newCanonicalBodyPageReader(ctx, tx)
	var projection ModelContextReducer
	for first := 0; first < len(selected); first += 128 {
		sources := make([]PathMetadata, 0, min(128, len(selected)-first))
		for _, id := range selected[first:min(first+128, len(selected))] {
			sources = append(sources, byID[id])
		}
		page, err := materializeProjectionPage(PathMetadataWindow{LeafID: meta.LeafID, Entries: sources}, func(source PathMetadata) []string {
			entry := source.Entry
			local := entry.Type == EntryToolCall || entry.Type == EntryToolResult
			control := controls[controlCallIdentity{entry.ThreadID, entry.TurnID, entry.RunID, entry.Message.ToolCallID}]
			var fields []string
			for _, body := range source.Bodies {
				if body.Field == "activity_json" {
					continue
				}
				if local && !control && entry.Message.ToolName != "goal" && (body.Field == "arguments" || body.Field == "content") {
					continue
				}
				fields = append(fields, body.Field)
			}
			return fields
		}, bodies.prefetch, func(_ string, field CanonicalField) (string, error) {
			return bodies.read(canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes})
		})
		if err != nil {
			return nil, contextSourceRef{}, err
		}
		for _, source := range page.Entries {
			if err := projection.Append([]Entry{source.Entry}); err != nil {
				return nil, contextSourceRef{}, err
			}
		}
	}
	var refs []contextSourceRef
	for _, message := range projection.ModelContexts() {
		entry, found := byID[message.EntryID]
		if !found {
			return nil, contextSourceRef{}, ErrAuthorityCorrupt
		}
		refs = append(refs, contextSourceRef{ID: entry.Entry.ID, Hash: entry.Entry.RawHash, Depth: entry.Entry.PathDepth})
		if len(refs) > 2 {
			refs = refs[len(refs)-2:]
		}
	}
	return refs, leaf, nil
}

func appendModelContextSources(ctx context.Context, tx spi.WriteTx, entry Entry, update *canonicalMembershipAppend) error {
	var stored storedModelContextSources
	if err := readItemJSON(tx, modelContextSourcesKey(entry.ThreadID), &stored); err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return err
	}
	invalidate := func() error {
		return deleteCanonicalOptionalRecord(tx, itemMembershipNamespace, modelContextSourcesKey(entry.ThreadID))
	}
	if stored.Leaf.ID != entry.ParentID {
		return invalidate()
	}
	// Only compaction and Retry replace the selected context path. Steering,
	// interactions and controls validate their exact dependency group instead
	// of repeatedly reading every historical message body.
	reselect := entry.Type == EntryCompaction
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		source, err := CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return err
		}
		reselect = source != nil
	}
	if reselect {
		refs, _, err := canonicalModelContextSources(ctx, tx, ThreadMeta{ID: entry.ThreadID, LeafID: entry.ID})
		if err != nil {
			return err
		}
		stored.Sources = refs
	} else {
		var snapshot *session.ModelContextSnapshot
		var err error
		switch {
		case entry.Type == EntrySteeringRequested || entry.Type == EntrySteeringApplied || entry.Type == EntrySteeringCancelled:
			meta := ThreadMeta{ID: entry.ThreadID, LeafID: entry.ID}
			leaf, readErr := readEntryLocator(tx, entry.ThreadID, entry.ID)
			if readErr != nil {
				return readErr
			}
			groups, readErr := projectionSourceGroups(entry)
			if readErr != nil {
				return readErr
			}
			var projection steeringProjection
			for _, group := range groups {
				if group.Kind != "steering" {
					continue
				}
				err = visitProjectionSources(ctx, tx, meta, leaf, group, func(source Entry) error {
					complete, err := completeLocatedEntry(ctx, tx, source)
					if err != nil {
						return err
					}
					projected, err := projection.observe(complete)
					if err == nil && source.ID == entry.ID && entry.Type == EntrySteeringApplied {
						snapshot = projected.Message.ModelContextSnapshot
					}
					return err
				})
				if err != nil {
					break
				}
			}
		case entry.Type == EntryInteractionDone || entry.Message.ControlSignal != nil || entry.Type == EntryToolResult:
			if readErr := update.load(ctx, tx, entry); readErr != nil {
				return readErr
			}
			var projection ContextReducer
			err = projection.Append(update.entries)
			if err == nil {
				for _, message := range projection.Messages() {
					if message.EntryID == entry.ID {
						snapshot = message.ModelContextSnapshot
					}
				}
			}

		default:
			snapshot, err = modelContextSourceSnapshot(entry, func(string) (Entry, error) { return Entry{}, ErrAuthorityCorrupt })
		}
		if err != nil {
			return err
		}
		if snapshot != nil {
			stored.Sources = append(stored.Sources, contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth})
			if len(stored.Sources) > 2 {
				stored.Sources = stored.Sources[len(stored.Sources)-2:]
			}
		}
	}
	stored.Leaf = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
	return putItemJSON(tx, modelContextSourcesKey(entry.ThreadID), stored)
}
func readModelContextSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) (ModelContextSources, error) {
	var stored storedModelContextSources
	if err := readItemJSON(tx, modelContextSourcesKey(meta.ID), &stored); err != nil {
		return ModelContextSources{}, err
	}
	if stored.Leaf.ID != meta.LeafID {
		return ModelContextSources{}, spi.ErrConflict
	}
	if len(stored.Sources) > 2 {
		return ModelContextSources{}, ErrAuthorityCorrupt
	}
	if meta.LeafID != "" {
		leaf, err := activeLocatedEntry(ctx, tx, meta, meta.LeafID)
		if err != nil || stored.Leaf.Hash != leaf.RawHash || stored.Leaf.Depth != leaf.PathDepth {
			return ModelContextSources{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	} else if stored.Leaf.Hash != "" || stored.Leaf.Depth != 0 || len(stored.Sources) != 0 {
		return ModelContextSources{}, ErrAuthorityCorrupt
	}
	result := ModelContextSources{LeafID: meta.LeafID}
	seen := map[string]bool{}
	for _, source := range stored.Sources {
		if source.ID == "" || seen[source.ID] {
			return ModelContextSources{}, ErrAuthorityCorrupt
		}
		seen[source.ID] = true
		entry, err := activeLocatedEntry(ctx, tx, meta, source.ID)
		if err != nil || entry.RawHash != source.Hash || entry.PathDepth != source.Depth {
			return ModelContextSources{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		snapshot, err := modelContextSourceSnapshot(entry, func(id string) (Entry, error) {
			source, err := activeLocatedEntry(ctx, tx, meta, id)
			if err != nil {
				return Entry{}, err
			}
			return completeLocatedEntry(ctx, tx, source)
		})
		if err != nil {
			return ModelContextSources{}, err
		}
		if snapshot == nil {
			return ModelContextSources{}, ErrAuthorityCorrupt
		}
		result.Entries = append(result.Entries, Entry{ID: entry.ID, ThreadID: entry.ThreadID, TurnID: entry.TurnID, RunID: entry.RunID, CreatedAt: entry.CreatedAt, PathDepth: entry.PathDepth})
		result.Messages = append(result.Messages, session.Message{EntryID: entry.ID, ModelContextSnapshot: session.CloneModelContext(snapshot)})
	}
	return result, nil
}
func (repo *BackendRepo) ReadModelContextSources(ctx context.Context, thread, leaf string) (ModelContextSources, error) {
	if err := repo.prepareWindowReadSources(ctx, thread, windowPreparationNeeds{model: true}); err != nil {
		return ModelContextSources{}, err
	}
	var result ModelContextSources
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		if leaf != "" && meta.LeafID != leaf {
			return spi.ErrConflict
		}
		var err error
		result, err = readModelContextSources(ctx, newProjectionSourceTx(tx), meta)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadModelContextSources(ctx, thread, leaf)
		return err
	})
	return result, err
}
func (repo *MemoryRepo) ReadModelContextSources(ctx context.Context, thread, leaf string) (ModelContextSources, error) {
	entries, err := repo.Path(ctx, thread, leaf)
	if err != nil {
		return ModelContextSources{}, err
	}
	refs, err := modelContextSourceRefs(entries)
	if err != nil {
		return ModelContextSources{}, err
	}
	result := ModelContextSources{LeafID: leaf}
	if len(entries) > 0 {
		result.LeafID = entries[len(entries)-1].ID
	}
	byID := map[string]Entry{}
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	for _, ref := range refs {
		entry := byID[ref.ID]
		snapshot, err := modelContextSourceSnapshot(entry, func(id string) (Entry, error) {
			source, found := byID[id]
			if !found {
				return Entry{}, ErrAuthorityCorrupt
			}
			return source, nil
		})
		if err != nil || snapshot == nil {
			return ModelContextSources{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		result.Entries = append(result.Entries, Entry{ID: entry.ID, ThreadID: entry.ThreadID, TurnID: entry.TurnID, RunID: entry.RunID, CreatedAt: entry.CreatedAt, PathDepth: entry.PathDepth})
		result.Messages = append(result.Messages, session.Message{EntryID: entry.ID, ModelContextSnapshot: session.CloneModelContext(snapshot)})
	}
	return result, nil
}
