package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// Both stores project receipts in canonical path order. Timestamps and the
// source child's execution do not determine the owning parent turn.
type parentEventProjectionState struct {
	records      []ParentEventRecord
	positions    map[string]int
	parentTurnID string
	ordinal      uint64
}

func (projection *parentEventProjectionState) observe(entry Entry) error {
	projection.ordinal++
	switch entry.Type {
	case EntryTurnMarker:
		if entry.TurnStatus == TurnStarted && entry.Metadata[TurnKindMetadataKey] != TurnKindContextCompaction {
			projection.parentTurnID = entry.TurnID
		}
	case EntryParentEvent:
		event, err := decodeParentEvent(entry)
		if err != nil {
			return err
		}
		if _, duplicate := projection.positions[event.ID]; duplicate {
			return ErrAuthorityCorrupt
		}
		projection.positions[event.ID] = len(projection.records)
		projection.records = append(projection.records, ParentEventRecord{ParentEvent: event, ParentTurnID: projection.parentTurnID, CanonicalOrdinal: projection.ordinal})
	case EntryParentEventsAdmitted:
		var admission ParentEventAdmission
		if decodeBackendDomainValue(backendDomainFormat{label: "parent event"}, backendDomainRecord{Value: entry.Payload}, &admission) != nil {
			return ErrAuthorityCorrupt
		}
		first, f := projection.positions[admission.FirstEventID]
		last, l := projection.positions[admission.LastEventID]
		if !f || !l || first > last {
			return ErrAuthorityCorrupt
		}
		for i := first; i <= last; i++ {
			if projection.records[i].Admitted {
				return ErrAuthorityCorrupt
			}
			projection.records[i].Admitted = true
		}
	}
	return nil
}

func canonicalParentEventRecords(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) ([]ParentEventRecord, error) {
	projection := parentEventProjectionState{positions: map[string]int{}}
	visit := func(entry Entry) error {
		projection.ordinal = uint64(entry.PathDepth - 1)
		return projection.observe(entry)
	}
	through, err := sparseCoverage(tx, meta.ID)
	if err != nil {
		return nil, err
	}
	count, err := readJournalCount(tx, meta.ID)
	if err != nil {
		return nil, err
	}
	if through == count {
		var leaf entryLocator
		if meta.LeafID != "" {
			leaf, err = readEntryLocator(tx, meta.ID, meta.LeafID)
		}
		if err != nil {
			return nil, err
		}
		err = visitSparseSources(ctx, tx, meta.ID, "parent", func(entry Entry) error {
			if entry.PathDepth > leaf.Depth {
				return nil
			}
			ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, entry.PathDepth)
			if err != nil {
				return err
			}
			if ancestor.ID != entry.ID {
				return nil
			}
			return visit(entry)
		})
	} else {
		// Canonical write validation may run before a released store's derived
		// locators are prepared. It retains the exact original source traversal.
		err = visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, visit)
	}
	if err != nil {
		return nil, err
	}
	if err := resolveCanonicalParentInteractions(ctx, tx, projection.records); err != nil {
		return nil, err
	}
	return projection.records, nil
}

func resolveCanonicalParentInteractions(ctx context.Context, tx spi.ReadTx, records []ParentEventRecord) error {
	type sourceTurn struct{ threadID, turnID string }
	pending := map[sourceTurn][]runtimePendingInteraction{}
	for i := range records {
		event := &records[i]
		if event.InteractionID == "" {
			continue
		}
		key := sourceTurn{event.SourceThreadID, event.SourceTurnID}
		interactions, loaded := pending[key]
		if !loaded {
			if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, key.threadID); errors.Is(err, ErrThreadDeleted) {
				interactions = nil
			} else if err != nil {
				return err
			} else {
				entries, err := canonicalRuntimeTurnEntries(ctx, tx, key.threadID, key.turnID)
				if err != nil {
					return err
				}
				interactions = runtimePendingInteractionsIn(entries, key.turnID)
			}
			pending[key] = interactions
		}
		event.Resolved = true
		for _, interaction := range interactions {
			if interaction.ID == event.InteractionID {
				event.Resolved = false
				break
			}
		}
	}
	return nil
}

func (repo *BackendRepo) ParentEvents(ctx context.Context, query ParentEventQuery) (ParentEventPage, error) {
	for {
		page, err := repo.parentEventPage(ctx, query)
		var missing *parentInteractionPreparationRequired
		if !errors.As(err, &missing) {
			return page, err
		}
		if err := repo.prepareProjectionSources(ctx, missing.thread); err != nil {
			return ParentEventPage{}, err
		}
	}
}

func (repo *BackendRepo) parentEventPage(ctx context.Context, query ParentEventQuery) (ParentEventPage, error) {
	if err := repo.prepareProjectionSources(ctx, query.ThreadID); err != nil {
		return ParentEventPage{}, err
	}
	var result ParentEventPage
	err := repo.canonicalThreadView(ctx, query.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = readCanonicalParentEventPage(ctx, tx, meta, query)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ParentEvents(ctx, query)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ReportToParent(ctx context.Context, req ParentReportRequest) (ParentEvent, error) {
	if err := validateParentReport(req.Message); err != nil {
		return ParentEvent{}, err
	}
	if err := validateEffectInvocation(req.Invocation); err != nil {
		return ParentEvent{}, err
	}
	if strings.TrimSpace(req.RequestKey) == "" {
		return ParentEvent{}, ErrRequestConflict
	}
	inv := req.Invocation
	var result ParentEvent
	err := repo.canonicalJournalUpdate(ctx, inv.ThreadID, func(mutation *canonicalJournalMutation) error {
		if !mutation.repo.parentEventsEnabled() {
			return errors.New("parent events are disabled")
		}
		if mutation.meta.ParentThreadID == "" {
			return ErrInvalidParent
		}
		parent, err := readPublishedThreadMeta(mutation.tx, backendDomainV32Format, mutation.meta.ParentThreadID)
		if errors.Is(err, ErrThreadNotFound) {
			return ErrInvalidParent
		}
		if err != nil {
			return err
		}
		if err := lifecycleRejectsWrite(parent); err != nil {
			return err
		}
		id := "parent-event:" + StableHash(strings.Join([]string{inv.ThreadID, inv.TurnID, inv.RunID, inv.ToolCallID}, "\x00"))[:32]
		if existing, err := readCanonicalEntryMetadata(mutation.tx, parent.ID, id); err == nil {
			event, err := decodeParentEvent(existing)
			if err != nil {
				return err
			}
			source, err := readCanonicalEntryMetadata(mutation.tx, inv.ThreadID, event.SourceEntryID)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			attempt, err := decodeEffectAttempt(source)
			if err != nil || attempt.Invocation != inv || event.Message != req.Message {
				return ErrRequestConflict
			}
			result = event
			return nil
		} else if !errors.Is(err, ErrEntryNotFound) {
			return err
		}
		cancelled, err := canonicalTurnAuthority(mutation.tx, inv.ThreadID, inv.TurnID, inv.RunID)
		if err != nil {
			return err
		}
		if cancelled {
			return ErrStaleAuthority
		}
		if err := lifecycleRejectsWrite(mutation.meta); err != nil {
			return err
		}
		attempt, err := readCanonicalEffectAttempt(mutation.tx, inv.ThreadID, effectAttemptID(inv))
		if errors.Is(err, ErrEffectAttemptNotFound) || err == nil && (attempt.Invocation != inv || attempt.State != EffectAttemptDispatching) {
			return ErrStaleAuthority
		}
		if err != nil {
			return err
		}
		result = ParentEvent{ID: id, ParentThreadID: parent.ID, SourceThreadID: inv.ThreadID, SourceTurnID: inv.TurnID, SourceRunID: inv.RunID, SourceEntryID: effectAttemptEntryID(attempt.EffectAttemptID, attempt.State), Kind: "report", Message: req.Message, ReceivedAt: mutation.repo.now().UTC()}
		appended, err := appendCanonicalParentEvent(ctx, mutation.tx, result)
		if appended {
			mutation.parentID = parent.ID
		}
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReportToParent(ctx, req)
		return err
	})
	if err != nil {
		return ParentEvent{}, err
	}
	return result, nil
}

func (repo *BackendRepo) AdmitParentEvents(ctx context.Context, threadID, turnID, runID string) (Entry, error) {
	var result Entry
	err := repo.canonicalJournalUpdate(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		if !mutation.repo.parentEventsEnabled() {
			return nil
		}
		cancelled, err := canonicalTurnAuthority(mutation.tx, threadID, turnID, runID)
		if err != nil {
			return err
		}
		if cancelled {
			return ErrStaleAuthority
		}
		entries, err := canonicalRuntimeTurnEntries(ctx, mutation.tx, threadID, turnID)
		if err != nil {
			return err
		}
		if len(runtimePendingToolCallsIn(entries, turnID)) != 0 {
			return ErrRequestConflict
		}
		records, err := canonicalParentEventRecords(ctx, mutation.tx, mutation.meta)
		if err != nil {
			return err
		}
		pending := records[:0]
		for _, record := range records {
			if !record.Admitted {
				pending = append(pending, record)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		text, err := renderParentEvents(pending)
		if err != nil {
			return err
		}
		payload, _ := json.Marshal(ParentEventAdmission{FirstEventID: pending[0].ID, LastEventID: pending[len(pending)-1].ID})
		id := "parent-admitted:" + StableHash(string(payload))[:32]
		result, err = mutation.append(Entry{ID: id, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryParentEventsAdmitted, Payload: payload, Message: session.Message{Role: session.User, Kind: session.MessageKindParentEvents, Content: text}}, AppendOptions{ID: id})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.AdmitParentEvents(ctx, threadID, turnID, runID)
		return err
	})
	if err != nil {
		return Entry{}, err
	}
	return result, nil
}
