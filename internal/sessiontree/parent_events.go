package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"

	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

const (
	EntryParentEvent          EntryType = "parent_event"
	EntryParentEventsAdmitted EntryType = "parent_events_admitted"
	ParentReportMaxBytes                = 8192
	ParentEventMaxBytes                 = 2048
	ParentEventMaxTokens                = 512
	ParentEventBatchMaxBytes            = 8192
	ParentEventBatchMaxTokens           = 2048
)

// ParentEvent stores an explicit report once, or references a canonical source.
// Receipt never changes the recipient's execution or interaction state.
type ParentEvent struct {
	ID             string    `json:"id"`
	ParentThreadID string    `json:"parent_thread_id"`
	SourceThreadID string    `json:"source_thread_id"`
	SourceTurnID   string    `json:"source_turn_id"`
	SourceRunID    string    `json:"source_run_id"`
	SourceEntryID  string    `json:"source_entry_id"`
	Kind           string    `json:"kind"`
	ReasonCode     string    `json:"reason_code,omitempty"`
	InteractionID  string    `json:"interaction_id,omitempty"`
	Message        string    `json:"message,omitempty"`
	ReceivedAt     time.Time `json:"received_at"`
}

type ParentEventAdmission struct {
	FirstEventID string `json:"first_event_id"`
	LastEventID  string `json:"last_event_id"`
}

type ParentReportRequest struct {
	Invocation EffectInvocationIdentity
	RequestKey string
	Message    string
}

type ParentEventQuery struct {
	ThreadID        string
	SourceThreadIDs []string
	EventID         string
	FirstEventID    string
	LastEventID     string
	AfterEventID    string
	Limit           int
	PendingOnly     bool
}

type ParentEventRecord struct {
	ParentEvent
	Admitted bool `json:"admitted"`
	Resolved bool `json:"resolved,omitempty"`
}

type ParentEventPage struct {
	Events           []ParentEventRecord
	FirstEventID     string
	LastEventID      string
	NextAfterEventID string
}

type ParentEventRepo interface {
	ReportToParent(context.Context, ParentReportRequest) (ParentEvent, error)
	ParentEvents(context.Context, ParentEventQuery) (ParentEventPage, error)
	AdmitParentEvents(context.Context, string, string, string) (Entry, error)
}

func decodeParentEvent(entry Entry) (ParentEvent, error) {
	var event ParentEvent
	if err := decodeBackendDomainValue(backendDomainFormat{label: "parent event"}, backendDomainRecord{Value: entry.Payload}, &event); err != nil {
		return event, fmt.Errorf("%w: invalid parent event: %v", ErrAuthorityCorrupt, err)
	}
	if event.ID != entry.ID || event.ParentThreadID == "" || event.SourceThreadID == "" || event.SourceTurnID == "" || event.SourceRunID == "" || event.SourceEntryID == "" || event.ReceivedAt.IsZero() {
		return event, ErrAuthorityCorrupt
	}
	switch event.Kind {
	case "report":
		if err := validateParentReport(event.Message); err != nil {
			return event, err
		}
	case "completed", "failed", "cancelled", "interrupted", "timed_out", "input_required", "approval_required":
		if event.Message != "" {
			return event, ErrAuthorityCorrupt
		}
	default:
		return event, ErrAuthorityCorrupt
	}
	return event, nil
}

func validateParentReport(message string) error {
	if !utf8.ValidString(message) || strings.TrimSpace(message) == "" || len(message) > ParentReportMaxBytes {
		return errors.New("parent report requires nonempty UTF-8 text of at most 8192 bytes")
	}
	return nil
}

func parentEventID(source Entry, kind, identity string) string {
	return "parent-event:" + StableHash(strings.Join([]string{source.ThreadID, kind, identity}, "\x00"))[:32]
}

// Automatic references share the transaction that commits their source fact.
func (r *MemoryRepo) appendAutomaticParentEventLocked(source Entry) {
	if event, ok := r.automaticParentEventLocked(source); ok {
		r.appendParentEventLocked(event)
	}
}

func (r *MemoryRepo) automaticParentEventLocked(source Entry) (ParentEvent, bool) {
	if !r.parentEvents {
		return ParentEvent{}, false
	}
	child, ok := r.threads[source.ThreadID]
	if !ok || child.ParentThreadID == "" || source.Metadata["authority_kind"] == "branch_boundary" || source.TurnID == "" || source.RunID == "" {
		return ParentEvent{}, false
	}
	for _, ordinal := range r.turnEntryOrdinals[source.ThreadID][source.TurnID] {
		start := r.entries[source.ThreadID][ordinal]
		if start.Type == EntryTurnMarker && start.TurnStatus == TurnStarted && start.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction {
			return ParentEvent{}, false
		}
	}
	event := ParentEvent{ParentThreadID: child.ParentThreadID, SourceThreadID: source.ThreadID, SourceTurnID: source.TurnID, SourceRunID: source.RunID, SourceEntryID: source.ID, ReceivedAt: source.CreatedAt}
	identity := source.ID
	switch {
	case source.Type == EntryTurnMarker && terminalTurnMarker(source.TurnStatus):
		event.ReasonCode = source.Metadata[TurnFailureCodeMetadataKey]
		if event.ReasonCode != "" && !ValidTurnFailureCode(event.ReasonCode) {
			event.ReasonCode = TurnFailureEngineContract
		}
		switch source.TurnStatus {
		case TurnCompleted:
			event.Kind = "completed"
		case TurnFailed:
			event.Kind = "failed"
		case TurnAborted:
			event.Kind = "interrupted"
			if event.ReasonCode == TurnFailureCancelled {
				event.Kind = "cancelled"
			}
		}
		var timeout provider.RequestTimeoutDetails
		if raw := source.Metadata["provider_timeout"]; raw != "" && json.Unmarshal([]byte(raw), &timeout) == nil && timeout.Validate() == nil {
			event.Kind = "timed_out"
			event.ReasonCode = "provider_timeout"
		}
	case source.Type == EntryInteractionAsked:
		var interaction struct{ Kind string }
		if json.Unmarshal(source.Payload, &interaction) != nil {
			return ParentEvent{}, false
		}
		event.InteractionID = strings.TrimPrefix(source.ID, "interaction-requested:")
		event.Kind = "input_required"
		if interaction.Kind == "approval" {
			event.Kind = "approval_required"
		}
		identity = event.InteractionID
	case source.Type == EntryToolCall && source.Message.Kind == session.MessageKindControlSignal && controlstate.Classify(source.Message.ControlSignal) == controlstate.WaitingInput:
		event.Kind, event.InteractionID = "input_required", source.Message.ControlSignal.CallID
		identity = event.InteractionID
	case source.Type == EntryToolResult && ReadyToolInputResultIDs(r.entries[source.ThreadID])[source.ID]:
		event.Kind, event.InteractionID = "input_required", ToolInputInteractionID(source.ID)
		identity = event.InteractionID
	default:
		return ParentEvent{}, false
	}
	if event.InteractionID != "" {
		event.ID = parentEventID(source, "interaction", identity)
	} else {
		event.ID = parentEventID(source, "terminal", identity)
	}
	return event, true
}

func (r *MemoryRepo) appendParentEventLocked(event ParentEvent) {
	parent, ok := r.threads[event.ParentThreadID]
	if !ok {
		return
	}
	if _, exists := r.entryOrdinals[parent.ID][event.ID]; exists {
		return
	}
	payload, _ := json.Marshal(event)
	entry := PrepareEntry(Entry{ID: event.ID, ThreadID: parent.ID, ParentID: parent.LeafID, Type: EntryParentEvent, Payload: payload, CreatedAt: event.ReceivedAt})
	r.appendIndexedEntriesLocked(parent.ID, entry)
	parent.LeafID, parent.UpdatedAt = entry.ID, entry.CreatedAt
	r.setThreadLocked(parent.ID, parent)
	if state, ok := r.providerStates[parent.ID]; ok && state.LeafEntryID == entry.ParentID {
		state.LeafEntryID, state.UpdatedAt = entry.ID, entry.CreatedAt
		r.setProviderStateLocked(parent.ID, state)
	}
}

func (r *MemoryRepo) ReportToParent(ctx context.Context, req ParentReportRequest) (ParentEvent, error) {
	if err := ctx.Err(); err != nil {
		return ParentEvent{}, err
	}
	if err := validateParentReport(req.Message); err != nil {
		return ParentEvent{}, err
	}
	if err := validateEffectInvocation(req.Invocation); err != nil {
		return ParentEvent{}, err
	}
	if strings.TrimSpace(req.RequestKey) == "" {
		return ParentEvent{}, ErrRequestConflict
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.parentEvents {
		return ParentEvent{}, errors.New("parent events are disabled")
	}
	inv := req.Invocation
	child, ok := r.threads[inv.ThreadID]
	if !ok || child.ParentThreadID == "" {
		return ParentEvent{}, ErrInvalidParent
	}
	parent, exists := r.threads[child.ParentThreadID]
	if !exists {
		return ParentEvent{}, ErrInvalidParent
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return ParentEvent{}, err
	}
	id := "parent-event:" + StableHash(strings.Join([]string{inv.ThreadID, inv.TurnID, inv.RunID, inv.ToolCallID}, "\x00"))[:32]
	if existing, found := findEntry(r.entries[child.ParentThreadID], id); found {
		event, err := decodeParentEvent(existing)
		if err != nil {
			return ParentEvent{}, err
		}
		source, found := findEntry(r.entries[child.ID], event.SourceEntryID)
		attempt, decodeErr := decodeEffectAttempt(source)
		if !found || decodeErr != nil || attempt.Invocation != inv || event.Message != req.Message {
			return ParentEvent{}, ErrRequestConflict
		}
		return event, nil
	}
	turnID, active := runtimeActiveTurn(r.entries[child.ID])
	runID, err := activeRunIdentity(r.entries[child.ID], inv.TurnID)
	if !active || turnID != inv.TurnID || err != nil || runID != inv.RunID || turnCancellationRequested(r.entries[child.ID], inv.TurnID) {
		return ParentEvent{}, ErrStaleAuthority
	}
	if err := lifecycleRejectsWrite(child); err != nil {
		return ParentEvent{}, err
	}
	sourceID := ""
	for _, attempt := range canonicalEffectAttemptsForTurn(r.entries[child.ID], inv.TurnID) {
		if attempt.Invocation == inv && attempt.State == EffectAttemptDispatching {
			sourceID = effectAttemptEntryID(attempt.EffectAttemptID, attempt.State)
			break
		}
	}
	if sourceID == "" {
		return ParentEvent{}, ErrStaleAuthority
	}
	event := ParentEvent{ID: id, ParentThreadID: child.ParentThreadID, SourceThreadID: child.ID, SourceTurnID: inv.TurnID, SourceRunID: inv.RunID, SourceEntryID: sourceID, Kind: "report", Message: req.Message, ReceivedAt: r.now().UTC()}
	r.appendParentEventLocked(event)
	return event, nil
}

func (r *MemoryRepo) parentEventRecordsLocked(threadID string) ([]ParentEventRecord, error) {
	meta, ok := r.threads[threadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
	if err != nil {
		return nil, err
	}
	var records []ParentEventRecord
	positions := map[string]int{}
	for _, entry := range path {
		switch entry.Type {
		case EntryParentEvent:
			event, err := decodeParentEvent(entry)
			if err != nil {
				return nil, err
			}
			positions[event.ID] = len(records)
			records = append(records, ParentEventRecord{ParentEvent: event})
		case EntryParentEventsAdmitted:
			var admission ParentEventAdmission
			if decodeBackendDomainValue(backendDomainFormat{label: "parent event"}, backendDomainRecord{Value: entry.Payload}, &admission) != nil {
				return nil, ErrAuthorityCorrupt
			}
			first, f := positions[admission.FirstEventID]
			last, l := positions[admission.LastEventID]
			if !f || !l || first > last {
				return nil, ErrAuthorityCorrupt
			}
			for i := first; i <= last; i++ {
				if records[i].Admitted {
					return nil, ErrAuthorityCorrupt
				}
				records[i].Admitted = true
			}
		}
	}
	for i := range records {
		event := &records[i]
		if event.InteractionID == "" {
			continue
		}
		event.Resolved = true
		for _, interaction := range runtimePendingInteractions(r.entries[event.SourceThreadID], event.SourceTurnID) {
			if interaction.ID == event.InteractionID {
				event.Resolved = false
				break
			}
		}
	}
	return records, nil
}

func (r *MemoryRepo) ParentEvents(ctx context.Context, query ParentEventQuery) (ParentEventPage, error) {
	if err := ctx.Err(); err != nil {
		return ParentEventPage{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	records, err := r.parentEventRecordsLocked(query.ThreadID)
	if err != nil {
		return ParentEventPage{}, err
	}
	positions := map[string]int{}
	for i, record := range records {
		positions[record.ID] = i
	}
	first, last := 0, len(records)-1
	if query.EventID != "" {
		query.FirstEventID, query.LastEventID = query.EventID, query.EventID
	}
	if (query.FirstEventID == "") != (query.LastEventID == "") {
		return ParentEventPage{}, ErrRequestConflict
	}
	if query.FirstEventID != "" {
		var f, l bool
		first, f = positions[query.FirstEventID]
		last, l = positions[query.LastEventID]
		if !f || !l || first > last {
			return ParentEventPage{}, ErrEntryNotFound
		}
	}
	page := ParentEventPage{Events: []ParentEventRecord{}}
	if len(records) == 0 {
		return page, nil
	}
	page.FirstEventID, page.LastEventID = records[first].ID, records[last].ID
	if query.AfterEventID != "" {
		after, ok := positions[query.AfterEventID]
		if !ok || after < first || after > last {
			return ParentEventPage{}, ErrEntryNotFound
		}
		first = after + 1
	}
	limit := query.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 {
		return ParentEventPage{}, errors.New("parent event page limit must be between 1 and 100")
	}
	selected := map[string]bool{}
	for _, id := range query.SourceThreadIDs {
		selected[id] = true
	}
	for i := first; i <= last; i++ {
		record := records[i]
		if query.PendingOnly && record.Admitted || len(selected) > 0 && !selected[record.SourceThreadID] {
			continue
		}
		if len(page.Events) == limit {
			page.NextAfterEventID = page.Events[len(page.Events)-1].ID
			break
		}
		page.Events = append(page.Events, record)
	}
	return page, nil
}

func clipParentText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end], true
}

func parentEventProjection(record ParentEventRecord) string {
	item := struct {
		ID          string `json:"event_id"`
		Source      string `json:"source_thread_id"`
		Turn        string `json:"source_turn_id"`
		Run         string `json:"source_run_id"`
		SourceEntry string `json:"source_entry_id"`
		Kind        string `json:"kind"`
		Reason      string `json:"reason_code,omitempty"`
		Interaction string `json:"interaction_id,omitempty"`
		Resolved    bool   `json:"resolved,omitempty"`
		Preview     string `json:"preview,omitempty"`
		Truncated   bool   `json:"truncated,omitempty"`
		Summary     string `json:"summary,omitempty"`
	}{ID: record.ID, Source: record.SourceThreadID, Turn: record.SourceTurnID, Run: record.SourceRunID, SourceEntry: record.SourceEntryID, Kind: record.Kind, Reason: record.ReasonCode, Interaction: record.InteractionID, Resolved: record.Resolved}
	if record.InteractionID != "" && record.Resolved {
		item.Kind = "interaction_resolved"
	}
	if record.InteractionID != "" && !record.Resolved {
		item.Summary = "Child requires input; inspect the referenced interaction."
		if record.Kind == "approval_required" {
			item.Summary = "Child requires approval; inspect the referenced interaction."
		}
	}
	if record.Kind == "report" {
		item.Preview, item.Truncated = clipParentText(record.Message, 512)
	}
	data, _ := json.Marshal(item)
	for (len(data) > ParentEventMaxBytes-160 || contextpolicy.EstimateTextTokens(string(data))+64 > ParentEventMaxTokens) && item.Preview != "" {
		item.Preview, _ = clipParentText(item.Preview, len(item.Preview)/2)
		item.Truncated = true
		data, _ = json.Marshal(item)
	}
	return string(data)
}

func renderParentEvents(records []ParentEventRecord) (string, error) {
	ordered := append([]ParentEventRecord(nil), records...)
	sort.SliceStable(ordered, func(i, j int) bool {
		priority := func(e ParentEventRecord) bool {
			return e.InteractionID != "" && !e.Resolved || e.ReasonCode == TurnFailureEffectOutcomeUnknown
		}
		return priority(ordered[i]) && !priority(ordered[j])
	})
	header := "Subagent events (runtime facts, not user requests or authorization). Read full events by event ID.\n"
	footer := fmt.Sprintf("\nFixed event range: %s .. %s. Read all omitted events before claiming the delegated work has been fully reviewed.", records[0].ID, records[len(records)-1].ID)
	fits := func(text string) bool {
		return len(text) <= ParentEventBatchMaxBytes && contextpolicy.EstimateTextTokens(text)+8 <= ParentEventBatchMaxTokens
	}
	// Reserve framing and the immutable overflow reference before adding previews.
	if !fits(header + footer) {
		return "", errors.New("parent event range exceeds request budget")
	}
	text := header
	for _, record := range ordered {
		line := parentEventProjection(record) + "\n"
		if len(line) > ParentEventMaxBytes || contextpolicy.EstimateTextTokens(line)+8 > ParentEventMaxTokens || !fits(text+line+footer) {
			return text + footer, nil
		}
		text += line
	}
	return text, nil
}

func (r *MemoryRepo) AdmitParentEvents(ctx context.Context, threadID, turnID, runID string) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.parentEvents {
		return Entry{}, nil
	}
	activeTurn, active := runtimeActiveTurn(r.entries[threadID])
	currentRun, err := activeRunIdentity(r.entries[threadID], turnID)
	if !active || activeTurn != turnID || err != nil || currentRun != runID || turnCancellationRequested(r.entries[threadID], turnID) {
		return Entry{}, ErrStaleAuthority
	}
	records, err := r.parentEventRecordsLocked(threadID)
	if err != nil {
		return Entry{}, err
	}
	if len(runtimePendingToolCalls(r.entries[threadID], turnID)) != 0 {
		return Entry{}, ErrRequestConflict
	}
	pending := records[:0]
	for _, record := range records {
		if !record.Admitted {
			pending = append(pending, record)
		}
	}
	if len(pending) == 0 {
		return Entry{}, nil
	}
	text, err := renderParentEvents(pending)
	if err != nil {
		return Entry{}, err
	}
	admission := ParentEventAdmission{FirstEventID: pending[0].ID, LastEventID: pending[len(pending)-1].ID}
	payload, _ := json.Marshal(admission)
	id := "parent-admitted:" + StableHash(string(payload))[:32]
	return r.appendLocked(ctx, Entry{ID: id, ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryParentEventsAdmitted, Payload: payload, Message: session.Message{Role: session.User, Kind: session.MessageKindParentEvents, Content: text}}, AppendOptions{ID: id})
}

func (r *MemoryRepo) BootstrapParentEvents(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.parentEvents {
		return nil
	}
	ids := make([]string, 0, len(r.threads))
	for id := range r.threads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if r.threads[id].ParentThreadID == "" {
			continue
		}
		for _, entry := range append([]Entry(nil), r.entries[id]...) {
			if entry.Type != EntryInteractionAsked && entry.Type != EntryToolCall && entry.Type != EntryToolResult {
				continue
			}
			for _, pending := range runtimePendingInteractions(r.entries[id], entry.TurnID) {
				if entry.ID == "interaction-requested:"+pending.ID || entry.Type == EntryToolCall && entry.Message.ControlSignal != nil && entry.Message.ControlSignal.CallID == pending.ID || entry.Type == EntryToolResult && ToolInputInteractionID(entry.ID) == pending.ID {
					r.appendAutomaticParentEventLocked(entry)
				}
			}
		}
	}
	return nil
}

type parentEventsContextKey struct{}

func WithParentEvents(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, parentEventsContextKey{}, enabled)
}
func parentEventsEnabled(ctx context.Context) bool {
	enabled, _ := ctx.Value(parentEventsContextKey{}).(bool)
	return enabled
}

func validateParentEventEntry(memory *MemoryRepo, threadID string, entry Entry) error {
	switch entry.Type {
	case EntryParentEvent:
		event, err := decodeParentEvent(entry)
		if err != nil {
			return err
		}
		if entry.TurnID != "" || entry.RunID != "" || entry.Message.Role != "" {
			return ErrAuthorityCorrupt
		}
		if event.ParentThreadID != threadID {
			ancestors, err := validatedForkAncestors(memory)
			if err != nil {
				return err
			}
			if _, ok := ancestors[threadID][event.ParentThreadID]; !ok {
				return ErrAuthorityCorrupt
			}
		}
		// A retained reference survives canonical source deletion. A live source must
		// still prove the exact relationship and immutable fact named by the receipt.
		child, exists := memory.threads[event.SourceThreadID]
		if !exists {
			if _, deleted := memory.tombstones[event.SourceThreadID]; deleted {
				return nil
			}
			return ErrAuthorityCorrupt
		}
		if child.ParentThreadID != event.ParentThreadID {
			return ErrAuthorityCorrupt
		}
		source, found := findEntry(memory.entries[event.SourceThreadID], event.SourceEntryID)
		if !found || source.TurnID != event.SourceTurnID || source.RunID != event.SourceRunID {
			return ErrAuthorityCorrupt
		}
		if event.Kind == "report" {
			attempt, err := decodeEffectAttempt(source)
			if err != nil || attempt.State != EffectAttemptDispatching {
				return ErrAuthorityCorrupt
			}
		} else {
			expected, ok := (&MemoryRepo{parentEvents: true, threads: memory.threads, entries: memory.entries, turnEntryOrdinals: memory.turnEntryOrdinals}).automaticParentEventLocked(source)
			sameTime := expected.ReceivedAt.Equal(event.ReceivedAt)
			expected.ReceivedAt, event.ReceivedAt = time.Time{}, time.Time{}
			if !ok || !sameTime || expected != event {
				return ErrAuthorityCorrupt
			}
		}
	case EntryParentEventsAdmitted:
		if entry.Message.Role != session.User || entry.Message.Kind != session.MessageKindParentEvents || entry.TurnID == "" || entry.RunID == "" || len(entry.Message.Content) > ParentEventBatchMaxBytes || contextpolicy.EstimateTextTokens(entry.Message.Content)+8 > ParentEventBatchMaxTokens {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func validateParentEventRecords(memory *MemoryRepo) error {
	for threadID, entries := range memory.entries {
		hasEvents := false
		for _, entry := range entries {
			if entry.Type != EntryParentEvent && entry.Type != EntryParentEventsAdmitted {
				continue
			}
			hasEvents = true
			if err := validateParentEventEntry(memory, threadID, entry); err != nil {
				return err
			}
		}
		if hasEvents {
			if _, err := memory.parentEventRecordsLocked(threadID); err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareParentEventsInTransaction bootstraps only unresolved interactions.
// It is called inside the Host's existing startup transaction.
func (repo *BackendRepo) PrepareParentEventsInTransaction(ctx context.Context, tx spi.WriteTx, enabled bool) error {
	before := repo.domainMemory
	if !enabled {
		before.parentEvents = false
		return nil
	}
	next := cloneMemoryRepoForStartup(before)
	next.parentEvents = enabled
	if err := next.BootstrapParentEvents(ctx); err != nil {
		return err
	}
	if _, err := persistBackendDomainV19Changes(tx, before, next); err != nil {
		return err
	}
	repo.domainMemory = next
	return nil
}
