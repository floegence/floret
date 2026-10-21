package sessiontree

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"unicode/utf8"
)

type queryPart struct{ field, text string }
type queryRecord struct {
	key        string
	row        threadquery.Record
	sources    []string
	parts      []queryPart
	activities []*tools.ActivityPresentation
}
type queryThread struct {
	leaf                  string
	records               map[string]*queryRecord
	complete              bool
	hostedPending         map[string]map[string]*queryRecord
	lifecycle             CanonicalLifecycle
	previous              *queryRecord
	previousAssistant     bool
	previousContentSource string
	order                 int64
}

// queryProjector is the only canonical-to-search projection. It owns no
// inverted index; its records are disposable source views for one thread.
type queryProjector struct {
	metadataOnly bool
	changed      map[string]*queryRecord
	lookup       func(string) (*queryRecord, error)
}

func (projector *queryProjector) replace(state *queryThread, record *queryRecord) {
	if projector.metadataOnly {
		record.parts, record.activities = nil, nil
	}
	state.records[record.key] = record
	if projector.changed != nil {
		projector.changed[record.key] = record
	}
}

func (state *queryThread) record(key string) *queryRecord {
	return state.records[key]
}

func (projector *queryProjector) read(state *queryThread, key string) (*queryRecord, error) {
	if record := state.record(key); record != nil {
		return record, nil
	}
	if projector.lookup != nil {
		return projector.lookup(key)
	}
	return nil, nil
}
func newQueryThread() *queryThread {
	return &queryThread{complete: true, records: map[string]*queryRecord{}, hostedPending: map[string]map[string]*queryRecord{}, lifecycle: CanonicalLifecycle{Status: "idle"}}
}
func (r *MemoryRepo) projectQueryThread(ctx context.Context, id string) (*queryThread, error) {
	return r.projectQueryThreadMode(ctx, id, false)
}

func (r *MemoryRepo) projectQueryThreadMode(ctx context.Context, id string, metadataOnly bool) (*queryThread, error) {
	meta, found := r.threads[id]
	if !found {
		return nil, nil
	}
	path, err := r.readCanonicalPathLocked(ctx, id, meta.LeafID, !metadataOnly)
	if err != nil {
		return nil, err
	}
	state := newQueryThread()
	projector := &queryProjector{metadataOnly: metadataOnly}
	for _, entry := range path {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Type == EntrySteeringApplied {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return nil, err
			}
			ordinal, ok := r.entryOrdinals[id][resolution.SteeringID]
			if !ok {
				return nil, ErrAuthorityCorrupt
			}
			source := r.entries[id][ordinal]
			if !metadataOnly {
				var sourceErr error
				source, sourceErr = r.completeEntryLocked(source)
				if sourceErr != nil {
					return nil, sourceErr
				}
			}
			entry.Type = EntryUserMessage
			entry.Message = source.Message
			entry.ID = source.ID
			entry.bodyFields = source.bodyFields
		}
		if err := projector.observe(state, id, entry); err != nil {
			return nil, err
		}
	}
	projector.finish(state, id, meta)
	return state, nil
}

// queryEntryParts is shared by projection and lazy canonical materialization.
func queryEntryParts(entry Entry) []queryPart {
	var parts []queryPart
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage, EntrySteeringRequested:
		parts = append(parts, queryPart{"body", entry.Message.Content})
		for _, attachment := range entry.Message.Attachments {
			parts = append(parts, queryPart{"attachment", session.AttachmentLabel(attachment)})
		}
		for _, ref := range entry.Message.References {
			parts = append(parts, queryPart{"reference_label", ref.Label}, queryPart{"reference_text", ref.Text})
			if ref.ReferencedThreadID != "" {
				parts = append(parts, queryPart{"referenced_thread_id", ref.ReferencedThreadID})
			}
		}
	case EntryToolCall:
		parts = append(parts, queryPart{"tool_name", entry.Message.ToolName}, queryPart{"arguments", entry.Message.ToolArgs})
	case EntryToolResult:
		parts = append(parts, queryPart{"result", entry.Message.Content})
		if entry.Message.ToolResult != nil {
			for _, attachment := range entry.Message.ToolResult.Attachments {
				parts = append(parts, queryPart{"attachment", session.AttachmentLabel(attachment)})
			}
		}
	}
	return parts
}
func (projector *queryProjector) observe(state *queryThread, threadID string, entry Entry) error {
	at, err := EntryOccurrenceTime(entry)
	if err != nil {
		return err
	}
	state.lifecycle.Observe(entry, true)
	base := threadquery.Record{ThreadID: identity.ThreadID(threadID), TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), Time: at.UTC(), Order: state.order}
	normal := entry.Message.Kind == session.MessageKindNormal
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage:
		if !normal {
			return nil
		}
		assistant := entry.Type == EntryAssistantMessage
		if assistant && entry.Message.Content == "" && entry.bodyFields["content"].Bytes == 0 {
			return nil
		}
		base.Object = threadquery.Messages
		base.Role = "user"
		if assistant {
			base.Role = "assistant"
		}
		record := &queryRecord{key: threadID + "\x00messages\x00" + entry.ID, row: base, parts: queryEntryParts(entry), sources: []string{entry.ID}}
		if assistant && state.previous != nil && state.previousAssistant && state.previousContentSource == entry.Message.ContentSourceID && entry.Message.ContentSourceID != "" && state.previous.row.TurnID.String() == entry.TurnID && state.previous.row.RunID.String() == entry.RunID {
			previous, err := projector.read(state, state.previous.key)
			if err != nil {
				return err
			}
			if previous == nil {
				return ErrAuthorityCorrupt
			}
			record = projector.mutableRecord(previous)
			record.parts = append(record.parts, queryPart{"body", entry.Message.Content})
			record.sources = append(record.sources, entry.ID)
		} else {
			state.order++
		}
		projector.replace(state, record)
		state.previous = record
		state.previousAssistant = assistant
		state.previousContentSource = entry.Message.ContentSourceID
	case EntryToolCall, EntryToolResult:
		if !normal {
			if entry.Message.Kind == session.MessageKindControlSignal {
				state.previousAssistant = false
			}
			return nil
		}
		state.previousAssistant = false
		base.Object = threadquery.ToolCalls
		base.ToolCallID = entry.Message.ToolCallID
		base.ToolName = entry.Message.ToolName
		base.ToolKind = "local"
		base.Status = "unconfirmed"
		key := threadID + "\x00tool_calls\x00" + entry.TurnID + "\x00" + entry.RunID + "\x00" + entry.Message.ToolCallID
		record := &queryRecord{key: key, row: base, sources: []string{entry.ID}}
		if entry.Type == EntryToolCall {
			record.parts = queryEntryParts(entry)
			state.order++
		} else {
			previous, err := projector.read(state, key)
			if err != nil {
				return err
			}
			// A canonical imported transcript may contain a standalone tool message.
			// It is not an invocation or a typed confirmation and cannot create one.
			if previous == nil || entry.Message.ToolResult == nil {
				return nil
			}
			if previous.row.ToolName != entry.Message.ToolName {
				return ErrAuthorityCorrupt
			}
			record = projector.mutableRecord(previous)
			record.row.CompletedAt = at.UTC()
			record.row.Elapsed = at.Sub(record.row.Time)
			record.row.Status = entry.Message.ToolResult.Status
			record.parts = append(record.parts, queryEntryParts(entry)...)
			record.sources = append(record.sources, entry.ID)
		}
		projector.replace(state, record)
	case EntryCustom:
		if entry.Metadata["kind"] != HostedToolEntryKind {
			return nil
		}
		state.previousAssistant = false
		ev, err := HostedToolObservation(entry)
		if err != nil {
			return err
		}
		base.Object = threadquery.ToolCalls
		base.ToolCallID = ev.ToolID
		base.ToolName = ev.ToolName
		base.ToolKind = "hosted"
		base.Status = "unconfirmed"
		key := threadID + "\x00tool_calls\x00" + entry.TurnID + "\x00" + entry.RunID + "\x00" + ev.ToolID
		record := &queryRecord{key: key, row: base, parts: []queryPart{{"tool_name", ev.ToolName}}, sources: []string{entry.ID}}
		previous, err := projector.read(state, key)
		if err != nil {
			return err
		}
		if previous != nil {
			record = projector.mutableRecord(previous)
			record.sources = append(record.sources, entry.ID)
		} else {
			state.order++
		}
		if ev.Type == observation.EventTypeHostedToolResult {
			record.row.CompletedAt = at.UTC()
			record.row.Elapsed = at.Sub(record.row.Time)
			record.row.Status = "success"
			if ev.Metadata["error_present"] == true {
				record.row.Status = "error"
			}
		}
		if entry.Message.Activity != nil {
			record.activities = append(record.activities, entry.Message.Activity)
		}

		executionKey := entry.TurnID + "\x00" + entry.RunID
		if state.hostedPending[executionKey] == nil {
			state.hostedPending[executionKey] = map[string]*queryRecord{}
		}
		if record.row.Status == "unconfirmed" {
			state.hostedPending[executionKey][key] = record
		} else {
			delete(state.hostedPending[executionKey], key)
		}
		projector.replace(state, record)
	case EntryTurnMarker:
		if entry.TurnStatus == TurnCompleted || entry.TurnStatus == TurnFailed || entry.TurnStatus == TurnAborted {
			executionKey := entry.TurnID + "\x00" + entry.RunID
			for _, previous := range state.hostedPending[executionKey] {
				if previous.row.ToolKind == "hosted" && previous.row.TurnID.String() == entry.TurnID && previous.row.RunID.String() == entry.RunID && previous.row.Status == "unconfirmed" {
					complete, err := projector.read(state, previous.key)
					if err != nil {
						return err
					}
					if complete == nil {
						return ErrAuthorityCorrupt
					}
					record := projector.mutableRecord(complete)
					record.row.Status = "canceled"
					record.row.CompletedAt = at.UTC()
					record.row.Elapsed = at.Sub(record.row.Time)
					projector.replace(state, record)
				}
			}
			delete(state.hostedPending, executionKey)
		}
	}
	return nil
}

// Full chronological projection owns its records. Only transaction overlays
// copy prior published locators; a giant source list grows once during startup.
func (projector *queryProjector) mutableRecord(record *queryRecord) *queryRecord {
	if projector.changed == nil {
		return record
	}
	return copyQueryRecord(record)
}

func copyQueryRecord(record *queryRecord) *queryRecord {
	result := *record
	result.parts = append([]queryPart(nil), record.parts...)
	result.sources = append([]string(nil), record.sources...)
	result.activities = append([]*tools.ActivityPresentation(nil), record.activities...)
	return &result
}

func (record *queryRecord) textPrefix(limit int) string {
	field := queryReadField{}
	for _, part := range record.parts {
		if field.bytes > 0 && part.field != "body" {
			field.append("\n")
		}
		field.append(part.text)
		if field.bytes >= limit {
			return field.chunk(0, limit)
		}
	}
	if len(record.activities) > 0 {
		raw, _ := json.Marshal(record.activities)
		if field.bytes > 0 {
			field.append("\n")
		}
		field.append(string(raw))
	}
	return field.chunk(0, limit)
}

func (record *queryRecord) estimateBytes() int64 {
	size := int64(768 + len(record.key) + len(record.row.Title))
	for _, part := range record.parts {
		size += int64(len(part.text) + 32)
	}
	for _, source := range record.sources {
		size += int64(len(source) + 16)
	}
	if len(record.activities) > 0 {
		raw, _ := json.Marshal(record.activities)
		size += int64(len(raw))
	}
	return size
}
func (idx *querySnapshots) checkAuthority(scope threadquery.Scope) (map[string]bool, error) {
	return ValidateQueryScope(scope)
}

// ValidateQueryScope validates the product-neutral host authority boundary.
func ValidateQueryScope(scope threadquery.Scope) (map[string]bool, error) {
	if strings.TrimSpace(scope.Authority) == "" || !utf8.ValidString(scope.Authority) {
		return nil, fmt.Errorf("%w: host authority is required", threadquery.ErrInvalidQuery)
	}
	authorized := map[string]bool{}
	for _, id := range scope.ThreadIDs {
		if _, err := identity.ParseThreadID(id.String()); err != nil {
			return nil, threadquery.ErrInvalidQuery
		}
		authorized[id.String()] = true
	}
	return authorized, nil
}

func (projector *queryProjector) finish(state *queryThread, id string, meta ThreadMeta) {
	state.leaf = meta.LeafID
	activity, outcome := "idle", ""
	switch state.lifecycle.Status {
	case "running", "waiting":
		activity = "active"
	case "completed", "failed", "interrupted", "cancelled":
		outcome = state.lifecycle.Status
	}
	record := &queryRecord{key: id + "\x00threads", row: threadquery.Record{Object: threadquery.Threads, ThreadID: identity.ThreadID(id), ParentThreadID: identity.ThreadID(meta.ParentThreadID), Title: meta.Title, Activity: activity, Outcome: outcome, Status: string(meta.Lifecycle), Time: meta.CreatedAt.UTC(), CreatedAt: meta.CreatedAt.UTC(), UpdatedAt: meta.UpdatedAt.UTC()}, parts: []queryPart{{"title", meta.Title}}}
	projector.replace(state, record)
}

func cloneQueryThread(source *queryThread) *queryThread {
	result := *source
	// The transaction owns changed records only. Historical records are read
	// through their canonical index locators, never copied into an overlay.
	result.records = map[string]*queryRecord{}
	result.complete = false
	result.lifecycle = source.lifecycle.transactionCopy()
	result.hostedPending = map[string]map[string]*queryRecord{}
	for key, values := range source.hostedPending {
		result.hostedPending[key] = map[string]*queryRecord{}
		for id, record := range values {
			result.hostedPending[key][id] = record
		}
	}
	return &result
}
func (r *MemoryRepo) extendQueryThread(ctx context.Context, id string, previous *queryThread, lookup func(string) (*queryRecord, error)) (*queryThread, map[string]*queryRecord, error) {
	meta, ok := r.threads[id]
	if !ok {
		return nil, nil, nil
	}
	var tail []Entry
	current := meta.LeafID
	if previous != nil {
		for current != "" && current != previous.leaf {
			ordinal, ok := r.entryOrdinals[id][current]
			if !ok {
				return nil, nil, ErrAuthorityCorrupt
			}
			entry := r.entries[id][ordinal]
			tail = append(tail, entry)
			current = entry.ParentID
		}
		if current != previous.leaf {
			previous = nil
		}
	}
	if previous == nil {
		state, err := r.projectQueryThread(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		return state, state.records, nil
	}
	state := cloneQueryThread(previous)
	projector := &queryProjector{changed: map[string]*queryRecord{}, lookup: lookup}
	for i := len(tail) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		entry := tail[i]
		if entry.Type == EntrySteeringApplied {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return nil, nil, err
			}
			ordinal, ok := r.entryOrdinals[id][resolution.SteeringID]
			if !ok {
				return nil, nil, ErrAuthorityCorrupt
			}
			source, sourceErr := r.completeEntryLocked(r.entries[id][ordinal])
			if sourceErr != nil {
				return nil, nil, sourceErr
			}
			entry.Type = EntryUserMessage
			entry.Message = source.Message
			entry.ID = source.ID
		}
		if err := projector.observe(state, id, entry); err != nil {
			return nil, nil, err
		}
	}
	projector.finish(state, id, meta)
	return state, projector.changed, nil
}
