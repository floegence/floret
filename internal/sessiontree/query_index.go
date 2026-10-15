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
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type queryPart struct{ field, text string }
type queryRecord struct {
	indexID    uint32
	key        string
	row        threadquery.Record
	parts      []queryPart
	activities []*tools.ActivityPresentation
	postings   []uint32
	// Positions are derived Unicode word/rune positions, never query text copies.
	positions map[string][]int
}
type queryThread struct {
	reasoningOpen     map[string]bool
	lastReasoning     map[string]string
	leaf              string
	records           map[string]*queryRecord
	segmented         map[string]bool
	hostedPending     map[string]map[string]*queryRecord
	lifecycle         CanonicalLifecycle
	previous          *queryRecord
	previousAssistant bool
	order             int64
}
type queryTimeRow struct {
	at     time.Time
	record *queryRecord
}
type queryIndex struct {
	timeCounts       map[string]int
	recordsByID      []*queryRecord
	freeRecordSlots  []uint32
	postingSlots     []*queryPosting
	freePostingSlots []uint32
	records          map[string]*queryRecord
	postings         map[string]*queryPosting
	times            map[string][]queryTimeRow
	threads          map[string]*queryThread
	dirty            map[string]bool
	snapshots        map[string]*querySnapshot
	snapshotBytes    int64
	verified         int // candidate verification counter for deterministic performance tests
}

func newQueryIndex() *queryIndex {
	return &queryIndex{timeCounts: map[string]int{}, records: map[string]*queryRecord{}, postings: map[string]*queryPosting{}, times: map[string][]queryTimeRow{}, threads: map[string]*queryThread{}, dirty: map[string]bool{}, snapshots: map[string]*querySnapshot{}}
}
func (r *MemoryRepo) markQueryDirty(threadID string) {
	if r.queryIndex != nil && r.mutation == nil {
		r.queryIndex.dirty[threadID] = true
	}
}
func (r *MemoryRepo) syncQueryIndex(ctx context.Context) error {
	if r.queryIndex == nil {
		r.queryIndex = newQueryIndex()
		for id := range r.threads {
			r.queryIndex.dirty[id] = true
		}
	}
	for id := range r.queryIndex.dirty {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.updateQueryThread(id); err != nil {
			return err
		}
		delete(r.queryIndex.dirty, id)
	}
	r.queryIndex.compactTimes()
	return nil
}

func (r *MemoryRepo) updateQueryThread(id string) error {
	idx := r.queryIndex
	meta, found := r.threads[id]
	state := idx.threads[id]
	if !found {
		if state != nil {
			for _, record := range state.records {
				idx.remove(record)
			}
			delete(idx.threads, id)
		}
		// Deletion invalidates whole snapshots, including earlier pages and Read refs.
		for key, snapshot := range idx.snapshots {
			if snapshot.authorized[id] {
				idx.dropSnapshot(key)
			}
		}
		return nil
	}
	var tail []Entry
	if state != nil && state.leaf != meta.LeafID {
		current := meta.LeafID
		for current != "" && current != state.leaf {
			ordinal, ok := r.entryOrdinals[id][current]
			if !ok {
				return ErrAuthorityCorrupt
			}
			entry := r.entries[id][ordinal]
			tail = append(tail, entry)
			current = entry.ParentID
		}
		if current != state.leaf {
			state = nil
			tail = nil
		}
		for left, right := 0, len(tail)-1; left < right; left, right = left+1, right-1 {
			tail[left], tail[right] = tail[right], tail[left]
		}
	}
	if state == nil {
		if previous := idx.threads[id]; previous != nil {
			for _, record := range previous.records {
				idx.remove(record)
			}
		}
		var err error
		tail, err = pathLocked(r.threads, r.entries, id, meta.LeafID)
		if err != nil {
			return err
		}
		state = &queryThread{reasoningOpen: map[string]bool{}, lastReasoning: map[string]string{}, records: map[string]*queryRecord{}, segmented: map[string]bool{}, hostedPending: map[string]map[string]*queryRecord{}, lifecycle: CanonicalLifecycle{Status: "idle"}}
		idx.threads[id] = state
	}
	for _, entry := range tail {
		if entry.Type == EntrySteeringApplied {
			resolution, err := DecodeSteeringResolution(entry)
			if err != nil {
				return err
			}
			ordinal, exists := r.entryOrdinals[id][resolution.SteeringID]
			if !exists {
				return ErrAuthorityCorrupt
			}
			source := r.entries[id][ordinal]
			entry.Type = EntryUserMessage
			entry.Message = source.Message
			entry.ID = source.ID
		}
		if err := idx.observe(state, id, entry); err != nil {
			return err
		}
	}
	state.leaf = meta.LeafID
	key := id + "\x00threads"
	activity, outcome := "idle", ""
	switch state.lifecycle.Status {
	case "running", "waiting":
		activity = "active"
	case "completed", "failed", "interrupted", "cancelled":
		outcome = state.lifecycle.Status
	}
	record := &queryRecord{key: key, row: threadquery.Record{Object: threadquery.Threads, ThreadID: identity.ThreadID(id), ParentThreadID: identity.ThreadID(meta.ParentThreadID), Title: meta.Title, Activity: activity, Outcome: outcome, Status: string(meta.Lifecycle), Time: meta.CreatedAt.UTC(), CreatedAt: meta.CreatedAt.UTC(), UpdatedAt: meta.UpdatedAt.UTC()}, parts: []queryPart{{"title", meta.Title}}}
	idx.replace(state, record)
	return nil
}

func (idx *queryIndex) observe(state *queryThread, threadID string, entry Entry) error {
	at, err := EntryOccurrenceTime(entry)
	if err != nil {
		return err
	}
	state.lifecycle.Observe(entry, true)
	if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		state.segmented[entry.TurnID] = entry.Metadata[ResponseSegmentsMetadataKey] == "1"
	}
	base := threadquery.Record{ThreadID: identity.ThreadID(threadID), TurnID: identity.TurnID(entry.TurnID), RunID: identity.RunID(entry.RunID), Time: at.UTC(), Order: state.order}
	normal := entry.Message.Kind == session.MessageKindNormal
	switch entry.Type {
	case EntryUserMessage, EntryAssistantMessage:
		if !normal {
			return nil
		}
		assistant := entry.Type == EntryAssistantMessage
		if assistant {
			executionKey := entry.TurnID + "\x00" + entry.RunID
			if AppendReasoningSegment(entry.Message.Reasoning, state.lastReasoning[executionKey], state.reasoningOpen[executionKey]) {
				state.previousAssistant = false
				state.reasoningOpen[executionKey] = true
				state.lastReasoning[executionKey] = entry.Message.Reasoning
			}
		}
		if assistant && entry.Message.Content == "" {
			return nil
		}
		base.Object = threadquery.Messages
		base.Role = "user"
		if assistant {
			base.Role = "assistant"
		}
		record := &queryRecord{key: threadID + "\x00messages\x00" + entry.ID, row: base, parts: []queryPart{{"body", entry.Message.Content}}}
		for _, ref := range entry.Message.References {
			record.parts = append(record.parts, queryPart{"reference_label", ref.Label}, queryPart{"reference_text", ref.Text})
			if ref.ReferencedThreadID != "" {
				record.parts = append(record.parts, queryPart{"referenced_thread_id", ref.ReferencedThreadID})
			}
		}
		if assistant && state.previous != nil && MergeAssistantSegment(state.segmented[entry.TurnID], state.previousAssistant, state.previous.row.TurnID.String(), state.previous.row.RunID.String(), entry.TurnID, entry.RunID) {
			record = copyQueryRecord(state.previous)
			record.parts = append(record.parts, queryPart{"body", entry.Message.Content})
		} else {
			state.order++
		}
		idx.replace(state, record)
		state.previous = record
		state.previousAssistant = assistant
	case EntryToolCall, EntryToolResult:
		if entry.Type == EntryToolResult {
			state.reasoningOpen[entry.TurnID+"\x00"+entry.RunID] = false
		}
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
		record := &queryRecord{key: key, row: base}
		if entry.Type == EntryToolCall {
			record.parts = []queryPart{{"tool_name", entry.Message.ToolName}, {"arguments", entry.Message.ToolArgs}}
			state.order++
		} else {
			previous := state.records[key]
			// A canonical imported transcript may contain a standalone tool message.
			// It is not an invocation or a typed confirmation and cannot create one.
			if previous == nil || entry.Message.ToolResult == nil {
				return nil
			}
			if previous.row.ToolName != entry.Message.ToolName {
				return ErrAuthorityCorrupt
			}
			record = copyQueryRecord(previous)
			record.row.CompletedAt = at.UTC()
			record.row.Elapsed = at.Sub(record.row.Time)
			record.row.Status = entry.Message.ToolResult.Status
			record.parts = append(record.parts, queryPart{"result", entry.Message.Content})
		}
		idx.replace(state, record)
	case EntryCustom:
		if entry.Metadata["kind"] != HostedToolEntryKind {
			return nil
		}
		state.previousAssistant = false
		state.reasoningOpen[entry.TurnID+"\x00"+entry.RunID] = false
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
		record := &queryRecord{key: key, row: base, parts: []queryPart{{"tool_name", ev.ToolName}}}
		if previous := state.records[key]; previous != nil {
			record = copyQueryRecord(previous)
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
		idx.replace(state, record)
	case EntryTurnMarker:
		if entry.TurnStatus == TurnCompleted || entry.TurnStatus == TurnFailed || entry.TurnStatus == TurnAborted {
			executionKey := entry.TurnID + "\x00" + entry.RunID
			for _, previous := range state.hostedPending[executionKey] {
				if previous.row.ToolKind == "hosted" && previous.row.TurnID.String() == entry.TurnID && previous.row.RunID.String() == entry.RunID && previous.row.Status == "unconfirmed" {
					record := copyQueryRecord(previous)
					record.row.Status = "canceled"
					record.row.CompletedAt = at.UTC()
					record.row.Elapsed = at.Sub(record.row.Time)
					idx.replace(state, record)
				}
			}
			delete(state.hostedPending, executionKey)
		}
	}
	return nil
}
func copyQueryRecord(record *queryRecord) *queryRecord {
	result := *record
	result.parts = append([]queryPart(nil), record.parts...)
	result.activities = append([]*tools.ActivityPresentation(nil), record.activities...)
	result.postings = nil
	result.positions = nil
	return &result
}
func (idx *queryIndex) replace(state *queryThread, record *queryRecord) {
	if previous := idx.records[record.key]; previous != nil {
		idx.remove(previous)
	}
	idx.allocateRecordSlot(record)
	idx.records[record.key] = record
	state.records[record.key] = record
	values := map[string]string{"object": string(record.row.Object), "thread": record.row.ThreadID.String(), "parent": record.row.ParentThreadID.String(), "turn": record.row.TurnID.String(), "run": record.row.RunID.String(), "role": record.row.Role, "tool_name": record.row.ToolName, "tool_kind": record.row.ToolKind, "status": record.row.Status, "activity": record.row.Activity, "outcome": record.row.Outcome}
	keys := map[string]bool{}
	for field, value := range values {
		keys["f\x00"+field+"\x00"+value] = true
	}
	// N-grams cover punctuation, Chinese, code and fragments inside words. Word
	// positions additionally preserve Unicode token locations for diagnostics.
	record.positions = map[string][]int{}
	for _, field := range []string{"text", "title", "arguments", "result"} {
		runes := []rune(strings.ToLower(record.text(field)))
		for n := 1; n <= 3; n++ {
			for i := 0; i+n <= len(runes); i++ {
				keys["g\x00"+field+"\x00"+string(runes[i:i+n])] = true
			}
		}
		start := -1
		for i := 0; i <= len(runes); i++ {
			word := i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_')
			if word && start < 0 {
				start = i
			}
			if !word && start >= 0 {
				term := field + "\x00" + string(runes[start:i])
				record.positions[term] = append(record.positions[term], start)
				start = -1
			}
		}
	}
	for key := range keys {
		idx.addPosting(key, record)
	}

	for field, at := range record.times() {
		if at.IsZero() {
			continue
		}
		key := string(record.row.Object) + "\x00" + field
		idx.timeCounts[key]++
		rows := idx.times[key]
		p := sort.Search(len(rows), func(i int) bool { return rows[i].at.After(at) })
		rows = append(rows, queryTimeRow{})
		copy(rows[p+1:], rows[p:])
		rows[p] = queryTimeRow{at, record}
		idx.times[key] = rows
	}
}
func (idx *queryIndex) remove(record *queryRecord) {
	delete(idx.records, record.key)
	idx.recordsByID[record.indexID] = nil
	idx.freeRecordSlots = append(idx.freeRecordSlots, record.indexID)
	for _, slot := range record.postings {
		posting := idx.postingSlots[slot]
		posting.remove(record)
		if posting.total == 0 {
			delete(idx.postings, posting.key)
			idx.postingSlots[slot] = nil
			idx.freePostingSlots = append(idx.freePostingSlots, slot)
		}
	}

	for field, at := range record.times() {
		if !at.IsZero() {
			idx.timeCounts[string(record.row.Object)+"\x00"+field]--
		}
	}
	// Time rows are immutable and checked against the current identity map. Bound
	// retired rows through occasional compaction, avoiding O(history) per commit.
}
func (record *queryRecord) text(field string) string {
	var result strings.Builder
	for _, part := range record.parts {
		selected := field == "text" || field == part.field
		if !selected {
			continue
		}
		if result.Len() > 0 && part.field != "body" {
			result.WriteByte('\n')
		}
		result.WriteString(part.text)
	}
	if len(record.activities) > 0 && (field == "text" || field == "presentation") {
		raw, _ := json.Marshal(record.activities)
		if result.Len() > 0 {
			result.WriteByte('\n')
		}
		result.Write(raw)
	}
	return result.String()
}
func (record *queryRecord) times() map[string]time.Time {
	return map[string]time.Time{"time": record.row.Time, "created": record.row.CreatedAt, "updated": record.row.UpdatedAt, "completed": record.row.CompletedAt}
}
func (idx *queryIndex) compactTimes() {
	for key, rows := range idx.times {
		live := idx.timeCounts[key]
		if len(rows) <= live*2+256 {
			continue
		}
		kept := make([]queryTimeRow, 0, live)
		for _, row := range rows {
			if idx.records[row.record.key] == row.record {
				kept = append(kept, row)
			}
		}
		idx.times[key] = kept
	}
}
func (record *queryRecord) estimateBytes() int64 {
	size := int64(768 + len(record.key) + len(record.row.Title) + len(record.postings)*4)
	for key, positions := range record.positions {
		size += int64(64 + len(key) + len(positions)*8)
	}
	for _, part := range record.parts {
		size += int64(len(part.text) + 32)
	}
	if len(record.activities) > 0 {
		raw, _ := json.Marshal(record.activities)
		size += int64(len(raw))
	}
	return size
}
func (idx *queryIndex) checkAuthority(scope threadquery.Scope) (map[string]bool, error) {
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
