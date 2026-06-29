package runtime

import (
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/observation"
)

type ThreadTurnProjectionSegmentKind string

const (
	ThreadTurnProjectionSegmentAssistantText    ThreadTurnProjectionSegmentKind = "assistant_text"
	ThreadTurnProjectionSegmentActivityTimeline ThreadTurnProjectionSegmentKind = "activity_timeline"
	ThreadTurnProjectionSegmentControlSignal    ThreadTurnProjectionSegmentKind = "control_signal"
)

type ProjectThreadTurnRequest struct {
	ThreadID ThreadID
	TurnID   TurnID
	RunID    RunID
	TraceID  TraceID
	Events   []ThreadDetailEvent
}

type ThreadTurnProjection struct {
	ThreadID  ThreadID                      `json:"thread_id,omitempty"`
	TurnID    TurnID                        `json:"turn_id,omitempty"`
	RunID     RunID                         `json:"run_id,omitempty"`
	TraceID   TraceID                       `json:"trace_id,omitempty"`
	Segments  []ThreadTurnProjectionSegment `json:"segments,omitempty"`
	Projected time.Time                     `json:"projected_at,omitempty"`
}

type ThreadTurnProjectionSegment struct {
	Kind             ThreadTurnProjectionSegmentKind `json:"kind"`
	Text             string                          `json:"text,omitempty"`
	ActivityTimeline *observation.ActivityTimeline   `json:"activity_timeline,omitempty"`
	Signal           *ThreadTurnProjectionSignal     `json:"signal,omitempty"`
	EventIDs         []string                        `json:"event_ids,omitempty"`
}

type ThreadTurnProjectionSignal struct {
	Name   string `json:"name,omitempty"`
	CallID string `json:"call_id,omitempty"`
	Text   string `json:"text,omitempty"`
}

func ProjectThreadTurn(req ProjectThreadTurnRequest) ThreadTurnProjection {
	projection := ThreadTurnProjection{
		ThreadID:  req.ThreadID,
		TurnID:    req.TurnID,
		RunID:     req.RunID,
		TraceID:   req.TraceID,
		Projected: time.Now().UTC(),
	}
	events := threadTurnProjectionEvents(req.Events, req.TurnID)
	if len(events) == 0 {
		return projection
	}

	var text strings.Builder
	var activityEvents []ThreadDetailEvent
	flushText := func() {
		content := text.String()
		if strings.TrimSpace(content) == "" {
			text.Reset()
			return
		}
		projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
			Kind: ThreadTurnProjectionSegmentAssistantText,
			Text: content,
		})
		text.Reset()
	}
	addActivity := func(ev ThreadDetailEvent) {
		activityEvents = append(activityEvents, ev)
	}
	flushActivity := func() {
		if len(activityEvents) == 0 {
			return
		}
		timeline := threadTurnProjectionActivityTimeline(
			observation.ActivityRunMeta{
				RunID:    strings.TrimSpace(string(req.RunID)),
				ThreadID: strings.TrimSpace(string(req.ThreadID)),
				TurnID:   strings.TrimSpace(string(req.TurnID)),
				TraceID:  strings.TrimSpace(string(req.TraceID)),
			},
			activityEvents,
		)
		if len(timeline.Items) > 0 {
			projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
				Kind:             ThreadTurnProjectionSegmentActivityTimeline,
				ActivityTimeline: &timeline,
				EventIDs:         threadTurnProjectionEventIDs(activityEvents),
			})
		}
		activityEvents = nil
	}

	for _, ev := range events {
		switch ev.Kind {
		case ThreadDetailEventAssistantMessage:
			if ev.Message == nil {
				continue
			}
			if strings.TrimSpace(ev.Message.Kind) == "control_signal" {
				flushText()
				flushActivity()
				projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
					Kind: ThreadTurnProjectionSegmentControlSignal,
					Text: threadTurnProjectionMessageText(ev.Message),
					EventIDs: []string{
						strings.TrimSpace(ev.ID),
					},
				})
				continue
			}
			content := threadTurnProjectionMessageText(ev.Message)
			if strings.TrimSpace(content) == "" {
				continue
			}
			flushActivity()
			text.WriteString(content)
		case ThreadDetailEventToolCall:
			if ev.ToolCall == nil {
				continue
			}
			flushText()
			if ev.Message != nil && strings.TrimSpace(ev.Message.Kind) == "control_signal" {
				projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
					Kind: ThreadTurnProjectionSegmentControlSignal,
					Signal: &ThreadTurnProjectionSignal{
						Name:   strings.TrimSpace(ev.ToolCall.Name),
						CallID: strings.TrimSpace(ev.ToolCall.ID),
						Text:   threadTurnProjectionMessageText(ev.Message),
					},
					EventIDs: []string{strings.TrimSpace(ev.ID)},
				})
			}
			addActivity(ev)
		case ThreadDetailEventToolResult, ThreadDetailEventApproval, ThreadDetailEventTurnMarker, ThreadDetailEventError:
			flushText()
			addActivity(ev)
		}
	}
	flushText()
	flushActivity()
	return projection
}

func threadTurnProjectionEvents(events []ThreadDetailEvent, turnID TurnID) []ThreadDetailEvent {
	out := make([]ThreadDetailEvent, 0, len(events))
	turnIDText := strings.TrimSpace(string(turnID))
	for _, ev := range events {
		if turnIDText != "" && strings.TrimSpace(string(ev.TurnID)) != turnIDText {
			continue
		}
		out = append(out, ev)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if left.Ordinal != right.Ordinal {
			return left.Ordinal < right.Ordinal
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	return out
}

func threadTurnProjectionMessageText(msg *ThreadDetailMessage) string {
	if msg == nil {
		return ""
	}
	if strings.TrimSpace(msg.Content) != "" {
		return msg.Content
	}
	return msg.Preview
}

func threadTurnProjectionActivityTimeline(meta observation.ActivityRunMeta, detailEvents []ThreadDetailEvent) observation.ActivityTimeline {
	if len(detailEvents) == 0 {
		return observation.ActivityTimeline{}
	}
	events := make([]observation.Event, 0, len(detailEvents))
	for _, detail := range detailEvents {
		if ev, ok := threadTurnProjectionObservationEvent(meta, detail); ok {
			events = append(events, ev)
		}
	}
	timeline := observation.BuildActivityTimeline(meta, events, 0)
	if len(timeline.Items) == 0 {
		return observation.ActivityTimeline{}
	}
	if timeline.SchemaVersion <= 0 {
		timeline.SchemaVersion = observation.ActivityTimelineSchemaVersion
	}
	timeline.Summary = threadTurnProjectionActivitySummary(timeline.Items)
	return timeline
}

func threadTurnProjectionObservationEvent(meta observation.ActivityRunMeta, detail ThreadDetailEvent) (observation.Event, bool) {
	base := observation.Event{
		RunID:      firstProjectionNonEmpty(meta.RunID, strings.TrimSpace(string(detail.TurnID))),
		ThreadID:   firstProjectionNonEmpty(meta.ThreadID, strings.TrimSpace(string(detail.ThreadID))),
		TurnID:     firstProjectionNonEmpty(meta.TurnID, strings.TrimSpace(string(detail.TurnID))),
		TraceID:    strings.TrimSpace(meta.TraceID),
		Step:       int(detail.Ordinal),
		ObservedAt: detail.CreatedAt,
	}
	if detail.ActivityTimeline != nil && len(detail.ActivityTimeline.Items) > 0 {
		item := detail.ActivityTimeline.Items[0]
		base.Activity = threadTurnProjectionActivityPresentationFromItem(item)
		base.Metadata = threadTurnProjectionAnyMetadata(item.Metadata)
	}
	switch detail.Kind {
	case ThreadDetailEventToolCall:
		if detail.ToolCall == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolCall
		base.ToolID = strings.TrimSpace(detail.ToolCall.ID)
		base.ToolName = strings.TrimSpace(detail.ToolCall.Name)
		base.ToolKind = "local"
		if detail.Message != nil && strings.TrimSpace(detail.Message.Kind) == "control_signal" {
			base.Type = observation.EventTypeControlSignal
			base.ToolKind = "control"
			base.Message = threadTurnProjectionMessageText(detail.Message)
			base.Metadata = threadTurnProjectionMergeAnyMetadata(base.Metadata, map[string]any{"control_disposition": "terminal"})
		}
		return base, true
	case ThreadDetailEventToolResult:
		if detail.ToolResult == nil {
			return observation.Event{}, false
		}
		base.Type = observation.EventTypeToolResult
		base.ToolID = strings.TrimSpace(detail.ToolResult.CallID)
		base.ToolName = strings.TrimSpace(detail.ToolResult.ToolName)
		base.ToolKind = "local"
		base.Error = strings.TrimSpace(detail.Error)
		if base.Error == "" && strings.TrimSpace(detail.ToolResult.Status) == string(observation.ActivityStatusError) {
			base.Error = "tool_result_error"
		}
		base.Metadata = threadTurnProjectionMergeAnyMetadata(threadTurnProjectionAnyMetadata(detail.Metadata), base.Metadata)
		if strings.TrimSpace(detail.ToolResult.Status) == string(observation.ActivityStatusRunning) {
			base.Metadata = threadTurnProjectionMergeAnyMetadata(base.Metadata, map[string]any{"pending_tool_result": true, "pending_state": string(observation.ActivityStatusRunning)})
		}
		return base, true
	case ThreadDetailEventApproval:
		if detail.Approval == nil {
			return observation.Event{}, false
		}
		base.Type = threadTurnProjectionApprovalEventType(detail)
		base.ToolID = strings.TrimSpace(detail.Approval.ToolID)
		base.ToolName = strings.TrimSpace(detail.Approval.ToolName)
		base.ToolKind = firstProjectionNonEmpty(strings.TrimSpace(detail.Approval.ToolKind), "local")
		base.ArgsHash = strings.TrimSpace(detail.Approval.ArgsHash)
		base.Metadata = threadTurnProjectionMergeAnyMetadata(threadTurnProjectionAnyMetadata(detail.Approval.Metadata), base.Metadata)
		switch strings.TrimSpace(detail.Approval.State) {
		case "rejected", "timed_out":
			base.Error = firstProjectionNonEmpty(strings.TrimSpace(detail.Approval.Reason), "tool_approval_"+strings.TrimSpace(detail.Approval.State))
		default:
			base.Error = strings.TrimSpace(detail.Error)
		}
		return base, true
	case ThreadDetailEventTurnMarker:
		if detail.TurnMarker == nil {
			return observation.Event{}, false
		}
		status := strings.TrimSpace(detail.TurnMarker.Status)
		base.Type = observation.EventTypeRunEnd
		base.Message = threadTurnProjectionRunEndStatus(status)
		base.Metadata = threadTurnProjectionAnyMetadata(detail.TurnMarker.Metadata)
		base.Error = strings.TrimSpace(detail.Error)
		if base.Error == "" && (status == "failed" || status == string(observation.ActivityStatusError)) {
			base.Error = firstProjectionNonEmpty(threadTurnProjectionMetadataString(base.Metadata, "failure_reason"), "turn_failed")
		}
		return base, true
	case ThreadDetailEventError:
		base.Type = observation.EventTypeRunEnd
		base.Message = string(observation.ActivityStatusError)
		base.Error = firstProjectionNonEmpty(strings.TrimSpace(detail.Error), "thread_error")
		base.Metadata = threadTurnProjectionAnyMetadata(detail.Metadata)
		return base, true
	default:
		return observation.Event{}, false
	}
}

func threadTurnProjectionRunEndStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "aborted", "cancelled":
		return string(observation.ActivityStatusCanceled)
	default:
		return strings.TrimSpace(status)
	}
}

func threadTurnProjectionApprovalEventType(detail ThreadDetailEvent) string {
	switch strings.TrimSpace(detail.Type) {
	case observation.EventTypeToolApprovalRequested,
		observation.EventTypeToolApprovalApproved,
		observation.EventTypeToolApprovalRejected,
		observation.EventTypeToolApprovalTimedOut,
		observation.EventTypeToolApprovalCanceled:
		return strings.TrimSpace(detail.Type)
	}
	if detail.Approval == nil {
		return observation.EventTypeToolApprovalRequested
	}
	switch strings.TrimSpace(detail.Approval.State) {
	case "approved":
		return observation.EventTypeToolApprovalApproved
	case "rejected":
		return observation.EventTypeToolApprovalRejected
	case "timed_out":
		return observation.EventTypeToolApprovalTimedOut
	case "canceled", "cancelled":
		return observation.EventTypeToolApprovalCanceled
	default:
		return observation.EventTypeToolApprovalRequested
	}
}

func threadTurnProjectionActivityPresentationFromItem(item observation.ActivityItem) *observation.ActivityPresentation {
	if strings.TrimSpace(item.Label) == "" &&
		strings.TrimSpace(item.Description) == "" &&
		item.Renderer == "" &&
		len(item.Chips) == 0 &&
		len(item.TargetRefs) == 0 &&
		len(item.Payload) == 0 {
		return nil
	}
	return &observation.ActivityPresentation{
		Label:       item.Label,
		Description: item.Description,
		Renderer:    item.Renderer,
		Chips:       append([]observation.ActivityChip(nil), item.Chips...),
		TargetRefs:  append([]observation.ActivityTargetRef(nil), item.TargetRefs...),
		Payload:     cloneProjectionActivityPayload(item.Payload),
	}
}

func threadTurnProjectionAnyMetadata(in map[string]string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if key = strings.TrimSpace(key); key != "" && strings.TrimSpace(value) != "" {
			out[key] = strings.TrimSpace(value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func threadTurnProjectionMergeAnyMetadata(left, right map[string]any) map[string]any {
	if len(left) == 0 {
		return cloneProjectionAnyMap(right)
	}
	out := cloneProjectionAnyMap(left)
	for key, value := range right {
		if strings.TrimSpace(key) != "" && value != nil {
			out[key] = value
		}
	}
	return out
}

func threadTurnProjectionMetadataString(meta map[string]any, key string) string {
	value, ok := meta[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

func firstProjectionNonEmpty(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func cloneProjectionActivityPayload(in map[string]any) map[string]any {
	return cloneProjectionAnyMap(in)
}

func cloneProjectionAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func threadTurnProjectionActivitySummary(items []observation.ActivityItem) observation.ActivitySummary {
	summary := observation.ActivitySummary{
		Status:     observation.ActivityStatusPending,
		Severity:   observation.ActivitySeverityQuiet,
		TotalItems: len(items),
	}
	attentionSeen := map[observation.ActivityAttentionReason]struct{}{}
	for _, item := range items {
		switch item.Status {
		case observation.ActivityStatusPending:
			summary.Counts.Pending++
		case observation.ActivityStatusRunning:
			summary.Counts.Running++
		case observation.ActivityStatusWaiting:
			summary.Counts.Waiting++
		case observation.ActivityStatusSuccess:
			summary.Counts.Success++
		case observation.ActivityStatusError:
			summary.Counts.Error++
		case observation.ActivityStatusCanceled:
			summary.Counts.Canceled++
		}
		if item.RequiresApproval {
			summary.Counts.Approval++
		}
		if item.NeedsAttention {
			summary.NeedsAttention = true
		}
		summary.Severity = threadTurnProjectionMaxSeverity(summary.Severity, item.Severity)
		for _, reason := range item.AttentionReasons {
			if _, ok := attentionSeen[reason]; ok {
				continue
			}
			attentionSeen[reason] = struct{}{}
			summary.AttentionReasons = append(summary.AttentionReasons, reason)
		}
	}
	if len(summary.AttentionReasons) > 0 {
		summary.NeedsAttention = true
	}
	switch {
	case summary.Counts.Waiting > 0:
		summary.Status = observation.ActivityStatusWaiting
	case summary.Counts.Running > 0:
		summary.Status = observation.ActivityStatusRunning
	case summary.Counts.Pending > 0:
		summary.Status = observation.ActivityStatusPending
	case summary.Counts.Error > 0:
		summary.Status = observation.ActivityStatusError
	case summary.Counts.Canceled > 0 && summary.Counts.Success == 0:
		summary.Status = observation.ActivityStatusCanceled
	default:
		summary.Status = observation.ActivityStatusSuccess
	}
	if summary.Counts.Error > 0 && summary.Status != observation.ActivityStatusWaiting {
		summary.Status = observation.ActivityStatusError
	}
	return summary
}

func threadTurnProjectionMaxSeverity(left observation.ActivitySeverity, right observation.ActivitySeverity) observation.ActivitySeverity {
	if threadTurnProjectionSeverityRank(right) > threadTurnProjectionSeverityRank(left) {
		return right
	}
	return left
}

func threadTurnProjectionSeverityRank(severity observation.ActivitySeverity) int {
	switch severity {
	case observation.ActivitySeverityBlocking:
		return 4
	case observation.ActivitySeverityError:
		return 3
	case observation.ActivitySeverityWarning:
		return 2
	case observation.ActivitySeverityNormal:
		return 1
	default:
		return 0
	}
}

func threadTurnProjectionEventIDs(events []ThreadDetailEvent) []string {
	out := make([]string, 0, len(events))
	seen := map[string]struct{}{}
	for _, ev := range events {
		id := strings.TrimSpace(ev.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
