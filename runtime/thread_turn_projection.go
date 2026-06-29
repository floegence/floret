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
	ThreadID         ThreadID
	TurnID           TurnID
	RunID            RunID
	TraceID          TraceID
	Events           []ThreadDetailEvent
	ActivityTimeline observation.ActivityTimeline
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
		if len(req.ActivityTimeline.Items) > 0 {
			timeline := *observation.CloneActivityTimeline(&req.ActivityTimeline)
			timeline.Summary = threadTurnProjectionActivitySummary(timeline.Items)
			projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
				Kind:             ThreadTurnProjectionSegmentActivityTimeline,
				ActivityTimeline: &timeline,
			})
		}
		return projection
	}

	var text strings.Builder
	var activityKeys []string
	activityKeySet := map[string]struct{}{}
	var activityEvents []ThreadDetailEvent
	var activityTimelines []observation.ActivityTimeline
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
	addActivityKey := func(key string) {
		key = strings.TrimSpace(key)
		if key == "" {
			return
		}
		if _, ok := activityKeySet[key]; ok {
			return
		}
		activityKeySet[key] = struct{}{}
		activityKeys = append(activityKeys, key)
	}
	addActivity := func(ev ThreadDetailEvent) {
		activityEvents = append(activityEvents, ev)
		addActivityKey(threadTurnProjectionEventToolID(ev))
		addActivityKey(ev.ID)
		if ev.ActivityTimeline != nil && len(ev.ActivityTimeline.Items) > 0 {
			activityTimelines = append(activityTimelines, *observation.CloneActivityTimeline(ev.ActivityTimeline))
			for _, item := range ev.ActivityTimeline.Items {
				addActivityKey(item.ItemID)
				addActivityKey(item.ToolID)
			}
		}
	}
	flushActivity := func() {
		if len(activityEvents) == 0 && len(activityTimelines) == 0 {
			return
		}
		timeline := threadTurnProjectionActivityTimeline(req.ActivityTimeline, activityKeys, activityTimelines)
		if len(timeline.Items) > 0 {
			projection.Segments = append(projection.Segments, ThreadTurnProjectionSegment{
				Kind:             ThreadTurnProjectionSegmentActivityTimeline,
				ActivityTimeline: &timeline,
				EventIDs:         threadTurnProjectionEventIDs(activityEvents),
			})
		}
		activityKeys = nil
		activityKeySet = map[string]struct{}{}
		activityEvents = nil
		activityTimelines = nil
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
		case ThreadDetailEventToolResult, ThreadDetailEventApproval, ThreadDetailEventInput, ThreadDetailEventCustom:
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

func threadTurnProjectionEventToolID(ev ThreadDetailEvent) string {
	switch ev.Kind {
	case ThreadDetailEventToolCall:
		if ev.ToolCall != nil {
			return ev.ToolCall.ID
		}
	case ThreadDetailEventToolResult:
		if ev.ToolResult != nil {
			return ev.ToolResult.CallID
		}
	case ThreadDetailEventApproval:
		if ev.Approval != nil {
			return ev.Approval.ToolID
		}
	}
	return ""
}

func threadTurnProjectionActivityTimeline(canonical observation.ActivityTimeline, keys []string, rowTimelines []observation.ActivityTimeline) observation.ActivityTimeline {
	if len(keys) == 0 && len(rowTimelines) == 0 {
		return observation.ActivityTimeline{}
	}
	keySet := map[string]struct{}{}
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			keySet[key] = struct{}{}
		}
	}
	items := make([]observation.ActivityItem, 0)
	if len(canonical.Items) > 0 && len(keySet) > 0 {
		for _, item := range canonical.Items {
			if threadTurnProjectionActivityItemMatches(item, keySet) {
				items = append(items, item)
			}
		}
	}
	if len(items) == 0 {
		seen := map[string]struct{}{}
		for _, timeline := range rowTimelines {
			for _, item := range timeline.Items {
				id := strings.TrimSpace(item.ItemID)
				if id == "" {
					continue
				}
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				items = append(items, item)
			}
		}
	}
	if len(items) == 0 {
		return observation.ActivityTimeline{}
	}
	timeline := canonical
	if timeline.SchemaVersion == 0 {
		timeline.SchemaVersion = observation.ActivityTimelineSchemaVersion
	}
	timeline.Items = items
	timeline.Summary = threadTurnProjectionActivitySummary(items)
	return timeline
}

func threadTurnProjectionActivityItemMatches(item observation.ActivityItem, keys map[string]struct{}) bool {
	for _, value := range []string{item.ItemID, item.ToolID} {
		if _, ok := keys[strings.TrimSpace(value)]; ok {
			return true
		}
	}
	return false
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
