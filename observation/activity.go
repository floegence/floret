package observation

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	EventTypeToolCall              = "tool_call"
	EventTypeToolResult            = "tool_result"
	EventTypeToolApprovalRequested = "tool_approval_requested"
	EventTypeToolApprovalApproved  = "tool_approval_approved"
	EventTypeToolApprovalRejected  = "tool_approval_rejected"
	EventTypeToolApprovalTimedOut  = "tool_approval_timed_out"
	EventTypeToolApprovalCanceled  = "tool_approval_canceled"
	EventTypeHostedToolCall        = "hosted_tool_call"
	EventTypeHostedToolResult      = "hosted_tool_result"
	EventTypeControlSignal         = "control_signal"
	EventTypeBudgetExceeded        = "budget_exceeded"
	EventTypeRunEnd                = "run_end"

	ActivityTimelineSchemaVersion = 1

	ActivityKindTool     ActivityKind = "tool"
	ActivityKindHosted   ActivityKind = "hosted_tool"
	ActivityKindApproval ActivityKind = "approval"
	ActivityKindControl  ActivityKind = "control"
	ActivityKindBudget   ActivityKind = "budget"
)

type ActivityKind string

type ActivityStatus string

const (
	ActivityStatusPending  ActivityStatus = "pending"
	ActivityStatusRunning  ActivityStatus = "running"
	ActivityStatusWaiting  ActivityStatus = "waiting"
	ActivityStatusSuccess  ActivityStatus = "success"
	ActivityStatusError    ActivityStatus = "error"
	ActivityStatusCanceled ActivityStatus = "canceled"
)

type ActivitySeverity string

const (
	ActivitySeverityQuiet    ActivitySeverity = "quiet"
	ActivitySeverityNormal   ActivitySeverity = "normal"
	ActivitySeverityWarning  ActivitySeverity = "warning"
	ActivitySeverityError    ActivitySeverity = "error"
	ActivitySeverityBlocking ActivitySeverity = "blocking"
)

type ActivityAttentionReason string

const (
	ActivityAttentionRunning  ActivityAttentionReason = "running"
	ActivityAttentionWaiting  ActivityAttentionReason = "waiting"
	ActivityAttentionApproval ActivityAttentionReason = "approval"
	ActivityAttentionError    ActivityAttentionReason = "error"
)

type ActivityRunMeta struct {
	RunID    string `json:"run_id,omitempty"`
	ThreadID string `json:"thread_id,omitempty"`
	TurnID   string `json:"turn_id,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
}

type ActivityRenderer string

const (
	ActivityRendererStructured ActivityRenderer = "structured"
	ActivityRendererTerminal   ActivityRenderer = "terminal"
	ActivityRendererFile       ActivityRenderer = "file"
	ActivityRendererPatch      ActivityRenderer = "patch"
	ActivityRendererWebSearch  ActivityRenderer = "web_search"
	ActivityRendererTodos      ActivityRenderer = "todos"
	ActivityRendererQuestion   ActivityRenderer = "question"
	ActivityRendererCompletion ActivityRenderer = "completion"
)

type ActivityChip struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Value string `json:"value,omitempty"`
	Tone  string `json:"tone,omitempty"`
}

type ActivityTargetRef struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	URI   string `json:"uri,omitempty"`
	Path  string `json:"path,omitempty"`
	Line  int    `json:"line,omitempty"`
}

type ActivityPresentation struct {
	Label       string              `json:"label,omitempty"`
	Description string              `json:"description,omitempty"`
	Renderer    ActivityRenderer    `json:"renderer,omitempty"`
	Chips       []ActivityChip      `json:"chips,omitempty"`
	TargetRefs  []ActivityTargetRef `json:"target_refs,omitempty"`
	// Payload is host-supplied public display data. Floret preserves the generic
	// activity shape and lifecycle, but product field policy belongs to the host.
	Payload map[string]any `json:"payload,omitempty"`
}

type ActivityItem struct {
	ItemID           string                    `json:"item_id"`
	ToolID           string                    `json:"tool_id,omitempty"`
	ToolName         string                    `json:"tool_name,omitempty"`
	Kind             ActivityKind              `json:"kind"`
	Status           ActivityStatus            `json:"status"`
	Severity         ActivitySeverity          `json:"severity"`
	NeedsAttention   bool                      `json:"needs_attention"`
	AttentionReasons []ActivityAttentionReason `json:"attention_reasons,omitempty"`
	RequiresApproval bool                      `json:"requires_approval"`
	ApprovalState    string                    `json:"approval_state,omitempty"`
	StartedAtUnixMS  int64                     `json:"started_at_unix_ms,omitempty"`
	EndedAtUnixMS    int64                     `json:"ended_at_unix_ms,omitempty"`
	Label            string                    `json:"label,omitempty"`
	Description      string                    `json:"description,omitempty"`
	Renderer         ActivityRenderer          `json:"renderer,omitempty"`
	Chips            []ActivityChip            `json:"chips,omitempty"`
	TargetRefs       []ActivityTargetRef       `json:"target_refs,omitempty"`
	Payload          map[string]any            `json:"payload,omitempty"`
	Metadata         map[string]string         `json:"metadata,omitempty"`
}

type ActivityCounts struct {
	Pending  int `json:"pending,omitempty"`
	Running  int `json:"running,omitempty"`
	Waiting  int `json:"waiting,omitempty"`
	Success  int `json:"success,omitempty"`
	Error    int `json:"error,omitempty"`
	Canceled int `json:"canceled,omitempty"`
	Approval int `json:"approval,omitempty"`
}

type ActivitySummary struct {
	Status           ActivityStatus            `json:"status"`
	Severity         ActivitySeverity          `json:"severity"`
	NeedsAttention   bool                      `json:"needs_attention"`
	AttentionReasons []ActivityAttentionReason `json:"attention_reasons,omitempty"`
	TotalItems       int                       `json:"total_items"`
	Counts           ActivityCounts            `json:"counts"`
	DurationMS       int64                     `json:"duration_ms,omitempty"`
}

type ActivityTimeline struct {
	SchemaVersion int             `json:"schema_version"`
	RunID         string          `json:"run_id,omitempty"`
	ThreadID      string          `json:"thread_id,omitempty"`
	TurnID        string          `json:"turn_id,omitempty"`
	TraceID       string          `json:"trace_id,omitempty"`
	Summary       ActivitySummary `json:"summary"`
	Items         []ActivityItem  `json:"items"`
}

func CloneActivityPresentation(in *ActivityPresentation) *ActivityPresentation {
	if in == nil {
		return nil
	}
	return &ActivityPresentation{
		Label:       in.Label,
		Description: in.Description,
		Renderer:    in.Renderer,
		Chips:       cloneActivityChips(in.Chips),
		TargetRefs:  cloneActivityTargetRefs(in.TargetRefs),
		Payload:     cloneActivityPayload(in.Payload),
	}
}

func CloneActivityTimeline(in *ActivityTimeline) *ActivityTimeline {
	if in == nil {
		return nil
	}
	out := *in
	out.Summary.AttentionReasons = append([]ActivityAttentionReason(nil), in.Summary.AttentionReasons...)
	out.Items = make([]ActivityItem, len(in.Items))
	for i, item := range in.Items {
		out.Items[i] = cloneActivityItem(item)
	}
	return &out
}

func cloneActivityItem(in ActivityItem) ActivityItem {
	in.AttentionReasons = append([]ActivityAttentionReason(nil), in.AttentionReasons...)
	in.Chips = cloneActivityChips(in.Chips)
	in.TargetRefs = cloneActivityTargetRefs(in.TargetRefs)
	in.Payload = cloneActivityPayload(in.Payload)
	in.Metadata = cloneActivityMetadata(in.Metadata)
	return in
}

type activityItemState struct {
	item     ActivityItem
	order    int
	lastSeen int64
}

// BuildActivityTimeline projects sanitized runtime events into a stable
// activity summary. Tool-facing display details enter the timeline only through
// an explicit ActivityPresentation that has already crossed the event sanitizer.
func BuildActivityTimeline(meta ActivityRunMeta, events []Event, nowUnixMS int64) ActivityTimeline {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		RunID:         strings.TrimSpace(meta.RunID),
		ThreadID:      strings.TrimSpace(meta.ThreadID),
		TurnID:        strings.TrimSpace(meta.TurnID),
		TraceID:       strings.TrimSpace(meta.TraceID),
		Summary: ActivitySummary{
			Status:   ActivityStatusPending,
			Severity: ActivitySeverityQuiet,
		},
		Items: []ActivityItem{},
	}
	if nowUnixMS <= 0 {
		nowUnixMS = time.Now().UnixMilli()
	}
	items := map[string]*activityItemState{}
	order := []string{}
	var runEnd *Event
	var firstAt int64
	var lastAt int64
	hasExplicitControlActivity := false
	for index, ev := range events {
		if timeline.RunID == "" {
			timeline.RunID = strings.TrimSpace(ev.RunID)
		}
		if timeline.ThreadID == "" {
			timeline.ThreadID = strings.TrimSpace(ev.ThreadID)
		}
		if timeline.TurnID == "" {
			timeline.TurnID = strings.TrimSpace(ev.TurnID)
		}
		if timeline.TraceID == "" {
			timeline.TraceID = strings.TrimSpace(ev.TraceID)
		}
		observedAt := eventUnixMS(ev, nowUnixMS)
		switch ev.Type {
		case EventTypeToolCall, EventTypeHostedToolCall:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityToolKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:          key,
					ToolID:          strings.TrimSpace(ev.ToolID),
					ToolName:        strings.TrimSpace(ev.ToolName),
					Kind:            activityToolKind(ev),
					Status:          ActivityStatusRunning,
					Severity:        ActivitySeverityNormal,
					StartedAtUnixMS: observedAt,
					Metadata:        activityMetadata(ev),
				}
			})
			state.item.ToolID = firstNonEmpty(state.item.ToolID, strings.TrimSpace(ev.ToolID))
			state.item.ToolName = firstNonEmpty(state.item.ToolName, strings.TrimSpace(ev.ToolName))
			state.item.Kind = firstNonEmptyActivityKind(state.item.Kind, activityToolKind(ev))
			if state.item.StartedAtUnixMS == 0 {
				state.item.StartedAtUnixMS = observedAt
			}
			if state.item.Status == ActivityStatusPending {
				state.item.Status = ActivityStatusRunning
			}
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			if activityMetadataBool(ev.Metadata, "pending_tool_result") {
				state.item.Severity = ActivitySeverityWarning
			}
			state.lastSeen = observedAt
		case EventTypeToolResult, EventTypeHostedToolResult:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityToolKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				startedAt := int64(0)
				if ev.DurationMS > 0 && observedAt > ev.DurationMS {
					startedAt = observedAt - ev.DurationMS
				}
				return ActivityItem{
					ItemID:          key,
					ToolID:          strings.TrimSpace(ev.ToolID),
					ToolName:        strings.TrimSpace(ev.ToolName),
					Kind:            activityToolKind(ev),
					Status:          ActivityStatusPending,
					Severity:        ActivitySeverityNormal,
					StartedAtUnixMS: startedAt,
				}
			})
			state.item.ToolID = firstNonEmpty(state.item.ToolID, strings.TrimSpace(ev.ToolID))
			state.item.ToolName = firstNonEmpty(state.item.ToolName, strings.TrimSpace(ev.ToolName))
			state.item.Kind = firstNonEmptyActivityKind(state.item.Kind, activityToolKind(ev))
			state.item.EndedAtUnixMS = observedAt
			if ev.DurationMS > 0 && observedAt > ev.DurationMS {
				durationStart := observedAt - ev.DurationMS
				if state.item.StartedAtUnixMS == 0 || state.item.StartedAtUnixMS > durationStart {
					state.item.StartedAtUnixMS = durationStart
				}
			}
			resultStatus := activityMetadataValue(ev, "tool_result_status")
			if activityEventHasError(ev) || resultStatus == string(ActivityStatusError) {
				state.item.Status = ActivityStatusError
				state.item.Severity = ActivitySeverityError
			} else if activityMetadataBool(ev.Metadata, "pending_tool_result") {
				state.item.Status = ActivityStatusRunning
				state.item.Severity = ActivitySeverityWarning
				state.item.EndedAtUnixMS = 0
			} else if resultStatus == string(ActivityStatusCanceled) {
				state.item.Status = ActivityStatusCanceled
				state.item.Severity = ActivitySeverityWarning
			} else {
				state.item.Status = ActivityStatusSuccess
				state.item.Severity = ActivitySeverityNormal
			}
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			if state.item.Status != ActivityStatusRunning {
				pending := activityHasPendingMetadata(state.item.Metadata) || activityHasPendingPayload(state.item.Payload)
				state.item.Metadata = activityTerminalMetadata(state.item.Metadata)
				state.item.Payload = activityTerminalPayload(state.item.Payload)
				state.item.Chips = activityTerminalChips(state.item.Chips, pending)
			}
			state.lastSeen = observedAt
		case EventTypeToolApprovalRequested:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			state := ensureActivityItem(items, &order, activityApprovalKey(ev, index), len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:           activityApprovalKey(ev, index),
					ToolID:           strings.TrimSpace(ev.ToolID),
					ToolName:         strings.TrimSpace(ev.ToolName),
					Kind:             ActivityKindApproval,
					Status:           ActivityStatusWaiting,
					Severity:         ActivitySeverityBlocking,
					RequiresApproval: true,
					ApprovalState:    "requested",
					StartedAtUnixMS:  observedAt,
					Metadata:         activityMetadata(ev),
				}
			})
			state.item.Status = ActivityStatusWaiting
			state.item.Severity = ActivitySeverityBlocking
			state.item.RequiresApproval = true
			state.item.ApprovalState = "requested"
			state.item.EndedAtUnixMS = 0
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			state.lastSeen = observedAt
		case EventTypeToolApprovalApproved, EventTypeToolApprovalRejected, EventTypeToolApprovalTimedOut, EventTypeToolApprovalCanceled:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityApprovalKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:           key,
					ToolID:           strings.TrimSpace(ev.ToolID),
					ToolName:         strings.TrimSpace(ev.ToolName),
					Kind:             ActivityKindApproval,
					Status:           ActivityStatusPending,
					Severity:         ActivitySeverityNormal,
					RequiresApproval: true,
					StartedAtUnixMS:  observedAt,
				}
			})
			state.item.EndedAtUnixMS = observedAt
			state.item.RequiresApproval = true
			switch ev.Type {
			case EventTypeToolApprovalApproved:
				state.item.Status = ActivityStatusSuccess
				state.item.Severity = ActivitySeverityNormal
				state.item.ApprovalState = "approved"
			case EventTypeToolApprovalRejected:
				state.item.Status = ActivityStatusError
				state.item.Severity = ActivitySeverityError
				state.item.ApprovalState = "rejected"
			case EventTypeToolApprovalTimedOut:
				state.item.Status = ActivityStatusError
				state.item.Severity = ActivitySeverityBlocking
				state.item.ApprovalState = "timed_out"
			case EventTypeToolApprovalCanceled:
				state.item.Status = ActivityStatusCanceled
				state.item.Severity = ActivitySeverityWarning
				state.item.ApprovalState = "canceled"
			}
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			state.lastSeen = observedAt
		case EventTypeControlSignal:
			hasExplicitControlActivity = true
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityControlKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:          key,
					ToolID:          strings.TrimSpace(ev.ToolID),
					ToolName:        strings.TrimSpace(ev.ToolName),
					Kind:            ActivityKindControl,
					Status:          activityControlStatus(ev),
					Severity:        activityControlSeverity(ev),
					StartedAtUnixMS: observedAt,
					EndedAtUnixMS:   observedAt,
					Metadata:        activityMetadata(ev),
				}
			})
			state.item.ToolID = firstNonEmpty(state.item.ToolID, strings.TrimSpace(ev.ToolID))
			state.item.ToolName = firstNonEmpty(state.item.ToolName, strings.TrimSpace(ev.ToolName))
			state.item.Kind = ActivityKindControl
			state.item.Status = activityControlStatus(ev)
			state.item.Severity = activityControlSeverity(ev)
			state.item.StartedAtUnixMS = observedAt
			state.item.EndedAtUnixMS = observedAt
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			state.lastSeen = observedAt
		case EventTypeBudgetExceeded:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := fmt.Sprintf("budget:%d:%d", ev.Step, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:          key,
					Kind:            ActivityKindBudget,
					Status:          ActivityStatusError,
					Severity:        ActivitySeverityBlocking,
					StartedAtUnixMS: observedAt,
					EndedAtUnixMS:   observedAt,
				}
			})
			state.item.Status = ActivityStatusError
			state.item.Severity = ActivitySeverityBlocking
			state.lastSeen = observedAt
		case EventTypeRunEnd:
			evCopy := ev
			runEnd = &evCopy
			if item, ok := activityRunEndControlItem(ev, index, observedAt); ok && !hasExplicitControlActivity {
				noteActivityTime(observedAt, &firstAt, &lastAt)
				key := item.ItemID
				state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
					return item
				})
				state.item.Status = item.Status
				state.item.Severity = item.Severity
				state.item.StartedAtUnixMS = item.StartedAtUnixMS
				state.item.EndedAtUnixMS = item.EndedAtUnixMS
				mergeActivityPresentationIntoItem(&state.item, ev.Activity)
				state.lastSeen = observedAt
			}
		}
	}
	for _, key := range order {
		state := items[key]
		if runEnd != nil {
			settleUnresolvedActivityItemAtRunEnd(&state.item, *runEnd, nowUnixMS)
		}
		state.item.AttentionReasons = activityItemAttentionReasons(state.item)
		state.item.NeedsAttention = len(state.item.AttentionReasons) > 0
		timeline.Items = append(timeline.Items, state.item)
	}
	sort.SliceStable(timeline.Items, func(i, j int) bool {
		left := activityItemSortTime(timeline.Items[i])
		right := activityItemSortTime(timeline.Items[j])
		if left != 0 && right != 0 && left != right {
			return left < right
		}
		return slices.Index(order, timeline.Items[i].ItemID) < slices.Index(order, timeline.Items[j].ItemID)
	})
	timeline.Summary = activitySummary(timeline.Items, runEnd, firstAt, lastAt, nowUnixMS)
	return timeline
}

func ValidateActivityTimeline(timeline ActivityTimeline) error {
	if timeline.SchemaVersion != ActivityTimelineSchemaVersion {
		return fmt.Errorf("activity timeline schema version %d is not supported", timeline.SchemaVersion)
	}
	if err := validateActivityStatus(timeline.Summary.Status); err != nil {
		return fmt.Errorf("summary status: %w", err)
	}
	if err := validateActivitySeverity(timeline.Summary.Severity); err != nil {
		return fmt.Errorf("summary severity: %w", err)
	}
	for _, reason := range timeline.Summary.AttentionReasons {
		if err := validateActivityAttentionReason(reason); err != nil {
			return fmt.Errorf("summary attention reason: %w", err)
		}
	}
	seen := map[string]struct{}{}
	for i, item := range timeline.Items {
		if strings.TrimSpace(item.ItemID) == "" {
			return fmt.Errorf("item %d item_id is required", i)
		}
		if _, ok := seen[item.ItemID]; ok {
			return fmt.Errorf("item_id %q is duplicated", item.ItemID)
		}
		seen[item.ItemID] = struct{}{}
		if strings.TrimSpace(string(item.Kind)) == "" {
			return fmt.Errorf("item %q kind is required", item.ItemID)
		}
		if err := validateActivityKind(item.Kind); err != nil {
			return fmt.Errorf("item %q kind: %w", item.ItemID, err)
		}
		if err := validateActivityStatus(item.Status); err != nil {
			return fmt.Errorf("item %q status: %w", item.ItemID, err)
		}
		if err := validateActivitySeverity(item.Severity); err != nil {
			return fmt.Errorf("item %q severity: %w", item.ItemID, err)
		}
		if item.StartedAtUnixMS > 0 && item.EndedAtUnixMS > 0 && item.EndedAtUnixMS < item.StartedAtUnixMS {
			return fmt.Errorf("item %q ended_at_unix_ms must not be before started_at_unix_ms", item.ItemID)
		}
		for _, reason := range item.AttentionReasons {
			if err := validateActivityAttentionReason(reason); err != nil {
				return fmt.Errorf("item %q attention reason: %w", item.ItemID, err)
			}
		}
		if item.ApprovalState != "" {
			if err := validateActivityApprovalState(item.ApprovalState); err != nil {
				return fmt.Errorf("item %q approval state: %w", item.ItemID, err)
			}
			if err := validateActivityItemApprovalLifecycle(item); err != nil {
				return fmt.Errorf("item %q approval lifecycle: %w", item.ItemID, err)
			}
		}
		if err := validateActivityItemPresentation(item); err != nil {
			return fmt.Errorf("item %q presentation: %w", item.ItemID, err)
		}
		if err := validateActivityItemMetadata(item.Metadata); err != nil {
			return fmt.Errorf("item %q metadata: %w", item.ItemID, err)
		}
	}
	return nil
}

func mergeActivityPresentationIntoItem(item *ActivityItem, presentation *ActivityPresentation) {
	if item == nil || presentation == nil {
		return
	}
	if value := strings.TrimSpace(presentation.Label); value != "" {
		item.Label = value
	}
	if value := strings.TrimSpace(presentation.Description); value != "" {
		item.Description = value
	}
	if presentation.Renderer != "" {
		item.Renderer = presentation.Renderer
	}
	if len(presentation.Chips) > 0 {
		item.Chips = cloneActivityChips(presentation.Chips)
	}
	if len(presentation.TargetRefs) > 0 {
		item.TargetRefs = cloneActivityTargetRefs(presentation.TargetRefs)
	}
	if len(presentation.Payload) > 0 {
		item.Payload = mergeActivityPayload(item.Payload, presentation.Payload)
	}
}

func ensureActivityItem(items map[string]*activityItemState, order *[]string, key string, index int, create func() ActivityItem) *activityItemState {
	if state, ok := items[key]; ok {
		return state
	}
	item := create()
	if item.ItemID == "" {
		item.ItemID = key
	}
	if item.Status == "" {
		item.Status = ActivityStatusPending
	}
	if item.Severity == "" {
		item.Severity = ActivitySeverityQuiet
	}
	state := &activityItemState{item: item, order: index}
	items[key] = state
	*order = append(*order, key)
	return state
}

func activityToolKey(ev Event, index int) string {
	if ev.ToolID != "" {
		return "tool:" + ev.ToolID
	}
	if ev.ToolName != "" {
		return fmt.Sprintf("tool:%s:%d:%d", ev.ToolName, ev.Step, index)
	}
	return fmt.Sprintf("tool:%d:%d", ev.Step, index)
}

func activityApprovalKey(ev Event, index int) string {
	if id := activityMetadataValue(ev, "approval_id_hash"); id != "" {
		return "approval:" + id
	}
	if id := activityRawMetadataString(ev.Metadata, "approval_id"); id != "" {
		return "approval:" + hashActivityToken(id)
	}
	if ev.ToolID != "" {
		return "approval:" + ev.ToolID
	}
	return fmt.Sprintf("approval:%d:%d", ev.Step, index)
}

func activityControlKey(ev Event, index int) string {
	if ev.ToolID != "" {
		return "control:" + ev.ToolID
	}
	if ev.ToolName != "" {
		return fmt.Sprintf("control:%s:%d:%d", ev.ToolName, ev.Step, index)
	}
	return fmt.Sprintf("control:%d:%d", ev.Step, index)
}

func activityToolKind(ev Event) ActivityKind {
	if ev.ToolKind == "control" {
		return ActivityKindControl
	}
	if ev.Type == EventTypeHostedToolCall || ev.Type == EventTypeHostedToolResult || ev.ToolKind == "hosted" {
		return ActivityKindHosted
	}
	return ActivityKindTool
}

func activityRunEndControlItem(ev Event, index int, observedAt int64) (ActivityItem, bool) {
	status := strings.TrimSpace(ev.Message)
	switch status {
	case string(ActivityStatusWaiting):
		return ActivityItem{
			ItemID:          fmt.Sprintf("control:waiting:%d:%d", ev.Step, index),
			Kind:            ActivityKindControl,
			Status:          ActivityStatusWaiting,
			Severity:        ActivitySeverityBlocking,
			StartedAtUnixMS: observedAt,
			EndedAtUnixMS:   observedAt,
			Metadata:        activityMetadata(ev),
		}, true
	default:
		return ActivityItem{}, false
	}
}

func settleUnresolvedActivityItemAtRunEnd(item *ActivityItem, runEnd Event, nowUnixMS int64) {
	if item == nil {
		return
	}
	if (activityHasPendingMetadata(item.Metadata) || activityHasPendingPayload(item.Payload)) &&
		!activityRunEndIsCanceled(runEnd) &&
		!activityEventHasError(runEnd) {
		return
	}
	if item.RequiresApproval && !activityRunEndIsCanceled(runEnd) && !activityEventHasError(runEnd) {
		return
	}
	switch item.Status {
	case ActivityStatusPending, ActivityStatusRunning:
	case ActivityStatusWaiting:
		if !item.RequiresApproval {
			return
		}
	default:
		return
	}
	status, severity := activityRunEndSettlement(runEnd)
	item.Status = status
	item.Severity = severity
	if item.EndedAtUnixMS == 0 {
		item.EndedAtUnixMS = eventUnixMS(runEnd, nowUnixMS)
	}
	pending := activityHasPendingMetadata(item.Metadata) || activityHasPendingPayload(item.Payload)
	item.Metadata = activityTerminalMetadata(item.Metadata)
	item.Payload = activityTerminalPayload(item.Payload)
	item.Chips = activityTerminalChips(item.Chips, pending)
	if pending {
		item.Label = ""
		item.Description = ""
	}
	if item.RequiresApproval {
		item.ApprovalState = approvalTerminalStateForRunEnd(runEnd, item.ApprovalState)
	}
}

func activityRunEndSettlement(ev Event) (ActivityStatus, ActivitySeverity) {
	switch {
	case activityRunEndIsCanceled(ev):
		return ActivityStatusCanceled, ActivitySeverityWarning
	case activityEventHasError(ev):
		return ActivityStatusError, ActivitySeverityError
	default:
		return ActivityStatusSuccess, ActivitySeverityNormal
	}
}

func activityRunEndIsCanceled(ev Event) bool {
	switch strings.TrimSpace(ev.Message) {
	case string(ActivityStatusCanceled), "cancelled":
		return true
	default:
		return false
	}
}

func approvalTerminalStateForRunEnd(ev Event, current string) string {
	current = strings.TrimSpace(current)
	switch current {
	case "approved", "rejected", "timed_out", "canceled":
		return current
	}
	if activityRunEndIsCanceled(ev) {
		return "canceled"
	}
	if activityEventHasError(ev) {
		return "timed_out"
	}
	return current
}

func activityTerminalMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if strings.HasPrefix(key, "pending_") {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func activityTerminalPayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		if strings.HasPrefix(key, "pending_") {
			continue
		}
		out[key] = cloneActivityPayloadValue(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func activityTerminalChips(chips []ActivityChip, pending bool) []ActivityChip {
	if len(chips) == 0 {
		return nil
	}
	out := make([]ActivityChip, 0, len(chips))
	for _, chip := range chips {
		if pending && activityPendingChip(chip) {
			continue
		}
		out = append(out, chip)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func activityPendingChip(chip ActivityChip) bool {
	kind := strings.TrimSpace(chip.Kind)
	value := strings.TrimSpace(chip.Value)
	return kind == "handle" || kind == "state" && value == string(ActivityStatusRunning)
}

func activityHasPendingMetadata(metadata map[string]string) bool {
	for key := range metadata {
		if strings.HasPrefix(key, "pending_") {
			return true
		}
	}
	return false
}

func activityHasPendingPayload(payload map[string]any) bool {
	for key := range payload {
		if strings.HasPrefix(key, "pending_") {
			return true
		}
	}
	return false
}

func activityControlStatus(ev Event) ActivityStatus {
	switch activityMetadataValue(ev, "control_disposition") {
	case "waiting":
		return ActivityStatusWaiting
	case "terminal", "continue":
		return ActivityStatusSuccess
	default:
		if activityEventHasError(ev) {
			return ActivityStatusError
		}
		return ActivityStatusSuccess
	}
}

func activityControlSeverity(ev Event) ActivitySeverity {
	switch activityControlStatus(ev) {
	case ActivityStatusWaiting:
		return ActivitySeverityBlocking
	case ActivityStatusError:
		return ActivitySeverityError
	default:
		return ActivitySeverityNormal
	}
}

func activityMetadata(ev Event) map[string]string {
	out := map[string]string{}
	for _, key := range activityMetadataKeys {
		if value := activityMetadataValue(ev, key); value != "" {
			out[key] = value
		}
	}
	if approvalID := activityRawMetadataString(ev.Metadata, "approval_id"); approvalID != "" {
		out["approval_id_hash"] = hashActivityToken(approvalID)
	}
	if value := activityNormalizeMetadataValue("args_hash", ev.ArgsHash); value != "" {
		out["args_hash"] = value
	}
	if ev.DurationMS > 0 {
		out["duration_ms"] = fmt.Sprintf("%d", ev.DurationMS)
	}
	if value := activityNormalizeMetadataValue("finish_reason", ev.FinishReason); value != "" {
		out["finish_reason"] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var activityMetadataKeys = []string{
	"args_hash",
	"approval_id_hash",
	"artifact_count",
	"artifact_sha256",
	"batch_index",
	"batch_size",
	"content_sha256",
	"control_disposition",
	"destructive",
	"duration_ms",
	"effects",
	"finish_reason",
	"open_world",
	"original_bytes",
	"original_lines",
	"pending_handle",
	"pending_state",
	"pending_tool_result",
	"read_only",
	"result_count",
	"strategy",
	"tool_result_status",
	"truncated",
	"visible_bytes",
	"visible_lines",
}

func activityEventHasError(ev Event) bool {
	return strings.TrimSpace(ev.Error) != "" || activityMetadataBool(ev.Metadata, "error_present")
}

func activityMetadataValue(ev Event, key string) string {
	if ev.Metadata == nil {
		return ""
	}
	value, ok := ev.Metadata[key]
	if !ok || value == nil {
		return ""
	}
	return activityNormalizeMetadataValue(key, value)
}

func activityRawMetadataString(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	value, ok := meta[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		text := strings.TrimSpace(v)
		if text == "" || len(text) > 240 {
			return ""
		}
		return text
	case fmt.Stringer:
		text := strings.TrimSpace(v.String())
		if text == "" || len(text) > 240 {
			return ""
		}
		return text
	default:
		return ""
	}
}

func activityMetadataBool(meta map[string]any, key string) bool {
	if meta == nil {
		return false
	}
	value, ok := meta[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	case bool:
		return v
	default:
		return false
	}
}

func activityNormalizeMetadataValue(key string, value any) string {
	switch key {
	case "args_hash", "approval_id_hash", "artifact_sha256", "content_sha256":
		return activityHashMetadataValue(value)
	case "artifact_count", "batch_index", "batch_size", "duration_ms", "original_bytes", "original_lines", "result_count", "visible_bytes", "visible_lines":
		return activityIntegerMetadataValue(value)
	case "destructive", "open_world", "pending_tool_result", "read_only", "truncated":
		return activityBooleanMetadataValue(value)
	case "effects":
		return activityEffectsMetadataValue(value)
	case "finish_reason":
		return activityEnumMetadataValue(value, map[string]struct{}{
			"unknown":        {},
			"stop":           {},
			"tool_calls":     {},
			"length":         {},
			"content_filter": {},
			"error":          {},
			"cancelled":      {},
			"canceled":       {},
		})
	case "control_disposition":
		return activityEnumMetadataValue(value, map[string]struct{}{
			"continue": {},
			"waiting":  {},
			"terminal": {},
		})
	case "strategy":
		return activityEnumMetadataValue(value, map[string]struct{}{
			"head": {},
			"tail": {},
		})
	case "pending_state":
		return activityEnumMetadataValue(value, map[string]struct{}{
			"running": {},
		})
	case "pending_handle":
		return activityPublicTokenMetadataValue(value)
	case "tool_result_status":
		return activityEnumMetadataValue(value, map[string]struct{}{
			"success":  {},
			"error":    {},
			"canceled": {},
		})
	default:
		return ""
	}
}

func activityPublicTokenMetadataValue(value any) string {
	text := strings.TrimSpace(activityScalarString(value))
	if text == "" || len(text) > 240 {
		return ""
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':', '/', '@':
			continue
		default:
			return ""
		}
	}
	return text
}

func activityHashMetadataValue(value any) string {
	text := strings.TrimSpace(activityScalarString(value))
	if len(text) < 8 || len(text) > 128 {
		return ""
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') || (r >= '0' && r <= '9') {
			continue
		}
		return ""
	}
	return strings.ToLower(text)
}

func activityIntegerMetadataValue(value any) string {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return ""
		}
		return strconv.FormatInt(int64(v), 10)
	case int8:
		if v < 0 {
			return ""
		}
		return strconv.FormatInt(int64(v), 10)
	case int16:
		if v < 0 {
			return ""
		}
		return strconv.FormatInt(int64(v), 10)
	case int32:
		if v < 0 {
			return ""
		}
		return strconv.FormatInt(int64(v), 10)
	case int64:
		if v < 0 {
			return ""
		}
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return activityFloatIntegerMetadataValue(float64(v))
	case float64:
		return activityFloatIntegerMetadataValue(v)
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return ""
		}
		parsed, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return ""
		}
		return strconv.FormatUint(parsed, 10)
	default:
		return ""
	}
}

func activityFloatIntegerMetadataValue(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || math.Trunc(value) != value {
		return ""
	}
	return strconv.FormatUint(uint64(value), 10)
}

func activityBooleanMetadataValue(value any) string {
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v)
	case string:
		text := strings.ToLower(strings.TrimSpace(v))
		if text == "true" || text == "false" {
			return text
		}
		return ""
	default:
		return ""
	}
}

func activityEffectsMetadataValue(value any) string {
	values := []string{}
	switch v := value.(type) {
	case string:
		values = strings.Split(v, ",")
	case []string:
		values = append(values, v...)
	case []any:
		for _, item := range v {
			if text := activityScalarString(item); text != "" {
				values = append(values, text)
			}
		}
	default:
		return ""
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		text := strings.ToLower(strings.TrimSpace(value))
		switch text {
		case "read", "write", "shell", "network":
			out = append(out, text)
		default:
			return ""
		}
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, ",")
}

func activityEnumMetadataValue(value any, allowed map[string]struct{}) string {
	text := strings.ToLower(strings.TrimSpace(activityScalarString(value)))
	if text == "" {
		return ""
	}
	if _, ok := allowed[text]; !ok {
		return ""
	}
	return text
}

func activityScalarString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func validateActivityItemMetadata(metadata map[string]string) error {
	for key, value := range metadata {
		if activityNormalizeMetadataValue(key, value) == "" {
			return fmt.Errorf("%s has unsupported value", key)
		}
	}
	return nil
}

func validateActivityItemPresentation(item ActivityItem) error {
	if len([]rune(strings.TrimSpace(item.Label))) > 200 {
		return errors.New("label is too long")
	}
	if len([]rune(strings.TrimSpace(item.Description))) > 500 {
		return errors.New("description is too long")
	}
	if item.Renderer != "" {
		if err := validateActivityRenderer(item.Renderer); err != nil {
			return fmt.Errorf("renderer: %w", err)
		}
	}
	for i, chip := range item.Chips {
		if err := validateActivityChip(chip); err != nil {
			return fmt.Errorf("chip %d: %w", i, err)
		}
	}
	for i, ref := range item.TargetRefs {
		if err := validateActivityTargetRef(ref); err != nil {
			return fmt.Errorf("target ref %d: %w", i, err)
		}
	}
	if err := validateActivityPayload(item.Payload, 0); err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	return nil
}

func validateActivityRenderer(value ActivityRenderer) error {
	switch value {
	case ActivityRendererStructured,
		ActivityRendererTerminal,
		ActivityRendererFile,
		ActivityRendererPatch,
		ActivityRendererWebSearch,
		ActivityRendererTodos,
		ActivityRendererQuestion,
		ActivityRendererCompletion:
		return nil
	default:
		return fmt.Errorf("%q is not supported", value)
	}
}

func validateActivityChip(chip ActivityChip) error {
	if !activityTokenIsValid(chip.Kind, 64) {
		return errors.New("kind is required")
	}
	if strings.TrimSpace(chip.Label) == "" || len([]rune(strings.TrimSpace(chip.Label))) > 120 {
		return errors.New("label is required")
	}
	if len([]rune(strings.TrimSpace(chip.Value))) > 120 {
		return errors.New("value is too long")
	}
	if chip.Tone != "" && !activityTokenIsValid(chip.Tone, 32) {
		return errors.New("tone is invalid")
	}
	return nil
}

func validateActivityTargetRef(ref ActivityTargetRef) error {
	if !activityTokenIsValid(ref.Kind, 64) {
		return errors.New("kind is required")
	}
	if strings.TrimSpace(ref.Label) == "" || len([]rune(strings.TrimSpace(ref.Label))) > 240 {
		return errors.New("label is required")
	}
	if ref.Line < 0 {
		return errors.New("line must be non-negative")
	}
	if ref.URI != "" && !activityURIIsValid(ref.URI) {
		return errors.New("uri is invalid")
	}
	if len([]rune(strings.TrimSpace(ref.Path))) > 500 {
		return errors.New("path is too long")
	}
	return nil
}

func validateActivityPayload(value any, depth int) error {
	if value == nil {
		return nil
	}
	if depth > 5 {
		return errors.New("too deeply nested")
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if !activityTokenIsValid(key, 80) {
				return fmt.Errorf("key %q is invalid", key)
			}
			if err := validateActivityPayload(item, depth+1); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	case []any:
		for i, item := range typed {
			if err := validateActivityPayload(item, depth+1); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	case string:
		if len([]rune(typed)) > 8000 {
			return errors.New("string is too long")
		}
	case bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return nil
	default:
		return fmt.Errorf("%T is unsupported", value)
	}
	return nil
}

func activityTokenIsValid(value string, limit int) bool {
	value = strings.TrimSpace(value)
	if value == "" || len([]rune(value)) > limit {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':':
			continue
		default:
			return false
		}
	}
	return true
}

func activityURIIsValid(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "http://") ||
		strings.HasPrefix(value, "https://") ||
		strings.HasPrefix(value, "artifact://")
}

func mergeActivityMetadata(left, right map[string]string) map[string]string {
	if len(left) == 0 {
		return cloneActivityMetadata(right)
	}
	out := cloneActivityMetadata(left)
	for key, value := range right {
		if value != "" {
			out[key] = value
		}
	}
	return out
}

func cloneActivityMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneActivityChips(in []ActivityChip) []ActivityChip {
	if len(in) == 0 {
		return nil
	}
	return append([]ActivityChip(nil), in...)
}

func cloneActivityTargetRefs(in []ActivityTargetRef) []ActivityTargetRef {
	if len(in) == 0 {
		return nil
	}
	return append([]ActivityTargetRef(nil), in...)
}

func cloneActivityPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneActivityPayloadValue(value)
	}
	return out
}

func mergeActivityPayload(left, right map[string]any) map[string]any {
	if len(left) == 0 {
		return cloneActivityPayload(right)
	}
	out := cloneActivityPayload(left)
	for key, value := range right {
		out[key] = cloneActivityPayloadValue(value)
	}
	return out
}

func cloneActivityPayloadValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneActivityPayload(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneActivityPayloadValue(item)
		}
		return out
	case []string:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = item
		}
		return out
	default:
		return typed
	}
}

func activityItemAttentionReasons(item ActivityItem) []ActivityAttentionReason {
	reasons := []ActivityAttentionReason{}
	switch item.Status {
	case ActivityStatusRunning:
		reasons = append(reasons, ActivityAttentionRunning)
	case ActivityStatusWaiting:
		reasons = append(reasons, ActivityAttentionWaiting)
	case ActivityStatusError:
		reasons = append(reasons, ActivityAttentionError)
	}
	if item.RequiresApproval && item.Status == ActivityStatusWaiting {
		reasons = append(reasons, ActivityAttentionApproval)
	}
	return uniqueActivityReasons(reasons)
}

func activitySummary(items []ActivityItem, runEnd *Event, firstAt, lastAt, nowUnixMS int64) ActivitySummary {
	summary := ActivitySummary{
		Status:     ActivityStatusPending,
		Severity:   ActivitySeverityQuiet,
		TotalItems: len(items),
	}
	for _, item := range items {
		switch item.Status {
		case ActivityStatusPending:
			summary.Counts.Pending++
		case ActivityStatusRunning:
			summary.Counts.Running++
		case ActivityStatusWaiting:
			summary.Counts.Waiting++
		case ActivityStatusSuccess:
			summary.Counts.Success++
		case ActivityStatusError:
			summary.Counts.Error++
		case ActivityStatusCanceled:
			summary.Counts.Canceled++
		}
		if item.RequiresApproval {
			summary.Counts.Approval++
		}
		summary.AttentionReasons = append(summary.AttentionReasons, item.AttentionReasons...)
		summary.Severity = maxActivitySeverity(summary.Severity, item.Severity)
	}
	if runEnd != nil {
		if activityRunEndIsCanceled(*runEnd) {
			summary.Status = ActivityStatusCanceled
		} else if activityEventHasError(*runEnd) {
			summary.Status = ActivityStatusError
			summary.Severity = maxActivitySeverity(summary.Severity, ActivitySeverityError)
			summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionError)
		} else {
			switch strings.TrimSpace(runEnd.Message) {
			case string(ActivityStatusWaiting):
				summary.Status = ActivityStatusWaiting
				summary.Severity = maxActivitySeverity(summary.Severity, ActivitySeverityBlocking)
				summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionWaiting)
			default:
				if summary.TotalItems == 0 || summary.Counts.Error == 0 && summary.Counts.Waiting == 0 && summary.Counts.Running == 0 && summary.Counts.Pending == 0 {
					summary.Status = ActivityStatusSuccess
				}
			}
		}
	} else {
		switch {
		case summary.Counts.Waiting > 0:
			summary.Status = ActivityStatusWaiting
		case summary.Counts.Running > 0:
			summary.Status = ActivityStatusRunning
		case summary.Counts.Pending > 0:
			summary.Status = ActivityStatusPending
		}
	}
	if summary.Status == ActivityStatusPending {
		switch {
		case summary.Counts.Error > 0:
			summary.Status = ActivityStatusError
		case summary.Counts.Waiting > 0:
			summary.Status = ActivityStatusWaiting
		case summary.Counts.Running > 0:
			summary.Status = ActivityStatusRunning
		case summary.Counts.Pending > 0:
			summary.Status = ActivityStatusPending
		case summary.Counts.Canceled > 0 && summary.Counts.Success == 0:
			summary.Status = ActivityStatusCanceled
		case summary.TotalItems > 0:
			summary.Status = ActivityStatusSuccess
		}
	}
	if summary.Counts.Error > 0 && summary.Status != ActivityStatusWaiting {
		summary.Status = ActivityStatusError
	}
	summary.AttentionReasons = uniqueActivityReasons(summary.AttentionReasons)
	summary.NeedsAttention = len(summary.AttentionReasons) > 0
	if summary.NeedsAttention && summary.Severity == ActivitySeverityQuiet {
		summary.Severity = ActivitySeverityWarning
	}
	if firstAt > 0 {
		end := lastAt
		if runEnd != nil {
			end = eventUnixMS(*runEnd, nowUnixMS)
		}
		if end == 0 && (summary.Status == ActivityStatusRunning || summary.Status == ActivityStatusWaiting || summary.Status == ActivityStatusPending) {
			end = nowUnixMS
		}
		if end > firstAt {
			summary.DurationMS = end - firstAt
		}
	}
	return summary
}

func noteActivityTime(observedAt int64, firstAt *int64, lastAt *int64) {
	if observedAt <= 0 {
		return
	}
	if firstAt != nil && (*firstAt == 0 || observedAt < *firstAt) {
		*firstAt = observedAt
	}
	if lastAt != nil && observedAt > *lastAt {
		*lastAt = observedAt
	}
}

func uniqueActivityReasons(in []ActivityAttentionReason) []ActivityAttentionReason {
	out := []ActivityAttentionReason{}
	seen := map[ActivityAttentionReason]struct{}{}
	for _, reason := range in {
		if reason == "" {
			continue
		}
		if _, ok := seen[reason]; ok {
			continue
		}
		seen[reason] = struct{}{}
		out = append(out, reason)
	}
	return out
}

func maxActivitySeverity(left, right ActivitySeverity) ActivitySeverity {
	if activitySeverityRank(right) > activitySeverityRank(left) {
		return right
	}
	return left
}

func activitySeverityRank(severity ActivitySeverity) int {
	switch severity {
	case ActivitySeverityQuiet:
		return 0
	case ActivitySeverityNormal:
		return 1
	case ActivitySeverityWarning:
		return 2
	case ActivitySeverityError:
		return 3
	case ActivitySeverityBlocking:
		return 4
	default:
		return -1
	}
}

func activityItemSortTime(item ActivityItem) int64 {
	if item.StartedAtUnixMS > 0 {
		return item.StartedAtUnixMS
	}
	return item.EndedAtUnixMS
}

func eventUnixMS(ev Event, nowUnixMS int64) int64 {
	if !ev.ObservedAt.IsZero() {
		return ev.ObservedAt.UnixMilli()
	}
	return nowUnixMS
}

func firstNonEmpty(left, right string) string {
	if strings.TrimSpace(left) != "" {
		return left
	}
	return right
}

func firstNonEmptyActivityKind(left, right ActivityKind) ActivityKind {
	if strings.TrimSpace(string(left)) != "" {
		return left
	}
	return right
}

func validateActivityStatus(status ActivityStatus) error {
	switch status {
	case ActivityStatusPending, ActivityStatusRunning, ActivityStatusWaiting, ActivityStatusSuccess, ActivityStatusError, ActivityStatusCanceled:
		return nil
	default:
		return errors.New("unknown activity status")
	}
}

func validateActivityKind(kind ActivityKind) error {
	switch kind {
	case ActivityKindTool, ActivityKindHosted, ActivityKindApproval, ActivityKindControl, ActivityKindBudget:
		return nil
	default:
		return errors.New("unknown activity kind")
	}
}

func validateActivitySeverity(severity ActivitySeverity) error {
	switch severity {
	case ActivitySeverityQuiet, ActivitySeverityNormal, ActivitySeverityWarning, ActivitySeverityError, ActivitySeverityBlocking:
		return nil
	default:
		return errors.New("unknown activity severity")
	}
}

func validateActivityAttentionReason(reason ActivityAttentionReason) error {
	switch reason {
	case ActivityAttentionRunning, ActivityAttentionWaiting, ActivityAttentionApproval, ActivityAttentionError:
		return nil
	default:
		return errors.New("unknown activity attention reason")
	}
}

func validateActivityApprovalState(state string) error {
	switch state {
	case "requested", "approved", "rejected", "timed_out", "canceled":
		return nil
	default:
		return errors.New("unknown activity approval state")
	}
}

func validateActivityItemApprovalLifecycle(item ActivityItem) error {
	if item.Kind != ActivityKindApproval {
		return errors.New("approval_state is only valid on approval items")
	}
	switch item.ApprovalState {
	case "requested":
		if item.Status != ActivityStatusWaiting {
			return fmt.Errorf("requested approval status is %q, want %q", item.Status, ActivityStatusWaiting)
		}
		if item.EndedAtUnixMS != 0 {
			return errors.New("requested approval must not be ended")
		}
	case "approved":
		if item.Status != ActivityStatusSuccess {
			return fmt.Errorf("approved approval status is %q, want %q", item.Status, ActivityStatusSuccess)
		}
	case "rejected", "timed_out":
		if item.Status != ActivityStatusError {
			return fmt.Errorf("%s approval status is %q, want %q", item.ApprovalState, item.Status, ActivityStatusError)
		}
	case "canceled":
		if item.Status != ActivityStatusCanceled {
			return fmt.Errorf("canceled approval status is %q, want %q", item.Status, ActivityStatusCanceled)
		}
	}
	return nil
}

func hashActivityToken(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}
