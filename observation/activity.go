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

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/tools"
)

const (
	ActivityTimelineSchemaVersion = 1

	ActivityKindTool    ActivityKind = "tool"
	ActivityKindHosted  ActivityKind = "hosted_tool"
	ActivityKindControl ActivityKind = "control"
	ActivityKindBudget  ActivityKind = "budget"
)

type ActivityKind string

type ActivityStatus string

const (
	ActivityStatusPending  ActivityStatus = "pending"
	ActivityStatusRunning  ActivityStatus = "running"
	ActivityStatusWaiting  ActivityStatus = "waiting"
	ActivityStatusSuccess  ActivityStatus = "success"
	ActivityStatusError    ActivityStatus = "error"
	ActivityStatusDeclined ActivityStatus = "declined"
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
	RunID    identity.RunID    `json:"run_id,omitempty"`
	ThreadID identity.ThreadID `json:"thread_id,omitempty"`
	TurnID   identity.TurnID   `json:"turn_id,omitempty"`
	TraceID  identity.TraceID  `json:"trace_id,omitempty"`
}

type ActivityItem struct {
	ItemID           string                      `json:"item_id"`
	ToolID           string                      `json:"tool_id,omitempty"`
	ToolName         string                      `json:"tool_name,omitempty"`
	Kind             ActivityKind                `json:"kind"`
	Status           ActivityStatus              `json:"status"`
	Severity         ActivitySeverity            `json:"severity"`
	NeedsAttention   bool                        `json:"needs_attention"`
	AttentionReasons []ActivityAttentionReason   `json:"attention_reasons,omitempty"`
	RequiresApproval bool                        `json:"requires_approval"`
	ApprovalState    string                      `json:"approval_state,omitempty"`
	StartedAtUnixMS  int64                       `json:"started_at_unix_ms,omitempty"`
	EndedAtUnixMS    int64                       `json:"ended_at_unix_ms,omitempty"`
	Presentation     *tools.ActivityPresentation `json:"presentation,omitempty"`
	Metadata         map[string]string           `json:"metadata,omitempty"`
}

type ActivityCounts struct {
	Pending  int `json:"pending,omitempty"`
	Running  int `json:"running,omitempty"`
	Waiting  int `json:"waiting,omitempty"`
	Success  int `json:"success,omitempty"`
	Error    int `json:"error,omitempty"`
	Declined int `json:"declined,omitempty"`
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
	SchemaVersion int               `json:"schema_version"`
	RunID         identity.RunID    `json:"run_id,omitempty"`
	ThreadID      identity.ThreadID `json:"thread_id,omitempty"`
	TurnID        identity.TurnID   `json:"turn_id,omitempty"`
	TraceID       identity.TraceID  `json:"trace_id,omitempty"`
	Summary       ActivitySummary   `json:"summary"`
	Items         []ActivityItem    `json:"items"`
}

func CloneActivityPresentation(in *tools.ActivityPresentation) *tools.ActivityPresentation {
	return tools.CloneActivityPresentation(in)
}

func CloneActivityTimeline(in *ActivityTimeline) *ActivityTimeline {
	if in == nil {
		return nil
	}
	out := *in
	if in.Summary.AttentionReasons != nil {
		out.Summary.AttentionReasons = append([]ActivityAttentionReason{}, in.Summary.AttentionReasons...)
	}
	if in.Items != nil {
		out.Items = make([]ActivityItem, len(in.Items))
		for i, item := range in.Items {
			out.Items[i] = cloneActivityItem(item)
		}
	}
	return &out
}

// RebuildActivitySummary recomputes item-derived summary state while preserving
// the timeline duration and settled run-level error or canceled status.
func RebuildActivitySummary(timeline ActivityTimeline) ActivitySummary {
	summary := activityItemSummary(timeline.Items)
	original := timeline.Summary
	if !activitySummaryHasActiveItems(summary) {
		switch original.Status {
		case ActivityStatusError:
			summary.Status = ActivityStatusError
			severity := original.Severity
			if severity == "" {
				severity = ActivitySeverityError
			}
			summary.Severity = maxActivitySeverity(summary.Severity, severity)
			summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionError)
		case ActivityStatusCanceled:
			summary.Status = ActivityStatusCanceled
			severity := original.Severity
			if severity == "" {
				severity = ActivitySeverityWarning
			}
			summary.Severity = maxActivitySeverity(summary.Severity, severity)
		}
	}
	summary.DurationMS = original.DurationMS
	finalizeActivitySummary(&summary)
	return summary
}

func cloneActivityItem(in ActivityItem) ActivityItem {
	if in.AttentionReasons != nil {
		in.AttentionReasons = append([]ActivityAttentionReason{}, in.AttentionReasons...)
	}
	in.Presentation = tools.CloneActivityPresentation(in.Presentation)
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
// an explicit tools.ActivityPresentation that has already crossed the event sanitizer.
func BuildActivityTimeline(meta ActivityRunMeta, events []Event, nowUnixMS int64) ActivityTimeline {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		RunID:         identity.RunID(strings.TrimSpace(meta.RunID.String())),
		ThreadID:      identity.ThreadID(strings.TrimSpace(meta.ThreadID.String())),
		TurnID:        identity.TurnID(strings.TrimSpace(meta.TurnID.String())),
		TraceID:       identity.TraceID(strings.TrimSpace(meta.TraceID.String())),
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
			timeline.RunID = identity.RunID(strings.TrimSpace(ev.RunID.String()))
		}
		if timeline.ThreadID == "" {
			timeline.ThreadID = identity.ThreadID(strings.TrimSpace(ev.ThreadID.String()))
		}
		if timeline.TurnID == "" {
			timeline.TurnID = identity.TurnID(strings.TrimSpace(ev.TurnID.String()))
		}
		if timeline.TraceID == "" {
			timeline.TraceID = identity.TraceID(strings.TrimSpace(ev.TraceID.String()))
		}
		observedAt := eventUnixMS(ev, nowUnixMS)
		switch ev.Type {
		case EventTypeToolCall, EventTypeHostedToolCall:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityToolKey(ev, index)
			initialStatus := ActivityStatusPending
			initialSeverity := ActivitySeverityQuiet
			if ev.Type == EventTypeHostedToolCall {
				initialStatus = ActivityStatusRunning
				initialSeverity = ActivitySeverityNormal
			}
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:          key,
					ToolID:          strings.TrimSpace(ev.ToolID),
					ToolName:        strings.TrimSpace(ev.ToolName),
					Kind:            activityToolKind(ev),
					Status:          initialStatus,
					Severity:        initialSeverity,
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
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			if activityMetadataBool(ev.Metadata, "pending_tool_result") {
				state.item.Status = ActivityStatusRunning
				state.item.Severity = ActivitySeverityWarning
			}
			state.lastSeen = observedAt
		case EventTypeToolDispatchStarted:
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
			state.item.StartedAtUnixMS = observedAt
			state.item.Status = ActivityStatusRunning
			state.item.Severity = ActivitySeverityNormal
			state.item.EndedAtUnixMS = 0
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
			state.lastSeen = observedAt
		case EventTypeToolActivityUpdated:
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
			if !activityStatusIsTerminal(state.item.Status) && state.item.Status != ActivityStatusWaiting {
				state.item.Status = ActivityStatusRunning
				state.item.Severity = ActivitySeverityNormal
				state.item.EndedAtUnixMS = 0
			}
			mergeActivityPresentationIntoItem(&state.item, ev.Activity)
			state.item.Metadata = mergeActivityMetadata(state.item.Metadata, activityMetadata(ev))
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
				state.item.StartedAtUnixMS = durationStart
			}
			resultStatus := activityMetadataValue(ev, "tool_result_status")
			if state.item.ApprovalState == "rejected" || resultStatus == string(ActivityStatusDeclined) {
				// A canonical user decision is terminal and must not be
				// overwritten by a legacy/success-shaped tool result event. A
				// structured declined result is sufficient terminal evidence when
				// the matching approval event is not present in this projection.
				state.item.Status = ActivityStatusDeclined
				state.item.Severity = ActivitySeverityQuiet
				state.item.RequiresApproval = false
				state.item.ApprovalState = "rejected"
			} else if activityEventHasError(ev) || resultStatus == string(ActivityStatusError) {
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
				state.item.Metadata = activityTerminalMetadata(state.item.Metadata)
				if state.item.Status == ActivityStatusDeclined {
					state.item.Presentation = tools.FinalizeActivityPresentation(state.item.Presentation, string(state.item.Status))
				} else {
					state.item.Presentation = tools.ClearPendingActivity(state.item.Presentation)
				}
			}
			state.lastSeen = observedAt
		case EventTypeToolApprovalRequested:
			noteActivityTime(observedAt, &firstAt, &lastAt)
			key := activityToolKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:           key,
					ToolID:           strings.TrimSpace(ev.ToolID),
					ToolName:         strings.TrimSpace(ev.ToolName),
					Kind:             activityToolKind(ev),
					Status:           ActivityStatusWaiting,
					Severity:         ActivitySeverityBlocking,
					RequiresApproval: true,
					ApprovalState:    "requested",
					StartedAtUnixMS:  observedAt,
					Metadata:         activityMetadata(ev),
				}
			})
			if activityStatusIsTerminal(state.item.Status) {
				// A late or replayed request must not reopen a canonical terminal
				// tool result for the same stable tool identity.
				state.lastSeen = observedAt
				continue
			}
			state.item.ToolID = firstNonEmpty(state.item.ToolID, strings.TrimSpace(ev.ToolID))
			state.item.ToolName = firstNonEmpty(state.item.ToolName, strings.TrimSpace(ev.ToolName))
			state.item.Kind = firstNonEmptyActivityKind(state.item.Kind, activityToolKind(ev))
			if state.item.StartedAtUnixMS == 0 {
				state.item.StartedAtUnixMS = observedAt
			}
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
			key := activityToolKey(ev, index)
			state := ensureActivityItem(items, &order, key, len(order), func() ActivityItem {
				return ActivityItem{
					ItemID:           key,
					ToolID:           strings.TrimSpace(ev.ToolID),
					ToolName:         strings.TrimSpace(ev.ToolName),
					Kind:             activityToolKind(ev),
					Status:           ActivityStatusPending,
					Severity:         ActivitySeverityNormal,
					RequiresApproval: true,
					StartedAtUnixMS:  observedAt,
				}
			})
			state.item.ToolID = firstNonEmpty(state.item.ToolID, strings.TrimSpace(ev.ToolID))
			state.item.ToolName = firstNonEmpty(state.item.ToolName, strings.TrimSpace(ev.ToolName))
			state.item.Kind = firstNonEmptyActivityKind(state.item.Kind, activityToolKind(ev))
			if state.item.StartedAtUnixMS == 0 {
				state.item.StartedAtUnixMS = observedAt
			}
			state.item.RequiresApproval = true
			switch ev.Type {
			case EventTypeToolApprovalApproved:
				state.item.Status = ActivityStatusPending
				state.item.Severity = ActivitySeverityNormal
				state.item.ApprovalState = "approved"
				state.item.EndedAtUnixMS = 0
			case EventTypeToolApprovalRejected:
				state.item.Status = ActivityStatusDeclined
				state.item.Severity = ActivitySeverityQuiet
				state.item.RequiresApproval = false
				state.item.ApprovalState = "rejected"
				state.item.EndedAtUnixMS = observedAt
			case EventTypeToolApprovalTimedOut:
				state.item.Status = ActivityStatusError
				state.item.Severity = ActivitySeverityBlocking
				state.item.ApprovalState = "timed_out"
				state.item.EndedAtUnixMS = observedAt
			case EventTypeToolApprovalCanceled:
				state.item.Status = ActivityStatusCanceled
				state.item.Severity = ActivitySeverityWarning
				state.item.ApprovalState = "canceled"
				state.item.EndedAtUnixMS = observedAt
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
		if runEnd == nil && state.item.RequiresApproval && state.item.ApprovalState == "requested" {
			// A terminal tool result can commit before interrupted-turn recovery
			// appends its run marker. Close the approval from that durable outcome
			// so the intermediate timeline remains readable after a restart.
			switch state.item.Status {
			case ActivityStatusError:
				state.item.ApprovalState = "timed_out"
			case ActivityStatusCanceled:
				state.item.ApprovalState = "canceled"
			}
		}
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
		if item.RequiresApproval && strings.TrimSpace(item.ApprovalState) == "" {
			return fmt.Errorf("item %q approval state is required when requires_approval is true", item.ItemID)
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

func mergeActivityPresentationIntoItem(item *ActivityItem, presentation *tools.ActivityPresentation) {
	if item == nil || presentation == nil {
		return
	}
	item.Presentation = tools.MergeActivityPresentations(item.Presentation, presentation)
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

func activityStatusIsTerminal(status ActivityStatus) bool {
	switch status {
	case ActivityStatusSuccess, ActivityStatusError, ActivityStatusDeclined, ActivityStatusCanceled:
		return true
	default:
		return false
	}
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
	if !activityRunEndIsTerminal(runEnd) {
		return
	}
	if activityHasPendingMetadata(item.Metadata) &&
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
	status, severity, ok := activityRunEndSettlement(runEnd)
	if !ok {
		return
	}
	item.Status = status
	item.Severity = severity
	if item.EndedAtUnixMS == 0 {
		item.EndedAtUnixMS = eventUnixMS(runEnd, nowUnixMS)
	}
	pending := activityHasPendingMetadata(item.Metadata)
	item.Metadata = activityTerminalMetadata(item.Metadata)
	item.Presentation = tools.ClearPendingActivity(item.Presentation)
	if pending && item.Presentation != nil {
		item.Presentation.Label = ""
		item.Presentation.Description = ""
	}
	if item.RequiresApproval {
		item.ApprovalState = approvalTerminalStateForRunEnd(runEnd, item.ApprovalState)
	}
}

func activityRunEndSettlement(ev Event) (ActivityStatus, ActivitySeverity, bool) {
	switch {
	case activityRunEndIsCanceled(ev):
		return ActivityStatusCanceled, ActivitySeverityWarning, true
	case activityEventHasError(ev):
		return ActivityStatusError, ActivitySeverityError, true
	case activityRunEndIsSuccess(ev):
		return ActivityStatusSuccess, ActivitySeverityNormal, true
	default:
		return "", "", false
	}
}

func activityRunEndIsTerminal(ev Event) bool {
	if activityRunEndIsCanceled(ev) || activityEventHasError(ev) || activityRunEndIsSuccess(ev) {
		return true
	}
	switch strings.TrimSpace(ev.Message) {
	case "failed", string(ActivityStatusError):
		return true
	default:
		return false
	}
}

func activityRunEndIsSuccess(ev Event) bool {
	switch strings.TrimSpace(ev.Message) {
	case "completed", string(ActivityStatusSuccess):
		return true
	default:
		return false
	}
}

func activityRunEndIsCanceled(ev Event) bool {
	switch strings.TrimSpace(ev.Message) {
	case "aborted", string(ActivityStatusCanceled), "cancelled":
		return true
	default:
		return false
	}
}

func approvalTerminalStateForRunEnd(ev Event, current string) string {
	current = strings.TrimSpace(current)
	if activityRunEndIsCanceled(ev) && (current == "requested" || current == "timed_out") {
		return "canceled"
	}
	switch current {
	case "approved", "rejected", "timed_out", "canceled":
		return current
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

func activityHasPendingMetadata(metadata map[string]string) bool {
	for key := range metadata {
		if strings.HasPrefix(key, "pending_") {
			return true
		}
	}
	return false
}

func activityControlStatus(ev Event) ActivityStatus {
	if activityEventHasError(ev) {
		return ActivityStatusError
	}
	switch activityMetadataValue(ev, "control_disposition") {
	case "waiting":
		return ActivityStatusWaiting
	case "terminal", "continue":
		return ActivityStatusSuccess
	default:
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
	"control_error_code",
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
	case "control_error_code":
		return activityEnumMetadataValue(value, map[string]struct{}{"control_error": {}})
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
			"declined": {},
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
	if item.Presentation == nil {
		return nil
	}
	return item.Presentation.Validate()
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
	summary := activityItemSummary(items)
	summary.AttentionReasons = append([]ActivityAttentionReason(nil), summary.AttentionReasons...)
	if runEnd != nil {
		if status, severity, ok := activityRunEndSettlement(*runEnd); ok {
			switch status {
			case ActivityStatusCanceled:
				summary.Status = ActivityStatusCanceled
				summary.Severity = maxActivitySeverity(summary.Severity, severity)
			case ActivityStatusError:
				summary.Status = ActivityStatusError
				summary.Severity = maxActivitySeverity(summary.Severity, severity)
				summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionError)
			case ActivityStatusSuccess:
				if summary.TotalItems == 0 || summary.Counts.Error == 0 && !activitySummaryHasActiveItems(summary) {
					summary.Status = ActivityStatusSuccess
					summary.Severity = maxActivitySeverity(summary.Severity, severity)
				}
			}
		} else if activityEventHasError(*runEnd) {
			summary.Status = ActivityStatusError
			summary.Severity = maxActivitySeverity(summary.Severity, ActivitySeverityError)
			summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionError)
		} else if strings.TrimSpace(runEnd.Message) == string(ActivityStatusWaiting) {
			summary.Status = ActivityStatusWaiting
			summary.Severity = maxActivitySeverity(summary.Severity, ActivitySeverityBlocking)
			summary.AttentionReasons = append(summary.AttentionReasons, ActivityAttentionWaiting)
		}
	}
	if summary.Counts.Error > 0 && summary.Status != ActivityStatusWaiting {
		summary.Status = ActivityStatusError
	}
	finalizeActivitySummary(&summary)
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

func activityItemSummary(items []ActivityItem) ActivitySummary {
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
		case ActivityStatusDeclined:
			summary.Counts.Declined++
		case ActivityStatusCanceled:
			summary.Counts.Canceled++
		}
		if item.RequiresApproval {
			summary.Counts.Approval++
		}
		summary.AttentionReasons = append(summary.AttentionReasons, activityItemAttentionReasons(item)...)
		summary.Severity = maxActivitySeverity(summary.Severity, item.Severity)
	}
	switch {
	case summary.Counts.Waiting > 0:
		summary.Status = ActivityStatusWaiting
	case summary.Counts.Error > 0:
		summary.Status = ActivityStatusError
	case summary.Counts.Running > 0:
		summary.Status = ActivityStatusRunning
	case summary.Counts.Pending > 0:
		summary.Status = ActivityStatusPending
	case summary.Counts.Canceled > 0 && summary.Counts.Success == 0:
		summary.Status = ActivityStatusCanceled
	case summary.Counts.Declined > 0 && summary.Counts.Success == 0:
		summary.Status = ActivityStatusDeclined
	case summary.TotalItems > 0:
		summary.Status = ActivityStatusSuccess
	}
	finalizeActivitySummary(&summary)
	return summary
}

func activitySummaryHasActiveItems(summary ActivitySummary) bool {
	return summary.Counts.Pending > 0 || summary.Counts.Running > 0 || summary.Counts.Waiting > 0
}

func finalizeActivitySummary(summary *ActivitySummary) {
	if summary == nil {
		return
	}
	summary.AttentionReasons = uniqueActivityReasons(summary.AttentionReasons)
	summary.NeedsAttention = len(summary.AttentionReasons) > 0
	if summary.NeedsAttention && summary.Severity == ActivitySeverityQuiet {
		summary.Severity = ActivitySeverityWarning
	}
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
	case ActivityStatusPending, ActivityStatusRunning, ActivityStatusWaiting, ActivityStatusSuccess, ActivityStatusError, ActivityStatusDeclined, ActivityStatusCanceled:
		return nil
	default:
		return errors.New("unknown activity status")
	}
}

func validateActivityKind(kind ActivityKind) error {
	switch kind {
	case ActivityKindTool, ActivityKindHosted, ActivityKindControl, ActivityKindBudget:
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
	if !item.RequiresApproval && item.ApprovalState != "rejected" {
		return errors.New("approval_state requires requires_approval")
	}
	switch item.Kind {
	case ActivityKindTool, ActivityKindHosted:
	default:
		return errors.New("approval_state is only valid on tool activity items")
	}
	switch item.ApprovalState {
	case "requested":
		if item.Status != ActivityStatusWaiting {
			return fmt.Errorf("requested approval status is %q, want %q", item.Status, ActivityStatusWaiting)
		}
		if item.Severity != ActivitySeverityBlocking {
			return fmt.Errorf("requested approval severity is %q, want %q", item.Severity, ActivitySeverityBlocking)
		}
		if item.EndedAtUnixMS != 0 {
			return errors.New("requested approval must not be ended")
		}
	case "approved":
		switch item.Status {
		case ActivityStatusPending, ActivityStatusRunning, ActivityStatusSuccess, ActivityStatusError, ActivityStatusCanceled:
		default:
			return fmt.Errorf("approved approval status is %q, want pending, running, or terminal tool status", item.Status)
		}
	case "rejected":
		// Error/canceled are accepted only for older persisted projections.
		if item.Status != ActivityStatusDeclined && item.Status != ActivityStatusError && item.Status != ActivityStatusCanceled {
			return fmt.Errorf("%s approval status is %q, want %q", item.ApprovalState, item.Status, ActivityStatusDeclined)
		}
		if item.Status == ActivityStatusDeclined && (item.RequiresApproval || item.Severity != ActivitySeverityQuiet || item.NeedsAttention) {
			return errors.New("declined approval must be quiet, terminal, and not require attention")
		}
	case "timed_out":
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
