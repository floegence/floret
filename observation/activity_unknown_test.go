package observation

import (
	"slices"
	"testing"
	"time"
)

func TestUnknownActivityRemainsTerminalAndVisible(t *testing.T) {
	start := time.Unix(1700000000, 0)
	reducer := NewActivityReducer(ActivityRunMeta{RunID: "unknown-run"})
	reducer.Apply(Event{Type: EventTypeToolResult, ToolID: "work", ToolName: "shell", Metadata: map[string]any{"tool_result_status": "unknown"}, ObservedAt: start}, start.UnixMilli())
	// Late progress, approval, and a successful run must not erase the outcome.
	for _, ev := range []Event{
		{Type: EventTypeToolDispatchStarted, ToolID: "work", ToolName: "shell"},
		{Type: EventTypeToolApprovalRequested, ToolID: "work", ToolName: "shell"},
		{Type: EventTypeRunEnd, Message: "success"},
	} {
		ev.ObservedAt = start.Add(time.Second)
		reducer.Apply(ev, ev.ObservedAt.UnixMilli())
	}
	timeline := reducer.Snapshot(start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items=%+v", timeline.Items)
	}
	item := timeline.Items[0]
	if item.Status != ActivityStatus("unknown") || item.EndedAtUnixMS != start.UnixMilli() || item.RequiresApproval || !item.NeedsAttention || item.Severity != ActivitySeverityWarning || !slices.Contains(item.AttentionReasons, ActivityAttentionReason("unknown")) {
		t.Fatalf("unknown item=%+v", item)
	}
	for _, summary := range []ActivitySummary{timeline.Summary, RebuildActivitySummary(timeline)} {
		if summary.Status != ActivityStatus("unknown") || !summary.NeedsAttention || summary.Severity != ActivitySeverityWarning || summary.Counts.Unknown != 1 || summary.Counts.Success != 0 || summary.Counts.Running != 0 || !slices.Contains(summary.AttentionReasons, ActivityAttentionReason("unknown")) {
			t.Fatalf("unknown summary=%+v", summary)
		}
	}
}
