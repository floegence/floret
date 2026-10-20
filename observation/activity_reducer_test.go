package observation

import (
	"fmt"
	"reflect"
	"testing"
)

func TestActivityReducerChangesMatchBatchWithoutRetainingText(t *testing.T) {
	events := []Event{
		{Type: EventTypeToolCall, ToolID: "first", ToolName: "read"},
		{Type: EventTypeToolDispatchStarted, ToolID: "first"},
		{Type: EventTypeHostedToolCall, ToolID: "parallel"},
		{Type: EventTypeToolApprovalRequested, ToolID: "approval"},
		{Type: EventTypeToolApprovalApproved, ToolID: "approval"},
		{Type: EventTypeToolDispatchStarted, ToolID: "approval"},
		{Type: EventTypeToolResult, ToolID: "first"},
		{Type: EventTypeToolApprovalRejected, ToolID: "declined"},
		{Type: EventTypeToolApprovalRequested, ToolID: "declined"},
		{Type: EventTypeHostedToolResult, ToolID: "parallel"},
		{Type: EventTypeRunEnd, Message: "cancelled"},
	}
	reducer := NewActivityReducer(ActivityRunMeta{RunID: "run", ThreadID: "thread", TurnID: "turn"})
	current := make(map[string]ActivityItem)
	for i, ev := range events {
		reducer.Apply(ev, 1000)
		for _, item := range reducer.ChangedItems(1000) {
			current[item.ItemID] = item
		}
		full := BuildActivityTimeline(ActivityRunMeta{RunID: "run", ThreadID: "thread", TurnID: "turn"}, events[:i+1], 1000)
		if len(current) != len(full.Items) {
			t.Fatalf("prefix %d: item count differs", i)
		}
		for _, item := range full.Items {
			if !reflect.DeepEqual(current[item.ItemID], item) {
				t.Fatalf("prefix %d: changed fact differs: %#v %#v", i, current[item.ItemID], item)
			}
		}
	}
	before := reducer.Snapshot(1000)
	for range 10000 {
		reducer.Apply(Event{Type: EventTypeProviderDelta, Message: "unrelated token"}, 1000)
	}
	if len(reducer.ChangedItems(1000)) != 0 || !reflect.DeepEqual(before, reducer.Snapshot(1000)) {
		t.Fatal("token growth changed activity")
	}
	detached := reducer.Snapshot(1000)
	detached.Items[0].Status = ActivityStatusRunning
	if !reflect.DeepEqual(before, reducer.Snapshot(1000)) {
		t.Fatal("public snapshot mutated reducer")
	}
}

func TestActivityReducerRetiresHostedAttempt(t *testing.T) {
	reducer := NewActivityReducer(ActivityRunMeta{})
	reducer.Apply(Event{Type: EventTypeHostedToolCall, ToolID: "retired"}, 1000)
	reducer.Apply(Event{Type: EventTypeToolCall, ToolID: "retained"}, 1000)
	reducer.RemoveItems([]string{"tool:retired"})
	got := reducer.Snapshot(1000)
	if len(got.Items) != 1 || got.Items[0].ToolID != "retained" {
		t.Fatalf("retired attempt survived: %+v", got)
	}
}

func TestActivityReducerTerminalToolCannotReopen(t *testing.T) {
	for _, late := range []EventType{EventTypeToolCall, EventTypeToolDispatchStarted, EventTypeToolActivityUpdated, EventTypeToolApprovalRequested, EventTypeToolApprovalApproved} {
		t.Run(string(late), func(t *testing.T) {
			reducer := NewActivityReducer(ActivityRunMeta{})
			reducer.Apply(Event{Type: EventTypeToolCall, ToolID: "tool"}, 1000)
			reducer.Apply(Event{Type: EventTypeToolResult, ToolID: "tool"}, 1001)
			reducer.Apply(Event{Type: late, ToolID: "tool", Metadata: map[string]any{"pending_tool_result": true}}, 1002)
			got := reducer.Snapshot(1002).Items[0]
			if got.Status != ActivityStatusSuccess || got.EndedAtUnixMS != 1001 {
				t.Fatalf("late fact reopened terminal tool: %+v", got)
			}
		})
	}
}

func BenchmarkActivityReducerSingleToolUpdate(b *testing.B) {
	for _, history := range []int{200, 2000} {
		b.Run(fmt.Sprintf("history_%d", history), func(b *testing.B) {
			reducer := NewActivityReducer(ActivityRunMeta{})
			for i := range history {
				id := fmt.Sprint(i)
				reducer.Apply(Event{Type: EventTypeToolCall, ToolID: id}, 1000)
				reducer.Apply(Event{Type: EventTypeToolResult, ToolID: id}, 1000)
			}
			reducer.ChangedItems(1000)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				reducer.Apply(Event{Type: EventTypeToolActivityUpdated, ToolID: "active"}, 1000)
				reducer.ChangedItems(1000)
			}
		})
	}
}
