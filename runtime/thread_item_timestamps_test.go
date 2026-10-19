package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestHistoricalToolExecutionTimingExcludesAdmissionWaitAndStaysWithinInvocation(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000).UTC()
	for _, status := range []string{"success", "error", "declined", "canceled"} {
		t.Run(status, func(t *testing.T) {
			entries := []sessiontree.Entry{
				{ID: "call", Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", CreatedAt: start, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
				{ID: "other-call", Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "other-run", CreatedAt: start, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
				{ID: "dispatch", Type: sessiontree.EntryCustom, TurnID: "turn", RunID: "run", CreatedAt: start.Add(time.Hour), Metadata: map[string]string{"kind": "tool_dispatch", sessiontree.OccurrenceTimeMetadataKey: start.Add(10 * time.Second).Format(time.RFC3339Nano)}, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
				{ID: "result", Type: sessiontree.EntryToolResult, TurnID: "turn", RunID: "run", CreatedAt: start.Add(2 * time.Hour), Metadata: map[string]string{sessiontree.OccurrenceTimeMetadataKey: start.Add(12 * time.Second).Format(time.RFC3339Nano)}, Message: session.Message{ToolCallID: "call", ToolName: "inspect", ToolResult: &session.ToolResultView{Status: status}}},
			}
			items, _, err := threadRuntimeItemsFromEntries(entries)
			if err != nil || len(items) != 2 {
				t.Fatalf("items=%+v err=%v", items, err)
			}
			activity := items[0].Activity
			if activity.StartedAtUnixMS != start.Add(10*time.Second).UnixMilli() || activity.EndedAtUnixMS != start.Add(12*time.Second).UnixMilli() {
				t.Fatalf("execution timing lost or includes admission wait: %+v", activity)
			}
			if !items[0].CreatedAt.Equal(start) || items[1].Activity.StartedAtUnixMS != 0 || items[1].Activity.EndedAtUnixMS != 0 {
				t.Fatalf("dispatch changed creation time or borrowed another Run identity: %+v", items)
			}
		})
	}
}

func TestHistoricalToolWithoutDispatchHasNoInventedExecutionStart(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000).UTC()
	entries := []sessiontree.Entry{
		{ID: "call", Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", CreatedAt: start, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}},
		{ID: "result", Type: sessiontree.EntryToolResult, TurnID: "turn", RunID: "run", CreatedAt: start.Add(time.Second), Message: session.Message{ToolCallID: "call", ToolName: "inspect", ToolResult: &session.ToolResultView{Status: "declined"}}},
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil || len(items) != 1 || items[0].Activity.StartedAtUnixMS != 0 || items[0].Activity.EndedAtUnixMS != start.Add(time.Second).UnixMilli() {
		t.Fatalf("missing dispatch should retain an unavailable execution interval: items=%+v err=%v", items, err)
	}
}

func TestHistoricalDispatchUsesCanonicalInvocationInsteadOfObservationMetadata(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000).UTC()
	call := sessiontree.Entry{ID: "call", Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", CreatedAt: start, Message: session.Message{ToolCallID: "call", ToolName: "inspect"}}
	dispatch := sessiontree.Entry{ID: "dispatch", Type: sessiontree.EntryCustom, TurnID: "turn", CreatedAt: start.Add(time.Second), Metadata: map[string]string{"kind": "tool_dispatch", "tool_id": "call", "run_id": "untrusted-observation-run"}}
	items, _, err := threadRuntimeItemsFromEntries([]sessiontree.Entry{call, dispatch})
	if err != nil || len(items) != 1 || items[0].RunID != "run" || items[0].Activity.StartedAtUnixMS != dispatch.CreatedAt.UnixMilli() {
		t.Fatalf("canonical invocation timing not preserved: items=%+v err=%v", items, err)
	}
	other := call
	other.ID, other.RunID = "other-call", "other-run"
	if _, _, err := threadRuntimeItemsFromEntries([]sessiontree.Entry{call, other, dispatch}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ambiguous observation timing accepted: %v", err)
	}
	dispatch.RunID = "unknown-run"
	if _, _, err := threadRuntimeItemsFromEntries([]sessiontree.Entry{call, dispatch}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("unmatched dispatch execution accepted: %v", err)
	}
}

func TestThreadLiveItemsRetainFirstOccurrenceTime(t *testing.T) {
	service, actor, sink, _ := liveActivityFixture(t)
	start := time.UnixMilli(1_700_000_000_000).UTC()
	for index, kind := range []StreamObservationType{StreamObservationReasoningDelta, StreamObservationAssistantDelta} {
		at := start.Add(time.Duration(index) * time.Second)
		for delta := range 2 {
			sink.EmitEvent(Event{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", outputSourceID: string(kind), Timestamp: at.Add(time.Duration(delta) * time.Millisecond), Stream: &StreamObservation{Type: kind, Text: "text"}})
		}
		item := service.currentView(actor).Items[index]
		if !item.CreatedAt.Equal(at) {
			t.Fatalf("%s created at %v, want first delta %v", item.Kind, item.CreatedAt, at)
		}
	}
	for index, status := range []observation.ActivityStatus{observation.ActivityStatusRunning, observation.ActivityStatusSuccess} {
		sink.EmitEvent(Event{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Timestamp: start.Add(5 * time.Second), ActivityTimeline: &observation.ActivityTimeline{
			ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Items: []observation.ActivityItem{{ItemID: "call", ToolID: "call", Kind: observation.ActivityKindTool, Status: status, StartedAtUnixMS: start.Add(2 * time.Second).UnixMilli()}},
		}})
		item := service.currentView(actor).Items[2]
		if !item.CreatedAt.Equal(start.Add(2 * time.Second)) {
			t.Fatalf("tool update %d created at %v, want original activity start", index, item.CreatedAt)
		}
	}
}

func TestThreadHistoricalToolItemsPreserveOccurrenceTime(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000).UTC()
	entries := []sessiontree.Entry{
		{ID: "call", Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", CreatedAt: start.Add(time.Hour), Metadata: map[string]string{sessiontree.OccurrenceTimeMetadataKey: start.Format(time.RFC3339Nano)}, Message: session.Message{ToolCallID: "call", ToolName: "read"}},
		{ID: "result", Type: sessiontree.EntryToolResult, TurnID: "turn", RunID: "run", CreatedAt: start.Add(2 * time.Hour), Message: session.Message{ToolCallID: "call", ToolName: "read"}},
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil || len(items) != 1 || !items[0].CreatedAt.Equal(start) {
		t.Fatalf("canonical tool occurrence lost: items=%+v err=%v", items, err)
	}
}
