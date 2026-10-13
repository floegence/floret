package runtime

import (
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestThreadLiveItemsRetainFirstOccurrenceTime(t *testing.T) {
	service, actor, sink, _ := liveActivityFixture(t)
	start := time.UnixMilli(1_700_000_000_000).UTC()
	for index, kind := range []StreamObservationType{StreamObservationReasoningDelta, StreamObservationAssistantDelta} {
		at := start.Add(time.Duration(index) * time.Second)
		for delta := range 2 {
			sink.EmitEvent(Event{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Timestamp: at.Add(time.Duration(delta) * time.Millisecond), Stream: &StreamObservation{Type: kind, Text: "text"}})
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
