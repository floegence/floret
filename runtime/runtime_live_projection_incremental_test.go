package runtime

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/tools"
)

func TestRuntimeLiveProjectionRecorderMatchesCanonicalProjectionAtEveryPrefix(t *testing.T) {
	t.Parallel()
	const (
		threadID = "thread-incremental"
		turnID   = "turn-incremental"
		runID    = "run-incremental"
	)
	start := time.UnixMilli(1_700_100_000_000).UTC()
	events := []ThreadDetailEvent{{
		ID: "started", Ordinal: 1, ThreadID: threadID, TurnID: turnID, RunID: runID,
		Kind: ThreadDetailEventTurnMarker, CreatedAt: start,
		TurnMarker: &ThreadDetailTurnMarker{Status: "started"},
	}, {
		ID: "text-before", Ordinal: 2, ThreadID: threadID, TurnID: turnID, RunID: runID,
		Kind: ThreadDetailEventAssistantMessage, CreatedAt: start.Add(time.Millisecond),
		Message: &ThreadDetailMessage{Role: "assistant", Content: "Before."},
	}, {
		ID: "call", Ordinal: 3, ThreadID: threadID, TurnID: turnID, RunID: runID,
		Kind: ThreadDetailEventToolCall, CreatedAt: start.Add(2 * time.Millisecond),
		Message: &ThreadDetailMessage{Activity: &tools.ActivityPresentation{
			Label: "run", Renderer: tools.ActivityRendererTerminal,
			Payload: tools.TerminalActivityPayload{Command: "printf hello"},
		}},
		ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
	}}
	for index := 0; index < 96; index++ {
		ordinal := int64(len(events) + 1)
		events = append(events, ThreadDetailEvent{
			ID: fmt.Sprintf("activity-%03d", index), Ordinal: ordinal,
			ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventToolActivity, CreatedAt: start.Add(time.Duration(ordinal) * time.Millisecond),
			Message: &ThreadDetailMessage{Activity: &tools.ActivityPresentation{
				Label: "run", Renderer: tools.ActivityRendererTerminal,
				Payload: tools.TerminalActivityPayload{Command: "printf hello", Output: fmt.Sprintf("line-%03d", index), Status: "running"},
			}},
			ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
		})
	}
	events = append(events,
		ThreadDetailEvent{
			ID: "result", Ordinal: int64(len(events) + 1), ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventToolResult, CreatedAt: start.Add(110 * time.Millisecond),
			ToolResult: &ThreadDetailToolResult{CallID: "call-1", ToolName: "terminal.exec", Status: string(observation.ActivityStatusSuccess)},
		},
		ThreadDetailEvent{
			ID: "save-point", Ordinal: int64(len(events) + 2), ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventTurnMarker, CreatedAt: start.Add(111 * time.Millisecond),
			TurnMarker: &ThreadDetailTurnMarker{Status: threadTurnProjectionSavePointStatus, Metadata: map[string]string{"reason": threadTurnProjectionToolResultBatchReason}},
		},
		ThreadDetailEvent{
			ID: "text-after", Ordinal: int64(len(events) + 3), ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventAssistantMessage, CreatedAt: start.Add(112 * time.Millisecond),
			Message: &ThreadDetailMessage{Role: "assistant", Content: "After."},
		},
		ThreadDetailEvent{
			ID: "completed", Ordinal: int64(len(events) + 4), ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventTurnMarker, CreatedAt: start.Add(113 * time.Millisecond),
			TurnMarker: &ThreadDetailTurnMarker{Status: "completed"},
		},
	)

	recorder := &runtimeLiveProjectionRecorder{}
	committed := make([]ThreadDetailEvent, 0, len(events))
	var previous *ThreadTurnProjection
	for index := range events {
		committed = append(committed, events[index])
		projection, delta := recorder.projectWithDelta(Event{
			Type: observation.EventTypeThreadEntryCommitted, ThreadID: threadID, TurnID: turnID, RunID: runID,
			Committed: &events[index], Timestamp: events[index].CreatedAt,
		})
		if projection == nil || delta == nil {
			var diffErr error
			if projection != nil {
				_, diffErr = DiffThreadTurnProjections(previous, *projection)
			}
			t.Fatalf("prefix %d did not produce projection and delta: projection_nil=%t delta_nil=%t diff=%v kind=%q type=%q", index+1, projection == nil, delta == nil, diffErr, events[index].Kind, events[index].Type)
		}
		want := ProjectThreadTurn(ProjectThreadTurnRequest{
			ThreadID: threadID, TurnID: turnID, RunID: runID, TraceID: runID,
			Events: cloneThreadDetailEvents(committed),
		})
		if want.Status == "" {
			want.Status = TurnStatusRunning
		}
		want.ProjectedAt = projection.ProjectedAt
		if !reflect.DeepEqual(*projection, want) {
			gotJSON, _ := json.MarshalIndent(projection, "", "  ")
			wantJSON, _ := json.MarshalIndent(want, "", "  ")
			var timelineDetail string
			if len(projection.Segments) > 1 && len(want.Segments) > 1 {
				timelineDetail = fmt.Sprintf("\n got timeline: %#v\nwant timeline: %#v\n got item: %#v\nwant item: %#v", *projection.Segments[1].ActivityTimeline, *want.Segments[1].ActivityTimeline, projection.Segments[1].ActivityTimeline.Items[0], want.Segments[1].ActivityTimeline.Items[0])
			}
			t.Fatalf("prefix %d projection mismatch\n got: %s\nwant: %s%s", index+1, gotJSON, wantJSON, timelineDetail)
		}
		applied, err := ApplyThreadTurnProjectionDelta(previous, *delta)
		if err != nil {
			t.Fatalf("prefix %d delta apply: %v", index+1, err)
		}
		if !reflect.DeepEqual(applied, *projection) {
			t.Fatalf("prefix %d applied delta mismatch", index+1)
		}
		previous = cloneThreadTurnProjectionPtr(projection)
	}
}

func TestRuntimeLiveProjectionRecorderAppliesSettlementThatPrecedesActivity(t *testing.T) {
	t.Parallel()
	start := time.UnixMilli(1_700_200_000_000).UTC()
	events := []ThreadDetailEvent{
		{
			ID: "settlement", Ordinal: 1, ThreadID: "thread-order", TurnID: "turn-order", RunID: "run-order",
			Kind: ThreadDetailEventToolResult, Type: threadTurnProjectionPendingToolSettlementType, CreatedAt: start,
			ToolResult: &ThreadDetailToolResult{CallID: "call-order", ToolName: "terminal.exec", Status: string(observation.ActivityStatusSuccess)},
		},
		{
			ID: "call", Ordinal: 2, ThreadID: "thread-order", TurnID: "turn-order", RunID: "run-order",
			Kind: ThreadDetailEventToolCall, CreatedAt: start.Add(time.Millisecond),
			ToolCall: &ThreadDetailToolCall{ID: "call-order", Name: "terminal.exec"},
			Message: &ThreadDetailMessage{Activity: &tools.ActivityPresentation{
				Label: "run", Renderer: tools.ActivityRendererTerminal,
				Payload: tools.TerminalActivityPayload{Command: "true", Status: "running"},
			}},
		},
	}

	recorder := &runtimeLiveProjectionRecorder{}
	for index := range events {
		projection, _ := recorder.projectWithDelta(Event{
			Type: observation.EventTypeThreadEntryCommitted, ThreadID: "thread-order", TurnID: "turn-order", RunID: "run-order",
			Committed: &events[index], Timestamp: events[index].CreatedAt,
		})
		want := ProjectThreadTurn(ProjectThreadTurnRequest{
			ThreadID: "thread-order", TurnID: "turn-order", RunID: "run-order", TraceID: "run-order",
			Events: cloneThreadDetailEvents(events[:index+1]),
		})
		if want.Status == "" {
			want.Status = TurnStatusRunning
		}
		want.ProjectedAt = projection.ProjectedAt
		if !reflect.DeepEqual(*projection, want) {
			gotJSON, _ := json.MarshalIndent(projection, "", "  ")
			wantJSON, _ := json.MarshalIndent(want, "", "  ")
			t.Fatalf("prefix %d projection mismatch\n got: %s\nwant: %s", index+1, gotJSON, wantJSON)
		}
	}
}

func TestRuntimeLiveProjectionRecorderMatchesCanonicalRandomMixedPrefixes(t *testing.T) {
	t.Parallel()
	const (
		threadID = "thread-random"
		turnID   = "turn-random"
		runID    = "run-random"
	)
	random := rand.New(rand.NewSource(20260805))
	start := time.UnixMilli(1_700_300_000_000).UTC()
	events := []ThreadDetailEvent{{
		ID: "started", Ordinal: 1, ThreadID: threadID, TurnID: turnID, RunID: runID,
		Kind: ThreadDetailEventTurnMarker, CreatedAt: start,
		TurnMarker: &ThreadDetailTurnMarker{Status: "started"},
	}}
	renderers := []tools.ActivityRenderer{tools.ActivityRendererTerminal, tools.ActivityRendererQuestion, tools.ActivityRendererSubAgent}
	type activeTool struct {
		id       string
		renderer tools.ActivityRenderer
	}
	active := make([]activeTool, 0, 16)
	for index := 0; index < 240; index++ {
		ordinal := int64(len(events) + 1)
		at := start.Add(time.Duration(ordinal) * time.Millisecond)
		operation := random.Intn(9)
		activeIndex := -1
		if len(active) > 0 {
			activeIndex = random.Intn(len(active))
		}
		toolID := ""
		renderer := renderers[random.Intn(len(renderers))]
		if activeIndex >= 0 {
			toolID = active[activeIndex].id
			renderer = active[activeIndex].renderer
		}
		event := ThreadDetailEvent{
			ID: fmt.Sprintf("random-%03d", index), Ordinal: ordinal,
			ThreadID: threadID, TurnID: turnID, RunID: runID, CreatedAt: at,
		}
		switch operation {
		case 0:
			event.Kind = ThreadDetailEventAssistantMessage
			event.Message = &ThreadDetailMessage{Role: "assistant", Content: fmt.Sprintf("part-%03d ", index)}
		case 1:
			event.Kind = ThreadDetailEventAssistantMessage
			event.Message = &ThreadDetailMessage{Role: "assistant", Kind: "control_signal", Content: fmt.Sprintf("signal-%03d", index)}
		case 2:
			toolID = fmt.Sprintf("call-%03d", index)
			renderer = renderers[random.Intn(len(renderers))]
			active = append(active, activeTool{id: toolID, renderer: renderer})
			event.Kind = ThreadDetailEventToolCall
			event.ToolCall = &ThreadDetailToolCall{ID: toolID, Name: "tool." + toolID}
			event.Message = &ThreadDetailMessage{Activity: randomProjectionActivity(renderer, toolID, index, "pending")}
		case 3:
			if activeIndex < 0 {
				toolID = fmt.Sprintf("call-%03d", index)
				active = append(active, activeTool{id: toolID, renderer: renderer})
				event.Kind = ThreadDetailEventToolCall
			} else {
				event.Kind = ThreadDetailEventToolDispatch
			}
			event.ToolCall = &ThreadDetailToolCall{ID: toolID, Name: "tool." + toolID}
			event.Message = &ThreadDetailMessage{Activity: randomProjectionActivity(renderer, toolID, index, "running")}
		case 4:
			if activeIndex < 0 {
				toolID = fmt.Sprintf("call-%03d", index)
				active = append(active, activeTool{id: toolID, renderer: renderer})
				event.Kind = ThreadDetailEventToolCall
			} else {
				event.Kind = ThreadDetailEventToolActivity
			}
			event.ToolCall = &ThreadDetailToolCall{ID: toolID, Name: "tool." + toolID}
			event.Message = &ThreadDetailMessage{Activity: randomProjectionActivity(renderer, toolID, index, "running")}
		case 5:
			toolID = fmt.Sprintf("approval-%03d", index)
			event.Kind = ThreadDetailEventApproval
			event.Type = string(observation.EventTypeToolApprovalRequested)
			event.Approval = &ThreadDetailApproval{State: "requested", ToolID: toolID, ToolName: "terminal.exec"}
			event.Message = &ThreadDetailMessage{Activity: randomProjectionActivity(tools.ActivityRendererTerminal, toolID, index, "waiting")}
		case 6:
			if activeIndex < 0 {
				event.Kind = ThreadDetailEventAssistantMessage
				event.Message = &ThreadDetailMessage{Role: "assistant", Content: fmt.Sprintf("fallback-%03d ", index)}
			} else {
				event.Kind = ThreadDetailEventToolResult
				event.ToolResult = &ThreadDetailToolResult{CallID: toolID, ToolName: "tool." + toolID, Status: string(observation.ActivityStatusSuccess)}
				active = append(active[:activeIndex], active[activeIndex+1:]...)
			}
		case 7:
			if activeIndex < 0 {
				event.Kind = ThreadDetailEventAssistantMessage
				event.Message = &ThreadDetailMessage{Role: "assistant", Content: fmt.Sprintf("fallback-%03d ", index)}
			} else {
				event.Kind = ThreadDetailEventToolResult
				event.Type = threadTurnProjectionPendingToolSettlementType
				event.ToolResult = &ThreadDetailToolResult{CallID: toolID, ToolName: "tool." + toolID, Status: string(observation.ActivityStatusCanceled)}
				active = append(active[:activeIndex], active[activeIndex+1:]...)
			}
		default:
			event.Kind = ThreadDetailEventTurnMarker
			event.TurnMarker = &ThreadDetailTurnMarker{Status: threadTurnProjectionSavePointStatus, Metadata: map[string]string{"reason": threadTurnProjectionToolResultBatchReason}}
		}
		events = append(events, event)
	}
	events = append(events, ThreadDetailEvent{
		ID: "completed", Ordinal: int64(len(events) + 1), ThreadID: threadID, TurnID: turnID, RunID: runID,
		Kind: ThreadDetailEventTurnMarker, CreatedAt: start.Add(time.Duration(len(events)+1) * time.Millisecond),
		TurnMarker: &ThreadDetailTurnMarker{Status: "completed"},
	})
	assertRuntimeLiveProjectionPrefixes(t, threadID, turnID, runID, events)
}

func BenchmarkRuntimeLiveProjectionRecorder(b *testing.B) {
	for _, eventCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("activity_updates_%d", eventCount), func(b *testing.B) {
			events := benchmarkRuntimeLiveProjectionEvents(eventCount, false)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				recorder := &runtimeLiveProjectionRecorder{}
				for index := range events {
					recorder.projectWithDelta(Event{Type: observation.EventTypeThreadEntryCommitted, ThreadID: "thread-bench", TurnID: "turn-bench", RunID: "run-bench", Committed: &events[index]})
				}
			}
		})
		b.Run(fmt.Sprintf("growing_segments_%d", eventCount), func(b *testing.B) {
			events := benchmarkRuntimeLiveProjectionEvents(eventCount, true)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				recorder := &runtimeLiveProjectionRecorder{}
				for index := range events {
					recorder.projectWithDelta(Event{Type: observation.EventTypeThreadEntryCommitted, ThreadID: "thread-bench", TurnID: "turn-bench", RunID: "run-bench", Committed: &events[index]})
				}
			}
		})
	}
}

func assertRuntimeLiveProjectionPrefixes(t *testing.T, threadID, turnID, runID string, events []ThreadDetailEvent) {
	recorder := &runtimeLiveProjectionRecorder{}
	var previous *ThreadTurnProjection
	for index := range events {
		projection, delta := recorder.projectWithDelta(Event{
			Type: observation.EventTypeThreadEntryCommitted, ThreadID: identity.ThreadID(threadID), TurnID: identity.TurnID(turnID), RunID: identity.RunID(runID),
			Committed: &events[index], Timestamp: events[index].CreatedAt,
		})
		if projection == nil || delta == nil {
			var diffErr error
			if projection != nil {
				_, diffErr = DiffThreadTurnProjections(previous, *projection)
			}
			t.Fatalf("prefix %d did not produce projection and delta: projection_nil=%t delta_nil=%t diff=%v kind=%q type=%q", index+1, projection == nil, delta == nil, diffErr, events[index].Kind, events[index].Type)
		}
		want := ProjectThreadTurn(ProjectThreadTurnRequest{
			ThreadID: identity.ThreadID(threadID), TurnID: identity.TurnID(turnID), RunID: identity.RunID(runID), TraceID: identity.TraceID(runID),
			Events: cloneThreadDetailEvents(events[:index+1]),
		})
		if want.Status == "" {
			want.Status = TurnStatusRunning
		}
		want.ProjectedAt = projection.ProjectedAt
		if !reflect.DeepEqual(*projection, want) {
			gotJSON, _ := json.MarshalIndent(projection, "", "  ")
			wantJSON, _ := json.MarshalIndent(want, "", "  ")
			t.Fatalf("prefix %d projection mismatch\n got: %s\nwant: %s", index+1, gotJSON, wantJSON)
		}
		applied, err := ApplyThreadTurnProjectionDelta(previous, *delta)
		if err != nil || !reflect.DeepEqual(applied, *projection) {
			t.Fatalf("prefix %d delta apply mismatch: %v", index+1, err)
		}
		previous = cloneThreadTurnProjectionPtr(projection)
	}
}

func randomProjectionActivity(renderer tools.ActivityRenderer, toolID string, index int, status string) *tools.ActivityPresentation {
	presentation := &tools.ActivityPresentation{Label: toolID, Renderer: renderer}
	switch renderer {
	case tools.ActivityRendererTerminal:
		presentation.Payload = tools.TerminalActivityPayload{Command: "printf random", Output: fmt.Sprintf("output-%03d", index), Status: status}
	case tools.ActivityRendererQuestion:
		presentation.Payload = tools.QuestionActivityPayload{PromptID: "prompt-1", Questions: []tools.QuestionActivityItem{{ID: "question-1", Question: "Choose an option"}}}
	case tools.ActivityRendererSubAgent:
		presentation.Payload = tools.SubAgentActivityPayload{ThreadID: "child-1", ParentThreadID: "thread-random", TaskName: "Inspect the task", Status: status}
	}
	return presentation
}

func benchmarkRuntimeLiveProjectionEvents(eventCount int, growingSegments bool) []ThreadDetailEvent {
	events := make([]ThreadDetailEvent, 0, eventCount)
	start := time.UnixMilli(1_700_400_000_000).UTC()
	for index := 0; index < eventCount; index++ {
		event := ThreadDetailEvent{
			ID: fmt.Sprintf("bench-%05d", index), Ordinal: int64(index + 1), ThreadID: "thread-bench", TurnID: "turn-bench", RunID: "run-bench",
			CreatedAt: start.Add(time.Duration(index) * time.Millisecond),
		}
		if growingSegments && index%2 == 0 {
			event.Kind = ThreadDetailEventAssistantMessage
			event.Message = &ThreadDetailMessage{Role: "assistant", Content: "text"}
		} else {
			event.Kind = ThreadDetailEventToolActivity
			event.ToolCall = &ThreadDetailToolCall{ID: "call-bench", Name: "terminal.exec"}
			event.Message = &ThreadDetailMessage{Activity: &tools.ActivityPresentation{
				Label: "run", Renderer: tools.ActivityRendererTerminal,
				Payload: tools.TerminalActivityPayload{Command: "printf bench", Output: fmt.Sprintf("%05d", index), Status: "running"},
			}}
		}
		events = append(events, event)
	}
	return events
}
