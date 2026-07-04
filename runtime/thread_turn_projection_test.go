package runtime

import (
	"testing"
	"time"

	"github.com/floegence/floret/observation"
)

func TestProjectThreadTurnOrdersTextActivityAndControlSegments(t *testing.T) {
	now := time.Unix(100, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		RunID:    "run-1",
		TraceID:  "run-1",
		Events: []ThreadDetailEvent{
			{
				ID:        "text-1",
				Ordinal:   1,
				ThreadID:  "thread-1",
				TurnID:    "turn-1",
				Kind:      ThreadDetailEventAssistantMessage,
				CreatedAt: now,
				Message:   &ThreadDetailMessage{Role: "assistant", Content: "Before."},
			},
			{
				ID:        "call-1-row",
				Ordinal:   2,
				ThreadID:  "thread-1",
				TurnID:    "turn-1",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(time.Second),
				ToolCall:  &ThreadDetailToolCall{ID: "call-1", Name: "read"},
			},
			{
				ID:        "result-1-row",
				Ordinal:   3,
				ThreadID:  "thread-1",
				TurnID:    "turn-1",
				Kind:      ThreadDetailEventToolResult,
				CreatedAt: now.Add(2 * time.Second),
				ToolResult: &ThreadDetailToolResult{
					CallID:   "call-1",
					ToolName: "read",
					Status:   string(observation.ActivityStatusSuccess),
				},
			},
			{
				ID:        "text-2",
				Ordinal:   4,
				ThreadID:  "thread-1",
				TurnID:    "turn-1",
				Kind:      ThreadDetailEventAssistantMessage,
				CreatedAt: now.Add(3 * time.Second),
				Message:   &ThreadDetailMessage{Role: "assistant", Content: "After."},
			},
			{
				ID:        "done-row",
				Ordinal:   5,
				ThreadID:  "thread-1",
				TurnID:    "turn-1",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(4 * time.Second),
				Message:   &ThreadDetailMessage{Role: "assistant", Kind: "control_signal", Preview: "tool_call"},
				ToolCall:  &ThreadDetailToolCall{ID: "done", Name: "task_complete"},
			},
		},
	})
	if projection.ThreadID != "thread-1" || projection.TurnID != "turn-1" || projection.RunID != "run-1" {
		t.Fatalf("projection identity = %#v", projection)
	}
	if len(projection.Segments) != 5 {
		t.Fatalf("segments = %#v", projection.Segments)
	}
	if projection.Segments[0].Kind != ThreadTurnProjectionSegmentAssistantText || projection.Segments[0].Text != "Before." {
		t.Fatalf("first segment = %#v", projection.Segments[0])
	}
	if projection.Segments[1].Kind != ThreadTurnProjectionSegmentActivityTimeline ||
		projection.Segments[1].ActivityTimeline == nil ||
		len(projection.Segments[1].ActivityTimeline.Items) != 1 ||
		projection.Segments[1].ActivityTimeline.Items[0].ToolID != "call-1" {
		t.Fatalf("tool activity segment = %#v", projection.Segments[1])
	}
	if projection.Segments[2].Kind != ThreadTurnProjectionSegmentAssistantText || projection.Segments[2].Text != "After." {
		t.Fatalf("third segment = %#v", projection.Segments[2])
	}
	if projection.Segments[3].Kind != ThreadTurnProjectionSegmentControlSignal ||
		projection.Segments[3].Signal == nil ||
		projection.Segments[3].Signal.Name != "task_complete" ||
		projection.Segments[3].Signal.CallID != "done" {
		t.Fatalf("control segment = %#v", projection.Segments[3])
	}
	if projection.Segments[4].Kind != ThreadTurnProjectionSegmentActivityTimeline ||
		projection.Segments[4].ActivityTimeline == nil ||
		projection.Segments[4].ActivityTimeline.Items[0].Kind != observation.ActivityKindControl {
		t.Fatalf("control activity segment = %#v", projection.Segments[4])
	}
	if err := observation.ValidateActivityTimeline(*projection.Segments[4].ActivityTimeline); err != nil {
		t.Fatalf("control activity invalid: %v", err)
	}
}

func TestProjectThreadTurnMergesToolInvocationAndResultPresentation(t *testing.T) {
	start := time.UnixMilli(1_700_000_100_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-terminal",
		TurnID:   "turn-terminal",
		RunID:    "run-terminal",
		TraceID:  "run-terminal",
		Events: []ThreadDetailEvent{
			{
				ID:        "call-row",
				Ordinal:   1,
				ThreadID:  "thread-terminal",
				TurnID:    "turn-terminal",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: start,
				Message: &ThreadDetailMessage{Role: "assistant", Activity: &observation.ActivityPresentation{
					Label:    "sleep 10s",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "sleep 10s"},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
			},
			{
				ID:        "result-row",
				Ordinal:   2,
				ThreadID:  "thread-terminal",
				TurnID:    "turn-terminal",
				Kind:      ThreadDetailEventToolResult,
				CreatedAt: start.Add(10 * time.Second),
				Message: &ThreadDetailMessage{Role: "tool", Activity: &observation.ActivityPresentation{
					Description: "Command completed",
					Chips:       []observation.ActivityChip{{Kind: "duration_ms", Label: "duration", Value: "10000 ms", Tone: "neutral"}},
					Payload: map[string]any{
						"duration_ms": int64(10_000),
						"exit_code":   0,
						"status":      "success",
					},
				}},
				ToolResult: &ThreadDetailToolResult{
					CallID:   "exec-1",
					ToolName: "terminal.exec",
					Status:   string(observation.ActivityStatusSuccess),
				},
			},
		},
	})
	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if err := observation.ValidateActivityTimeline(*timeline); err != nil {
		t.Fatalf("activity invalid: %v", err)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %#v", timeline.Items)
	}
	item := timeline.Items[0]
	if item.Label != "sleep 10s" ||
		item.Description != "Command completed" ||
		item.Payload["command"] != "sleep 10s" ||
		item.Payload["exit_code"] != 0 ||
		item.EndedAtUnixMS-item.StartedAtUnixMS != 10_000 {
		t.Fatalf("item = %#v", item)
	}
}

func TestProjectThreadTurnPreservesActivityAttentionSummary(t *testing.T) {
	now := time.Unix(200, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-attention",
		TurnID:   "turn-attention",
		RunID:    "run-attention",
		TraceID:  "run-attention",
		Events: []ThreadDetailEvent{{
			ID:        "approval-row",
			Ordinal:   1,
			ThreadID:  "thread-attention",
			TurnID:    "turn-attention",
			Kind:      ThreadDetailEventApproval,
			CreatedAt: now,
			Approval:  &ThreadDetailApproval{State: "requested", ToolID: "approval-1", ToolName: "terminal.exec"},
		}},
	})
	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	summary := projection.Segments[0].ActivityTimeline.Summary
	if summary.Status != observation.ActivityStatusWaiting ||
		summary.Severity != observation.ActivitySeverityBlocking ||
		!summary.NeedsAttention ||
		summary.Counts.Approval != 1 ||
		len(summary.AttentionReasons) != 2 {
		t.Fatalf("summary = %#v, want waiting attention summary", summary)
	}
}

func TestProjectThreadTurnKeepsRequestedApprovalWaitingAfterSuccessfulTurnMarker(t *testing.T) {
	now := time.Unix(250, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-waiting-approval",
		TurnID:   "turn-waiting-approval",
		RunID:    "run-waiting-approval",
		TraceID:  "run-waiting-approval",
		Events: []ThreadDetailEvent{
			{
				ID:        "approval-row",
				Ordinal:   1,
				ThreadID:  "thread-waiting-approval",
				TurnID:    "turn-waiting-approval",
				Kind:      ThreadDetailEventApproval,
				Type:      observation.EventTypeToolApprovalRequested,
				CreatedAt: now,
				Approval:  &ThreadDetailApproval{State: "requested", ToolID: "exec-1", ToolName: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-waiting-approval", "thread-waiting-approval", "turn-waiting-approval", observation.ActivityItem{
					ItemID:           "tool:exec-1",
					ToolID:           "exec-1",
					ToolName:         "terminal.exec",
					Kind:             observation.ActivityKindTool,
					Status:           observation.ActivityStatusWaiting,
					Severity:         observation.ActivitySeverityBlocking,
					RequiresApproval: true,
					ApprovalState:    "requested",
					Label:            "curl -s https://example.test",
					Renderer:         observation.ActivityRendererTerminal,
					Payload:          map[string]any{"command": "curl -s https://example.test"},
				}),
			},
			{
				ID:        "turn-success",
				Ordinal:   2,
				ThreadID:  "thread-waiting-approval",
				TurnID:    "turn-waiting-approval",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now.Add(time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status: string(observation.ActivityStatusSuccess),
				},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if timeline.Summary.Status != observation.ActivityStatusWaiting ||
		timeline.Summary.Counts.Waiting != 1 ||
		timeline.Summary.Counts.Success != 0 ||
		!timeline.Summary.NeedsAttention {
		t.Fatalf("summary should keep waiting approval: %#v", timeline.Summary)
	}
	item := timeline.Items[0]
	if item.Status != observation.ActivityStatusWaiting ||
		item.ApprovalState != "requested" ||
		item.EndedAtUnixMS != 0 ||
		item.Label != "curl -s https://example.test" {
		t.Fatalf("approval item should remain requested: %#v", item)
	}
	if err := observation.ValidateActivityTimeline(*timeline); err != nil {
		t.Fatalf("projection should validate: %v", err)
	}
}

func TestProjectThreadTurnIgnoresStartedMarkerBeforeApprovalActivity(t *testing.T) {
	now := time.UnixMilli(1_700_030_000_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-started-approval",
		TurnID:   "turn-started-approval",
		RunID:    "run-started-approval",
		TraceID:  "run-started-approval",
		Events: []ThreadDetailEvent{
			{
				ID:        "turn-started",
				Ordinal:   1,
				ThreadID:  "thread-started-approval",
				TurnID:    "turn-started-approval",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now,
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "started",
				},
			},
			{
				ID:        "exec-newsapi",
				Ordinal:   2,
				ThreadID:  "thread-started-approval",
				TurnID:    "turn-started-approval",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(3 * time.Second),
				Message: &ThreadDetailMessage{Role: "assistant", Activity: &observation.ActivityPresentation{
					Label:    "curl -s https://newsapi.example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl -s https://newsapi.example.test"},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-newsapi", Name: "terminal.exec"},
			},
			{
				ID:        "exec-search",
				Ordinal:   3,
				ThreadID:  "thread-started-approval",
				TurnID:    "turn-started-approval",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(3*time.Second + time.Millisecond),
				Message: &ThreadDetailMessage{Role: "assistant", Activity: &observation.ActivityPresentation{
					Label:    "curl -sL https://search.example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl -sL https://search.example.test"},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-search", Name: "terminal.exec"},
			},
			{
				ID:        "approval-newsapi",
				Ordinal:   4,
				ThreadID:  "thread-started-approval",
				TurnID:    "turn-started-approval",
				Kind:      ThreadDetailEventApproval,
				Type:      observation.EventTypeToolApprovalRequested,
				CreatedAt: now.Add(3*time.Second + 5*time.Millisecond),
				Approval: &ThreadDetailApproval{
					State:    "requested",
					ToolID:   "call-newsapi",
					ToolName: "terminal.exec",
					ToolKind: "local",
				},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if err := observation.ValidateActivityTimeline(*timeline); err != nil {
		t.Fatalf("projection should validate: %v; timeline=%#v", err, timeline)
	}
	if timeline.Summary.Status != observation.ActivityStatusWaiting ||
		timeline.Summary.Counts.Waiting != 1 ||
		timeline.Summary.Counts.Pending != 1 ||
		timeline.Summary.Counts.Running != 0 ||
		timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("summary should contain one waiting approval and one queued tool: %#v", timeline.Summary)
	}
	waiting := projectionToolItem(t, projection, "call-newsapi")
	if waiting.Status != observation.ActivityStatusWaiting ||
		waiting.ApprovalState != "requested" ||
		!waiting.RequiresApproval ||
		waiting.EndedAtUnixMS != 0 ||
		waiting.Label != "curl -s https://newsapi.example.test" {
		t.Fatalf("approval tool item mismatch: %#v", waiting)
	}
	queued := projectionToolItem(t, projection, "call-search")
	if queued.Status != observation.ActivityStatusPending ||
		queued.RequiresApproval ||
		queued.EndedAtUnixMS != 0 ||
		queued.Label != "curl -sL https://search.example.test" {
		t.Fatalf("second tool item mismatch: %#v", queued)
	}
}

func TestProjectThreadTurnPromotesToolDispatchToRunning(t *testing.T) {
	now := time.UnixMilli(1_700_031_000_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-dispatch",
		TurnID:   "turn-dispatch",
		RunID:    "run-dispatch",
		TraceID:  "run-dispatch",
		Events: []ThreadDetailEvent{
			{
				ID:        "tool-call",
				Ordinal:   1,
				ThreadID:  "thread-dispatch",
				TurnID:    "turn-dispatch",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now,
				Message: &ThreadDetailMessage{Role: "assistant", Activity: &observation.ActivityPresentation{
					Label:    "curl -s https://example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl -s https://example.test"},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
			},
			{
				ID:        "tool-dispatch",
				Ordinal:   2,
				ThreadID:  "thread-dispatch",
				TurnID:    "turn-dispatch",
				Kind:      ThreadDetailEventToolDispatch,
				Type:      observation.EventTypeToolDispatchStarted,
				CreatedAt: now.Add(25 * time.Millisecond),
				Message: &ThreadDetailMessage{Activity: &observation.ActivityPresentation{
					Label:    "curl -s https://example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl -s https://example.test"},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if err := observation.ValidateActivityTimeline(*timeline); err != nil {
		t.Fatalf("projection should validate: %v; timeline=%#v", err, timeline)
	}
	item := projectionToolItem(t, projection, "call-1")
	if item.Status != observation.ActivityStatusRunning ||
		item.Label != "curl -s https://example.test" ||
		item.EndedAtUnixMS != 0 {
		t.Fatalf("dispatch item mismatch: %#v", item)
	}
	if timeline.Summary.Counts.Running != 1 || timeline.Summary.Counts.Pending != 0 {
		t.Fatalf("summary should show running dispatch: %#v", timeline.Summary)
	}
}

func TestProjectThreadTurnMergesToolActivityUpdate(t *testing.T) {
	now := time.UnixMilli(1_700_031_000_000)
	command := "sleep 10"
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-terminal-live",
		TurnID:   "turn-terminal-live",
		RunID:    "run-terminal-live",
		TraceID:  "run-terminal-live",
		Events: []ThreadDetailEvent{
			{
				ID:        "tool-call",
				Ordinal:   1,
				ThreadID:  "thread-terminal-live",
				TurnID:    "turn-terminal-live",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now,
				Message: &ThreadDetailMessage{Role: "assistant", Activity: &observation.ActivityPresentation{
					Label:    command,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": command},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
			},
			{
				ID:        "tool-dispatch",
				Ordinal:   2,
				ThreadID:  "thread-terminal-live",
				TurnID:    "turn-terminal-live",
				Kind:      ThreadDetailEventToolDispatch,
				Type:      observation.EventTypeToolDispatchStarted,
				CreatedAt: now.Add(10 * time.Millisecond),
				ToolCall:  &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
			},
			{
				ID:        "tool-activity",
				Ordinal:   3,
				ThreadID:  "thread-terminal-live",
				TurnID:    "turn-terminal-live",
				Kind:      ThreadDetailEventToolActivity,
				Type:      observation.EventTypeToolActivityUpdated,
				CreatedAt: now.Add(20 * time.Millisecond),
				Message: &ThreadDetailMessage{Activity: &observation.ActivityPresentation{
					Renderer: observation.ActivityRendererTerminal,
					Payload: map[string]any{
						"command":            command,
						"status":             "running",
						"process_id":         "tp_live",
						"latest_output":      "tick 1\n",
						"last_seq":           1,
						"execution_location": "local_runtime",
					},
				}},
				ToolCall: &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].ActivityTimeline == nil {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	item := projectionToolItem(t, projection, "call-1")
	if item.Status != observation.ActivityStatusRunning || item.Payload["process_id"] != "tp_live" || item.Payload["latest_output"] != "tick 1\n" {
		t.Fatalf("activity update was not merged: %#v", item)
	}
	if err := observation.ValidateActivityTimeline(*projection.Segments[0].ActivityTimeline); err != nil {
		t.Fatalf("projection should validate: %v", err)
	}
}

func TestProjectThreadTurnSettlesApprovalAndToolFromDetailEvents(t *testing.T) {
	now := time.Unix(300, 0)

	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-settled",
		TurnID:   "turn-settled",
		RunID:    "run-settled",
		TraceID:  "run-settled",
		Events: []ThreadDetailEvent{
			{
				ID:        "approval-requested",
				Ordinal:   1,
				ThreadID:  "thread-settled",
				TurnID:    "turn-settled",
				Kind:      ThreadDetailEventApproval,
				Type:      observation.EventTypeToolApprovalRequested,
				CreatedAt: now,
				Approval:  &ThreadDetailApproval{State: "requested", ToolID: "call-1", ToolName: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-settled", "thread-settled", "turn-settled", observation.ActivityItem{
					ItemID:           "tool:call-1",
					ToolID:           "call-1",
					ToolName:         "terminal.exec",
					Kind:             observation.ActivityKindTool,
					Status:           observation.ActivityStatusWaiting,
					Severity:         observation.ActivitySeverityBlocking,
					RequiresApproval: true,
					ApprovalState:    "requested",
					Label:            "curl -s https://example.test",
					Renderer:         observation.ActivityRendererTerminal,
					Payload:          map[string]any{"command": "curl -s https://example.test"},
				}),
			},
			{
				ID:        "approval-approved",
				Ordinal:   2,
				ThreadID:  "thread-settled",
				TurnID:    "turn-settled",
				Kind:      ThreadDetailEventApproval,
				Type:      observation.EventTypeToolApprovalApproved,
				CreatedAt: now.Add(time.Second),
				Approval:  &ThreadDetailApproval{State: "approved", ToolID: "call-1", ToolName: "terminal.exec"},
			},
			{
				ID:        "tool-call",
				Ordinal:   3,
				ThreadID:  "thread-settled",
				TurnID:    "turn-settled",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(2 * time.Second),
				ToolCall:  &ThreadDetailToolCall{ID: "call-1", Name: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-settled", "thread-settled", "turn-settled", observation.ActivityItem{
					ItemID:   "tool:call-1",
					ToolID:   "call-1",
					ToolName: "terminal.exec",
					Kind:     observation.ActivityKindTool,
					Status:   observation.ActivityStatusRunning,
					Severity: observation.ActivitySeverityNormal,
					Label:    "curl -s https://example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl -s https://example.test"},
				}),
			},
			{
				ID:        "tool-result",
				Ordinal:   4,
				ThreadID:  "thread-settled",
				TurnID:    "turn-settled",
				Kind:      ThreadDetailEventToolResult,
				CreatedAt: now.Add(3 * time.Second),
				ToolResult: &ThreadDetailToolResult{
					CallID:   "call-1",
					ToolName: "terminal.exec",
					Status:   string(observation.ActivityStatusSuccess),
				},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].Kind != ThreadTurnProjectionSegmentActivityTimeline {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if timeline == nil || len(timeline.Items) != 1 {
		t.Fatalf("activity segment = %#v", projection.Segments[0])
	}
	if timeline.Summary.Status != observation.ActivityStatusSuccess ||
		timeline.Summary.Counts.Waiting != 0 ||
		timeline.Summary.Counts.Running != 0 ||
		timeline.Summary.Counts.Success != 1 ||
		timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("activity summary = %#v", timeline.Summary)
	}
	for _, item := range timeline.Items {
		if item.Status != observation.ActivityStatusSuccess {
			t.Fatalf("item should be settled: %#v", item)
		}
		if item.Label != "curl -s https://example.test" {
			t.Fatalf("item label=%q, want command label: %#v", item.Label, item)
		}
		if item.ItemID != "tool:call-1" || item.ApprovalState != "approved" || !item.RequiresApproval {
			t.Fatalf("item should keep approval lifecycle on the tool row: %#v", item)
		}
	}
	if err := observation.ValidateActivityTimeline(*timeline); err != nil {
		t.Fatalf("activity timeline invalid: %v", err)
	}
}

func TestProjectThreadTurnSettlesWaitingApprovalOnFailedTurn(t *testing.T) {
	now := time.Unix(400, 0)

	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-failed",
		TurnID:   "turn-failed",
		RunID:    "run-failed",
		TraceID:  "run-failed",
		Events: []ThreadDetailEvent{
			{
				ID:        "approval-requested",
				Ordinal:   1,
				ThreadID:  "thread-failed",
				TurnID:    "turn-failed",
				Kind:      ThreadDetailEventApproval,
				Type:      observation.EventTypeToolApprovalRequested,
				CreatedAt: now,
				Approval:  &ThreadDetailApproval{State: "requested", ToolID: "call-1", ToolName: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-failed", "thread-failed", "turn-failed", observation.ActivityItem{
					ItemID:           "tool:call-1",
					ToolID:           "call-1",
					ToolName:         "terminal.exec",
					Kind:             observation.ActivityKindTool,
					Status:           observation.ActivityStatusWaiting,
					Severity:         observation.ActivitySeverityBlocking,
					RequiresApproval: true,
					ApprovalState:    "requested",
					Label:            "curl -s https://example.test",
					Renderer:         observation.ActivityRendererTerminal,
					Payload:          map[string]any{"command": "curl -s https://example.test"},
				}),
			},
			{
				ID:        "turn-failed",
				Ordinal:   2,
				ThreadID:  "thread-failed",
				TurnID:    "turn-failed",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now.Add(time.Second),
				Error:     "provider failed",
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "failed",
				},
			},
		},
	})

	if len(projection.Segments) != 1 || projection.Segments[0].Kind != ThreadTurnProjectionSegmentActivityTimeline {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if timeline == nil || len(timeline.Items) != 1 {
		t.Fatalf("activity segment = %#v", projection.Segments[0])
	}
	item := timeline.Items[0]
	if item.Status != observation.ActivityStatusError ||
		item.ApprovalState != "timed_out" ||
		item.Label != "curl -s https://example.test" ||
		!item.RequiresApproval ||
		timeline.Summary.Counts.Waiting != 0 ||
		timeline.Summary.Status != observation.ActivityStatusError {
		t.Fatalf("approval should settle from failed turn: item=%#v summary=%#v", item, timeline.Summary)
	}
}

func TestProjectThreadTurnSettlesUnresolvedToolOnTerminalTurn(t *testing.T) {
	now := time.Unix(500, 0)
	tests := []struct {
		name          string
		turnStatus    string
		error         string
		wantStatus    observation.ActivityStatus
		wantSeverity  observation.ActivitySeverity
		wantSummary   observation.ActivityStatus
		wantAttention bool
	}{
		{name: "success", turnStatus: string(observation.ActivityStatusSuccess), wantStatus: observation.ActivityStatusRunning, wantSeverity: observation.ActivitySeverityWarning, wantSummary: observation.ActivityStatusRunning, wantAttention: true},
		{name: "completed", turnStatus: "completed", wantStatus: observation.ActivityStatusRunning, wantSeverity: observation.ActivitySeverityWarning, wantSummary: observation.ActivityStatusRunning, wantAttention: true},
		{name: "canceled", turnStatus: string(observation.ActivityStatusCanceled), wantStatus: observation.ActivityStatusCanceled, wantSeverity: observation.ActivitySeverityWarning, wantSummary: observation.ActivityStatusCanceled},
		{name: "cancelled spelling", turnStatus: "cancelled", wantStatus: observation.ActivityStatusCanceled, wantSeverity: observation.ActivitySeverityWarning, wantSummary: observation.ActivityStatusCanceled},
		{name: "failed", turnStatus: "failed", error: "provider failed", wantStatus: observation.ActivityStatusError, wantSeverity: observation.ActivitySeverityError, wantSummary: observation.ActivityStatusError, wantAttention: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projection := ProjectThreadTurn(ProjectThreadTurnRequest{
				ThreadID: "thread-terminal",
				TurnID:   "turn-terminal",
				RunID:    "run-terminal",
				TraceID:  "run-terminal",
				Events: []ThreadDetailEvent{
					{
						ID:        "tool-call",
						Ordinal:   1,
						ThreadID:  "thread-terminal",
						TurnID:    "turn-terminal",
						Kind:      ThreadDetailEventToolCall,
						CreatedAt: now,
						ToolCall:  &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
						ActivityTimeline: projectionSingleItemTimeline("run-terminal", "thread-terminal", "turn-terminal", observation.ActivityItem{
							ItemID:          "tool:exec-1",
							ToolID:          "exec-1",
							ToolName:        "terminal.exec",
							Kind:            observation.ActivityKindTool,
							Status:          observation.ActivityStatusRunning,
							Severity:        observation.ActivitySeverityWarning,
							Renderer:        observation.ActivityRendererTerminal,
							Payload:         map[string]any{"command": "npm test", "status": string(observation.ActivityStatusRunning)},
							Metadata:        map[string]string{"pending_tool_result": "true", "pending_handle": "terminal:job:123", "pending_state": "running"},
							StartedAtUnixMS: now.UnixMilli(),
						}),
					},
					{
						ID:        "turn-terminal",
						Ordinal:   2,
						ThreadID:  "thread-terminal",
						TurnID:    "turn-terminal",
						Kind:      ThreadDetailEventTurnMarker,
						CreatedAt: now.Add(time.Second),
						Error:     tt.error,
						TurnMarker: &ThreadDetailTurnMarker{
							Status: tt.turnStatus,
						},
					},
				},
			})

			if len(projection.Segments) != 1 || projection.Segments[0].Kind != ThreadTurnProjectionSegmentActivityTimeline {
				t.Fatalf("projection segments = %#v", projection.Segments)
			}
			timeline := projection.Segments[0].ActivityTimeline
			if timeline == nil || len(timeline.Items) != 1 {
				t.Fatalf("activity segment = %#v", projection.Segments[0])
			}
			item := timeline.Items[0]
			if item.Status != tt.wantStatus || item.Severity != tt.wantSeverity {
				t.Fatalf("item = %#v, want status=%s severity=%s", item, tt.wantStatus, tt.wantSeverity)
			}
			if tt.wantStatus == observation.ActivityStatusRunning {
				if item.EndedAtUnixMS != 0 {
					t.Fatalf("running item should not be ended: %#v", item)
				}
			} else if item.EndedAtUnixMS == 0 {
				t.Fatalf("terminal item should be ended: %#v", item)
			}
			if timeline.Summary.Status != tt.wantSummary || timeline.Summary.Counts.Pending != 0 {
				t.Fatalf("summary = %#v, want terminal %s", timeline.Summary, tt.wantSummary)
			}
			if tt.wantStatus == observation.ActivityStatusRunning && timeline.Summary.Counts.Running != 1 {
				t.Fatalf("summary should retain one running item: %#v", timeline.Summary)
			}
			if tt.wantStatus != observation.ActivityStatusRunning && timeline.Summary.Counts.Running != 0 {
				t.Fatalf("summary should have no running terminal items: %#v", timeline.Summary)
			}
			if item.NeedsAttention != tt.wantAttention {
				t.Fatalf("needs_attention=%v, want %v; item=%#v", item.NeedsAttention, tt.wantAttention, item)
			}
			if tt.wantStatus == observation.ActivityStatusRunning {
				for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
					if _, ok := item.Metadata[key]; !ok {
						t.Fatalf("running item lost %q metadata: %#v", key, item.Metadata)
					}
				}
			} else {
				for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
					if _, ok := item.Metadata[key]; ok {
						t.Fatalf("terminal item retained %q metadata: %#v", key, item.Metadata)
					}
				}
				if got := item.Payload["status"]; got != string(tt.wantStatus) {
					t.Fatalf("terminal item payload status=%#v, want %s; payload=%#v", got, tt.wantStatus, item.Payload)
				}
			}
			if err := observation.ValidateActivityTimeline(*timeline); err != nil {
				t.Fatalf("terminal projection should validate: %v", err)
			}
		})
	}
}

func TestProjectThreadTurnTerminalMarkerSettlesEarlierActivitySegments(t *testing.T) {
	start := time.UnixMilli(1_700_010_000_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-terminal-segments",
		TurnID:   "turn-terminal-segments",
		RunID:    "run-terminal-segments",
		TraceID:  "run-terminal-segments",
		Events: []ThreadDetailEvent{
			{
				ID:        "exec-call",
				Ordinal:   1,
				ThreadID:  "thread-terminal-segments",
				TurnID:    "turn-terminal-segments",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: start,
				ToolCall:  &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-terminal-segments", "thread-terminal-segments", "turn-terminal-segments", observation.ActivityItem{
					ItemID:          "tool:exec-1",
					ToolID:          "exec-1",
					ToolName:        "terminal.exec",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusRunning,
					Severity:        observation.ActivitySeverityNormal,
					Renderer:        observation.ActivityRendererTerminal,
					Payload:         map[string]any{"command": "npm test", "pending_result": "terminal"},
					Metadata:        map[string]string{"pending_tool_result": "true", "pending_state": "running", "pending_handle": "terminal:job:123"},
					StartedAtUnixMS: start.Add(2 * time.Second).UnixMilli(),
				}),
			},
			{
				ID:        "assistant-text",
				Ordinal:   2,
				ThreadID:  "thread-terminal-segments",
				TurnID:    "turn-terminal-segments",
				Kind:      ThreadDetailEventAssistantMessage,
				CreatedAt: start.Add(500 * time.Millisecond),
				Message:   &ThreadDetailMessage{Role: "assistant", Content: "The command is still running."},
			},
			{
				ID:        "read-call",
				Ordinal:   3,
				ThreadID:  "thread-terminal-segments",
				TurnID:    "turn-terminal-segments",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: start.Add(time.Second),
				ToolCall:  &ThreadDetailToolCall{ID: "read-1", Name: "read"},
			},
			{
				ID:        "read-result",
				Ordinal:   4,
				ThreadID:  "thread-terminal-segments",
				TurnID:    "turn-terminal-segments",
				Kind:      ThreadDetailEventToolResult,
				CreatedAt: start.Add(1500 * time.Millisecond),
				ToolResult: &ThreadDetailToolResult{
					CallID:   "read-1",
					ToolName: "read",
					Status:   string(observation.ActivityStatusSuccess),
				},
			},
			{
				ID:        "turn-canceled",
				Ordinal:   5,
				ThreadID:  "thread-terminal-segments",
				TurnID:    "turn-terminal-segments",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: start.Add(time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "aborted",
				},
			},
		},
	})

	item := projectionToolItem(t, projection, "exec-1")
	if item.Status != observation.ActivityStatusCanceled ||
		item.Severity != observation.ActivitySeverityWarning ||
		item.EndedAtUnixMS < item.StartedAtUnixMS {
		t.Fatalf("exec item should be canceled with clamped end time: %#v", item)
	}
	if item.Payload["pending_result"] != nil {
		t.Fatalf("terminal item retained pending payload: %#v", item.Payload)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := item.Metadata[key]; ok {
			t.Fatalf("terminal item retained %q metadata: %#v", key, item.Metadata)
		}
	}
	for _, segment := range projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		if err := observation.ValidateActivityTimeline(*segment.ActivityTimeline); err != nil {
			t.Fatalf("activity timeline invalid: %v; timeline=%#v", err, segment.ActivityTimeline)
		}
		for _, activity := range segment.ActivityTimeline.Items {
			if activity.Status == observation.ActivityStatusRunning || activity.Status == observation.ActivityStatusPending {
				t.Fatalf("terminal projection retained non-terminal item: %#v", activity)
			}
		}
	}
}

func TestProjectThreadTurnSavePointDoesNotSettleActivity(t *testing.T) {
	now := time.Unix(600, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-save-point",
		TurnID:   "turn-save-point",
		RunID:    "run-save-point",
		TraceID:  "run-save-point",
		Events: []ThreadDetailEvent{
			{
				ID:        "tool-call",
				Ordinal:   1,
				ThreadID:  "thread-save-point",
				TurnID:    "turn-save-point",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now,
				ToolCall:  &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-save-point", "thread-save-point", "turn-save-point", observation.ActivityItem{
					ItemID:          "tool:exec-1",
					ToolID:          "exec-1",
					ToolName:        "terminal.exec",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusRunning,
					Severity:        observation.ActivitySeverityNormal,
					StartedAtUnixMS: now.UnixMilli(),
					Metadata:        map[string]string{"pending_tool_result": "true", "pending_state": "running"},
				}),
			},
			{
				ID:        "save-point",
				Ordinal:   2,
				ThreadID:  "thread-save-point",
				TurnID:    "turn-save-point",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now.Add(time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "save_point",
				},
			},
		},
	})

	item := projectionToolItem(t, projection, "exec-1")
	if item.Status != observation.ActivityStatusRunning || item.EndedAtUnixMS != 0 {
		t.Fatalf("save point should not settle item: %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_state"} {
		if _, ok := item.Metadata[key]; !ok {
			t.Fatalf("save point should preserve pending metadata %q: %#v", key, item.Metadata)
		}
	}
}

func TestProjectThreadTurnToolResultBatchSavePointSplitsActivity(t *testing.T) {
	now := time.Unix(700, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-batch-save-point",
		TurnID:   "turn-batch-save-point",
		RunID:    "run-batch-save-point",
		TraceID:  "run-batch-save-point",
		Events: []ThreadDetailEvent{
			{
				ID:        "todo-call",
				Ordinal:   1,
				ThreadID:  "thread-batch-save-point",
				TurnID:    "turn-batch-save-point",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now,
				ToolCall:  &ThreadDetailToolCall{ID: "todo-1", Name: "write_todos"},
				ActivityTimeline: projectionSingleItemTimeline("run-batch-save-point", "thread-batch-save-point", "turn-batch-save-point", observation.ActivityItem{
					ItemID:          "tool:todo-1",
					ToolID:          "todo-1",
					ToolName:        "write_todos",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusSuccess,
					Severity:        observation.ActivitySeverityQuiet,
					StartedAtUnixMS: now.UnixMilli(),
					EndedAtUnixMS:   now.Add(time.Second).UnixMilli(),
				}),
			},
			{
				ID:        "batch-save-point",
				Ordinal:   2,
				ThreadID:  "thread-batch-save-point",
				TurnID:    "turn-batch-save-point",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now.Add(2 * time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status:   "save_point",
					Metadata: map[string]string{"reason": "tool_result_batch", "run_id": "run-batch-save-point"},
				},
			},
			{
				ID:        "subagent-call",
				Ordinal:   3,
				ThreadID:  "thread-batch-save-point",
				TurnID:    "turn-batch-save-point",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(3 * time.Second),
				ToolCall:  &ThreadDetailToolCall{ID: "subagent-1", Name: "subagents"},
				ActivityTimeline: projectionSingleItemTimeline("run-batch-save-point", "thread-batch-save-point", "turn-batch-save-point", observation.ActivityItem{
					ItemID:          "tool:subagent-1",
					ToolID:          "subagent-1",
					ToolName:        "subagents",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusSuccess,
					Severity:        observation.ActivitySeverityQuiet,
					StartedAtUnixMS: now.Add(3 * time.Second).UnixMilli(),
					EndedAtUnixMS:   now.Add(4 * time.Second).UnixMilli(),
				}),
			},
		},
	})

	segments := projectionActivitySegments(projection)
	if len(segments) != 2 {
		t.Fatalf("activity segments = %#v, want two", projection.Segments)
	}
	if got := segments[0].ActivityTimeline.Items[0].ToolName; got != "write_todos" {
		t.Fatalf("first segment tool = %q, want write_todos; segment=%#v", got, segments[0])
	}
	if got := segments[1].ActivityTimeline.Items[0].ToolName; got != "subagents" {
		t.Fatalf("second segment tool = %q, want subagents; segment=%#v", got, segments[1])
	}
	for _, segment := range segments {
		if len(segment.EventIDs) != 1 {
			t.Fatalf("save point should not be retained as an activity event: %#v", segment)
		}
		if err := observation.ValidateActivityTimeline(*segment.ActivityTimeline); err != nil {
			t.Fatalf("activity timeline invalid: %v; segment=%#v", err, segment)
		}
	}
}

func TestProjectThreadTurnGenericSavePointDoesNotSplitActivity(t *testing.T) {
	now := time.Unix(800, 0)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-generic-save-point",
		TurnID:   "turn-generic-save-point",
		RunID:    "run-generic-save-point",
		TraceID:  "run-generic-save-point",
		Events: []ThreadDetailEvent{
			{
				ID:        "first-call",
				Ordinal:   1,
				ThreadID:  "thread-generic-save-point",
				TurnID:    "turn-generic-save-point",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now,
				ToolCall:  &ThreadDetailToolCall{ID: "first-1", Name: "first.tool"},
				ActivityTimeline: projectionSingleItemTimeline("run-generic-save-point", "thread-generic-save-point", "turn-generic-save-point", observation.ActivityItem{
					ItemID:          "tool:first-1",
					ToolID:          "first-1",
					ToolName:        "first.tool",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusSuccess,
					Severity:        observation.ActivitySeverityQuiet,
					StartedAtUnixMS: now.UnixMilli(),
					EndedAtUnixMS:   now.Add(time.Second).UnixMilli(),
				}),
			},
			{
				ID:        "generic-save-point",
				Ordinal:   2,
				ThreadID:  "thread-generic-save-point",
				TurnID:    "turn-generic-save-point",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: now.Add(2 * time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status:   "save_point",
					Metadata: map[string]string{"reason": "manual_checkpoint"},
				},
			},
			{
				ID:        "second-call",
				Ordinal:   3,
				ThreadID:  "thread-generic-save-point",
				TurnID:    "turn-generic-save-point",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: now.Add(3 * time.Second),
				ToolCall:  &ThreadDetailToolCall{ID: "second-1", Name: "second.tool"},
				ActivityTimeline: projectionSingleItemTimeline("run-generic-save-point", "thread-generic-save-point", "turn-generic-save-point", observation.ActivityItem{
					ItemID:          "tool:second-1",
					ToolID:          "second-1",
					ToolName:        "second.tool",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusSuccess,
					Severity:        observation.ActivitySeverityQuiet,
					StartedAtUnixMS: now.Add(3 * time.Second).UnixMilli(),
					EndedAtUnixMS:   now.Add(4 * time.Second).UnixMilli(),
				}),
			},
		},
	})

	segments := projectionActivitySegments(projection)
	if len(segments) != 1 {
		t.Fatalf("generic save point should not split activity; segments=%#v", projection.Segments)
	}
	items := segments[0].ActivityTimeline.Items
	if len(items) != 2 || items[0].ToolName != "first.tool" || items[1].ToolName != "second.tool" {
		t.Fatalf("generic save point should preserve one continuous activity segment: %#v", segments[0])
	}
}

func TestProjectThreadTurnPendingSettlementOverridesTerminalProjectionAcrossSegments(t *testing.T) {
	start := time.UnixMilli(1_700_020_000_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-settlement-segments",
		TurnID:   "turn-settlement-segments",
		RunID:    "run-settlement-segments",
		TraceID:  "run-settlement-segments",
		Events: []ThreadDetailEvent{
			{
				ID:        "exec-call",
				Ordinal:   1,
				ThreadID:  "thread-settlement-segments",
				TurnID:    "turn-settlement-segments",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: start,
				ToolCall:  &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
				ActivityTimeline: projectionSingleItemTimeline("run-settlement-segments", "thread-settlement-segments", "turn-settlement-segments", observation.ActivityItem{
					ItemID:          "tool:exec-1",
					ToolID:          "exec-1",
					ToolName:        "terminal.exec",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusRunning,
					Severity:        observation.ActivitySeverityNormal,
					Renderer:        observation.ActivityRendererTerminal,
					Payload:         map[string]any{"command": "npm test"},
					Metadata:        map[string]string{"pending_tool_result": "true", "pending_state": "running"},
					StartedAtUnixMS: start.UnixMilli(),
				}),
			},
			{
				ID:        "assistant-text",
				Ordinal:   2,
				ThreadID:  "thread-settlement-segments",
				TurnID:    "turn-settlement-segments",
				Kind:      ThreadDetailEventAssistantMessage,
				CreatedAt: start.Add(time.Second),
				Message:   &ThreadDetailMessage{Role: "assistant", Content: "Waiting for terminal output."},
			},
			{
				ID:        "turn-completed",
				Ordinal:   3,
				ThreadID:  "thread-settlement-segments",
				TurnID:    "turn-settlement-segments",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: start.Add(2 * time.Second),
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "completed",
				},
			},
			{
				ID:        "settlement",
				Ordinal:   4,
				ThreadID:  "thread-settlement-segments",
				TurnID:    "turn-settlement-segments",
				Kind:      ThreadDetailEventToolResult,
				Type:      threadTurnProjectionPendingToolSettlementType,
				CreatedAt: start.Add(5 * time.Second),
				ToolResult: &ThreadDetailToolResult{
					CallID:   "exec-1",
					ToolName: "terminal.exec",
					Status:   string(observation.ActivityStatusCanceled),
				},
				ActivityTimeline: projectionSingleItemTimeline("run-settlement-segments", "thread-settlement-segments", "turn-settlement-segments", observation.ActivityItem{
					ItemID:      "tool:exec-1",
					ToolID:      "exec-1",
					ToolName:    "terminal.exec",
					Kind:        observation.ActivityKindTool,
					Status:      observation.ActivityStatusCanceled,
					Severity:    observation.ActivitySeverityWarning,
					Description: "Command canceled",
					Renderer:    observation.ActivityRendererTerminal,
					Payload:     map[string]any{"exit_code": -1},
				}),
			},
		},
	})

	if count := projectionToolItemCount(projection, "exec-1"); count != 1 {
		t.Fatalf("settlement should update original item without duplication; count=%d projection=%#v", count, projection)
	}
	item := projectionToolItem(t, projection, "exec-1")
	if item.Status != observation.ActivityStatusCanceled ||
		item.Description != "Command canceled" ||
		item.Payload["exit_code"] != -1 ||
		item.EndedAtUnixMS-start.UnixMilli() != 5_000 {
		t.Fatalf("settlement should override terminal projection: %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_state"} {
		if _, ok := item.Metadata[key]; ok {
			t.Fatalf("settled item retained %q metadata: %#v", key, item.Metadata)
		}
	}
}

func TestProjectThreadTurnPendingSettlementOverridesLaterPendingResult(t *testing.T) {
	start := time.UnixMilli(1_700_030_000_000)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID: "thread-settlement-early",
		TurnID:   "turn-settlement-early",
		RunID:    "run-settlement-early",
		TraceID:  "run-settlement-early",
		Events: []ThreadDetailEvent{
			{
				ID:        "exec-call",
				Ordinal:   1,
				ThreadID:  "thread-settlement-early",
				TurnID:    "turn-settlement-early",
				Kind:      ThreadDetailEventToolCall,
				CreatedAt: start,
				ToolCall:  &ThreadDetailToolCall{ID: "exec-1", Name: "terminal.exec"},
			},
			{
				ID:        "settlement",
				Ordinal:   2,
				ThreadID:  "thread-settlement-early",
				TurnID:    "turn-settlement-early",
				Kind:      ThreadDetailEventToolResult,
				Type:      threadTurnProjectionPendingToolSettlementType,
				CreatedAt: start.Add(2 * time.Second),
				Metadata:  map[string]string{"run_id": "run-settlement-early", "handle": "terminal:job:123"},
				ToolResult: &ThreadDetailToolResult{
					CallID:   "exec-1",
					ToolName: "terminal.exec",
					Status:   string(observation.ActivityStatusSuccess),
				},
				ActivityTimeline: projectionSingleItemTimeline("run-settlement-early", "thread-settlement-early", "turn-settlement-early", observation.ActivityItem{
					ItemID:      "tool:exec-1",
					ToolID:      "exec-1",
					ToolName:    "terminal.exec",
					Kind:        observation.ActivityKindTool,
					Status:      observation.ActivityStatusSuccess,
					Severity:    observation.ActivitySeverityNormal,
					Description: "Command completed",
					Renderer:    observation.ActivityRendererTerminal,
					Payload:     map[string]any{"exit_code": 0, "status": string(observation.ActivityStatusSuccess)},
				}),
			},
			{
				ID:        "pending-result",
				Ordinal:   3,
				ThreadID:  "thread-settlement-early",
				TurnID:    "turn-settlement-early",
				Kind:      ThreadDetailEventToolResult,
				CreatedAt: start.Add(time.Second),
				ToolResult: &ThreadDetailToolResult{
					CallID:   "exec-1",
					ToolName: "terminal.exec",
					Status:   string(observation.ActivityStatusRunning),
				},
				ActivityTimeline: projectionSingleItemTimeline("run-settlement-early", "thread-settlement-early", "turn-settlement-early", observation.ActivityItem{
					ItemID:          "tool:exec-1",
					ToolID:          "exec-1",
					ToolName:        "terminal.exec",
					Kind:            observation.ActivityKindTool,
					Status:          observation.ActivityStatusRunning,
					Severity:        observation.ActivitySeverityWarning,
					Renderer:        observation.ActivityRendererTerminal,
					Payload:         map[string]any{"command": "npm test", "status": string(observation.ActivityStatusRunning), "pending_handle": "terminal:job:123"},
					Metadata:        map[string]string{"pending_tool_result": "true", "pending_handle": "terminal:job:123", "pending_state": "running"},
					StartedAtUnixMS: start.UnixMilli(),
				}),
			},
			{
				ID:        "turn-failed",
				Ordinal:   4,
				ThreadID:  "thread-settlement-early",
				TurnID:    "turn-settlement-early",
				Kind:      ThreadDetailEventTurnMarker,
				CreatedAt: start.Add(3 * time.Second),
				Error:     "provider failed",
				TurnMarker: &ThreadDetailTurnMarker{
					Status: "failed",
				},
			},
		},
	})

	if count := projectionToolItemCount(projection, "exec-1"); count != 1 {
		t.Fatalf("settlement should not duplicate tool item; count=%d projection=%#v", count, projection)
	}
	item := projectionToolItem(t, projection, "exec-1")
	if item.Status != observation.ActivityStatusSuccess ||
		item.Description != "Command completed" ||
		item.Payload["status"] != string(observation.ActivityStatusSuccess) ||
		item.Payload["exit_code"] != 0 ||
		item.NeedsAttention {
		t.Fatalf("early settlement should override later pending result and failed marker: %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := item.Metadata[key]; ok {
			t.Fatalf("settled item retained %q metadata: %#v", key, item.Metadata)
		}
	}
}

func projectionSingleItemTimeline(runID, threadID, turnID string, item observation.ActivityItem) *observation.ActivityTimeline {
	timeline := observation.ActivityTimeline{
		SchemaVersion: observation.ActivityTimelineSchemaVersion,
		RunID:         runID,
		ThreadID:      threadID,
		TurnID:        turnID,
		TraceID:       runID,
		Items:         []observation.ActivityItem{item},
	}
	timeline.Summary = threadTurnProjectionActivitySummary(timeline.Items)
	return &timeline
}

func projectionToolItem(t *testing.T, projection ThreadTurnProjection, toolID string) observation.ActivityItem {
	t.Helper()
	for _, segment := range projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.ToolID == toolID {
				return item
			}
		}
	}
	t.Fatalf("tool item %q not found in projection %#v", toolID, projection)
	return observation.ActivityItem{}
}

func projectionToolItemCount(projection ThreadTurnProjection, toolID string) int {
	count := 0
	for _, segment := range projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.ToolID == toolID {
				count++
			}
		}
	}
	return count
}

func projectionActivitySegments(projection ThreadTurnProjection) []ThreadTurnProjectionSegment {
	var out []ThreadTurnProjectionSegment
	for _, segment := range projection.Segments {
		if segment.Kind == ThreadTurnProjectionSegmentActivityTimeline && segment.ActivityTimeline != nil {
			out = append(out, segment)
		}
	}
	return out
}
