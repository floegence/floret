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
							Payload:         map[string]any{"command": "npm test"},
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
			}
			if err := observation.ValidateActivityTimeline(*timeline); err != nil {
				t.Fatalf("terminal projection should validate: %v", err)
			}
		})
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
