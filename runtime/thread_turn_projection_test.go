package runtime

import (
	"testing"
	"time"

	"github.com/floegence/floret/observation"
)

func TestProjectThreadTurnOrdersTextActivityAndControlSegments(t *testing.T) {
	now := time.Unix(100, 0)
	canonical := observation.ActivityTimeline{
		SchemaVersion: observation.ActivityTimelineSchemaVersion,
		RunID:         "run-1",
		ThreadID:      "thread-1",
		TurnID:        "turn-1",
		TraceID:       "run-1",
		Items: []observation.ActivityItem{
			{
				ItemID:          "tool:call-1",
				ToolID:          "call-1",
				ToolName:        "read",
				Kind:            observation.ActivityKindTool,
				Status:          observation.ActivityStatusSuccess,
				Severity:        observation.ActivitySeverityNormal,
				StartedAtUnixMS: now.UnixMilli(),
				EndedAtUnixMS:   now.Add(time.Second).UnixMilli(),
			},
			{
				ItemID:          "control:done",
				ToolID:          "done",
				ToolName:        "task_complete",
				Kind:            observation.ActivityKindControl,
				Status:          observation.ActivityStatusSuccess,
				Severity:        observation.ActivitySeverityNormal,
				StartedAtUnixMS: now.Add(2 * time.Second).UnixMilli(),
				EndedAtUnixMS:   now.Add(2 * time.Second).UnixMilli(),
			},
		},
	}
	canonical.Summary = threadTurnProjectionActivitySummary(canonical.Items)
	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		RunID:            "run-1",
		TraceID:          "run-1",
		ActivityTimeline: canonical,
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

func TestProjectThreadTurnPreservesActivityAttentionSummary(t *testing.T) {
	now := time.Unix(200, 0)
	canonical := observation.ActivityTimeline{
		SchemaVersion: observation.ActivityTimelineSchemaVersion,
		RunID:         "run-attention",
		ThreadID:      "thread-attention",
		TurnID:        "turn-attention",
		TraceID:       "run-attention",
		Items: []observation.ActivityItem{{
			ItemID:           "tool:approval-1",
			ToolID:           "approval-1",
			ToolName:         "terminal.exec",
			Kind:             observation.ActivityKindApproval,
			Status:           observation.ActivityStatusWaiting,
			Severity:         observation.ActivitySeverityBlocking,
			NeedsAttention:   true,
			AttentionReasons: []observation.ActivityAttentionReason{observation.ActivityAttentionWaiting, observation.ActivityAttentionApproval},
			RequiresApproval: true,
			StartedAtUnixMS:  now.UnixMilli(),
		}},
	}
	canonical.Summary = threadTurnProjectionActivitySummary(canonical.Items)

	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID:         "thread-attention",
		TurnID:           "turn-attention",
		RunID:            "run-attention",
		TraceID:          "run-attention",
		ActivityTimeline: canonical,
		Events: []ThreadDetailEvent{{
			ID:        "approval-row",
			Ordinal:   1,
			ThreadID:  "thread-attention",
			TurnID:    "turn-attention",
			Kind:      ThreadDetailEventApproval,
			CreatedAt: now,
			Approval:  &ThreadDetailApproval{ToolID: "approval-1", ToolName: "terminal.exec"},
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

func TestProjectThreadTurnUsesCanonicalActivityWithoutDetailEvents(t *testing.T) {
	now := time.Unix(300, 0)
	canonical := observation.ActivityTimeline{
		SchemaVersion: observation.ActivityTimelineSchemaVersion,
		RunID:         "run-canceled",
		ThreadID:      "thread-canceled",
		TurnID:        "turn-canceled",
		TraceID:       "run-canceled",
		Items: []observation.ActivityItem{{
			ItemID:          "tool:call-1",
			ToolID:          "call-1",
			ToolName:        "terminal.exec",
			Kind:            observation.ActivityKindTool,
			Status:          observation.ActivityStatusCanceled,
			Severity:        observation.ActivitySeverityWarning,
			StartedAtUnixMS: now.UnixMilli(),
			EndedAtUnixMS:   now.Add(time.Second).UnixMilli(),
		}},
	}
	canonical.Summary = threadTurnProjectionActivitySummary(canonical.Items)

	projection := ProjectThreadTurn(ProjectThreadTurnRequest{
		ThreadID:         "thread-canceled",
		TurnID:           "turn-canceled",
		RunID:            "run-canceled",
		TraceID:          "run-canceled",
		ActivityTimeline: canonical,
	})

	if len(projection.Segments) != 1 || projection.Segments[0].Kind != ThreadTurnProjectionSegmentActivityTimeline {
		t.Fatalf("projection segments = %#v", projection.Segments)
	}
	timeline := projection.Segments[0].ActivityTimeline
	if timeline == nil || len(timeline.Items) != 1 || timeline.Items[0].Status != observation.ActivityStatusCanceled {
		t.Fatalf("activity segment = %#v", projection.Segments[0])
	}
	if timeline.Summary.Status != observation.ActivityStatusCanceled || timeline.Summary.Counts.Canceled != 1 || timeline.Summary.Counts.Running != 0 {
		t.Fatalf("activity summary = %#v", timeline.Summary)
	}
}
