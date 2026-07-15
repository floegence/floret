package observation

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRebuildActivitySummaryUsesItemsAndPreservesDuration(t *testing.T) {
	timeline := ActivityTimeline{
		Summary: ActivitySummary{
			Status:           ActivityStatusSuccess,
			Severity:         ActivitySeverityQuiet,
			NeedsAttention:   false,
			AttentionReasons: []ActivityAttentionReason{ActivityAttentionApproval},
			TotalItems:       99,
			Counts:           ActivityCounts{Success: 99},
			DurationMS:       4321,
		},
		Items: []ActivityItem{
			{ItemID: "pending", Status: ActivityStatusPending, Severity: ActivitySeverityNormal},
			{ItemID: "running", Status: ActivityStatusRunning, Severity: ActivitySeverityWarning},
			{ItemID: "waiting", Status: ActivityStatusWaiting, Severity: ActivitySeverityBlocking, RequiresApproval: true},
			{ItemID: "success", Status: ActivityStatusSuccess, Severity: ActivitySeverityNormal},
			{ItemID: "error", Status: ActivityStatusError, Severity: ActivitySeverityError},
			{ItemID: "canceled", Status: ActivityStatusCanceled, Severity: ActivitySeverityWarning},
		},
	}

	got := RebuildActivitySummary(timeline)
	if got.Status != ActivityStatusWaiting || got.Severity != ActivitySeverityBlocking || !got.NeedsAttention {
		t.Fatalf("summary state = %#v", got)
	}
	if got.TotalItems != 6 || got.DurationMS != 4321 || got.Counts != (ActivityCounts{Pending: 1, Running: 1, Waiting: 1, Success: 1, Error: 1, Canceled: 1, Approval: 1}) {
		t.Fatalf("summary counts = %#v", got)
	}
	wantReasons := []ActivityAttentionReason{ActivityAttentionRunning, ActivityAttentionWaiting, ActivityAttentionApproval, ActivityAttentionError}
	if !slices.Equal(got.AttentionReasons, wantReasons) {
		t.Fatalf("attention reasons = %#v, want %#v", got.AttentionReasons, wantReasons)
	}
}

func TestRebuildActivitySummaryPreservesSettledRunStatusWithoutActiveItems(t *testing.T) {
	for _, tc := range []struct {
		name     string
		original ActivitySummary
		want     ActivitySummary
	}{
		{
			name:     "error",
			original: ActivitySummary{Status: ActivityStatusError, Severity: ActivitySeverityError, DurationMS: 250},
			want: ActivitySummary{
				Status:           ActivityStatusError,
				Severity:         ActivitySeverityError,
				NeedsAttention:   true,
				AttentionReasons: []ActivityAttentionReason{ActivityAttentionError},
				TotalItems:       1,
				Counts:           ActivityCounts{Success: 1},
				DurationMS:       250,
			},
		},
		{
			name:     "canceled",
			original: ActivitySummary{Status: ActivityStatusCanceled, Severity: ActivitySeverityWarning, DurationMS: 400},
			want: ActivitySummary{
				Status:     ActivityStatusCanceled,
				Severity:   ActivitySeverityWarning,
				TotalItems: 1,
				Counts:     ActivityCounts{Success: 1},
				DurationMS: 400,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeline := ActivityTimeline{
				Summary: tc.original,
				Items:   []ActivityItem{{ItemID: "done", Status: ActivityStatusSuccess, Severity: ActivitySeverityNormal}},
			}
			got := RebuildActivitySummary(timeline)
			if got.Status != tc.want.Status || got.Severity != tc.want.Severity || got.NeedsAttention != tc.want.NeedsAttention || got.TotalItems != tc.want.TotalItems || got.Counts != tc.want.Counts || got.DurationMS != tc.want.DurationMS || !slices.Equal(got.AttentionReasons, tc.want.AttentionReasons) {
				t.Fatalf("summary = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestRebuildActivitySummaryDoesNotPreserveRunTerminalStatusOverActiveItems(t *testing.T) {
	got := RebuildActivitySummary(ActivityTimeline{
		Summary: ActivitySummary{Status: ActivityStatusError, Severity: ActivitySeverityError, DurationMS: 10},
		Items:   []ActivityItem{{ItemID: "running", Status: ActivityStatusRunning, Severity: ActivitySeverityNormal}},
	})
	if got.Status != ActivityStatusRunning || got.Severity != ActivitySeverityNormal || !slices.Equal(got.AttentionReasons, []ActivityAttentionReason{ActivityAttentionRunning}) {
		t.Fatalf("summary = %#v", got)
	}
}

func TestBuildActivityTimelineProjectsToolAndApprovalState(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", TraceID: "trace-1"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", TraceID: "trace-1", Step: 1, ToolID: "read-1", ToolName: "read_file", ToolKind: "local", ArgsHash: "aabbccddeeff0011", Metadata: map[string]any{"batch_index": 0, "path": "/Users/me/secret.txt"}, ObservedAt: start},
		{Type: EventTypeToolResult, RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", TraceID: "trace-1", Step: 1, ToolID: "read-1", ToolName: "read_file", ToolKind: "local", DurationMS: 125, Metadata: map[string]any{"visible_bytes": 88, "content_sha256": "abcdef123456"}, Result: "hidden result", ObservedAt: start.Add(125 * time.Millisecond)},
		{Type: EventTypeToolCall, RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", TraceID: "trace-1", Step: 2, ToolID: "write-1", ToolName: "write_file", ToolKind: "local", ArgsHash: "0011223344556677", ObservedAt: start.Add(200 * time.Millisecond)},
		{Type: EventTypeToolApprovalRequested, RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", TraceID: "trace-1", Step: 2, ToolID: "write-1", ToolName: "write_file", ToolKind: "local", ArgsHash: "0011223344556677", Metadata: map[string]any{"approval_id": "approval-1", "effects": []any{"write"}, "destructive": false, "resources": "/Users/me/private"}, ObservedAt: start.Add(210 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.SchemaVersion != ActivityTimelineSchemaVersion || timeline.RunID != "run-1" || timeline.TraceID != "trace-1" {
		t.Fatalf("identity not preserved: %#v", timeline)
	}
	if timeline.Summary.Status != ActivityStatusWaiting || !timeline.Summary.NeedsAttention {
		t.Fatalf("summary should wait for approval: %#v", timeline.Summary)
	}
	if timeline.Summary.Counts.Success != 1 || timeline.Summary.Counts.Running != 0 || timeline.Summary.Counts.Waiting != 1 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("counts mismatch: %#v", timeline.Summary.Counts)
	}
	if len(timeline.Items) != 2 {
		t.Fatalf("items = %d, want 2: %#v", len(timeline.Items), timeline.Items)
	}
	read := timeline.Items[0]
	if read.Status != ActivityStatusSuccess || read.Metadata["args_hash"] != "aabbccddeeff0011" || read.Metadata["content_sha256"] != "abcdef123456" {
		t.Fatalf("read item mismatch: %#v", read)
	}
	write := timeline.Items[1]
	if write.Kind != ActivityKindTool ||
		write.Status != ActivityStatusWaiting ||
		write.Severity != ActivitySeverityBlocking ||
		write.ApprovalState != "requested" ||
		!write.RequiresApproval ||
		!write.NeedsAttention {
		t.Fatalf("write item should be the waiting approval tool: %#v", write)
	}
	if write.Metadata["resources"] != "" || write.Metadata["approval_id"] != "" || write.Metadata["approval_id_hash"] == "" {
		t.Fatalf("approval metadata should be allowlisted only: %#v", write.Metadata)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"hidden result", "secret.txt", "/Users/me/private", "resources"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("timeline leaked %q: %s", forbidden, data)
		}
	}
}

func TestBuildActivityTimelineProjectsPendingToolResultAsRunning(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-pending", ThreadID: "thread-pending", TurnID: "turn-pending"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-pending", ThreadID: "thread-pending", TurnID: "turn-pending", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start},
		{Type: EventTypeToolResult, RunID: "run-pending", ThreadID: "thread-pending", TurnID: "turn-pending", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"pending_tool_result": true, "pending_handle": "terminal:job:123", "pending_state": "running"}, ObservedAt: start.Add(100 * time.Millisecond)},
		{Type: EventTypeRunEnd, RunID: "run-pending", ThreadID: "thread-pending", TurnID: "turn-pending", Step: 2, Message: string(ActivityStatusSuccess), ObservedAt: start.Add(200 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusRunning || !timeline.Summary.NeedsAttention || timeline.Summary.Counts.Running != 1 {
		t.Fatalf("summary should show running pending tool: %#v", timeline.Summary)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %#v", timeline.Items)
	}
	item := timeline.Items[0]
	if item.Status != ActivityStatusRunning || item.Severity != ActivitySeverityWarning || item.EndedAtUnixMS != 0 {
		t.Fatalf("pending item mismatch: %#v", item)
	}
	if item.Metadata["pending_handle"] != "terminal:job:123" || item.Metadata["pending_state"] != "running" || item.Metadata["pending_tool_result"] != "true" {
		t.Fatalf("pending metadata mismatch: %#v", item.Metadata)
	}
}

func TestBuildActivityTimelineKeepsToolCallPendingUntilDispatchStarts(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_500)
	command := "curl -s https://example.test"
	toolCall := Event{Type: EventTypeToolCall, RunID: "run-queued", ThreadID: "thread-queued", TurnID: "turn-queued", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start}

	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-queued", ThreadID: "thread-queued", TurnID: "turn-queued"}, []Event{toolCall}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v; timeline=%#v", err, timeline)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Status != ActivityStatusPending ||
		item.Severity != ActivitySeverityQuiet ||
		item.Label != command ||
		item.EndedAtUnixMS != 0 {
		t.Fatalf("queued tool item mismatch: %#v", item)
	}
	if timeline.Summary.Status != ActivityStatusPending ||
		timeline.Summary.Counts.Pending != 1 ||
		timeline.Summary.Counts.Running != 0 {
		t.Fatalf("summary should show one pending tool: %#v", timeline.Summary)
	}

	timeline = BuildActivityTimeline(ActivityRunMeta{RunID: "run-queued", ThreadID: "thread-queued", TurnID: "turn-queued"}, []Event{
		toolCall,
		{Type: EventTypeToolDispatchStarted, RunID: "run-queued", ThreadID: "thread-queued", TurnID: "turn-queued", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start.Add(25 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("dispatched timeline should validate: %v; timeline=%#v", err, timeline)
	}
	item = activityTestItemByToolID(timeline, "exec-1")
	if item.Status != ActivityStatusRunning ||
		item.Severity != ActivitySeverityNormal ||
		item.Label != command ||
		item.StartedAtUnixMS != start.Add(25*time.Millisecond).UnixMilli() ||
		item.EndedAtUnixMS != 0 {
		t.Fatalf("dispatched tool item mismatch: %#v", item)
	}
	if timeline.Summary.Status != ActivityStatusRunning ||
		timeline.Summary.Counts.Pending != 0 ||
		timeline.Summary.Counts.Running != 1 {
		t.Fatalf("summary should show one running tool: %#v", timeline.Summary)
	}
}

func TestBuildActivityTimelineMergesToolActivityUpdateIntoRunningTool(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_500)
	command := `for i in $(seq 1 10); do date; sleep 1; done`
	events := []Event{
		{Type: EventTypeToolCall, RunID: "run-terminal", ThreadID: "thread-terminal", TurnID: "turn-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start},
		{Type: EventTypeToolDispatchStarted, RunID: "run-terminal", ThreadID: "thread-terminal", TurnID: "turn-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start.Add(10 * time.Millisecond)},
		{Type: EventTypeToolActivityUpdated, RunID: "run-terminal", ThreadID: "thread-terminal", TurnID: "turn-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Renderer: ActivityRendererTerminal, Payload: map[string]any{
			"command":            command,
			"status":             "running",
			"process_id":         "tp_live",
			"latest_output":      "tick 1\n",
			"last_seq":           1,
			"total_bytes":        7,
			"execution_location": "local_runtime",
		}}, ObservedAt: start.Add(20 * time.Millisecond)},
	}

	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-terminal", ThreadID: "thread-terminal", TurnID: "turn-terminal"}, events, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v; timeline=%#v", err, timeline)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items=%#v, want single terminal tool item", timeline.Items)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Status != ActivityStatusRunning || item.EndedAtUnixMS != 0 {
		t.Fatalf("item status mismatch: %#v", item)
	}
	if item.Payload["process_id"] != "tp_live" || item.Payload["latest_output"] != "tick 1\n" {
		t.Fatalf("live payload was not merged: %#v", item.Payload)
	}
	if timeline.Summary.Counts.Running != 1 || timeline.Summary.Counts.Success != 0 {
		t.Fatalf("summary mismatch: %#v", timeline.Summary)
	}
}

func TestBuildActivityTimelineDoesNotReopenTerminalToolAfterResult(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_500)
	events := []Event{
		{Type: EventTypeToolDispatchStarted, RunID: "run-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start},
		{Type: EventTypeToolResult, RunID: "run-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", DurationMS: 1000, Metadata: map[string]any{"tool_result_status": string(ActivityStatusSuccess)}, Activity: &ActivityPresentation{Payload: map[string]any{"output": "done\n", "process_id": "tp_live"}}, ObservedAt: start.Add(time.Second)},
		{Type: EventTypeToolActivityUpdated, RunID: "run-terminal", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Payload: map[string]any{"latest_output": "late\n", "process_id": "tp_live"}}, ObservedAt: start.Add(1100 * time.Millisecond)},
	}

	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-terminal"}, events, start.Add(2*time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v; timeline=%#v", err, timeline)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Status != ActivityStatusSuccess || item.EndedAtUnixMS == 0 {
		t.Fatalf("terminal item was reopened: %#v", item)
	}
	if item.Payload["process_id"] != "tp_live" || item.Payload["output"] != "done\n" {
		t.Fatalf("terminal payload mismatch: %#v", item.Payload)
	}
}

func TestBuildActivityTimelineSettlesPendingToolResult(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-pending", ThreadID: "thread-pending", TurnID: "turn-pending"}, []Event{
		{
			Type:       EventTypeToolResult,
			RunID:      "run-pending",
			ThreadID:   "thread-pending",
			TurnID:     "turn-pending",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			Metadata:   map[string]any{"pending_tool_result": true, "pending_handle": "terminal:job:123", "pending_state": "running"},
			Activity:   &ActivityPresentation{Label: "Command is running", Payload: map[string]any{"pending_handle": "terminal:job:123"}},
			ObservedAt: start,
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-pending",
			ThreadID:   "thread-pending",
			TurnID:     "turn-pending",
			Step:       2,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			Metadata:   map[string]any{"tool_result_status": string(ActivityStatusSuccess)},
			Activity:   &ActivityPresentation{Label: "Command completed", Payload: map[string]any{"exit_code": 0}},
			ObservedAt: start.Add(time.Second),
		},
	}, start.Add(2*time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusSuccess || timeline.Summary.Counts.Success != 1 || timeline.Summary.Counts.Running != 0 {
		t.Fatalf("summary should show settled tool: %#v", timeline.Summary)
	}
	tool := activityTestItemByToolID(timeline, "exec-1")
	if tool.Status != ActivityStatusSuccess || tool.Severity != ActivitySeverityNormal || tool.Label != "Command completed" || tool.EndedAtUnixMS == 0 {
		t.Fatalf("settled tool mismatch: %#v", tool)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := tool.Metadata[key]; ok {
			t.Fatalf("settled tool metadata retained %q: %#v", key, tool.Metadata)
		}
		if _, ok := tool.Payload[key]; ok {
			t.Fatalf("settled tool payload retained %q: %#v", key, tool.Payload)
		}
	}
	if tool.Payload["exit_code"] != 0 {
		t.Fatalf("settled payload mismatch: %#v", tool.Payload)
	}
}

func TestBuildActivityTimelineCancelsUnresolvedToolAtRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"pending_tool_result": true, "pending_handle": "terminal:job:123", "pending_state": "running"}, ObservedAt: start},
		{Type: EventTypeRunEnd, RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled", Step: 1, Message: string(ActivityStatusCanceled), ObservedAt: start.Add(250 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusCanceled || timeline.Summary.Counts.Running != 0 || timeline.Summary.Counts.Pending != 0 {
		t.Fatalf("summary should show canceled terminal state: %#v", timeline.Summary)
	}
	tool := activityTestItemByToolID(timeline, "exec-1")
	if tool.Status != ActivityStatusCanceled || tool.Severity != ActivitySeverityWarning || tool.EndedAtUnixMS == 0 {
		t.Fatalf("tool item mismatch: %#v", tool)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := tool.Metadata[key]; ok {
			t.Fatalf("terminal tool metadata retained %q: %#v", key, tool.Metadata)
		}
	}
}

func TestBuildActivityTimelineFailsUnresolvedToolAtRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start},
		{Type: EventTypeRunEnd, RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed", Step: 1, Message: "failed", Error: "interrupted", ObservedAt: start.Add(250 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusError || timeline.Summary.Counts.Running != 0 || timeline.Summary.Counts.Pending != 0 {
		t.Fatalf("summary should show failed terminal state: %#v", timeline.Summary)
	}
	tool := activityTestItemByToolID(timeline, "exec-1")
	if tool.Status != ActivityStatusError || tool.Severity != ActivitySeverityError || tool.EndedAtUnixMS == 0 {
		t.Fatalf("tool item mismatch: %#v", tool)
	}
}

func TestBuildActivityTimelineKeepsRequestedApprovalWaitingAtSuccessfulRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-waiting-approval", ThreadID: "thread-waiting-approval", TurnID: "turn-waiting-approval"}, []Event{
		{
			Type:       EventTypeToolApprovalRequested,
			RunID:      "run-waiting-approval",
			ThreadID:   "thread-waiting-approval",
			TurnID:     "turn-waiting-approval",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			ArgsHash:   "abcdef1234567890",
			Metadata:   map[string]any{"approval_id": "approval-1"},
			ObservedAt: start,
		},
		{
			Type:       EventTypeRunEnd,
			RunID:      "run-waiting-approval",
			ThreadID:   "thread-waiting-approval",
			TurnID:     "turn-waiting-approval",
			Step:       2,
			Message:    string(ActivityStatusSuccess),
			ObservedAt: start.Add(250 * time.Millisecond),
		},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusWaiting ||
		timeline.Summary.Severity != ActivitySeverityBlocking ||
		!timeline.Summary.NeedsAttention ||
		timeline.Summary.Counts.Waiting != 1 ||
		timeline.Summary.Counts.Success != 0 ||
		timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("summary should keep waiting approval: %#v", timeline.Summary)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Kind != ActivityKindTool ||
		item.Status != ActivityStatusWaiting ||
		item.Severity != ActivitySeverityBlocking ||
		item.ApprovalState != "requested" ||
		item.EndedAtUnixMS != 0 ||
		!item.RequiresApproval ||
		!item.NeedsAttention {
		t.Fatalf("approval tool item should remain requested and waiting: %#v", item)
	}
}

func TestBuildActivityTimelineDoesNotSettleToolsForNonTerminalRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	tests := []struct {
		name            string
		runEndMessage   string
		wantSummary     ActivityStatus
		wantControlItem bool
	}{
		{name: "started", runEndMessage: "started", wantSummary: ActivityStatusPending},
		{name: "waiting", runEndMessage: string(ActivityStatusWaiting), wantSummary: ActivityStatusWaiting, wantControlItem: true},
		{name: "unknown", runEndMessage: "queued", wantSummary: ActivityStatusPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-non-terminal", ThreadID: "thread-non-terminal", TurnID: "turn-non-terminal"}, []Event{
				{
					Type:       EventTypeRunEnd,
					RunID:      "run-non-terminal",
					ThreadID:   "thread-non-terminal",
					TurnID:     "turn-non-terminal",
					Step:       0,
					Message:    tt.runEndMessage,
					ObservedAt: start,
				},
				{
					Type:       EventTypeToolCall,
					RunID:      "run-non-terminal",
					ThreadID:   "thread-non-terminal",
					TurnID:     "turn-non-terminal",
					Step:       1,
					ToolID:     "exec-1",
					ToolName:   "terminal.exec",
					ToolKind:   "local",
					ObservedAt: start.Add(5 * time.Millisecond),
				},
			}, start.Add(time.Second).UnixMilli())

			if err := ValidateActivityTimeline(timeline); err != nil {
				t.Fatalf("timeline should validate: %v; timeline=%#v", err, timeline)
			}
			tool := activityTestItemByToolID(timeline, "exec-1")
			if tool.Status != ActivityStatusPending ||
				tool.EndedAtUnixMS != 0 ||
				tool.StartedAtUnixMS != start.Add(5*time.Millisecond).UnixMilli() {
				t.Fatalf("tool should stay pending: %#v", tool)
			}
			if timeline.Summary.Status != tt.wantSummary ||
				timeline.Summary.Counts.Success != 0 ||
				timeline.Summary.Counts.Pending != 1 {
				t.Fatalf("summary mismatch: %#v", timeline.Summary)
			}
			controlItems := 0
			for _, item := range timeline.Items {
				if item.Kind == ActivityKindControl {
					controlItems++
				}
			}
			if tt.wantControlItem && controlItems != 1 {
				t.Fatalf("control item count = %d, want 1: %#v", controlItems, timeline.Items)
			}
			if !tt.wantControlItem && controlItems != 0 {
				t.Fatalf("control item count = %d, want 0: %#v", controlItems, timeline.Items)
			}
		})
	}
}

func TestBuildActivityTimelineCancelsUnresolvedApprovalAtRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled"}, []Event{
		{Type: EventTypeToolApprovalRequested, RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"approval_id": "approval-1"}, ObservedAt: start},
		{Type: EventTypeRunEnd, RunID: "run-canceled", ThreadID: "thread-canceled", TurnID: "turn-canceled", Step: 2, Message: string(ActivityStatusCanceled), ObservedAt: start.Add(250 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusCanceled || timeline.Summary.Counts.Waiting != 0 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("summary should show canceled terminal approval state: %#v", timeline.Summary)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Kind != ActivityKindTool ||
		item.Status != ActivityStatusCanceled ||
		item.Severity != ActivitySeverityWarning ||
		item.ApprovalState != "canceled" ||
		item.EndedAtUnixMS == 0 ||
		!item.RequiresApproval ||
		item.NeedsAttention {
		t.Fatalf("approval tool item mismatch: %#v", item)
	}
}

func TestBuildActivityTimelineFailsUnresolvedApprovalAtRunEnd(t *testing.T) {
	start := time.UnixMilli(1_700_000_001_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed"}, []Event{
		{Type: EventTypeToolApprovalRequested, RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"approval_id": "approval-1"}, ObservedAt: start},
		{Type: EventTypeRunEnd, RunID: "run-failed", ThreadID: "thread-failed", TurnID: "turn-failed", Step: 2, Message: "failed", Error: "provider failed", ObservedAt: start.Add(250 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusError || timeline.Summary.Counts.Waiting != 0 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("summary should show failed terminal approval state: %#v", timeline.Summary)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Kind != ActivityKindTool ||
		item.Status != ActivityStatusError ||
		item.Severity != ActivitySeverityError ||
		item.ApprovalState != "timed_out" ||
		item.EndedAtUnixMS == 0 ||
		!item.RequiresApproval ||
		!item.NeedsAttention {
		t.Fatalf("approval tool item mismatch: %#v", item)
	}
}

func TestBuildActivityTimelineKeepsApprovalLifecycleOnToolItem(t *testing.T) {
	start := time.UnixMilli(1_700_000_002_000)
	command := "curl -s https://example.test"
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start},
		{Type: EventTypeToolApprovalRequested, RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"approval_id": "approval-1"}, ObservedAt: start.Add(10 * time.Millisecond)},
		{Type: EventTypeToolApprovalApproved, RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start.Add(100 * time.Millisecond)},
		{Type: EventTypeToolDispatchStarted, RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start.Add(10 * time.Second)},
		{Type: EventTypeToolResult, RunID: "run-approved", ThreadID: "thread-approved", TurnID: "turn-approved", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", DurationMS: 500, Metadata: map[string]any{"tool_result_status": string(ActivityStatusSuccess)}, Activity: &ActivityPresentation{Description: "Command completed", Payload: map[string]any{"exit_code": 0}}, ObservedAt: start.Add(10500 * time.Millisecond)},
	}, start.Add(11*time.Second).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if len(timeline.Items) != 1 || timeline.Summary.Counts.Success != 1 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("timeline should contain one approved tool item: %#v", timeline)
	}
	item := timeline.Items[0]
	if item.ItemID != "tool:exec-1" ||
		item.Kind != ActivityKindTool ||
		item.Status != ActivityStatusSuccess ||
		item.ApprovalState != "approved" ||
		!item.RequiresApproval ||
		item.Label != command ||
		item.Payload["command"] != command ||
		item.Payload["exit_code"] != 0 ||
		item.EndedAtUnixMS-item.StartedAtUnixMS != 500 {
		t.Fatalf("approved tool item mismatch: %#v", item)
	}
}

func TestBuildActivityTimelineAllowsApprovedToolBeforeDispatch(t *testing.T) {
	start := time.UnixMilli(1_700_000_002_500)
	command := "curl -s https://example.test"
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-approved-pending", ThreadID: "thread-approved-pending", TurnID: "turn-approved-pending"}, []Event{
		{Type: EventTypeToolCall, RunID: "run-approved-pending", ThreadID: "thread-approved-pending", TurnID: "turn-approved-pending", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Activity: &ActivityPresentation{Label: command, Renderer: ActivityRendererTerminal, Payload: map[string]any{"command": command}}, ObservedAt: start},
		{Type: EventTypeToolApprovalRequested, RunID: "run-approved-pending", ThreadID: "thread-approved-pending", TurnID: "turn-approved-pending", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", Metadata: map[string]any{"approval_id": "approval-1"}, ObservedAt: start.Add(10 * time.Millisecond)},
		{Type: EventTypeToolApprovalApproved, RunID: "run-approved-pending", ThreadID: "thread-approved-pending", TurnID: "turn-approved-pending", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start.Add(100 * time.Millisecond)},
	}, start.Add(200*time.Millisecond).UnixMilli())

	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("approved pending timeline should validate: %v", err)
	}
	if len(timeline.Items) != 1 || timeline.Summary.Counts.Pending != 1 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("timeline should contain one approved pending tool item: %#v", timeline)
	}
	item := timeline.Items[0]
	if item.ItemID != "tool:exec-1" ||
		item.Kind != ActivityKindTool ||
		item.Status != ActivityStatusPending ||
		item.ApprovalState != "approved" ||
		!item.RequiresApproval ||
		item.Label != command ||
		item.Payload["command"] != command ||
		item.EndedAtUnixMS != 0 {
		t.Fatalf("approved pending tool item mismatch: %#v", item)
	}
}

func TestBuildActivityTimelineProjectsApprovalDenialAsSingleToolItem(t *testing.T) {
	start := time.UnixMilli(1_700_000_003_000)
	tests := []struct {
		name       string
		eventType  EventType
		wantStatus ActivityStatus
		wantState  string
	}{
		{name: "rejected", eventType: EventTypeToolApprovalRejected, wantStatus: ActivityStatusError, wantState: "rejected"},
		{name: "timed out", eventType: EventTypeToolApprovalTimedOut, wantStatus: ActivityStatusError, wantState: "timed_out"},
		{name: "canceled", eventType: EventTypeToolApprovalCanceled, wantStatus: ActivityStatusCanceled, wantState: "canceled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-denied"}, []Event{
				{Type: EventTypeToolCall, RunID: "run-denied", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start},
				{Type: EventTypeToolApprovalRequested, RunID: "run-denied", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start.Add(10 * time.Millisecond)},
				{Type: tt.eventType, RunID: "run-denied", Step: 1, ToolID: "exec-1", ToolName: "terminal.exec", ToolKind: "local", ObservedAt: start.Add(20 * time.Millisecond)},
			}, start.Add(time.Second).UnixMilli())

			if err := ValidateActivityTimeline(timeline); err != nil {
				t.Fatalf("timeline should validate: %v", err)
			}
			if len(timeline.Items) != 1 {
				t.Fatalf("items = %#v", timeline.Items)
			}
			item := timeline.Items[0]
			if item.Kind != ActivityKindTool ||
				item.Status != tt.wantStatus ||
				item.ApprovalState != tt.wantState ||
				!item.RequiresApproval ||
				item.EndedAtUnixMS == 0 {
				t.Fatalf("denied approval item mismatch: %#v", item)
			}
		})
	}
}

func TestBuildActivityTimelineMergesExplicitActivityPresentation(t *testing.T) {
	start := time.UnixMilli(1_700_000_010_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-present"}, []Event{
		{
			Type:     EventTypeToolCall,
			RunID:    "run-present",
			Step:     1,
			ToolID:   "exec-1",
			ToolName: "workspace.inspect",
			ToolKind: "local",
			Activity: &ActivityPresentation{
				Label:    "Inspect workspace",
				Renderer: ActivityRendererStructured,
				Chips:    []ActivityChip{{Kind: "effect", Label: "inspect", Tone: "neutral"}},
				TargetRefs: []ActivityTargetRef{{
					Kind:  "workspace",
					Label: "flower_ui",
					Path:  "internal/flower_ui",
				}},
				Payload: map[string]any{
					"operation":    "inspect",
					"display_name": "flower_ui",
				},
			},
			ObservedAt: start,
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-present",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "workspace.inspect",
			ToolKind:   "local",
			DurationMS: 42,
			Activity: &ActivityPresentation{
				Description: "Inspection completed",
				Payload: map[string]any{
					"status":      "ok",
					"duration_ms": 42,
					"summary":     "done",
				},
			},
			ObservedAt: start.Add(42 * time.Millisecond),
		},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(timeline.Items))
	}
	item := timeline.Items[0]
	if item.Label != "Inspect workspace" ||
		item.Description != "Inspection completed" ||
		item.Renderer != ActivityRendererStructured {
		t.Fatalf("presentation mismatch: %#v", item)
	}
	if len(item.Chips) != 1 || item.Chips[0].Label != "inspect" {
		t.Fatalf("chips mismatch: %#v", item.Chips)
	}
	if item.Payload["operation"] != "inspect" || item.Payload["display_name"] != "flower_ui" ||
		item.Payload["summary"] != "done" || item.Payload["status"] != "ok" {
		t.Fatalf("payload mismatch: %#v", item.Payload)
	}
}

func TestBuildActivityTimelineUsesResultDurationWhenCallTimestampIsLate(t *testing.T) {
	end := time.UnixMilli(1_700_000_020_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-late-call"}, []Event{
		{
			Type:       EventTypeToolCall,
			RunID:      "run-late-call",
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			ObservedAt: end.Add(-2 * time.Millisecond),
			Activity: &ActivityPresentation{
				Label:    "sleep 10s",
				Renderer: ActivityRendererTerminal,
				Payload:  map[string]any{"command": "sleep 10s"},
			},
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-late-call",
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			DurationMS: 10_039,
			Metadata:   map[string]any{"tool_result_status": string(ActivityStatusSuccess)},
			ObservedAt: end,
		},
	}, end.UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	item := activityTestItemByToolID(timeline, "exec-1")
	if item.Label != "sleep 10s" || item.Payload["command"] != "sleep 10s" {
		t.Fatalf("presentation = %#v", item)
	}
	if got := item.EndedAtUnixMS - item.StartedAtUnixMS; got != 10_039 {
		t.Fatalf("duration = %d, want 10039: %#v", got, item)
	}
}

func TestValidateActivityTimelineRejectsEndedBeforeStarted(t *testing.T) {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:   ActivityStatusError,
			Severity: ActivitySeverityError,
			Counts:   ActivityCounts{Error: 1},
		},
		Items: []ActivityItem{{
			ItemID:          "tool:exec-1",
			ToolID:          "exec-1",
			ToolName:        "terminal.exec",
			Kind:            ActivityKindTool,
			Status:          ActivityStatusError,
			Severity:        ActivitySeverityError,
			StartedAtUnixMS: 1_700_000_010_000,
			EndedAtUnixMS:   1_700_000_009_999,
		}},
	}
	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("ValidateActivityTimeline should reject ended_at before started_at")
	}
}

func TestBuildActivityTimelineSummarizesErrorsAndHostedResults(t *testing.T) {
	start := time.UnixMilli(2_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{}, []Event{
		{Type: EventTypeHostedToolCall, RunID: "run-2", Step: 1, ToolID: "search-1", ToolName: "web_search", ToolKind: "hosted", ObservedAt: start},
		{Type: EventTypeHostedToolResult, RunID: "run-2", Step: 1, ToolID: "search-1", ToolName: "web_search", ToolKind: "hosted", Metadata: map[string]any{"result_count": 15, "results": []any{"raw"}}, ObservedAt: start.Add(50 * time.Millisecond)},
		{Type: EventTypeToolCall, RunID: "run-2", Step: 2, ToolID: "shell-1", ToolName: "shell", ToolKind: "local", ArgsHash: "shell-hash", ObservedAt: start.Add(100 * time.Millisecond)},
		{Type: EventTypeToolResult, RunID: "run-2", Step: 2, ToolID: "shell-1", ToolName: "shell", ToolKind: "local", Error: "redacted", ObservedAt: start.Add(200 * time.Millisecond)},
		{Type: EventTypeRunEnd, RunID: "run-2", Message: "failed", Error: "redacted", ObservedAt: start.Add(250 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())
	if timeline.RunID != "run-2" {
		t.Fatalf("run id inferred from events: %#v", timeline)
	}
	if timeline.Summary.Status != ActivityStatusError || timeline.Summary.Severity != ActivitySeverityError || !timeline.Summary.NeedsAttention {
		t.Fatalf("summary should surface error: %#v", timeline.Summary)
	}
	if timeline.Items[0].Kind != ActivityKindHosted || timeline.Items[0].Metadata["result_count"] != "15" {
		t.Fatalf("hosted item mismatch: %#v", timeline.Items[0])
	}
	if _, ok := timeline.Items[0].Metadata["results"]; ok {
		t.Fatalf("raw hosted results should not be copied: %#v", timeline.Items[0].Metadata)
	}
	if timeline.Items[1].Status != ActivityStatusError {
		t.Fatalf("tool error item mismatch: %#v", timeline.Items[1])
	}
}

func TestBuildActivityTimelineSerializesEmptyItemsAsArray(t *testing.T) {
	start := time.UnixMilli(3_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-empty"}, []Event{{
		Type:       EventTypeRunEnd,
		RunID:      "run-empty",
		Message:    string(ActivityStatusSuccess),
		ObservedAt: start,
	}}, start.UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.TotalItems != 0 || len(timeline.Items) != 0 {
		t.Fatalf("timeline should have no items: %#v", timeline)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"items":[]`) {
		t.Fatalf("empty activity items must serialize as an array: %s", data)
	}
}

func TestValidateActivityTimelineRejectsInvalidPresentation(t *testing.T) {
	base := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:   ActivityStatusSuccess,
			Severity: ActivitySeverityNormal,
		},
		Items: []ActivityItem{{
			ItemID:   "tool-1",
			Kind:     ActivityKindTool,
			Status:   ActivityStatusSuccess,
			Severity: ActivitySeverityNormal,
			Renderer: ActivityRendererTerminal,
			Chips:    []ActivityChip{{Kind: "status", Label: "ok"}},
			TargetRefs: []ActivityTargetRef{{
				Kind:  "file",
				Label: "README.md",
				URI:   "https://example.test/readme",
			}},
			Payload: map[string]any{"command": "pwd"},
		}},
	}
	if err := ValidateActivityTimeline(base); err != nil {
		t.Fatalf("base timeline should validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*ActivityTimeline)
	}{
		{name: "renderer", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Renderer = ActivityRenderer("terminal/v2") }},
		{name: "chip kind", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Chips[0].Kind = "bad kind" }},
		{name: "target uri", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].TargetRefs[0].URI = "file:///secret" }},
		{name: "payload key", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Payload = map[string]any{"bad key": "x"} }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			timeline := base
			timeline.Items = append([]ActivityItem(nil), base.Items...)
			timeline.Items[0].Chips = append([]ActivityChip(nil), base.Items[0].Chips...)
			timeline.Items[0].TargetRefs = append([]ActivityTargetRef(nil), base.Items[0].TargetRefs...)
			timeline.Items[0].Payload = cloneActivityPayload(base.Items[0].Payload)
			tt.mutate(&timeline)
			if err := ValidateActivityTimeline(timeline); err == nil {
				t.Fatalf("ValidateActivityTimeline should reject %s", tt.name)
			}
		})
	}
}

func TestValidateActivityTimelineAllowsHostPublicDetailPayloads(t *testing.T) {
	baseItem := ActivityItem{
		ItemID:   "tool-1",
		ToolID:   "tool-1",
		ToolName: "tool",
		Kind:     ActivityKindTool,
		Status:   ActivityStatusSuccess,
		Severity: ActivitySeverityNormal,
	}
	cases := []struct {
		name     string
		renderer ActivityRenderer
		payload  map[string]any
	}{
		{
			name:     "terminal",
			renderer: ActivityRendererTerminal,
			payload: map[string]any{
				"command":            `curl -s https://example.test`,
				"output":             "ok\n",
				"latest_output":      "ok\n",
				"process_id":         "tp_123",
				"exit_code":          0,
				"duration_ms":        1200,
				"truncated":          false,
				"total_bytes":        3,
				"execution_location": "local_runtime",
			},
		},
		{
			name:     "file",
			renderer: ActivityRendererFile,
			payload: map[string]any{
				"display_name":   "README.md",
				"content":        "# Title\n",
				"line_offset":    1,
				"line_count":     1,
				"total_lines":    1,
				"file_action_id": "file-read-1",
				"truncated":      false,
			},
		},
		{
			name:     "patch",
			renderer: ActivityRendererPatch,
			payload: map[string]any{
				"files_changed": 1,
				"additions":     1,
				"deletions":     1,
				"mutations": []any{map[string]any{
					"display_name": "main.go",
					"change_type":  "edit",
					"unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
					"truncated":    false,
				}},
			},
		},
		{
			name:     "web search",
			renderer: ActivityRendererWebSearch,
			payload: map[string]any{
				"query":   "weather changsha",
				"results": []any{map[string]any{"title": "Weather", "url": "https://example.test/weather"}},
				"sources": []any{map[string]any{"title": "Source", "url": "https://example.test/source"}},
			},
		},
		{
			name:     "question",
			renderer: ActivityRendererQuestion,
			payload: map[string]any{
				"reason_code":        "needs_user_choice",
				"required_from_user": []any{"target"},
				"questions": []any{map[string]any{
					"id":       "target",
					"question": "Which target should I use?",
					"choices":  []any{map[string]any{"label": "Local"}},
				}},
			},
		},
		{
			name:     "completion",
			renderer: ActivityRendererCompletion,
			payload: map[string]any{
				"result":          "done",
				"evidence_refs":   []any{"test"},
				"remaining_risks": []any{"none"},
				"next_actions":    []any{"ship"},
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			item := baseItem
			item.ItemID = "tool-" + strings.ReplaceAll(tt.name, " ", "-")
			item.Renderer = tt.renderer
			item.Payload = tt.payload
			timeline := ActivityTimeline{
				SchemaVersion: ActivityTimelineSchemaVersion,
				RunID:         "run-detail-payload",
				Summary: ActivitySummary{
					Status:     ActivityStatusSuccess,
					Severity:   ActivitySeverityNormal,
					TotalItems: 1,
					Counts:     ActivityCounts{Success: 1},
				},
				Items: []ActivityItem{item},
			}
			if err := ValidateActivityTimeline(timeline); err != nil {
				t.Fatalf("ValidateActivityTimeline should allow %s public detail payload: %v", tt.name, err)
			}
		})
	}
}

func TestBuildActivityTimelinePreservesHostPublicTerminalPayload(t *testing.T) {
	start := time.UnixMilli(20_000)
	command := `curl -s https://example.test`
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-terminal-detail", ThreadID: "thread-terminal-detail", TurnID: "turn-terminal-detail"}, []Event{
		{
			Type:       EventTypeToolCall,
			RunID:      "run-terminal-detail",
			ThreadID:   "thread-terminal-detail",
			TurnID:     "turn-terminal-detail",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			ObservedAt: start,
			Activity: &ActivityPresentation{
				Label:    command,
				Renderer: ActivityRendererTerminal,
				Payload:  map[string]any{"command": command, "process_id": "tp_123", "latest_output": "starting\n"},
			},
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-terminal-detail",
			ThreadID:   "thread-terminal-detail",
			TurnID:     "turn-terminal-detail",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			DurationMS: 1200,
			ObservedAt: start.Add(1200 * time.Millisecond),
			Metadata:   map[string]any{"tool_result_status": string(ActivityStatusSuccess)},
			Activity: &ActivityPresentation{
				Renderer: ActivityRendererTerminal,
				Payload:  map[string]any{"output": "ok\n", "exit_code": 0, "duration_ms": 1200, "truncated": false},
			},
		},
	}, start.Add(2*time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items=%d, want 1: %#v", len(timeline.Items), timeline.Items)
	}
	item := timeline.Items[0]
	if item.Renderer != ActivityRendererTerminal || item.Label != command {
		t.Fatalf("terminal presentation mismatch: %#v", item)
	}
	for key, want := range map[string]any{
		"command":       command,
		"process_id":    "tp_123",
		"latest_output": "starting\n",
		"output":        "ok\n",
		"exit_code":     0,
		"duration_ms":   1200,
		"truncated":     false,
	} {
		if got := item.Payload[key]; got != want {
			t.Fatalf("payload[%s]=%#v, want %#v; payload=%#v", key, got, want, item.Payload)
		}
	}
	if _, ok := item.Payload["layout"]; ok {
		t.Fatalf("Floret must not add product UI layout fields: %#v", item.Payload)
	}
}

func TestBuildActivityTimelineKeepsSanitizedToolErrorSignal(t *testing.T) {
	start := time.UnixMilli(3_000)
	meta := map[string]any{"visible_bytes": 10, "error_present": true}
	timeline := BuildActivityTimeline(ActivityRunMeta{}, []Event{{
		Type:       EventTypeToolResult,
		RunID:      "run-3",
		Step:       1,
		ToolID:     "tool-1",
		ToolName:   "shell",
		ToolKind:   "local",
		Metadata:   meta,
		ObservedAt: start,
	}}, start.Add(time.Second).UnixMilli())
	if timeline.Summary.Status != ActivityStatusError || len(timeline.Items) != 1 || timeline.Items[0].Status != ActivityStatusError {
		t.Fatalf("timeline should surface sanitized tool failure: %#v", timeline)
	}
	if _, ok := timeline.Items[0].Metadata["error_present"]; ok {
		t.Fatalf("activity metadata should not expose the internal error marker: %#v", timeline.Items[0].Metadata)
	}
}

func TestBuildActivityTimelineDropsAdversarialAllowlistMetadata(t *testing.T) {
	start := time.UnixMilli(4_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{}, []Event{
		{Type: EventTypeToolResult, RunID: "run-4", Step: 1, ToolID: "tool-1", ToolName: "read_file", ToolKind: "local", Metadata: map[string]any{
			"content_sha256": "/Users/me/.ssh/id_rsa",
			"visible_bytes":  "12 token secret",
			"strategy":       "tail /private",
			"effects":        []any{"read", "/private"},
			"read_only":      "yes",
			"result_count":   float64(2.5),
		}, ObservedAt: start},
	}, start.Add(time.Second).UnixMilli())
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(timeline.Items))
	}
	if len(timeline.Items[0].Metadata) != 0 {
		t.Fatalf("unsafe allowlisted metadata should be dropped: %#v", timeline.Items[0].Metadata)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/Users/me/.ssh", "secret", "tail /private"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("timeline leaked %q: %s", forbidden, data)
		}
	}
}

func TestBuildActivityTimelineProjectsWaitingRunEndAsControlItem(t *testing.T) {
	start := time.UnixMilli(5_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-5", ThreadID: "thread-5", TurnID: "turn-5"}, []Event{
		{
			Type:       EventTypeRunEnd,
			RunID:      "run-5",
			ThreadID:   "thread-5",
			TurnID:     "turn-5",
			Step:       1,
			Message:    "waiting",
			Result:     "Which file contains token secret-value?",
			ObservedAt: start,
		},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusWaiting || timeline.Summary.Severity != ActivitySeverityBlocking || !timeline.Summary.NeedsAttention {
		t.Fatalf("summary should wait for control input: %#v", timeline.Summary)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %d, want 1: %#v", len(timeline.Items), timeline.Items)
	}
	item := timeline.Items[0]
	if item.Kind != ActivityKindControl || item.Status != ActivityStatusWaiting || !item.NeedsAttention {
		t.Fatalf("control item mismatch: %#v", item)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"Which file", "secret-value"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("timeline leaked waiting prompt %q: %s", forbidden, data)
		}
	}
}

func TestBuildActivityTimelineProjectsCustomControlSignal(t *testing.T) {
	start := time.UnixMilli(6_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-control", ThreadID: "thread-control", TurnID: "turn-control"}, []Event{
		{
			Type:       EventTypeControlSignal,
			RunID:      "run-control",
			ThreadID:   "thread-control",
			TurnID:     "turn-control",
			Step:       1,
			ToolID:     "control-call-1",
			ToolName:   "host_wait",
			ToolKind:   "control",
			ArgsHash:   "abcdef1234567890",
			Result:     "Pick a file with secret-value",
			Metadata:   map[string]any{"control_disposition": "waiting", "prompt": "Which file?", "secret": "token abc"},
			ObservedAt: start,
		},
		{
			Type:       EventTypeRunEnd,
			RunID:      "run-control",
			ThreadID:   "thread-control",
			TurnID:     "turn-control",
			Step:       1,
			Message:    "waiting",
			Result:     "Pick a file with secret-value",
			ObservedAt: start.Add(25 * time.Millisecond),
		},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("items = %d, want 1: %#v", len(timeline.Items), timeline.Items)
	}
	item := timeline.Items[0]
	if item.Kind != ActivityKindControl || item.ToolName != "host_wait" || item.ToolID != "control-call-1" {
		t.Fatalf("control identity mismatch: %#v", item)
	}
	if item.Status != ActivityStatusWaiting || item.Severity != ActivitySeverityBlocking || !item.NeedsAttention {
		t.Fatalf("control attention mismatch: %#v", item)
	}
	if item.Metadata["args_hash"] != "abcdef1234567890" || item.Metadata["control_disposition"] != "waiting" {
		t.Fatalf("control metadata mismatch: %#v", item.Metadata)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"Pick a file", "Which file", "secret-value", "token abc"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("timeline leaked control payload %q: %s", forbidden, data)
		}
	}
}

func TestBuildActivityTimelineDurationIgnoresNonActivityEvents(t *testing.T) {
	start := time.UnixMilli(7_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-duration"}, []Event{
		{Type: "skill_detected", RunID: "run-duration", ObservedAt: start},
		{Type: EventTypeToolCall, RunID: "run-duration", Step: 1, ToolID: "tool-1", ToolName: "read", ToolKind: "local", ObservedAt: start.Add(10 * time.Second)},
		{Type: EventTypeToolResult, RunID: "run-duration", Step: 1, ToolID: "tool-1", ToolName: "read", ToolKind: "local", ObservedAt: start.Add(10*time.Second + 50*time.Millisecond)},
		{Type: "provider_usage", RunID: "run-duration", ObservedAt: start.Add(20 * time.Second)},
		{Type: EventTypeRunEnd, RunID: "run-duration", Message: "completed", ObservedAt: start.Add(10*time.Second + 75*time.Millisecond)},
	}, start.Add(30*time.Second).UnixMilli())
	if timeline.Summary.DurationMS != 75 {
		t.Fatalf("duration = %d, want 75; timeline=%#v", timeline.Summary.DurationMS, timeline)
	}
}

func TestBuildActivityTimelineDeterministicForParallelOutOfOrderEvents(t *testing.T) {
	start := time.UnixMilli(8_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-parallel"}, []Event{
		{Type: EventTypeToolResult, RunID: "run-parallel", Step: 2, ToolID: "b", ToolName: "write_file", ToolKind: "local", DurationMS: 30, ObservedAt: start.Add(70 * time.Millisecond)},
		{Type: EventTypeToolResult, RunID: "run-parallel", Step: 3, ToolID: "orphan", ToolName: "shell", ToolKind: "local", DurationMS: 15, ObservedAt: start.Add(20 * time.Millisecond)},
		{Type: EventTypeToolCall, RunID: "run-parallel", Step: 1, ToolID: "a", ToolName: "read_file", ToolKind: "local", ObservedAt: start.Add(10 * time.Millisecond)},
		{Type: EventTypeToolCall, RunID: "run-parallel", Step: 2, ToolID: "b", ToolName: "write_file", ToolKind: "local", ObservedAt: start.Add(40 * time.Millisecond)},
		{Type: EventTypeToolResult, RunID: "run-parallel", Step: 1, ToolID: "a", ToolName: "read_file", ToolKind: "local", ObservedAt: start.Add(30 * time.Millisecond)},
		{Type: EventTypeToolResult, RunID: "run-parallel", Step: 1, ToolID: "a", ToolName: "read_file", ToolKind: "local", ObservedAt: start.Add(35 * time.Millisecond)},
	}, start.Add(time.Second).UnixMilli())
	if err := ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("timeline should validate: %v", err)
	}
	if timeline.Summary.Status != ActivityStatusSuccess || timeline.Summary.Counts.Success != 3 || timeline.Summary.TotalItems != 3 {
		t.Fatalf("summary mismatch: %#v", timeline.Summary)
	}
	gotOrder := []string{}
	for _, item := range timeline.Items {
		gotOrder = append(gotOrder, item.ItemID)
		if item.Status != ActivityStatusSuccess {
			t.Fatalf("item should be successful: %#v", item)
		}
	}
	wantOrder := []string{"tool:orphan", "tool:a", "tool:b"}
	if strings.Join(gotOrder, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("item order = %v, want %v; items=%#v", gotOrder, wantOrder, timeline.Items)
	}
	if timeline.Items[0].StartedAtUnixMS != start.Add(5*time.Millisecond).UnixMilli() {
		t.Fatalf("result without call should derive start from duration: %#v", timeline.Items[0])
	}
	if timeline.Items[1].StartedAtUnixMS != start.Add(10*time.Millisecond).UnixMilli() ||
		timeline.Items[1].EndedAtUnixMS != start.Add(35*time.Millisecond).UnixMilli() {
		t.Fatalf("duplicate results should keep deterministic latest end time: %#v", timeline.Items[1])
	}
	if timeline.Items[2].StartedAtUnixMS != start.Add(40*time.Millisecond).UnixMilli() {
		t.Fatalf("out-of-order call/result should keep the call start time: %#v", timeline.Items[2])
	}
}

func TestBuildActivityTimelineDoesNotAssumeRunAndTurnIdentity(t *testing.T) {
	start := time.UnixMilli(9_000)
	standalone := BuildActivityTimeline(ActivityRunMeta{RunID: "standalone-run"}, []Event{
		{Type: EventTypeRunEnd, RunID: "standalone-run", Message: "completed", ObservedAt: start},
	}, start.Add(time.Second).UnixMilli())
	if standalone.RunID != "standalone-run" || standalone.ThreadID != "" || standalone.TurnID != "" {
		t.Fatalf("standalone identity mismatch: %#v", standalone)
	}
	harness := BuildActivityTimeline(ActivityRunMeta{RunID: "engine-run-7", ThreadID: "thread-7", TurnID: "turn-7", TraceID: "trace-7"}, []Event{
		{Type: EventTypeToolCall, RunID: "engine-run-7", ThreadID: "thread-7", TurnID: "turn-7", TraceID: "trace-7", Step: 1, ToolID: "tool-1", ToolName: "read", ToolKind: "local", ObservedAt: start},
	}, start.Add(time.Second).UnixMilli())
	if harness.RunID != "engine-run-7" || harness.ThreadID != "thread-7" || harness.TurnID != "turn-7" || harness.TraceID != "trace-7" {
		t.Fatalf("harness identity mismatch: %#v", harness)
	}
	if harness.RunID == harness.TurnID {
		t.Fatalf("test requires distinct run_id and turn_id: %#v", harness)
	}
}

func TestActivityTimelineJSONWireShapeIsSnakeCase(t *testing.T) {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		RunID:         "run-json",
		ThreadID:      "thread-json",
		TurnID:        "turn-json",
		TraceID:       "trace-json",
		Summary: ActivitySummary{
			Status:           ActivityStatusWaiting,
			Severity:         ActivitySeverityBlocking,
			NeedsAttention:   true,
			AttentionReasons: []ActivityAttentionReason{ActivityAttentionWaiting, ActivityAttentionApproval},
			TotalItems:       1,
			Counts:           ActivityCounts{Waiting: 1, Approval: 1},
			DurationMS:       123,
		},
		Items: []ActivityItem{{
			ItemID:           "tool:tool-1",
			ToolID:           "tool-1",
			ToolName:         "write_file",
			Kind:             ActivityKindTool,
			Status:           ActivityStatusWaiting,
			Severity:         ActivitySeverityBlocking,
			NeedsAttention:   true,
			AttentionReasons: []ActivityAttentionReason{ActivityAttentionWaiting, ActivityAttentionApproval},
			RequiresApproval: true,
			ApprovalState:    "requested",
			StartedAtUnixMS:  10,
			Metadata:         map[string]string{"args_hash": "abcdef1234567890", "effects": "write"},
		}},
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"run_id":"run-json","thread_id":"thread-json","turn_id":"turn-json","trace_id":"trace-json","summary":{"status":"waiting","severity":"blocking","needs_attention":true,"attention_reasons":["waiting","approval"],"total_items":1,"counts":{"waiting":1,"approval":1},"duration_ms":123},"items":[{"item_id":"tool:tool-1","tool_id":"tool-1","tool_name":"write_file","kind":"tool","status":"waiting","severity":"blocking","needs_attention":true,"attention_reasons":["waiting","approval"],"requires_approval":true,"approval_state":"requested","started_at_unix_ms":10,"metadata":{"args_hash":"abcdef1234567890","effects":"write"}}]}`
	if string(data) != want {
		t.Fatalf("activity timeline JSON mismatch:\n got: %s\nwant: %s", data, want)
	}
}

func TestValidateActivityTimelineRejectsInconsistentApprovalLifecycle(t *testing.T) {
	base := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:     ActivityStatusWaiting,
			Severity:   ActivitySeverityBlocking,
			TotalItems: 1,
			Counts:     ActivityCounts{Approval: 1, Waiting: 1},
		},
		Items: []ActivityItem{{
			ItemID:           "tool:exec-1",
			ToolID:           "exec-1",
			ToolName:         "terminal.exec",
			Kind:             ActivityKindTool,
			Status:           ActivityStatusWaiting,
			Severity:         ActivitySeverityBlocking,
			RequiresApproval: true,
			ApprovalState:    "requested",
			StartedAtUnixMS:  10,
		}},
	}
	if err := ValidateActivityTimeline(base); err != nil {
		t.Fatalf("base timeline should validate: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ActivityItem)
	}{
		{name: "requested success", mutate: func(item *ActivityItem) { item.Status = ActivityStatusSuccess }},
		{name: "requested ended", mutate: func(item *ActivityItem) { item.EndedAtUnixMS = 20 }},
		{name: "approved waiting", mutate: func(item *ActivityItem) { item.ApprovalState = "approved" }},
		{name: "rejected waiting", mutate: func(item *ActivityItem) { item.ApprovalState = "rejected" }},
		{name: "canceled waiting", mutate: func(item *ActivityItem) { item.ApprovalState = "canceled" }},
		{name: "missing requires approval", mutate: func(item *ActivityItem) { item.RequiresApproval = false }},
		{name: "wrong kind", mutate: func(item *ActivityItem) { item.Kind = ActivityKindControl }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeline := base
			timeline.Items = append([]ActivityItem(nil), base.Items...)
			tt.mutate(&timeline.Items[0])
			if err := ValidateActivityTimeline(timeline); err == nil {
				t.Fatalf("ValidateActivityTimeline should reject %#v", timeline.Items[0])
			}
		})
	}
}

func TestValidateActivityTimelineRejectsLegacyToolApprovalDuplicate(t *testing.T) {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:   ActivityStatusWaiting,
			Severity: ActivitySeverityBlocking,
		},
		Items: []ActivityItem{
			{
				ItemID:          "tool:exec-1",
				ToolID:          "exec-1",
				ToolName:        "terminal.exec",
				Kind:            ActivityKindTool,
				Status:          ActivityStatusRunning,
				Severity:        ActivitySeverityNormal,
				StartedAtUnixMS: 10,
			},
			{
				ItemID:           "approval:exec-1",
				ToolID:           "exec-1",
				ToolName:         "terminal.exec",
				Kind:             ActivityKind("approval"),
				Status:           ActivityStatusWaiting,
				Severity:         ActivitySeverityBlocking,
				RequiresApproval: true,
				ApprovalState:    "requested",
				StartedAtUnixMS:  20,
			},
		},
	}

	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("ValidateActivityTimeline should reject legacy tool/approval duplicate rows")
	}
}

func TestBuildActivityTimelineJSONDoesNotLeakRuntimePayloads(t *testing.T) {
	start := time.UnixMilli(10_000)
	timeline := BuildActivityTimeline(ActivityRunMeta{RunID: "run-leak"}, []Event{
		{
			Type:       EventTypeToolCall,
			RunID:      "run-leak",
			Step:       1,
			ToolID:     "tool-1",
			ToolName:   "shell",
			ToolKind:   "local",
			ArgsHash:   "abcdef1234567890",
			Result:     "PRIVATE_RESULT /Users/alice/secret.txt",
			Error:      "token secret-value",
			Metadata:   map[string]any{"prompt": "private prompt", "path": "/Users/alice/work", "approval_id": "approval-secret-token", "visible_bytes": 42},
			ObservedAt: start,
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-leak",
			Step:       1,
			ToolID:     "tool-1",
			ToolName:   "shell",
			ToolKind:   "local",
			Result:     "PRIVATE_RESULT /Users/alice/secret.txt",
			Error:      "token secret-value",
			Metadata:   map[string]any{"error_present": true, "result": "PRIVATE_RESULT", "visible_bytes": 42},
			ObservedAt: start.Add(25 * time.Millisecond),
		},
	}, start.Add(time.Second).UnixMilli())
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	assertActivityTimelineDoesNotContain(t, string(data),
		"PRIVATE_RESULT",
		"/Users/alice",
		"secret.txt",
		"secret-value",
		"private prompt",
		"approval-secret-token",
		`"prompt"`,
		`"path"`,
		`"result"`,
		`"error_present"`,
	)
}

func TestValidateActivityTimelineRejectsUnknownWireValues(t *testing.T) {
	timeline := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:   ActivityStatusSuccess,
			Severity: ActivitySeverityNormal,
		},
		Items: []ActivityItem{{ItemID: "x", Kind: ActivityKindTool, Status: ActivityStatus("mystery"), Severity: ActivitySeverityNormal}},
	}
	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("expected unknown status to fail validation")
	}
	timeline.Items[0].Status = ActivityStatusSuccess
	timeline.SchemaVersion = 99
	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("expected unsupported schema version to fail validation")
	}
	timeline.SchemaVersion = ActivityTimelineSchemaVersion
	timeline.Items[0].Kind = ActivityKind("run")
	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("expected unknown kind to fail validation")
	}
	timeline.Items[0].Kind = ActivityKindTool
	timeline.Items[0].Metadata = map[string]string{"args_hash": "raw path /secret"}
	if err := ValidateActivityTimeline(timeline); err == nil {
		t.Fatal("expected unsafe metadata value to fail validation")
	}
}

func TestValidateActivityTimelineRejectsAllUnknownEnums(t *testing.T) {
	base := ActivityTimeline{
		SchemaVersion: ActivityTimelineSchemaVersion,
		Summary: ActivitySummary{
			Status:   ActivityStatusSuccess,
			Severity: ActivitySeverityNormal,
		},
		Items: []ActivityItem{{
			ItemID:           "tool:abc12345",
			ToolID:           "abc12345",
			Kind:             ActivityKindTool,
			Status:           ActivityStatusWaiting,
			Severity:         ActivitySeverityBlocking,
			ApprovalState:    "requested",
			RequiresApproval: true,
			AttentionReasons: []ActivityAttentionReason{
				ActivityAttentionWaiting,
				ActivityAttentionApproval,
			},
		}},
	}
	cases := []struct {
		name   string
		mutate func(*ActivityTimeline)
	}{
		{name: "summary status", mutate: func(timeline *ActivityTimeline) { timeline.Summary.Status = ActivityStatus("done-ish") }},
		{name: "summary severity", mutate: func(timeline *ActivityTimeline) { timeline.Summary.Severity = ActivitySeverity("fatal") }},
		{name: "summary attention reason", mutate: func(timeline *ActivityTimeline) {
			timeline.Summary.AttentionReasons = []ActivityAttentionReason{"blocked"}
		}},
		{name: "item kind", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Kind = ActivityKind("run") }},
		{name: "item status", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Status = ActivityStatus("done-ish") }},
		{name: "item severity", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].Severity = ActivitySeverity("fatal") }},
		{name: "item attention reason", mutate: func(timeline *ActivityTimeline) {
			timeline.Items[0].AttentionReasons = []ActivityAttentionReason{"blocked"}
		}},
		{name: "approval state", mutate: func(timeline *ActivityTimeline) { timeline.Items[0].ApprovalState = "required" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timeline := base
			timeline.Summary.AttentionReasons = append([]ActivityAttentionReason(nil), base.Summary.AttentionReasons...)
			timeline.Items = append([]ActivityItem(nil), base.Items...)
			timeline.Items[0].AttentionReasons = append([]ActivityAttentionReason(nil), base.Items[0].AttentionReasons...)
			tc.mutate(&timeline)
			if err := ValidateActivityTimeline(timeline); err == nil {
				t.Fatalf("expected validation to reject unknown %s", tc.name)
			}
		})
	}
}

func assertActivityTimelineDoesNotContain(t *testing.T, data string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if strings.Contains(data, value) {
			t.Fatalf("activity timeline leaked %q: %s", value, data)
		}
	}
}

func activityTestItemByToolID(timeline ActivityTimeline, toolID string) ActivityItem {
	for _, item := range timeline.Items {
		if item.ToolID == toolID {
			return item
		}
	}
	return ActivityItem{}
}

func activityTestItemByKind(timeline ActivityTimeline, kind ActivityKind) ActivityItem {
	for _, item := range timeline.Items {
		if item.Kind == kind {
			return item
		}
	}
	return ActivityItem{}
}
