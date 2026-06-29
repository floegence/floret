package observation

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

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
	if timeline.Summary.Counts.Success != 1 || timeline.Summary.Counts.Running != 1 || timeline.Summary.Counts.Waiting != 1 || timeline.Summary.Counts.Approval != 1 {
		t.Fatalf("counts mismatch: %#v", timeline.Summary.Counts)
	}
	if len(timeline.Items) != 3 {
		t.Fatalf("items = %d, want 3: %#v", len(timeline.Items), timeline.Items)
	}
	read := timeline.Items[0]
	if read.Status != ActivityStatusSuccess || read.Metadata["args_hash"] != "aabbccddeeff0011" || read.Metadata["content_sha256"] != "abcdef123456" {
		t.Fatalf("read item mismatch: %#v", read)
	}
	write := timeline.Items[1]
	if write.Status != ActivityStatusRunning || !write.NeedsAttention {
		t.Fatalf("write item should still be running while approval waits: %#v", write)
	}
	approval := timeline.Items[2]
	if approval.Status != ActivityStatusWaiting || approval.ApprovalState != "requested" || !approval.RequiresApproval {
		t.Fatalf("approval item mismatch: %#v", approval)
	}
	if approval.Metadata["resources"] != "" || approval.Metadata["approval_id"] != "" || approval.Metadata["approval_id_hash"] == "" {
		t.Fatalf("approval metadata should be allowlisted only: %#v", approval.Metadata)
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
	approval := activityTestItemByKind(timeline, ActivityKindApproval)
	if approval.Status != ActivityStatusCanceled ||
		approval.Severity != ActivitySeverityWarning ||
		approval.ApprovalState != "canceled" ||
		approval.EndedAtUnixMS == 0 ||
		approval.NeedsAttention {
		t.Fatalf("approval item mismatch: %#v", approval)
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
	approval := activityTestItemByKind(timeline, ActivityKindApproval)
	if approval.Status != ActivityStatusError ||
		approval.Severity != ActivitySeverityError ||
		approval.ApprovalState != "timed_out" ||
		approval.EndedAtUnixMS == 0 ||
		!approval.NeedsAttention {
		t.Fatalf("approval item mismatch: %#v", approval)
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
			ToolName: "terminal.exec",
			ToolKind: "local",
			Activity: &ActivityPresentation{
				Label:    "npm run build --workspace internal/flower_ui",
				Renderer: ActivityRendererTerminal,
				Chips:    []ActivityChip{{Kind: "effect", Label: "shell", Tone: "neutral"}},
				TargetRefs: []ActivityTargetRef{{
					Kind:  "workspace",
					Label: "flower_ui",
					Path:  "internal/flower_ui",
				}},
				Payload: map[string]any{
					"command": "npm run build --workspace internal/flower_ui",
					"cwd":     "internal/flower_ui",
				},
			},
			ObservedAt: start,
		},
		{
			Type:       EventTypeToolResult,
			RunID:      "run-present",
			Step:       1,
			ToolID:     "exec-1",
			ToolName:   "terminal.exec",
			ToolKind:   "local",
			DurationMS: 42,
			Activity: &ActivityPresentation{
				Description: "Command completed",
				Payload: map[string]any{
					"exit_code":   0,
					"duration_ms": 42,
					"stdout":      "done",
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
	if item.Label != "npm run build --workspace internal/flower_ui" ||
		item.Description != "Command completed" ||
		item.Renderer != ActivityRendererTerminal {
		t.Fatalf("presentation mismatch: %#v", item)
	}
	if len(item.Chips) != 1 || item.Chips[0].Label != "shell" {
		t.Fatalf("chips mismatch: %#v", item.Chips)
	}
	if item.Payload["command"] != "npm run build --workspace internal/flower_ui" || item.Payload["stdout"] != "done" || item.Payload["exit_code"] != 0 {
		t.Fatalf("payload mismatch: %#v", item.Payload)
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
			ItemID:           "approval:abc12345",
			ToolID:           "tool-1",
			ToolName:         "write_file",
			Kind:             ActivityKindApproval,
			Status:           ActivityStatusWaiting,
			Severity:         ActivitySeverityBlocking,
			NeedsAttention:   true,
			AttentionReasons: []ActivityAttentionReason{ActivityAttentionWaiting, ActivityAttentionApproval},
			RequiresApproval: true,
			ApprovalState:    "requested",
			StartedAtUnixMS:  10,
			EndedAtUnixMS:    20,
			Metadata:         map[string]string{"args_hash": "abcdef1234567890", "effects": "write"},
		}},
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"run_id":"run-json","thread_id":"thread-json","turn_id":"turn-json","trace_id":"trace-json","summary":{"status":"waiting","severity":"blocking","needs_attention":true,"attention_reasons":["waiting","approval"],"total_items":1,"counts":{"waiting":1,"approval":1},"duration_ms":123},"items":[{"item_id":"approval:abc12345","tool_id":"tool-1","tool_name":"write_file","kind":"approval","status":"waiting","severity":"blocking","needs_attention":true,"attention_reasons":["waiting","approval"],"requires_approval":true,"approval_state":"requested","started_at_unix_ms":10,"ended_at_unix_ms":20,"metadata":{"args_hash":"abcdef1234567890","effects":"write"}}]}`
	if string(data) != want {
		t.Fatalf("activity timeline JSON mismatch:\n got: %s\nwant: %s", data, want)
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
			ItemID:           "approval:abc12345",
			Kind:             ActivityKindApproval,
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
