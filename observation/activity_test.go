package observation

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	engineevent "github.com/floegence/floret/internal/event"
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

func TestBuildActivityTimelineKeepsSanitizedToolErrorSignal(t *testing.T) {
	start := time.UnixMilli(3_000)
	raw := engineevent.Event{
		Type:      engineevent.ToolResult,
		RunID:     "run-3",
		Step:      1,
		ToolID:    "tool-1",
		ToolName:  "shell",
		ToolKind:  "local",
		Result:    "raw result",
		Err:       "failed with secret token",
		Metadata:  map[string]any{"visible_bytes": 10},
		Timestamp: start,
	}
	sanitized := engineevent.Sanitize(raw)
	if sanitized.Err != "" {
		t.Fatalf("sanitized event should not expose error text: %#v", sanitized)
	}
	meta, ok := sanitized.Metadata.(map[string]any)
	if !ok || meta["error_present"] != true {
		t.Fatalf("sanitized event should preserve only an error signal: %#v", sanitized.Metadata)
	}
	timeline := BuildActivityTimeline(ActivityRunMeta{}, []Event{{
		Type:       string(sanitized.Type),
		RunID:      sanitized.RunID,
		Step:       sanitized.Step,
		ToolID:     sanitized.ToolID,
		ToolName:   sanitized.ToolName,
		ToolKind:   sanitized.ToolKind,
		DurationMS: sanitized.Duration,
		Error:      sanitized.Err,
		Metadata:   meta,
		ObservedAt: sanitized.Timestamp,
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
