package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTraceWriterWritesParseableRedactedJSONL(t *testing.T) {
	var buf bytes.Buffer
	writer := NewTraceWriter(&buf)
	writer.Emit(Event{
		Type:      ToolCall,
		TraceID:   "trace",
		RunID:     "run",
		ThreadID:  "session",
		Step:      1,
		Args:      `{"api_key":"secret-token","path":"a.go"}`,
		Timestamp: time.Unix(1, 0),
	})
	var decoded Event
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &decoded); err != nil {
		t.Fatalf("trace is not JSONL: %v\n%s", err, buf.String())
	}
	if decoded.TraceID != "trace" || decoded.ThreadID != "session" || decoded.Step != 1 {
		t.Fatalf("decoded event missing correlation fields: %#v", decoded)
	}
	if decoded.ArgsHash == "" || decoded.ArgsHash != StableHash(`{"api_key":"secret-token","path":"a.go"}`) {
		t.Fatalf("args hash not stable: %#v", decoded)
	}
	if decoded.Args != "" {
		t.Fatalf("trace exposed raw args: %s", decoded.Args)
	}
}

func TestRecorderSnapshotIsIsolated(t *testing.T) {
	rec := &Recorder{}
	rec.Emit(Event{Type: StepStart, RunID: "run"})
	snapshot := rec.Snapshot()
	snapshot[0].RunID = "mutated"
	if rec.Snapshot()[0].RunID != "run" {
		t.Fatalf("snapshot exposed recorder internals")
	}
}

func TestSanitizeRemovesRawPayloads(t *testing.T) {
	got := Sanitize(Event{
		Type:      ToolResult,
		RunID:     "run",
		Message:   "public-ish api_key secret-value",
		Args:      `{"token":"secret-token"}`,
		Result:    "tool secret result",
		Err:       "authorization bearer-secret failed",
		Metadata:  map[string]any{"path": "/private/workspace/report.txt", "exit_code": 7, "files": []string{"secret.txt"}},
		Artifacts: []Artifact{{Kind: "file", Path: "/private/workspace/report.txt", MIME: "text/plain"}},
	})
	if got.Args != "" || got.Result != "" {
		t.Fatalf("raw args/result exposed: %#v", got)
	}
	if got.ArgsHash != StableHash(`{"token":"secret-token"}`) {
		t.Fatalf("args hash missing: %#v", got)
	}
	if strings.Contains(got.Message, "secret-value") || got.Err != "" {
		t.Fatalf("secret text not redacted: %#v", got)
	}
	if len(got.Artifacts) != 1 || strings.Contains(got.Artifacts[0].Path, "/private/workspace") || !strings.Contains(got.Artifacts[0].Path, "report.txt#") {
		t.Fatalf("artifact path not sanitized: %#v", got.Artifacts)
	}
	meta, ok := got.Metadata.(map[string]any)
	if !ok || meta["exit_code"] != 7 || strings.Contains(fmt.Sprint(meta), "secret.txt") || strings.Contains(fmt.Sprint(meta), "/private") {
		t.Fatalf("metadata not sanitized: %#v", got.Metadata)
	}
}

func TestSanitizeRemovesProviderDeltaAndReasoning(t *testing.T) {
	for _, typ := range []Type{ProviderDelta, ProviderReasoning} {
		got := Sanitize(Event{Type: typ, RunID: "run", Message: "raw model text"})
		if got.Message != "" {
			t.Fatalf("%s message exposed: %#v", typ, got)
		}
	}
}

func TestSanitizeRemovesThreadEntryCommittedDetail(t *testing.T) {
	got := Sanitize(Event{
		Type:    ThreadEntryCommitted,
		RunID:   "run",
		Message: "raw assistant text",
		Args:    `{"token":"secret-value"}`,
		Result:  "raw tool result",
		Err:     "authorization bearer-secret failed",
		Metadata: map[string]any{
			"entry_id": "entry-1",
			"ordinal":  3,
			"detail":   map[string]any{"message": "raw assistant text"},
		},
	})
	if got.Message != "" || got.Args != "" || got.Result != "" || got.Err != "" {
		t.Fatalf("committed event exposed raw fields: %#v", got)
	}
	meta, ok := got.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
	if meta["ordinal"] != 3 {
		t.Fatalf("safe metadata missing: %#v", meta)
	}
	if _, ok := meta["detail"]; ok || strings.Contains(fmt.Sprint(meta), "raw assistant text") || strings.Contains(fmt.Sprint(meta), "entry-1") {
		t.Fatalf("detail leaked through metadata: %#v", meta)
	}
}

func TestSanitizeRawThreadEntryCommittedKeepsPayloadOutOfJSON(t *testing.T) {
	got := SanitizeWithPolicy(Event{
		Type: ThreadEntryCommitted,
		Metadata: map[string]any{
			"entry_id": "entry-1",
			"detail":   map[string]any{"message": "/private/workspace/secret.txt"},
		},
		Payload: map[string]any{"message": "/private/workspace/secret.txt"},
	}, SinkPolicy{AllowRaw: true, Redactor: SafePathRefsText})
	meta, ok := got.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
	if _, ok := meta["detail"]; ok {
		t.Fatalf("detail leaked through raw metadata: %#v", meta)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "/private/workspace") || strings.Contains(string(encoded), "secret.txt") {
		t.Fatalf("payload leaked through JSON: %s", encoded)
	}
}

func TestSanitizeKeepsSafeCapabilityMetadataReadable(t *testing.T) {
	got := Sanitize(Event{
		Type:  MCPServerFailed,
		RunID: "run",
		Metadata: map[string]any{
			"server_id":        "context7",
			"skill_id":         "code-review",
			"failure_category": "connection_failed",
			"next_action":      "Check downstream host config.",
			"path":             "/private/workspace/skill/SKILL.md",
			"message":          "token secret-value",
		},
	})
	meta, ok := got.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
	for key, want := range map[string]string{
		"server_id":        "context7",
		"skill_id":         "code-review",
		"failure_category": "connection_failed",
		"next_action":      "Check downstream host config.",
	} {
		if meta[key] != want {
			t.Fatalf("%s = %#v, want %q in %#v", key, meta[key], want, meta)
		}
	}
	if strings.Contains(fmt.Sprint(meta["path"]), "/private") || strings.Contains(fmt.Sprint(meta["message"]), "secret-value") {
		t.Fatalf("unsafe metadata exposed: %#v", meta)
	}
}

func TestSanitizeKeepsSafePressureMetadataReadable(t *testing.T) {
	got := Sanitize(Event{
		Type:  ProviderRequest,
		RunID: "run",
		Metadata: map[string]any{
			"pressure_signal":    "projected_request",
			"pressure_source":    "full_request_estimate",
			"confidence":         "conservative",
			"estimate_source":    "generic_request_json",
			"estimate_method":    "generic_payload_estimate",
			"compaction_trigger": "preflight",
			"trigger":            "provider_overflow",
			"reason":             "follow_up_pressure",
			"source":             "provider_usage",
			"prompt":             "secret raw user prompt",
		},
	})
	meta, ok := got.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
	for key, want := range map[string]string{
		"pressure_signal":    "projected_request",
		"pressure_source":    "full_request_estimate",
		"confidence":         "conservative",
		"estimate_source":    "generic_request_json",
		"estimate_method":    "generic_payload_estimate",
		"compaction_trigger": "preflight",
		"trigger":            "provider_overflow",
		"reason":             "follow_up_pressure",
		"source":             "provider_usage",
	} {
		if meta[key] != want {
			t.Fatalf("%s = %#v, want %q in %#v", key, meta[key], want, meta)
		}
	}
	if strings.Contains(fmt.Sprint(meta["prompt"]), "secret raw user prompt") {
		t.Fatalf("unsafe metadata exposed: %#v", meta)
	}
}

func TestSanitizeApprovalLifecycleEvents(t *testing.T) {
	rawArgs := `{"path":"/private/workspace/secret.txt","token":"secret-value"}`
	for _, typ := range []Type{ToolApprovalRequested, ToolApprovalApproved, ToolApprovalRejected, ToolApprovalTimedOut, ToolApprovalCanceled} {
		got := Sanitize(Event{
			Type:     typ,
			RunID:    "run",
			ToolID:   "tool-1",
			ToolName: "write",
			Args:     rawArgs,
			Result:   "approval result with token secret-value",
			Err:      "authorization bearer-secret failed",
			Metadata: map[string]any{
				"approval_id": "approval-1",
				"reason":      "token secret-value",
				"cwd":         "/private/workspace",
				"labels": map[string]string{
					"correlation.turn": "turn-1",
					"host.secret":      "token secret-value",
				},
			},
		})
		if got.Args != "" || got.Result != "" || got.Err != "" {
			t.Fatalf("%s exposed raw payloads: %#v", typ, got)
		}
		if got.ArgsHash == "" || got.ArgsHash == StableHash(rawArgs) {
			t.Fatalf("%s args hash = %q", typ, got.ArgsHash)
		}
		meta, ok := got.Metadata.(map[string]any)
		if !ok {
			t.Fatalf("%s metadata = %#v", typ, got.Metadata)
		}
		if meta["approval_id"] != "approval-1" {
			t.Fatalf("%s approval id should remain readable: %#v", typ, meta)
		}
		if meta["error_present"] != true {
			t.Fatalf("%s error signal should remain readable: %#v", typ, meta)
		}
		if strings.Contains(fmt.Sprint(meta), "/private") || strings.Contains(fmt.Sprint(meta), "secret-value") {
			t.Fatalf("%s metadata leaked path/secret: %#v", typ, meta)
		}
		labels, ok := meta["labels"].(map[string]string)
		if !ok || labels["correlation.turn"] != "turn-1" || strings.Contains(labels["host.secret"], "secret-value") {
			t.Fatalf("%s labels not sanitized/readable as expected: %#v", typ, meta["labels"])
		}
	}
}
