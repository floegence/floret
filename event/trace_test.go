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
		SessionID: "session",
		Step:      1,
		Args:      `{"api_key":"secret-token","path":"a.go"}`,
		Timestamp: time.Unix(1, 0),
	})
	var decoded Event
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &decoded); err != nil {
		t.Fatalf("trace is not JSONL: %v\n%s", err, buf.String())
	}
	if decoded.TraceID != "trace" || decoded.SessionID != "session" || decoded.Step != 1 {
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
