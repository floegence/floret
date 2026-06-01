package event

import (
	"bytes"
	"encoding/json"
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
	if strings.Contains(decoded.Args, "secret-token") {
		t.Fatalf("secret was not redacted: %s", decoded.Args)
	}
	if !strings.Contains(decoded.Args, "[REDACTED]") {
		t.Fatalf("redaction marker missing: %s", decoded.Args)
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
