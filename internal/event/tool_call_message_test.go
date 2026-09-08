package event

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestToolCallMessageNeverCrossesObservationBoundary(t *testing.T) {
	ev := Event{Type: ToolCall, ToolID: "call", ToolCallMessage: &session.Message{Reasoning: "private-canonical-reasoning"}}
	for _, policy := range []SinkPolicy{{}, {AllowRaw: true}} {
		if got := SanitizeWithPolicy(ev, policy); got.ToolCallMessage != nil {
			t.Fatal("internal call leaked through sanitizer")
		}
		recorder := &Recorder{}
		NewSerialSink(recorder, policy).Emit(ev)
		if recorder.Snapshot()[0].ToolCallMessage != nil {
			t.Fatal("internal call leaked through serial sink")
		}
	}
	if SanitizePathRefs(ev).ToolCallMessage != nil {
		t.Fatal("internal call leaked through path sanitizer")
	}
	encoded, err := json.Marshal(ev)
	if err != nil || strings.Contains(string(encoded), "canonical") || strings.Contains(string(encoded), "ToolCallMessage") {
		t.Fatalf("event JSON=%s err=%v", encoded, err)
	}
	if ev.ToolCallMessage == nil {
		t.Fatal("sanitization mutated internal event")
	}
}
