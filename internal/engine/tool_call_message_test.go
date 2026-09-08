package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestToolCallEventCarriesDetachedCanonicalMessage(t *testing.T) {
	for _, callReasoning := range []string{"", "call-specific reasoning"} {
		t.Run(callReasoning, func(t *testing.T) {
			recorder := &event.Recorder{}
			p := harness.NewScriptedProvider(
				harness.Step(
					provider.StreamEvent{Type: provider.Reasoning, Text: "step reasoning"},
					provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"page"}`, Reasoning: callReasoning}}},
					harness.DoneReason("tool_calls"),
				),
				harness.Step(harness.Text("done"), harness.Done()),
			)
			registry := tools.NewRegistry()
			mustRegister(t, registry, stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { return "page", nil }))
			e := newTestEngine(p, recorder)
			e.Tools = registry
			if result := e.Run(t.Context(), "read"); result.Status != engine.Completed {
				t.Fatalf("run: %+v", result)
			}
			messages, err := e.Store.Transcript("run")
			if err != nil {
				t.Fatal(err)
			}
			var canonical session.Message
			for _, message := range messages {
				if message.Role == session.Assistant && message.ToolCallID == "read-1" {
					canonical = message
				}
			}
			wantReasoning := callReasoning
			if wantReasoning == "" {
				wantReasoning = "step reasoning"
			}
			calls := 0
			for _, ev := range recorder.Snapshot() {
				if ev.Type != event.ToolCall {
					continue
				}
				calls++
				if ev.ToolCallMessage == nil || !reflect.DeepEqual(*ev.ToolCallMessage, canonical) || ev.ToolCallMessage.Reasoning != wantReasoning {
					t.Fatalf("event message=%+v canonical=%+v", ev.ToolCallMessage, canonical)
				}
				ev.ToolCallMessage.Reasoning = "observer mutation"
			}
			if calls != 1 {
				t.Fatalf("call events=%d", calls)
			}
			messages, err = e.Store.Transcript("run")
			if err != nil {
				t.Fatal(err)
			}
			for _, message := range messages {
				if message.Role == session.Assistant && message.ToolCallID == "read-1" && message.Reasoning != wantReasoning {
					t.Fatal("event mutation reached the transcript")
				}
			}
		})
	}
}
