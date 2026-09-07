package engine_test

import (
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

func TestInvalidToolHistoryFailsBeforeProviderDispatch(t *testing.T) {
	call := session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "old_tool"}
	result := session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "old_tool", Content: "result"}
	for name, history := range map[string][]session.Message{
		"duplicate": {call, result, result}, "orphan": {result}, "missing": {call},
	} {
		t.Run(name, func(t *testing.T) {
			p := harness.NewScriptedProvider(harness.Step(harness.Text("unexpected"), harness.Done()))
			e := newTestEngine(p, &event.Recorder{})
			got := e.RunTurn(t.Context(), engine.RunInput{RunID: "run", ThreadID: "thread", History: history})
			if got.Status != engine.Failed || got.FailureOrigin != engine.FailureOriginContract || len(p.Requests) != 0 {
				t.Fatalf("result=%#v dispatched=%d", got, len(p.Requests))
			}
		})
	}
}
