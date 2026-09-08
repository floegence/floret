package agentharness

import (
	"testing"

	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestCanonicalToolCallMessageRejectsMissingOrConflictingPayload(t *testing.T) {
	valid := session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "call", ToolName: "read", ToolArgs: `{}`, Reasoning: "complete reasoning"}
	for _, field := range []string{"missing", "role", "content", "kind", "call_id", "name", "args"} {
		t.Run(field, func(t *testing.T) {
			msg := session.CloneMessage(valid)
			ev := event.Event{Type: event.ToolCall, ToolID: "call", ToolName: "read", Args: `{}`, ToolCallMessage: &msg}
			switch field {
			case "missing":
				ev.ToolCallMessage = nil
			case "role":
				msg.Role = session.Tool
			case "content":
				msg.Content = "different"
			case "kind":
				msg.Kind = session.MessageKindControlSignal
			case "call_id":
				msg.ToolCallID = "other"
			case "name":
				msg.ToolName = "other"
			case "args":
				msg.ToolArgs = `{"other":true}`
			}
			projection := &turnProjection{}
			projection.Emit(ev)
			if projection.err == nil || turnProjectionFailureOrigin(projection.err) != "contract" || len(projection.pendingCalls) != 0 {
				t.Fatalf("error=%v pending=%v", projection.err, projection.pendingCalls)
			}
		})
	}
}

func TestCanonicalToolCallMessageDetachesNestedData(t *testing.T) {
	msg := session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "call", ToolName: "read", ToolArgs: `{}`, Reasoning: "canonical", Activity: &session.ActivityPresentation{TargetRefs: []tools.ActivityTargetRef{{Label: "original"}}}}
	projection := &turnProjection{reasoning: "unrelated streamed fragment"}
	projection.Emit(event.Event{Type: event.ToolCall, ToolID: "call", ToolName: "read", Args: `{}`, ToolCallMessage: &msg})
	if projection.err != nil || len(projection.pendingCalls) != 1 {
		t.Fatalf("error=%v", projection.err)
	}
	msg.Reasoning = "mutated"
	msg.Activity.TargetRefs[0].Label = "mutated"
	got := projection.pendingCalls[0].message
	if got.Reasoning != "canonical" || got.Activity.TargetRefs[0].Label != "original" {
		t.Fatalf("projected message changed: %#v", got)
	}
}
