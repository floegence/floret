package sessiontree

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestBuildContextProjectsInteractionAnswerOnceAsCanonicalToolResult(t *testing.T) {
	payload := json.RawMessage(`{"accepted":true,"input":{"choice":"yes"},"at":"0001-01-01T00:00:00Z"}`)
	path := []Entry{
		{ID: "user-1", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "begin"}},
		{ID: "tool-call", ParentID: "user-1", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "interaction", ToolName: "ask_user", ToolArgs: `{"questions":[{"id":"choice","header":"Choice","question":"Continue?","response_mode":"write","is_secret":false}]}`}},
		{ID: "interaction-resolved:interaction", ParentID: "tool-call", Type: EntryInteractionDone, Payload: payload},
		{ID: "assistant", ParentID: "interaction-resolved:interaction", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "continued"}},
		{ID: "user-2", ParentID: "assistant", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "next"}},
	}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"interaction_response","answers":{"choice":"yes"}}`
	count := 0
	for _, message := range messages {
		if message.Content == want {
			count++
			if message.Role != session.Tool || message.EntryID != "interaction-resolved:interaction" || message.ToolCallID != "interaction" || message.ToolName != "ask_user" || message.ToolResult == nil || message.ToolResult.Status != "success" {
				t.Fatalf("interaction response message=%#v", message)
			}
		}
	}
	if count != 1 {
		t.Fatalf("interaction answer appeared %d times in %#v", count, messages)
	}
}

func TestBuildContextProjectsOnlyRedactedMarkerForSecretAnswer(t *testing.T) {
	path := []Entry{
		{ID: "tool-call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "interaction", ToolName: "ask_user", ToolArgs: `{"questions":[{"id":"secret","header":"Secret","question":"Token?","response_mode":"write","is_secret":true}]}`}},
		{ID: "interaction-resolved:interaction", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":true,"redacted":true,"at":"0001-01-01T00:00:00Z"}`)},
	}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[1].Role != session.Tool || messages[1].ToolCallID != "interaction" || messages[1].Content != `{"type":"interaction_response","secret_answers_redacted":true}` {
		t.Fatalf("secret projection=%#v", messages)
	}
	if strings.Contains(messages[1].Content, "secret-value") {
		t.Fatalf("secret leaked into canonical message: %q", messages[1].Content)
	}
}

func TestBuildContextPreservesRemovedToolPairWithoutDefinition(t *testing.T) {
	path := []Entry{
		{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "retired-1", ToolName: "retired_tool", ToolArgs: `{"value":"old"}`}},
		{ID: "result", ParentID: "call", Type: EntryToolResult, Message: session.Message{Role: session.Tool, Content: "old result", ToolCallID: "retired-1", ToolName: "retired_tool", ToolResult: &session.ToolResultView{Status: "success"}}},
	}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].ToolName != "retired_tool" || messages[1].ToolName != "retired_tool" || messages[1].Role != session.Tool {
		t.Fatalf("removed tool pair=%#v", messages)
	}
}

func TestBuildContextPairsRetiredTerminalControlWithoutDefinition(t *testing.T) {
	path := []Entry{{
		ID: "terminal-control", Type: EntryToolCall,
		Message: session.Message{
			Role: session.Assistant, Content: "tool_call", Kind: session.MessageKindControlSignal,
			ToolCallID: "legacy-complete", ToolName: "retired_complete", ToolArgs: `{"summary":"done"}`,
			ControlSignal: &session.ControlSignalView{Name: "retired_complete", CallID: "legacy-complete", Disposition: "terminal", OutputText: "done"},
		},
	}}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Role != session.Assistant || messages[1].Role != session.Tool || messages[1].ToolCallID != "legacy-complete" || messages[1].ToolName != "retired_complete" || messages[1].ToolResult == nil || messages[1].ToolResult.Status != "success" {
		t.Fatalf("retired terminal control pair=%#v", messages)
	}
}

func TestBuildContextPairsFailedControlWithoutLeakingValidationDetails(t *testing.T) {
	for _, disposition := range []string{"failed", "waiting"} {
		t.Run(disposition, func(t *testing.T) {
			path := []Entry{{
				ID: "failed-control", Type: EntryToolCall,
				Message: session.Message{
					Role: session.Assistant, Content: "tool_call", Kind: session.MessageKindControlSignal,
					ToolCallID: "invalid-ask", ToolName: "ask_user", ToolArgs: `{"secret":"must-not-leak"}`,
					ControlSignal: &session.ControlSignalView{Name: "ask_user", CallID: "invalid-ask", Disposition: disposition, ErrorCode: session.ControlSignalErrorCodeControlError},
				},
			}}
			messages, err := BuildContextChecked(path, ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(messages) != 2 || messages[1].Role != session.Tool || messages[1].ToolCallID != "invalid-ask" || messages[1].ToolName != "ask_user" || messages[1].ToolResult == nil || messages[1].ToolResult.Status != "error" {
				t.Fatalf("failed control pair=%#v", messages)
			}
			if messages[1].Content != `{"type":"control_result","outcome":"failed"}` || strings.Contains(messages[1].Content, "must-not-leak") {
				t.Fatalf("failed control result=%q", messages[1].Content)
			}
		})
	}
}

func TestRuntimePendingInteractionsIgnoreOnlyMatchingFailedControl(t *testing.T) {
	failed := Entry{
		ID: "failed-control", TurnID: "turn", RunID: "run", Type: EntryToolCall,
		Message: session.Message{Kind: session.MessageKindControlSignal, ControlSignal: &session.ControlSignalView{
			Name: "ask_user", CallID: "invalid-ask", Disposition: "waiting", ErrorCode: session.ControlSignalErrorCodeControlError,
		}},
	}
	matching := Entry{ID: "interaction-requested:invalid-ask", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked}
	if got := runtimePendingInteractions([]Entry{failed, matching}, "turn"); len(got) != 0 {
		t.Fatalf("matching failed interaction remained pending: %#v", got)
	}
	unrelated := Entry{ID: "interaction-requested:other", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked}
	if got := runtimePendingInteractions([]Entry{failed, unrelated}, "turn"); len(got) != 1 || got[0].ID != "other" {
		t.Fatalf("unrelated interaction=%#v, want one pending interaction", got)
	}
}

func TestBuildContextRejectsInteractionAnswerWithoutAskUserCall(t *testing.T) {
	_, err := BuildContextChecked([]Entry{{
		ID: "interaction-resolved:missing", Type: EntryInteractionDone,
		Payload: json.RawMessage(`{"accepted":true,"input":{"choice":"yes"},"at":"0001-01-01T00:00:00Z"}`),
	}}, ContextOptions{})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("missing ask_user pair error=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestBuildContextRejectsMalformedInteractionResolution(t *testing.T) {
	_, err := BuildContextChecked([]Entry{{
		ID: "interaction", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":"yes"}`),
	}}, ContextOptions{})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("malformed resolution error=%v, want ErrAuthorityCorrupt", err)
	}
}
