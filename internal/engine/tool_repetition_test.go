package engine

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestStrictToolIdentityPreservesAllValues(t *testing.T) {
	for _, tc := range []struct {
		a, b  string
		equal bool
	}{
		{"{\"a\":1,\"description\":\"same\"}", "{ \"description\": \"same\", \"a\": 1 }", true},
		{"{\"a\":9007199254740992}", "{\"a\":9007199254740993}", false},
		{"{\"description\":\"same\"}", "{\"description\":\"same \"}", false},
		{"{\"a\":[1,2]}", "{\"a\":[2,1]}", false},
		{"{\"a\":1}", "{\"a\":\"1\"}", false},
		{"{}", "{\"a\":null}", false},
	} {
		if got := strictToolSignature(provider.ToolCall{Name: "read", Args: tc.a}) == strictToolSignature(provider.ToolCall{Name: "read", Args: tc.b}); got != tc.equal {
			t.Errorf("%s vs %s: equality=%v", tc.a, tc.b, got)
		}
	}
}
func TestRepetitionDetailsRequireCanonicalEnvelope(t *testing.T) {
	detail := tools.RepetitionDetails{ToolName: "inspect", CallID: "call", ConsecutiveCalls: 5, RepeatLimit: 3, FeedbackGiven: true, NotExecuted: true}
	raw, _ := json.Marshal(detail)
	message := ErrDuplicateTools.Error() + ": " + string(raw)
	if got := ParseToolRepetitionDetails(message); got == nil || *got != detail {
		t.Fatalf("details=%+v", got)
	}
	for _, invalid := range []string{"upstream: " + message, message + " trailing", ErrDuplicateTools.Error(), ErrDuplicateTools.Error() + ": {}"} {
		if ParseToolRepetitionDetails(invalid) != nil {
			t.Fatal("accepted noncanonical diagnostic")
		}
	}
}
func TestToolOrderCountsStartsOnceAndKeepsHostedBoundary(t *testing.T) {
	stream := make(chan provider.StreamEvent, 12)
	stream <- provider.StreamEvent{Type: provider.ToolCallStart, ToolCallStream: provider.ToolCallStream{ID: "a", Name: "inspect"}}
	stream <- provider.StreamEvent{Type: provider.ToolCallDelta, ToolCallStream: provider.ToolCallStream{ID: "a", Name: "inspect"}, Text: "{}"}
	stream <- provider.StreamEvent{Type: provider.ToolCallEnd, ToolCallStream: provider.ToolCallStream{ID: "a", Name: "inspect"}}
	stream <- provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: "search", Name: "web_search", Args: "{}"}}
	stream <- provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: provider.ToolCall{ID: "search", Name: "web_search", Args: "{}"}}
	stream <- provider.StreamEvent{Type: provider.ToolCallStart, ToolCallStream: provider.ToolCallStream{ID: "b", Name: "inspect"}}
	stream <- provider.StreamEvent{Type: provider.ToolCallEnd, ToolCallStream: provider.ToolCallStream{ID: "b", Name: "inspect"}}
	stream <- provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "a", Name: "inspect", Args: "{}"}, {ID: "b", Name: "inspect", Args: "{}"}}}
	stream <- provider.StreamEvent{Type: provider.Done, Reason: "tool_calls"}
	close(stream)
	out, err := (&Engine{}).consume(t.Context(), Options{}, 1, stream, provider.Request{HostedTools: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []toolInvocationOrder{{id: "a"}, {id: "search", hosted: true}, {id: "b"}}
	if len(out.toolOrder) != len(want) {
		t.Fatalf("order=%+v", out.toolOrder)
	}
	for i := range want {
		if out.toolOrder[i] != want[i] {
			t.Fatalf("order=%+v", out.toolOrder)
		}
	}
	guard := toolRepeatGuard{}
	rejected, _ := guard.prepare(nil, out.Calls, out.toolOrder, 1)
	if len(rejected) != 0 {
		t.Fatalf("hosted boundary ignored: %+v", rejected)
	}
}
