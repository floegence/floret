package control

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
)

func TestAskUserGuidanceDistinguishesExecutionFromDiscussion(t *testing.T) {
	definitions := ToolDefinitions()
	if len(definitions) != 1 || definitions[0].Name != AskUserTool {
		t.Fatalf("control definitions=%+v", definitions)
	}
	for _, boundary := range []string{"execution needs user input", "guided questions are requested", "Ordinary discussion and optional feedback may use prose"} {
		if !strings.Contains(definitions[0].Description, boundary) {
			t.Fatalf("question guidance missing %q: %s", boundary, definitions[0].Description)
		}
	}
}

func TestProjectValidatesControlToolArgsStrictly(t *testing.T) {
	cases := []struct {
		name string
		call provider.ToolCall
		want string
	}{
		{name: "ask missing questions", call: provider.ToolCall{Name: AskUserTool, Args: `{}`}, want: "is required"},
		{name: "ask unknown field", call: provider.ToolCall{Name: AskUserTool, Args: `{"questions":[],"reason_code":"missing_external_input","required_from_user":[],"evidence_refs":[],"extra":true}`}, want: "$.extra is not allowed"},
		{name: "ask trailing json", call: provider.ToolCall{Name: AskUserTool, Args: `{"questions":[]} {"questions":[]}`}, want: "expected exactly one JSON value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, err := Project(tc.call)
			if !ok || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ok=%v err=%v, want %q", ok, err, tc.want)
			}
		})
	}
}

func TestProjectReturnsSignalsForValidControlToolArgs(t *testing.T) {
	ask, ok, err := Project(provider.ToolCall{Name: AskUserTool, Args: `{"questions":[{"id":"file","header":"File","question":"Need file?","is_secret":false,"response_mode":"write"}],"reason_code":"missing_external_input","required_from_user":["file"],"evidence_refs":[]}`})
	if err != nil || !ok || ask.Kind != SignalAskUser || ask.Prompt != "Need file?" {
		t.Fatalf("ask signal = %#v ok=%v err=%v", ask, ok, err)
	}
	structuredAsk, ok, err := Project(provider.ToolCall{Name: AskUserTool, Args: `{"questions":[{"id":"branch","header":"Branch","question":"Which branch?","is_secret":false,"response_mode":"write"}],"reason_code":"missing_external_input","required_from_user":["branch"],"evidence_refs":["message:latest"]}`})
	if err != nil || !ok || structuredAsk.Kind != SignalAskUser || structuredAsk.Prompt != "Which branch?" || structuredAsk.Payload["reason_code"] != "missing_external_input" {
		t.Fatalf("structured ask signal = %#v ok=%v err=%v", structuredAsk, ok, err)
	}
}
