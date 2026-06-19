package control

import (
	"strings"
	"testing"

	"github.com/floegence/floret/internal/provider"
)

func TestProjectValidatesControlToolArgsStrictly(t *testing.T) {
	cases := []struct {
		name string
		call provider.ToolCall
		want string
	}{
		{name: "ask missing question", call: provider.ToolCall{Name: AskUserTool, Args: `{}`}, want: "question or questions[0].question is required"},
		{name: "ask unknown field", call: provider.ToolCall{Name: AskUserTool, Args: `{"question":"Need file?","extra":true}`}, want: "$.extra is not allowed"},
		{name: "ask trailing json", call: provider.ToolCall{Name: AskUserTool, Args: `{"question":"Need file?"} {"question":"again"}`}, want: "expected exactly one JSON value"},
		{name: "complete missing output", call: provider.ToolCall{Name: TaskCompleteTool, Args: `{}`}, want: "output or result is required"},
		{name: "complete wrong type", call: provider.ToolCall{Name: TaskCompleteTool, Args: `{"output":1}`}, want: "$.output must be a string"},
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
	ask, ok, err := Project(provider.ToolCall{Name: AskUserTool, Args: `{"question":"Need file?"}`})
	if err != nil || !ok || ask.Kind != SignalAskUser || ask.Prompt != "Need file?" {
		t.Fatalf("ask signal = %#v ok=%v err=%v", ask, ok, err)
	}
	structuredAsk, ok, err := Project(provider.ToolCall{Name: AskUserTool, Args: `{"questions":[{"id":"branch","question":"Which branch?","response_mode":"write"}],"reason_code":"missing_external_input","required_from_user":["branch"],"evidence_refs":["message:latest"]}`})
	if err != nil || !ok || structuredAsk.Kind != SignalAskUser || structuredAsk.Prompt != "Which branch?" || structuredAsk.Payload["reason_code"] != "missing_external_input" {
		t.Fatalf("structured ask signal = %#v ok=%v err=%v", structuredAsk, ok, err)
	}
	done, ok, err := Project(provider.ToolCall{Name: TaskCompleteTool, Args: `{"output":"done"}`})
	if err != nil || !ok || done.Kind != SignalTaskComplete || done.Output != "done" {
		t.Fatalf("done signal = %#v ok=%v err=%v", done, ok, err)
	}
	resultDone, ok, err := Project(provider.ToolCall{Name: TaskCompleteTool, Args: `{"result":"done","evidence_refs":["https://example.test"]}`})
	if err != nil || !ok || resultDone.Kind != SignalTaskComplete || resultDone.Output != "done" || resultDone.Payload["result"] != "done" {
		t.Fatalf("result done signal = %#v ok=%v err=%v", resultDone, ok, err)
	}
}
