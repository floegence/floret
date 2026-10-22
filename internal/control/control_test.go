package control

import (
	"reflect"
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

func TestAskUserSchemaOffersOnlyWriteAndSelectOrWrite(t *testing.T) {
	definitions := ToolDefinitions()
	properties := definitions[0].InputSchema["properties"].(map[string]any)
	questions := properties["questions"].(map[string]any)
	question := questions["items"].(map[string]any)
	questionProperties := question["properties"].(map[string]any)
	responseMode := questionProperties["response_mode"].(map[string]any)
	if got, want := responseMode["enum"], []string{"write", "select_or_write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("response_mode enum = %#v, want %#v", got, want)
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
	for _, mode := range []string{"write", "select_or_write"} {
		t.Run(mode, func(t *testing.T) {
			call := provider.ToolCall{Name: AskUserTool, Args: `{"questions":[{"id":"branch","header":"Branch","question":"Which branch?","is_secret":false,"response_mode":"` + mode + `"}],"reason_code":"missing_external_input","required_from_user":["branch"],"evidence_refs":["message:latest"]}`}
			structuredAsk, ok, err := Project(call)
			if err != nil || !ok || structuredAsk.Kind != SignalAskUser || structuredAsk.Prompt != "Which branch?" || structuredAsk.Payload["reason_code"] != "missing_external_input" {
				t.Fatalf("structured ask signal = %#v ok=%v err=%v", structuredAsk, ok, err)
			}
		})
	}
}

func TestProjectNormalizesLegacyAskUserSelectToWriteableChoices(t *testing.T) {
	call := provider.ToolCall{Name: AskUserTool, Args: `{"questions":[{"id":"branch","header":"Branch","question":"Which branch?","is_secret":false,"response_mode":"select","choices_exhaustive":true,"choices":[{"choice_id":"safe","label":"Staged rollout","kind":"select"}]}],"reason_code":"missing_external_input","required_from_user":["branch"],"evidence_refs":[]}`}
	signal, ok, err := Project(call)
	if err != nil || !ok {
		t.Fatalf("Project legacy select = %#v, %v, %v", signal, ok, err)
	}
	questions := signal.Payload["questions"].([]any)
	question := questions[0].(map[string]any)
	if question["response_mode"] != "select_or_write" || question["choices_exhaustive"] != false {
		t.Fatalf("legacy question was not made writeable: %#v", question)
	}
}

func TestNormalizeAskUserPayloadReturnsDetachedLegacyProjection(t *testing.T) {
	question := map[string]any{"response_mode": "select", "choices_exhaustive": true}
	payload := map[string]any{"questions": []any{question}}
	normalized, changed := NormalizeAskUserPayload(payload)
	if !changed || question["response_mode"] != "select" || question["choices_exhaustive"] != true {
		t.Fatalf("normalization changed source payload: changed=%v source=%#v", changed, question)
	}
	normalizedQuestion := normalized["questions"].([]any)[0].(map[string]any)
	if normalizedQuestion["response_mode"] != "select_or_write" || normalizedQuestion["choices_exhaustive"] != false {
		t.Fatalf("normalized payload = %#v", normalizedQuestion)
	}
}
