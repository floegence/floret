package agentharness

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestGoalArgumentsRejectBeforeEffectAuthority(t *testing.T) {
	for _, tc := range []struct{ args, field string }{
		{`{"operation":"set","expected_version":0}`, "text"},
		{`{"operation":"set","expected_version":0,"text":" \t"}`, "text"},
		{`{"operation":"delete","expected_version":0}`, "reason"},
		{`{"operation":"review","expected_version":0,"evidence":"Checked"}`, "outcome"},
		{`{"operation":"review","expected_version":0,"outcome":"completed"}`, "evidence"},
	} {
		t.Run(tc.args, func(t *testing.T) {
			tool := goalTool(nil)
			tool.Definition.Activity = func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
				t.Error("invalid goal reached presentation")
				return nil, nil
			}
			registry := tools.NewRegistry(tool)
			result := registry.Dispatch(t.Context(), tools.ToolCall{ID: "invalid", Name: "goal", Args: tc.args}, tools.DispatchOptions{EffectDispatcher: func(context.Context, tools.EffectDispatchRequest, func(context.Context) tools.Result) tools.Result {
				t.Error("invalid goal reached effect authority")
				return tools.Result{}
			}})
			if !result.IsError || !strings.Contains(result.Text, tc.field) || result.RequiresEffectFinalization() {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestGoalDecoderPreservesAcceptedInputs(t *testing.T) {
	for _, raw := range []string{
		`{"operation":"set","expected_version":0,"text":"  Preserve whitespace  ","new_task":true}`,
		`{"operation":"delete","expected_version":3,"reason":"  Cancelled by user  "}`,
		`{"operation":"review","expected_version":9,"outcome":"completed","evidence":" Verified "}`,
		`{"operation":"review","expected_version":9,"outcome":"continue","evidence":" More work "}`,
		`{"operation":"review","expected_version":9,"outcome":"blocked","evidence":" Missing user input "}`,
	} {
		var want goalToolInput
		if err := json.Unmarshal([]byte(raw), &want); err != nil {
			t.Fatal(err)
		}
		got, err := decodeGoalToolInput([]byte(raw))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded=%+v expected=%+v err=%v", got, want, err)
		}
	}
}
