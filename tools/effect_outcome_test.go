package tools

import (
	"context"
	"testing"
)

func TestReturnedEffectOutcomeValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result Result
		valid  bool
	}{
		{"returned_uncertainty", Result{IsError: true, EffectOutcome: EffectOutcomeUncertain}, true},
		{"ordinary", Result{Text: "ok"}, true},
		{"unknown_value", Result{IsError: true, EffectOutcome: "other"}, false},
		{"success", Result{EffectOutcome: EffectOutcomeUncertain}, false},
		{"input", Result{IsError: true, EffectOutcome: EffectOutcomeUncertain, InputRequired: &InputRequest{Summary: "input"}}, false},
		{"pending", Result{IsError: true, EffectOutcome: EffectOutcomeUncertain, Pending: &PendingToolResult{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry(testTool("inspect", true, func(context.Context, Invocation[testArgs]) (Result, error) { return tc.result, nil }))
			got := reg.Run(t.Context(), ToolCall{ID: "call", Name: "inspect", Args: "{\"value\":\"x\"}"}, nil)
			if (got.DispatchErr == nil) != tc.valid {
				t.Fatalf("result=%+v", got)
			}
		})
	}
}
