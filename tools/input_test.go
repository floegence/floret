package tools

import (
	"context"
	"testing"
)

func TestToolInputRequestValidationAndClone(t *testing.T) {
	valid := func() *InputRequest {
		return &InputRequest{Summary: "Complete the external step", Questions: []InputQuestion{{ID: "ready", Prompt: "Return control when ready", Kind: "select", Options: []string{"Continue", "Stop"}}}}
	}
	for _, mode := range []string{"valid", "empty", "duplicate_question", "duplicate_option", "bad_kind", "bad_options", "pending", "error", "canceled", "declined"} {
		t.Run(mode, func(t *testing.T) {
			request := valid()
			result := Result{Text: "Observed", InputRequired: request}
			switch mode {
			case "empty":
				request.Summary = ""
			case "duplicate_question":
				request.Questions = append(request.Questions, request.Questions[0])
			case "duplicate_option":
				request.Questions[0].Options = []string{"Continue", "Continue"}
			case "bad_kind":
				request.Questions[0].Kind = "secret"
			case "bad_options":
				request.Questions[0].Kind = "write"
			case "pending":
				result.Pending = &PendingToolResult{}
			case "error":
				result.IsError = true
			case "canceled":
				result.Structured = map[string]any{"outcome": ResultOutcomeCanceled}
			case "declined":
				result.Structured = map[string]any{"outcome": ResultOutcomeDeclined}
			}
			reg := NewRegistry(testTool("inspect", true, func(context.Context, Invocation[testArgs]) (Result, error) { return result, nil }))
			got := reg.Run(t.Context(), ToolCall{ID: "call", Name: "inspect", Args: `{"value":"x"}`}, nil)
			if mode != "valid" {
				if got.DispatchErr == nil || !got.IsError || got.InputRequired != nil {
					t.Fatalf("invalid host contract did not fail closed: %+v", got)
				}
				return
			}
			if got.IsError || got.DispatchErr != nil || got.InputRequired == nil {
				t.Fatalf("valid input rejected: %+v", got)
			}
			request.Questions[0].Options[0] = "changed"
			request.Questions[0].Prompt = "changed"
			if got.InputRequired.Questions[0].Options[0] != "Continue" || got.InputRequired.Questions[0].Prompt != "Return control when ready" {
				t.Fatal("returned input aliases host-owned slices")
			}
		})
	}
}
