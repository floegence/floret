package tools_test

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestPublicPreparationError(t *testing.T) {
	cause := errors.New("owner detail")
	failure := &tools.PreparationError{Code: "SOURCE_UNAVAILABLE", Message: "Source unavailable", Cause: cause}
	if !errors.Is(failure, cause) {
		t.Fatal("cause")
	}
	tool := tools.Define[map[string]any](tools.Definition{Name: "read", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, func(tools.Invocation[map[string]any]) ([]tools.ResourceRef, error) { return nil, failure }, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		t.Fatal("handler ran")
		return tools.Result{}, nil
	})
	got := tools.NewRegistry(tool).Dispatch(context.Background(), tools.ToolCall{ID: "call", Name: "read", Args: "{}"}, tools.DispatchOptions{EffectDispatcher: func(context.Context, tools.EffectDispatchRequest, func(context.Context) tools.Result) tools.Result {
		t.Fatal("dispatcher ran")
		return tools.Result{}
	}})
	if !got.IsError || got.Structured["execution"].(map[string]any)["started"] != false || got.Activity.Payload.(tools.StructuredActivityPayload).Error.Code != "SOURCE_UNAVAILABLE" {
		t.Fatalf("%#v", got)
	}
}
