package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestPreparationErrorPreservesFactsWithoutDispatch(t *testing.T) {
	cause := errors.New("private transport detail")
	failure := &PreparationError{Code: "TARGET_UNAVAILABLE", Message: "The selected target is unavailable.", Cause: cause}
	if !errors.Is(failure, cause) {
		t.Fatal("cause lost")
	}
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			invoked, dispatched := false, false
			tool := Define[map[string]any](Definition{Name: "inspect", InputSchema: StrictObject(nil, nil), Permission: PermissionSpec{Mode: PermissionAllow}, Activity: func(Invocation[any]) (*ActivityPresentation, error) {
				return &ActivityPresentation{Label: "Inspect chosen target", Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Operation: "inspect", Inputs: []StructuredActivityRow{{Content: "explicit input", Format: StructuredActivityRowFormatText}}}}, nil
			}}, nil, func(Invocation[map[string]any]) ([]ResourceRef, error) {
				return nil, fmt.Errorf("wrapper: %w", failure)
			}, func(context.Context, Invocation[map[string]any]) (Result, error) {
				invoked = true
				return Result{}, nil
			})
			reg := NewRegistry(tool)
			opts := DispatchOptions{EffectDispatcher: func(ctx context.Context, req EffectDispatchRequest, run func(context.Context) Result) Result {
				dispatched = true
				return run(ctx)
			}}
			call := ToolCall{ID: "call-1", Name: "inspect", Args: "{}"}
			var got Result
			if batch {
				got = reg.DispatchBatch(context.Background(), []ToolCall{call}, opts)[0]
			} else {
				got = reg.Dispatch(context.Background(), call, opts)
			}
			if invoked || dispatched || !got.IsError || got.RequiresEffectFinalization() || got.DispatchErr != nil {
				t.Fatalf("invalid boundary: %#v", got)
			}
			if got.CallID != call.ID || got.Name != call.Name {
				t.Fatalf("identity/content: %#v", got)
			}
			if got.Structured["execution"].(map[string]any)["started"] != false || got.Structured["error"].(map[string]any)["code"] != failure.Code {
				t.Fatalf("facts: %#v", got.Structured)
			}
			var model map[string]any
			if err := json.Unmarshal([]byte(got.Text), &model); err != nil || !reflect.DeepEqual(model, got.Structured) {
				t.Fatalf("model lost preparation facts: %s (%v)", got.Text, err)
			}
			payload := got.Activity.Payload.(StructuredActivityPayload)
			if payload.Error.Code != failure.Code || payload.Operation != "inspect" || len(payload.Inputs) != 1 {
				t.Fatalf("activity: %#v", payload)
			}
			if err := got.Activity.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparationErrorRejectsMalformedFactsAndPreservesUntypedContract(t *testing.T) {
	call := ToolCall{ID: "c", Name: "inspect"}
	for _, err := range []error{&PreparationError{Code: "invalid\ncode", Message: "detail"}, &PreparationError{Code: "VALID", Message: ""}} {
		result := resourcePreparationFailure(call, Definition{}, erasedInvocation{}, err)
		if !result.IsError || result.Structured != nil || result.Text != "tool preparation returned invalid error facts" {
			t.Fatalf("malformed: %#v", result)
		}
	}
	result := resourcePreparationFailure(call, Definition{}, erasedInvocation{}, errors.New("ordinary"))
	if result.Text != "tool \"inspect\" resource extraction failed: ordinary" || result.Structured != nil {
		t.Fatalf("legacy contract: %#v", result)
	}
}
