package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestDecoderValidationReturnsFeedbackBeforeAuthority(t *testing.T) {
	for _, activity := range []bool{false, true} {
		t.Run(fmt.Sprint(activity), func(t *testing.T) {
			p := harness.NewScriptedProvider(
				harness.Step(harness.Tool("invalid", "write", `{}`), harness.DoneReason("tool_calls")),
				harness.Step(harness.Tool("valid", "write", `{"value":"kept"}`), harness.DoneReason("tool_calls")),
				harness.Step(harness.Text("done"), harness.Done()),
			)
			rec := &event.Recorder{}
			e := newTestEngine(p, rec)
			resources, permissions, executions := 0, 0, 0
			definition := tools.Definition{Name: "write", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("Required nonempty value.")}, []string{}), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, PermissionFor: func(tools.PermissionRequest) (tools.PermissionSpec, error) {
				permissions++
				return tools.PermissionSpec{Mode: tools.PermissionAllow}, nil
			}}
			if activity {
				definition.Activity = func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
					return &tools.ActivityPresentation{Label: "Write", Renderer: tools.ActivityRendererStructured}, nil
				}
			}
			e.Tools = tools.NewRegistry(tools.Define[stringArgs](definition, func(raw []byte) (stringArgs, error) {
				var args stringArgs
				if err := json.Unmarshal(raw, &args); err != nil {
					return args, err
				}
				if strings.TrimSpace(args.Value) == "" {
					return args, errors.New("value is required")
				}
				return args, nil
			}, func(tools.Invocation[stringArgs]) ([]tools.ResourceRef, error) { resources++; return nil, nil }, func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
				executions++
				return tools.Result{Text: "written"}, nil
			}))
			got := e.Run(t.Context(), "write")
			if got.Status != engine.Completed || len(p.Requests) != 3 || resources != 1 || permissions != 1 || executions != 1 {
				t.Fatalf("result=%+v requests=%d resources=%d permissions=%d executions=%d", got, len(p.Requests), resources, permissions, executions)
			}
			pairs := 0
			for _, message := range p.Requests[1].Messages {
				if message.ToolCallID != "invalid" {
					continue
				}
				if message.Kind != session.MessageKindToolValidationError {
					t.Fatalf("missing validation kind: %+v", message)
				}
				if message.Role == session.Tool && (!strings.Contains(message.Content, "not executed") || !strings.Contains(message.Content, "value is required")) {
					t.Fatalf("feedback=%+v", message)
				}
				pairs++
			}
			if pairs != 2 {
				t.Fatalf("validation pairs=%d", pairs)
			}
			for _, ev := range rec.Events {
				if ev.ToolID == "invalid" && (ev.Type == event.ToolDispatchStarted || ev.Activity != nil) {
					t.Fatalf("invalid call acquired execution or public activity: %+v", ev)
				}
			}
		})
	}
}

func TestActivityProjectorSchemaErrorRemainsContractFailure(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Tool("call", "read", `{}`), harness.DoneReason("tool_calls")))
	e := newTestEngine(p, &event.Recorder{})
	cause := fmt.Errorf("projector failed: %w", tools.ErrSchema)
	e.Tools = tools.NewRegistry(tools.Define[map[string]any](tools.Definition{Name: "read", InputSchema: tools.StrictObject(nil, nil), ReadOnly: true, Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) { return nil, cause }}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		t.Fatal("handler ran")
		return tools.Result{}, nil
	}))
	got := e.Run(t.Context(), "read")
	if got.Status != engine.Failed || got.FailureOrigin != engine.FailureOriginContract || !errors.Is(got.Err, cause) || len(p.Requests) != 1 {
		t.Fatalf("result=%+v requests=%d", got, len(p.Requests))
	}
}

func TestDecoderValidationUsesExistingCorrectionLimit(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 4; i++ {
		steps = append(steps, harness.Step(harness.Tool(fmt.Sprintf("invalid-%d", i), "write", `{}`), harness.DoneReason("tool_calls")))
	}
	p := harness.NewScriptedProvider(steps...)
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = tools.NewRegistry(tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, func([]byte) (map[string]any, error) { return nil, errors.New("missing operation input") }, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		t.Fatal("invalid handler ran")
		return tools.Result{}, nil
	}))
	got := e.Run(t.Context(), "write")
	if got.Status != engine.Failed || got.FailureOrigin == engine.FailureOriginContract || !strings.Contains(got.Err.Error(), "tool argument correction exhausted") || len(p.Requests) != 3 {
		t.Fatalf("result=%+v requests=%d", got, len(p.Requests))
	}
}
