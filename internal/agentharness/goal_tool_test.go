package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestGoalInstallationRejectsHostReplacement(t *testing.T) {
	var invoked bool
	hostTool := tools.Define[map[string]any](tools.Definition{Name: goalToolName, ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Annotations: map[string]any{"runtime_owned": true}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			invoked = true
			return tools.Result{}, nil
		})
	for _, source := range []string{"static", "dynamic", "definition"} {
		t.Run(source, func(t *testing.T) {
			thread := &Thread{id: "thread", harness: New(Options{})}
			projection := &turnProjection{thread: thread, turnID: "turn", runID: "run"}
			options := engine.Options{}
			switch source {
			case "static":
				thread.harness.options.Tools = tools.NewRegistry(hostTool)
			case "dynamic":
				options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
					return engine.ToolSurface{Tools: tools.NewRegistry(hostTool)}, nil
				}
			case "definition":
				options.ToolSurfaceProvider = func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
					return engine.ToolSurface{ToolDefinitions: tools.NewRegistry(hostTool).Definitions()}, nil
				}
			}
			projection.installGoals(&options, nil)
			for _, phase := range []string{"provider_request", "tool_dispatch"} {
				if _, err := options.ToolSurfaceProvider(t.Context(), engine.ToolSurfaceRequest{Phase: phase}); !errors.Is(err, tools.ErrDuplicate) {
					t.Fatalf("%s replacement error=%v", phase, err)
				}
			}
		})
	}
	if invoked {
		t.Fatal("host replacement executed")
	}
}

func TestGoalInstallationComposesIndependentSurface(t *testing.T) {
	other := tools.Define[map[string]any](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil)}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{}, nil
		})
	base := tools.NewRegistry(other)
	definitions := make([]tools.ToolDefinition, 1, 2)
	definitions[0] = base.Definitions()[0]
	backing := definitions[:2]
	backing[1] = tools.ToolDefinition{Name: "sentinel"}
	projection := &turnProjection{thread: &Thread{id: "thread", harness: New(Options{})}, turnID: "turn", runID: "run"}
	options := engine.Options{ToolSurfaceProvider: func(context.Context, engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		return engine.ToolSurface{Tools: base, ToolDefinitions: definitions, SystemPrompt: "Host instructions."}, nil
	}}
	projection.installGoals(&options, nil)
	for range 2 {
		surface, err := options.ToolSurfaceProvider(t.Context(), engine.ToolSurfaceRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(surface.ToolDefinitions) != 2 || surface.ToolDefinitions[1].Name != goalToolName || surface.SystemPrompt != "Host instructions."+goalInstructions {
			t.Fatalf("incorrect composed surface: %+v", surface)
		}
		if len(base.Definitions()) != 1 || backing[1].Name != "sentinel" {
			t.Fatal("installation mutated host registry or definition slice")
		}
		if err := surface.Tools.Register(other); err == nil {
			t.Fatal("composed registry was not sealed")
		}
	}
}

func TestGoalInstallationBindsDispatchAndResultToExecution(t *testing.T) {
	projection := &turnProjection{thread: &Thread{id: "thread", harness: New(Options{})}, turnID: "turn", runID: "run"}
	options := engine.Options{}
	projection.installGoals(&options, nil)
	valid := tools.EffectDispatchRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", CallID: "call", Name: goalToolName, Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}
	for name, mutate := range map[string]func(*tools.EffectDispatchRequest){
		"thread":          func(r *tools.EffectDispatchRequest) { r.ThreadID = "other" },
		"turn":            func(r *tools.EffectDispatchRequest) { r.TurnID = "other" },
		"run":             func(r *tools.EffectDispatchRequest) { r.RunID = "other" },
		"call":            func(r *tools.EffectDispatchRequest) { r.CallID = "" },
		"read_only":       func(r *tools.EffectDispatchRequest) { r.ReadOnly = true },
		"external_effect": func(r *tools.EffectDispatchRequest) { r.Effects = []tools.Effect{tools.EffectNetwork} },
		"permission":      func(r *tools.EffectDispatchRequest) { r.Permission.Mode = tools.PermissionAsk },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			called := false
			result := options.EffectDispatcher(t.Context(), request, func(context.Context) tools.Result { called = true; return tools.Result{} })
			if called || !errors.Is(result.DispatchErr, ErrAuthorizationContract) {
				t.Fatalf("called=%v result=%+v", called, result)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := options.EffectDispatcher(ctx, valid, func(context.Context) tools.Result { t.Error("cancelled dispatch ran"); return tools.Result{} })
	if !errors.Is(result.DispatchErr, context.Canceled) {
		t.Fatalf("cancellation error=%v", result.DispatchErr)
	}
	validResult := engine.EffectResultFinalizationRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", Message: session.Message{Role: session.Tool, ToolName: goalToolName, ToolCallID: "call"}}
	for name, mutate := range map[string]func(*engine.EffectResultFinalizationRequest){
		"thread": func(r *engine.EffectResultFinalizationRequest) { r.ThreadID = "other" },
		"turn":   func(r *engine.EffectResultFinalizationRequest) { r.TurnID = "other" },
		"run":    func(r *engine.EffectResultFinalizationRequest) { r.RunID = "other" },
		"call":   func(r *engine.EffectResultFinalizationRequest) { r.ToolCallID = "other" },
		"role":   func(r *engine.EffectResultFinalizationRequest) { r.Message.Role = session.Assistant },
	} {
		t.Run("result_"+name, func(t *testing.T) {
			request := validResult
			mutate(&request)
			if _, err := options.EffectResultFinalizer(t.Context(), request); !errors.Is(err, ErrAuthorizationContract) {
				t.Fatalf("result identity error=%v", err)
			}
		})
	}
	if result, err := options.EffectResultFinalizer(t.Context(), validResult); err != nil || result.Handled {
		t.Fatalf("paired result must use the canonical message writer: %+v %v", result, err)
	}
}
