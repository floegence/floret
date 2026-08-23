package runtime

import (
	"context"
	"slices"
	"testing"

	"github.com/floegence/floret/v5/internal/engine"
	internalprovider "github.com/floegence/floret/v5/internal/provider"
	"github.com/floegence/floret/v5/internal/testing/harness"
	publicprovider "github.com/floegence/floret/v5/provider"
	"github.com/floegence/floret/v5/tools"
)

func TestToolDefinitionAdaptersPreserveNilAndExplicitEmpty(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		if got := normalizeToolDefinitions(nil); got != nil {
			t.Fatalf("nil definitions normalized to %#v, want nil", got)
		}
		if got := normalizeToolDefinitions([]tools.ToolDefinition{}); got == nil || len(got) != 0 {
			t.Fatalf("explicit empty definitions normalized to %#v, want non-nil empty", got)
		}
	})

	t.Run("hosted", func(t *testing.T) {
		if got := providerHostedToolDefinitions(nil); got != nil {
			t.Fatalf("nil hosted definitions normalized to %#v, want nil", got)
		}
		if got := providerHostedToolDefinitions([]publicprovider.HostedToolDefinition{}); got == nil || len(got) != 0 {
			t.Fatalf("explicit empty hosted definitions normalized to %#v, want non-nil empty", got)
		}
	})
}

func TestRuntimeToolSurfaceProviderPreservesRegistryDefinitionInheritance(t *testing.T) {
	registry := terminalExecRegistry(t)

	t.Run("nil inherits registry definitions", func(t *testing.T) {
		request := runToolSurfaceEngine(t, registry, nil)
		assertProviderToolNames(t, request, "ask_user", "terminal.exec")
	})

	t.Run("explicit empty clears registry definitions", func(t *testing.T) {
		request := runToolSurfaceEngine(t, registry, []tools.ToolDefinition{})
		assertProviderToolNames(t, request, "ask_user")
	})
}

func runToolSurfaceEngine(t *testing.T, registry *tools.Registry, definitions []tools.ToolDefinition) internalprovider.Request {
	t.Helper()
	signals, err := engineTurnSignalSpec(TurnSignalSpec{
		Definitions: CoreControlDefinitions(false),
		Project:     ProjectCoreControlSignal,
	}, engine.CompletionNaturalStop)
	if err != nil {
		t.Fatal(err)
	}
	provider := harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done()))
	eng, err := engine.New(engine.Config{
		Provider:     provider,
		SystemPrompt: "Use the available tools when needed.",
		Options: engine.Options{
			RunID:       "run-tool-surface",
			ControlSpec: signals,
			ToolSurfaceProvider: runtimeToolSurfaceProvider(func(context.Context, ToolSurfaceRequest) (ToolSurface, error) {
				return ToolSurface{Tools: registry, ToolDefinitions: definitions}, nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := eng.Run(t.Context(), "inspect")
	if result.Status != engine.Completed || result.Err != nil {
		t.Fatalf("run result = %#v", result)
	}
	if len(provider.Requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(provider.Requests))
	}
	return provider.Requests[0]
}

func terminalExecRegistry(t *testing.T) *tools.Registry {
	t.Helper()
	registry, err := tools.NewRegistryE(tools.Define(
		tools.Definition{
			Name:        "terminal.exec",
			Description: "Run a terminal command.",
			InputSchema: tools.StrictObject(map[string]any{
				"command": tools.String("Command to run."),
			}, []string{"command"}),
			Effects:    []tools.Effect{tools.EffectShell},
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		},
		func(raw []byte) (map[string]any, error) { return map[string]any{}, nil },
		nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{Text: "done"}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func assertProviderToolNames(t *testing.T, request internalprovider.Request, want ...string) {
	t.Helper()
	got := make([]string, 0, len(request.Tools))
	for _, definition := range request.Tools {
		got = append(got, definition.Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("provider tools = %v, want %v", got, want)
	}
}
