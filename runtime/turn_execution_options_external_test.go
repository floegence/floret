package runtime_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type optionTestGateway struct{}

func (optionTestGateway) StreamModel(context.Context, floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	stream := make(chan floretruntime.ModelEvent)
	close(stream)
	return stream, nil
}

type optionTestGate struct{}

func (optionTestGate) Dispatch(context.Context, floretruntime.EffectAuthorizationRequest, floretruntime.AuthorizedEffect) (floretruntime.EffectDispatchResult, error) {
	return floretruntime.EffectDispatchResult{}, nil
}

func TestNewTurnExecutionHostOptionsUsesSafeDefaultsAndRejectsDuplicateCategories(t *testing.T) {
	cfg := config.Config{Provider: config.ProviderFake, Model: "fake", FakeResponse: "hello"}
	_, err := floretruntime.NewTurnExecutionHostOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = floretruntime.NewTurnExecutionHostOptions(cfg,
		floretruntime.WithTurnLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 2}),
		floretruntime.WithTurnLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 3}),
	)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate option error=%v", err)
	}
	if _, err := floretruntime.NewTurnExecutionHostOptions(cfg, floretruntime.TurnExecutionOption{}); err == nil {
		t.Fatal("zero-value opaque option was accepted")
	}
}

func TestWithLoopLimitsRejectsNegativeValues(t *testing.T) {
	tests := []floretruntime.LoopLimits{
		{MaxEmptyProviderRetries: -1},
		{NoProgressLimit: -1},
		{DuplicateToolLimit: -1},
		{WallTime: -time.Nanosecond},
	}
	for _, limits := range tests {
		if _, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, floretruntime.WithTurnLoopLimits(limits)); err == nil {
			t.Fatalf("negative loop limits accepted: %#v", limits)
		}
	}
	want := floretruntime.LoopLimits{MaxEmptyProviderRetries: 1, NoProgressLimit: 2, DuplicateToolLimit: 3, WallTime: time.Second}
	_, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, floretruntime.WithTurnLoopLimits(want))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithModelGatewayKeepsIdentityAndCapabilitiesAtomic(t *testing.T) {
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	identity := floretruntime.ModelGatewayIdentity{
		Provider: "host", Model: "model", StateCompatibilityKey: "host:model:v1",
	}
	gatewayConfig := config.Config{ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}
	_, err := floretruntime.NewTurnExecutionHostOptions(gatewayConfig, floretruntime.WithTurnModelGateway(
		optionTestGateway{}, identity, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
	))
	if err != nil {
		t.Fatal(err)
	}
	_, err = floretruntime.NewTurnExecutionHostOptions(gatewayConfig, floretruntime.WithTurnModelGateway(
		optionTestGateway{}, identity, floretruntime.ModelGatewayCapabilities{},
	))
	if err == nil || !strings.Contains(err.Error(), "reasoning capability") {
		t.Fatalf("incomplete gateway error=%v", err)
	}
	_, err = floretruntime.NewTurnExecutionHostOptions(config.Config{}, floretruntime.WithTurnModelGateway(
		optionTestGateway{}, identity, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
	))
	if err == nil || !strings.Contains(err.Error(), "context window") {
		t.Fatalf("incomplete gateway config error=%v", err)
	}
}

func TestWithReadOnlyToolsBuildsIndependentValidatedSnapshot(t *testing.T) {
	inputSchema := tools.StrictObject(map[string]any{"path": tools.String("path")}, []string{"path"})
	outputSchema := map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}}
	nestedAnnotations := map[string]string{"scope": "workspace"}
	annotations := map[string]any{"owner": "host", "nested": nestedAnnotations}
	item := tools.Define[map[string]string](tools.Definition{
		Name: "read_file", InputSchema: inputSchema, OutputSchema: outputSchema, Annotations: annotations,
		ReadOnly: true, Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(context.Context, tools.Invocation[map[string]string]) (tools.Result, error) {
		return tools.Result{Text: "ok"}, nil
	})
	option := floretruntime.WithTurnReadOnlyTools(item)
	inputSchema["type"] = "array"
	outputSchema["type"] = "string"
	annotations["owner"] = "mutated"
	nestedAnnotations["scope"] = "mutated"
	_, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, option)
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, option)
			if err != nil {
				t.Errorf("concurrent sealed option build: %v", err)
			}
		}()
	}
	wait.Wait()
}

func TestWithReadOnlyToolsRejectsUnprovenSafety(t *testing.T) {
	tests := []tools.Definition{
		{Name: "ask", ReadOnly: true, Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}},
		{Name: "deny", ReadOnly: true, Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionDeny}},
		{Name: "write", ReadOnly: true, Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		{Name: "shell", ReadOnly: true, Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		{Name: "network", ReadOnly: true, OpenWorld: true, Effects: []tools.Effect{tools.EffectNetwork}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}},
		{Name: "dynamic", ReadOnly: true, Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, PermissionFor: func(tools.PermissionRequest) (tools.PermissionSpec, error) {
			return tools.PermissionSpec{Mode: tools.PermissionAllow}, nil
		}},
	}
	for _, definition := range tests {
		t.Run(definition.Name, func(t *testing.T) {
			item := tools.Define[struct{}](definition, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
				return tools.Result{}, nil
			})
			if _, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, floretruntime.WithTurnReadOnlyTools(item)); err == nil {
				t.Fatalf("unsafe read-only tool accepted: %#v", definition)
			}
		})
	}
}

func TestWithEffectfulToolsRequiresGateAndConflictsWithReadOnlyTools(t *testing.T) {
	registry := tools.NewRegistry()
	if _, err := floretruntime.NewTurnExecutionHostOptions(config.Config{}, floretruntime.WithTurnEffectfulTools(registry, nil)); err == nil {
		t.Fatal("effectful tools accepted a nil gate")
	}
	readOnly := tools.Define[struct{}](tools.Definition{Name: "read", ReadOnly: true}, nil, nil,
		func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) { return tools.Result{}, nil })
	_, err := floretruntime.NewTurnExecutionHostOptions(config.Config{},
		floretruntime.WithTurnReadOnlyTools(readOnly),
		floretruntime.WithTurnEffectfulTools(registry, optionTestGate{}),
	)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("tool category conflict error=%v", err)
	}
}
