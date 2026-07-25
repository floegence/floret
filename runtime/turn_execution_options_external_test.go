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

func TestNewTurnExecutionOptionsUsesSafeDefaultsAndRejectsDuplicateCategories(t *testing.T) {
	cfg := config.Config{Provider: config.ProviderFake, Model: "fake", FakeResponse: "hello"}
	options, err := floretruntime.NewTurnExecutionOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if options.Config.Provider != cfg.Provider || options.Tools != nil || options.ModelGateway != nil {
		t.Fatalf("default options = %#v", options)
	}
	_, err = floretruntime.NewTurnExecutionOptions(cfg,
		floretruntime.WithLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 2}),
		floretruntime.WithLoopLimits(floretruntime.LoopLimits{NoProgressLimit: 3}),
	)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate option error=%v", err)
	}
	if _, err := floretruntime.NewTurnExecutionOptions(cfg, floretruntime.TurnExecutionOption{}); err == nil {
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
		if _, err := floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithLoopLimits(limits)); err == nil {
			t.Fatalf("negative loop limits accepted: %#v", limits)
		}
	}
	want := floretruntime.LoopLimits{MaxEmptyProviderRetries: 1, NoProgressLimit: 2, DuplicateToolLimit: 3, WallTime: time.Second}
	options, err := floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithLoopLimits(want))
	if err != nil {
		t.Fatal(err)
	}
	if options.LoopLimits != want {
		t.Fatalf("loop limits = %#v, want %#v", options.LoopLimits, want)
	}
}

func TestWithModelGatewayKeepsIdentityAndCapabilitiesAtomic(t *testing.T) {
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	identity := floretruntime.ModelGatewayIdentity{
		Provider: "host", Model: "model", StateCompatibilityKey: "host:model:v1",
	}
	options, err := floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithModelGateway(
		optionTestGateway{}, identity, floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
	))
	if err != nil {
		t.Fatal(err)
	}
	if options.ModelGateway == nil || options.ModelGatewayIdentity != identity || options.ModelGatewayCapabilities.Reasoning == nil {
		t.Fatalf("gateway options = %#v", options)
	}
	_, err = floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithModelGateway(
		optionTestGateway{}, identity, floretruntime.ModelGatewayCapabilities{},
	))
	if err == nil || !strings.Contains(err.Error(), "reasoning capability") {
		t.Fatalf("incomplete gateway error=%v", err)
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
	option := floretruntime.WithReadOnlyTools(item)
	inputSchema["type"] = "array"
	outputSchema["type"] = "string"
	annotations["owner"] = "mutated"
	nestedAnnotations["scope"] = "mutated"
	options, err := floretruntime.NewTurnExecutionOptions(config.Config{}, option)
	if err != nil {
		t.Fatal(err)
	}
	definition, ok := options.Tools.Definition("read_file")
	if !ok {
		t.Fatal("read-only snapshot omitted tool")
	}
	if definition.InputSchema["type"] != "object" || definition.OutputSchema["type"] != "object" || definition.Annotations["owner"] != "host" {
		t.Fatalf("read-only snapshot was mutated: %#v", definition)
	}
	nested, ok := definition.Annotations["nested"].(map[string]string)
	if !ok || nested["scope"] != "workspace" {
		t.Fatalf("nested read-only metadata was mutated: %#v", definition.Annotations["nested"])
	}
	if err := options.Tools.Register(tools.Define[struct{}](tools.Definition{
		Name: "write_file", Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
		return tools.Result{}, nil
	})); err == nil || !strings.Contains(err.Error(), "sealed") {
		t.Fatalf("read-only snapshot accepted later registration: %v", err)
	}

	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			built, err := floretruntime.NewTurnExecutionOptions(config.Config{}, option)
			if err != nil || built.Tools != options.Tools {
				t.Errorf("concurrent sealed option build: options=%#v err=%v", built, err)
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
			if _, err := floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithReadOnlyTools(item)); err == nil {
				t.Fatalf("unsafe read-only tool accepted: %#v", definition)
			}
		})
	}
}

func TestWithEffectfulToolsRequiresGateAndConflictsWithReadOnlyTools(t *testing.T) {
	registry := tools.NewRegistry()
	if _, err := floretruntime.NewTurnExecutionOptions(config.Config{}, floretruntime.WithEffectfulTools(registry, nil)); err == nil {
		t.Fatal("effectful tools accepted a nil gate")
	}
	readOnly := tools.Define[struct{}](tools.Definition{Name: "read", ReadOnly: true}, nil, nil,
		func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) { return tools.Result{}, nil })
	_, err := floretruntime.NewTurnExecutionOptions(config.Config{},
		floretruntime.WithReadOnlyTools(readOnly),
		floretruntime.WithEffectfulTools(registry, optionTestGate{}),
	)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("tool category conflict error=%v", err)
	}
}
