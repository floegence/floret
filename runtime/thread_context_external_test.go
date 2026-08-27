package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v5/config"
	"github.com/floegence/floret/v5/florettest"
	"github.com/floegence/floret/v5/provider"
	"github.com/floegence/floret/v5/runtime"
	"github.com/floegence/floret/v5/storage"
)

func TestThreadContextReaderAccumulatesFinalProviderUsageFromEngineEvents(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "context-usage", StateCompatibilityKey: "test:context-usage:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventUsage, Usage: provider.Usage{
				InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
				WindowInputTokens: 100, TotalTokens: 120, Source: "native", Available: true,
			}},
			{Type: provider.EventDelta, Text: "done"},
			{Type: provider.EventDone, Reason: "stop"},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "test", Name: "Test"},
		SystemPrompt: "Test.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	reader := service.(runtime.ThreadContextReader)
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-context-usage"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{
		ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "measure"}, RequestKey: "send-context-usage",
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, readErr := reader.Context(t.Context(), created.ThreadID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if snapshot.UsageTotals != nil {
			if *snapshot.UsageTotals != (runtime.ThreadTokenUsageTotals{
				InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
			}) {
				t.Fatalf("usage totals=%#v", snapshot.UsageTotals)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("final provider usage was not projected into canonical thread context")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHostThreadServiceExposesCanonicalThreadContextReader(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "context-reader", StateCompatibilityKey: "test:context-reader:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "test", Name: "Test"},
		SystemPrompt: "Test.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := service.(runtime.ThreadContextReader)
	if !ok {
		t.Fatal("Host.ThreadService must implement runtime.ThreadContextReader")
	}
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-context-reader"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 0 {
		t.Fatalf("new thread compactions=%#v", snapshot.Compactions)
	}
	if snapshot.UsageTotals != nil {
		t.Fatalf("new thread usage totals=%#v, want nil", snapshot.UsageTotals)
	}
}
