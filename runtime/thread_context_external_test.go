package runtime_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/florettest"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/runtime"
	"github.com/floegence/floret/v4/storage"
)

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
}
