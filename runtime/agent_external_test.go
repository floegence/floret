package runtime_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/provider"
	floretruntime "github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/tools"
)

type agentGateway struct{}

func (agentGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"}
}

func (agentGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (agentGateway) Stream(context.Context, provider.Request) (<-chan provider.Event, error) {
	stream := make(chan provider.Event, 1)
	stream <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	close(stream)
	return stream, nil
}

func TestAgentOwnsImmutableExecutionConfiguration(t *testing.T) {
	configuration := config.AgentConfig{
		Profile:      config.AgentProfile{ID: "reviewer", Name: "Reviewer"},
		SystemPrompt: "Review carefully.",
		Context:      config.ContextPolicy{ContextWindowTokens: 32_000},
	}
	inputSchema := tools.StrictObject(map[string]any{}, nil)
	tool := tools.Define[struct{}](tools.Definition{
		Name: "inspect", Description: "Inspect input.", InputSchema: inputSchema,
		ReadOnly: true, Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
		return tools.Result{Text: "ok"}, nil
	})
	agent, err := floretruntime.NewAgent(configuration, agentGateway{}, floretruntime.WithAgentTools(tool))
	if err != nil {
		t.Fatal(err)
	}

	configuration.Profile.Name = "mutated"
	configuration.SystemPrompt = "mutated"
	inputSchema["type"] = "array"
	got := agent.Config()
	if got.Profile.Name != "Reviewer" || got.SystemPrompt != "Review carefully." {
		t.Fatalf("Agent config mutated: %#v", got)
	}
	definitions := agent.ToolDefinitions()
	if len(definitions) != 1 || definitions[0].InputSchema["type"] != "object" {
		t.Fatalf("Agent tools mutated: %#v", definitions)
	}
	definitions[0].InputSchema["type"] = "number"
	if agent.ToolDefinitions()[0].InputSchema["type"] != "object" {
		t.Fatal("Agent returned aliased tool definitions")
	}
	if agent.ProviderIdentity() != (provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"}) {
		t.Fatalf("provider identity = %#v", agent.ProviderIdentity())
	}
}

func TestAgentRejectsInvalidGatewayContract(t *testing.T) {
	configuration := config.AgentConfig{
		Profile: config.AgentProfile{ID: "agent", Name: "Agent"}, SystemPrompt: "Act.",
		Context: config.ContextPolicy{ContextWindowTokens: 32_000},
	}
	if _, err := floretruntime.NewAgent(configuration, nil); err == nil {
		t.Fatal("nil gateway accepted")
	}
}
