package runtime_test

import (
	"context"
	"maps"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestWithAgentRunLabelsSnapshotsPublicProviderContext(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "labels", StateCompatibilityKey: "test:labels:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	correlation := map[string]string{"request": "request-1"}
	hostContext := map[string]string{"permission_snapshot_id": "snapshot-1"}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentRunLabels(runtime.RunLabels{Correlation: correlation, Host: hostContext}))
	if err != nil {
		t.Fatal(err)
	}
	correlation["request"] = "mutated"
	hostContext["permission_snapshot_id"] = "mutated"

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
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-labels"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "send-labels", Input: runtime.UserInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	request := gateway.Requests()[0]
	if !maps.Equal(request.Labels.Correlation, map[string]string{"request": "request-1"}) ||
		!maps.Equal(request.Labels.Host, map[string]string{"permission_snapshot_id": "snapshot-1"}) {
		t.Fatalf("provider labels=%v host=%v", request.Labels.Correlation, request.Labels.Host)
	}
}
