package florettest_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v6/florettest"
	"github.com/floegence/floret/v6/provider"
)

func TestScriptedGateway(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDone, Reason: "stop"}}},
	)
	stream, err := gateway.Stream(context.Background(), provider.Request{
		RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event := <-stream; event.Type != provider.EventDone {
		t.Fatalf("event = %#v", event)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatalf("requests = %d", len(gateway.Requests()))
	}
}
