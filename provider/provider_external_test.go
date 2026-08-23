package provider_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v5/provider"
)

type contractGateway struct{}

func (contractGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "contract", Model: "model", StateCompatibilityKey: "contract:model:v1"}
}

func (contractGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (contractGateway) Stream(context.Context, provider.Request) (<-chan provider.Event, error) {
	events := make(chan provider.Event, 1)
	events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func TestGatewayOwnsProviderContract(t *testing.T) {
	var gateway provider.Gateway = contractGateway{}
	if err := gateway.Identity().Validate(); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Capabilities().Validate(); err != nil {
		t.Fatal(err)
	}
	request := provider.Request{
		RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1", PromptScopeID: "thread-1",
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	stream, err := gateway.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if event := <-stream; event.Type != provider.EventDone {
		t.Fatalf("event = %#v", event)
	}
}
