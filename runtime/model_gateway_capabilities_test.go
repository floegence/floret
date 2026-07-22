package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
)

func TestTurnExecutionHostRejectsMissingModelGatewayCapabilities(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	capabilities := mustTestCapabilities(t, store)
	createRequest := testCreateThreadRequest("thread")
	create, err := capabilities.create.Bind(createRequest.ThreadID, createRequest.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, createRequest); err != nil {
		t.Fatal(err)
	}
	factory, err := capabilities.turn.Bind("thread")
	if err != nil {
		t.Fatal(err)
	}
	_, err = factory.NewHost(ctx, TurnExecutionHostOptions{
		Config: runtimeGatewayConfig("missing capability"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("unused"), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("model"),
	})
	if err == nil || !strings.Contains(err.Error(), "model gateway reasoning capability is required") {
		t.Fatalf("NewHost error = %v, want missing model gateway reasoning capability", err)
	}
}

func TestModelGatewayCapabilitiesValidateExplicitAuthority(t *testing.T) {
	t.Parallel()

	gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("unused"), nil
	})
	none := ReasoningCapability{Kind: config.ReasoningKindNone}
	zero := ReasoningCapability{}
	unknown := ReasoningCapability{Kind: "typo"}
	negativeBudget := ReasoningCapability{Kind: config.ReasoningKindBudget, Budget: config.ReasoningBudget{MinTokens: -1}}
	tests := []struct {
		name         string
		gateway      ModelGateway
		capabilities ModelGatewayCapabilities
		wantError    string
	}{
		{name: "gateway missing capability", gateway: gateway, wantError: "reasoning capability is required"},
		{name: "gateway zero capability", gateway: gateway, capabilities: ModelGatewayCapabilities{Reasoning: &zero}, wantError: "must be explicit"},
		{name: "gateway explicit none", gateway: gateway, capabilities: ModelGatewayCapabilities{Reasoning: &none}},
		{name: "gateway unknown kind", gateway: gateway, capabilities: ModelGatewayCapabilities{Reasoning: &unknown}, wantError: "unsupported reasoning capability kind"},
		{name: "gateway invalid budget", gateway: gateway, capabilities: ModelGatewayCapabilities{Reasoning: &negativeBudget}, wantError: "cannot be negative"},
		{name: "native without gateway capability"},
		{name: "native with gateway capability", capabilities: ModelGatewayCapabilities{Reasoning: &none}, wantError: "native provider host must not provide"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.capabilities.validate(tt.gateway)
			if tt.wantError == "" && err != nil {
				t.Fatalf("validate() error = %v", err)
			}
			if tt.wantError != "" && (err == nil || !strings.Contains(err.Error(), tt.wantError)) {
				t.Fatalf("validate() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}
