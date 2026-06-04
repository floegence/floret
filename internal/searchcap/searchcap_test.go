package searchcap

import (
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/modelcatalog"
)

func TestResolveUsesCapabilityInsteadOfProviderName(t *testing.T) {
	providers := []string{
		modelcatalog.ProviderOpenAI,
		modelcatalog.ProviderDeepSeek,
		modelcatalog.ProviderOpenRouter,
		modelcatalog.ProviderOpenAICompatible,
		modelcatalog.ProviderGoogle,
		modelcatalog.ProviderFake,
	}
	for _, providerID := range providers {
		t.Run(providerID, func(t *testing.T) {
			resolved, err := Resolve(ResolveInput{
				Provider: providerID,
				Capability: Capability{ProviderHosted: ProviderHostedConfig{
					Enabled:             true,
					WireShape:           WireShapeOpenAIChatWebSearchOptions,
					SupportedWireShapes: []string{WireShapeOpenAIChatWebSearchOptions},
				}},
				ClientAvailable: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !resolved.ProviderHosted || resolved.Client || len(resolved.HostedTools) != 1 || len(resolved.LocalToolNames) != 0 {
				t.Fatalf("resolved = %#v", resolved)
			}
		})
	}
}

func TestResolveProviderHostedClientAndUnavailableStates(t *testing.T) {
	tests := []struct {
		name       string
		capability Capability
		clientKey  bool
		wantHosted bool
		wantClient bool
		wantLocal  []string
		wantReason string
		wantError  string
		wantWire   string
	}{
		{
			name: "provider hosted wins",
			capability: Capability{ProviderHosted: ProviderHostedConfig{
				Enabled:             true,
				WireShape:           WireShapeOpenAIChatWebSearchOptions,
				SupportedWireShapes: []string{WireShapeOpenAIChatWebSearchOptions},
			}, Client: ClientConfig{Enabled: true, Provider: ClientProviderBrave}},
			clientKey:  true,
			wantHosted: true,
			wantWire:   WireShapeOpenAIChatWebSearchOptions,
		},
		{
			name: "client enabled requires key",
			capability: Capability{Client: ClientConfig{
				Enabled:  true,
				Provider: ClientProviderBrave,
			}},
			wantReason: "Brave Search API key is not configured",
		},
		{
			name: "client enabled with key",
			capability: Capability{Client: ClientConfig{
				Enabled:  true,
				Provider: ClientProviderBrave,
			}},
			clientKey:  true,
			wantClient: true,
			wantLocal:  []string{ToolWebSearch},
		},
		{
			name:       "disabled",
			capability: Capability{Disabled: true},
			wantReason: "web search disabled",
		},
		{
			name:       "not enabled",
			capability: Capability{},
			wantReason: "web search is not enabled",
		},
		{
			name: "unsupported wire shape",
			capability: Capability{ProviderHosted: ProviderHostedConfig{
				Enabled:             true,
				WireShape:           "custom_shape",
				SupportedWireShapes: []string{WireShapeOpenAIChatWebSearchOptions},
			}},
			wantError: `wire shape "custom_shape" is not supported`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved, err := Resolve(ResolveInput{
				Provider:        modelcatalog.ProviderFake,
				Capability:      tt.capability,
				ClientAvailable: tt.clientKey,
			})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if resolved.ProviderHosted != tt.wantHosted || resolved.Client != tt.wantClient {
				t.Fatalf("resolved = %#v", resolved)
			}
			if tt.wantWire != "" && resolved.WireShape != tt.wantWire {
				t.Fatalf("wire shape = %q", resolved.WireShape)
			}
			if !slices.Equal(resolved.LocalToolNames, tt.wantLocal) {
				t.Fatalf("local names = %#v, want %#v", resolved.LocalToolNames, tt.wantLocal)
			}
			if tt.wantReason != "" && !slices.Contains(resolved.UnavailableReasons, tt.wantReason) {
				t.Fatalf("reasons = %#v, want %q", resolved.UnavailableReasons, tt.wantReason)
			}
			if resolved.ProviderHosted && resolved.Client {
				t.Fatalf("hosted and client search must be mutually exclusive: %#v", resolved)
			}
		})
	}
}

func TestDefaultCapabilityUsesAPIKindNotProviderSpecificSearchLogic(t *testing.T) {
	chatProviders := []string{
		modelcatalog.ProviderOpenAI,
		modelcatalog.ProviderDeepSeek,
		modelcatalog.ProviderOpenRouter,
		modelcatalog.ProviderOpenAICompatible,
	}
	for _, providerID := range chatProviders {
		capability := DefaultCapability(providerID)
		if !capability.ProviderHosted.Enabled || capability.ProviderHosted.WireShape != WireShapeOpenAIChatWebSearchOptions {
			t.Fatalf("%s capability = %#v", providerID, capability)
		}
	}
	anthropic := DefaultCapability(modelcatalog.ProviderAnthropic)
	if !anthropic.ProviderHosted.Enabled || anthropic.ProviderHosted.WireShape != WireShapeAnthropicServerWebSearch {
		t.Fatalf("anthropic capability = %#v", anthropic)
	}
	fake := DefaultCapability(modelcatalog.ProviderFake)
	if fake.ProviderHosted.Enabled || fake.Client.Enabled {
		t.Fatalf("fake should not auto-enable search: %#v", fake)
	}
}

func TestNormalizeCapabilityPreservesExplicitDisabledAndClientChoice(t *testing.T) {
	disabled := NormalizeCapability(modelcatalog.ProviderOpenAICompatible, Capability{Disabled: true})
	if !disabled.Disabled || disabled.ProviderHosted.Enabled || disabled.Client.Enabled {
		t.Fatalf("disabled capability = %#v", disabled)
	}

	client := NormalizeCapability(modelcatalog.ProviderOpenAICompatible, Capability{Client: ClientConfig{Enabled: true}})
	if client.ProviderHosted.Enabled || !client.Client.Enabled || client.Client.Provider != ClientProviderBrave {
		t.Fatalf("client capability = %#v", client)
	}
	if len(client.ProviderHosted.SupportedWireShapes) == 0 {
		t.Fatalf("supported wire shapes should still be exposed for UI configuration: %#v", client)
	}
}
