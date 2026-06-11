package searchcap

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/provider/catalog"
)

func TestResolveUsesCapabilityAndCatalogWireShapeNotProviderName(t *testing.T) {
	for _, providerID := range []string{
		catalog.ProviderOpenAI,
		catalog.ProviderAnthropic,
		catalog.ProviderDeepSeek,
		catalog.ProviderOpenRouter,
		catalog.ProviderOpenAICompatible,
		catalog.ProviderGoogle,
		catalog.ProviderFake,
	} {
		t.Run(providerID, func(t *testing.T) {
			resolved, err := Resolve(ResolveInput{
				Provider: providerID,
				Capability: Capability{
					Source: WebSearchExternalBrave,
					Brave:  BraveConfig{Provider: ExternalProviderBrave},
				},
				BraveAvailable: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Source != WebSearchExternalBrave || !resolved.Available || resolved.Status != ResolveReady {
				t.Fatalf("resolved = %#v", resolved)
			}
			if !slices.Equal(resolved.LocalToolNames, []string{ToolWebSearch}) || len(resolved.HostedTools) != 0 {
				t.Fatalf("tools = local %#v hosted %#v", resolved.LocalToolNames, resolved.HostedTools)
			}
		})
	}
}

func TestResolveSingleSourceStates(t *testing.T) {
	tests := []struct {
		name         string
		provider     string
		capability   Capability
		braveKey     bool
		wantSource   WebSearchSource
		wantStatus   ResolveStatus
		wantAvail    bool
		wantLocal    []string
		wantHosted   bool
		wantReason   string
		wantError    string
		wantWire     HostedWireShape
		wantExternal string
	}{
		{
			name:       "provider hosted",
			provider:   catalog.ProviderAnthropic,
			capability: Capability{Source: WebSearchProviderHosted, Hosted: HostedConfig{WireShape: WireShapeAnthropicServerWebSearch}},
			wantSource: WebSearchProviderHosted,
			wantStatus: ResolveReady,
			wantAvail:  true,
			wantHosted: true,
			wantWire:   WireShapeAnthropicServerWebSearch,
		},
		{
			name:         "external brave requires key but keeps source",
			provider:     catalog.ProviderFake,
			capability:   Capability{Source: WebSearchExternalBrave, Brave: BraveConfig{Provider: ExternalProviderBrave}},
			wantSource:   WebSearchExternalBrave,
			wantStatus:   ResolveUnavailable,
			wantExternal: ExternalProviderBrave,
			wantReason:   "Brave Search API key is not configured",
		},
		{
			name:         "external brave with key",
			provider:     catalog.ProviderFake,
			capability:   Capability{Source: WebSearchExternalBrave, Brave: BraveConfig{Provider: ExternalProviderBrave}},
			braveKey:     true,
			wantSource:   WebSearchExternalBrave,
			wantStatus:   ResolveReady,
			wantAvail:    true,
			wantLocal:    []string{ToolWebSearch},
			wantExternal: ExternalProviderBrave,
		},
		{
			name:       "external provider must not silently become brave",
			provider:   catalog.ProviderFake,
			capability: Capability{Source: WebSearchExternalBrave, Brave: BraveConfig{Provider: "serpapi"}},
			wantSource: WebSearchExternalBrave,
			wantError:  `unsupported external web_search provider "serpapi"`,
		},
		{
			name:       "disabled",
			provider:   catalog.ProviderOpenAI,
			capability: Capability{Source: WebSearchDisabled},
			wantSource: WebSearchDisabled,
			wantStatus: ResolveUnavailable,
			wantReason: "web search disabled",
		},
		{
			name:       "unsupported hosted shape",
			provider:   catalog.ProviderOpenAI,
			capability: Capability{Source: WebSearchProviderHosted, Hosted: HostedConfig{WireShape: "custom_shape"}},
			wantSource: WebSearchProviderHosted,
			wantError:  `unsupported hosted web_search wire shape "custom_shape"`,
		},
		{
			name:       "hosted not supported by provider",
			provider:   catalog.ProviderDeepSeek,
			capability: Capability{Source: WebSearchProviderHosted, Hosted: HostedConfig{WireShape: WireShapeAnthropicServerWebSearch}},
			wantSource: WebSearchProviderHosted,
			wantError:  "provider-hosted web_search is not supported by this profile",
		},
		{
			name:       "mixed payload is invalid",
			provider:   catalog.ProviderAnthropic,
			capability: Capability{Source: WebSearchProviderHosted, Hosted: HostedConfig{WireShape: WireShapeAnthropicServerWebSearch}, Brave: BraveConfig{Provider: ExternalProviderBrave}},
			wantSource: WebSearchProviderHosted,
			wantError:  "cannot include external Brave configuration",
		},
		{
			name:       "unknown source is invalid",
			provider:   catalog.ProviderFake,
			capability: Capability{Source: "search_everywhere"},
			wantSource: "search_everywhere",
			wantError:  `unsupported web_search source "search_everywhere"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved, err := Resolve(ResolveInput{
				Provider:       tt.provider,
				Capability:     tt.capability,
				BraveAvailable: tt.braveKey,
			})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantError)
				}
				if resolved.Source != tt.wantSource || resolved.Status != ResolveInvalid {
					t.Fatalf("invalid resolved = %#v", resolved)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Source != tt.wantSource || resolved.Status != tt.wantStatus || resolved.Available != tt.wantAvail {
				t.Fatalf("resolved = %#v", resolved)
			}
			if !slices.Equal(resolved.LocalToolNames, tt.wantLocal) {
				t.Fatalf("local names = %#v, want %#v", resolved.LocalToolNames, tt.wantLocal)
			}
			if (len(resolved.HostedTools) == 1) != tt.wantHosted {
				t.Fatalf("hosted tools = %#v", resolved.HostedTools)
			}
			if tt.wantWire != "" && resolved.WireShape != tt.wantWire {
				t.Fatalf("wire shape = %q", resolved.WireShape)
			}
			if tt.wantExternal != "" && resolved.ExternalProvider != tt.wantExternal {
				t.Fatalf("external provider = %q", resolved.ExternalProvider)
			}
			if tt.wantReason != "" && !slices.Contains(resolved.UnavailableReasons, tt.wantReason) {
				t.Fatalf("reasons = %#v, want %q", resolved.UnavailableReasons, tt.wantReason)
			}
			if len(resolved.HostedTools) > 0 && len(resolved.LocalToolNames) > 0 {
				t.Fatalf("hosted and external search must be mutually exclusive: %#v", resolved)
			}
		})
	}
}

func TestDefaultCapabilityIsDisabledAndProviderPresetIsExplicit(t *testing.T) {
	for _, providerID := range []string{
		catalog.ProviderOpenAI,
		catalog.ProviderAnthropic,
		catalog.ProviderDeepSeek,
		catalog.ProviderOpenRouter,
		catalog.ProviderOpenAICompatible,
		catalog.ProviderGoogle,
		catalog.ProviderFake,
	} {
		capability := DefaultCapability(providerID)
		if capability.Source != WebSearchDisabled {
			t.Fatalf("%s empty capability should remain disabled: %#v", providerID, capability)
		}
	}

	openAI := ProviderPresetCapability(catalog.ProviderOpenAI)
	if openAI.Source != WebSearchDisabled {
		t.Fatalf("openai preset = %#v", openAI)
	}
	anthropic := ProviderPresetCapability(catalog.ProviderAnthropic)
	if anthropic.Source != WebSearchProviderHosted || anthropic.Hosted.WireShape != WireShapeAnthropicServerWebSearch {
		t.Fatalf("anthropic preset = %#v", anthropic)
	}
	for _, providerID := range []string{
		catalog.ProviderOpenAI,
		catalog.ProviderDeepSeek,
		catalog.ProviderOpenRouter,
		catalog.ProviderOpenAICompatible,
		catalog.ProviderGoogle,
		catalog.ProviderFake,
	} {
		capability := ProviderPresetCapability(providerID)
		if capability.Source != WebSearchDisabled {
			t.Fatalf("%s should not preset search: %#v", providerID, capability)
		}
	}
}

func TestNormalizeCapabilityCanonicalizesSingleSource(t *testing.T) {
	disabled := NormalizeCapability(catalog.ProviderOpenAI, Capability{Source: WebSearchDisabled})
	if disabled.Source != WebSearchDisabled || disabled.Hosted.WireShape != "" || disabled.Brave.Provider != "" {
		t.Fatalf("disabled capability = %#v", disabled)
	}

	external := NormalizeCapability(catalog.ProviderOpenAI, Capability{Source: WebSearchExternalBrave})
	if external.Source != WebSearchExternalBrave || external.Brave.Provider != ExternalProviderBrave || external.Hosted.WireShape != "" {
		t.Fatalf("external capability = %#v", external)
	}

	hosted := NormalizeCapability(catalog.ProviderAnthropic, Capability{Source: WebSearchProviderHosted})
	if hosted.Source != WebSearchProviderHosted || hosted.Hosted.WireShape != WireShapeAnthropicServerWebSearch || hosted.Brave.Provider != "" {
		t.Fatalf("hosted capability = %#v", hosted)
	}

	data, err := json.Marshal(external)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"source":"external_brave"`) || !strings.Contains(text, `"brave"`) {
		t.Fatalf("canonical json missing explicit source or brave config: %s", text)
	}
}

func TestCapabilityUnmarshalUsesCanonicalFields(t *testing.T) {
	var capability Capability
	if err := json.Unmarshal([]byte(`{"source":"external_brave","brave":{"provider":"brave"}}`), &capability); err != nil {
		t.Fatal(err)
	}
	if capability.Source != WebSearchExternalBrave || capability.Brave.Provider != ExternalProviderBrave || capability.Hosted.WireShape != "" {
		t.Fatalf("capability = %#v", capability)
	}

	var nullCapability Capability
	if err := json.Unmarshal([]byte(`null`), &nullCapability); err != nil {
		t.Fatal(err)
	}
	if nullCapability != (Capability{}) {
		t.Fatalf("null capability = %#v", nullCapability)
	}
}
