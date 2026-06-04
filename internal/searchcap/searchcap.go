package searchcap

import (
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/provider"
)

const (
	ToolWebSearch = "web_search"

	WireShapeOpenAIChatWebSearchOptions = "openai_chat_web_search_options"
	WireShapeAnthropicServerWebSearch   = "anthropic_server_web_search"

	ClientProviderBrave = "brave"
)

type ProviderHostedConfig struct {
	Enabled             bool     `json:"enabled"`
	WireShape           string   `json:"wire_shape,omitempty"`
	SupportedWireShapes []string `json:"supported_wire_shapes,omitempty"`
}

type ClientConfig struct {
	Enabled  bool   `json:"enabled"`
	Provider string `json:"provider,omitempty"`
}

type Capability struct {
	ProviderHosted ProviderHostedConfig `json:"provider_hosted"`
	Client         ClientConfig         `json:"client"`
	Disabled       bool                 `json:"disabled,omitempty"`
}

type ResolveInput struct {
	Provider        string
	Capability      Capability
	ClientAvailable bool
}

type Resolved struct {
	LocalToolNames      []string
	HostedTools         []provider.HostedToolDefinition
	UnavailableReasons  []string
	ProviderHosted      bool
	Client              bool
	WireShape           string
	ClientProvider      string
	ProviderSearchKnown bool
}

func AvailableWireShapes() []string {
	return []string{
		WireShapeOpenAIChatWebSearchOptions,
		WireShapeAnthropicServerWebSearch,
	}
}

// IMPORTANT: Web search source selection must be derived from provider profile
// capability plus explicit wire shape support. Do not add provider-name special
// cases, hidden fallback, or runtime probing here; callers must surface
// unavailable capability state instead of exposing guaranteed-failing tools.
func Resolve(input ResolveInput) (Resolved, error) {
	capability := NormalizeCapability(input.Provider, input.Capability)
	var out Resolved
	if capability.Disabled {
		out.UnavailableReasons = append(out.UnavailableReasons, "web search disabled")
		return out, nil
	}
	if capability.ProviderHosted.Enabled {
		shape := strings.TrimSpace(capability.ProviderHosted.WireShape)
		if shape == "" {
			return Resolved{}, fmt.Errorf("provider web_search is enabled but no hosted wire shape is configured")
		}
		if !slices.Contains(capability.ProviderHosted.SupportedWireShapes, shape) {
			return Resolved{}, fmt.Errorf("provider web_search wire shape %q is not supported by this profile", shape)
		}
		if err := ValidateWireShape(shape); err != nil {
			return Resolved{}, err
		}
		out.ProviderHosted = true
		out.WireShape = shape
		out.ProviderSearchKnown = true
		out.HostedTools = []provider.HostedToolDefinition{{
			Name: ToolWebSearch,
			Type: ToolWebSearch,
			Options: map[string]any{
				"wire_shape": shape,
			},
		}}
		return out, nil
	}
	if capability.Client.Enabled {
		clientProvider := strings.TrimSpace(capability.Client.Provider)
		if clientProvider == "" {
			clientProvider = ClientProviderBrave
		}
		if clientProvider != ClientProviderBrave {
			return Resolved{}, fmt.Errorf("unsupported client web_search provider %q", clientProvider)
		}
		if !input.ClientAvailable {
			out.UnavailableReasons = append(out.UnavailableReasons, "Brave Search API key is not configured")
			return out, nil
		}
		out.Client = true
		out.ClientProvider = clientProvider
		out.LocalToolNames = []string{ToolWebSearch}
		return out, nil
	}
	out.UnavailableReasons = append(out.UnavailableReasons, "web search is not enabled")
	return out, nil
}

func NormalizeCapability(providerID string, capability Capability) Capability {
	providerID = modelcatalog.NormalizeProvider(providerID)
	defaults := DefaultCapability(providerID)
	if len(capability.ProviderHosted.SupportedWireShapes) == 0 {
		capability.ProviderHosted.SupportedWireShapes = append([]string(nil), defaults.ProviderHosted.SupportedWireShapes...)
	}
	if capability.ProviderHosted.WireShape == "" {
		capability.ProviderHosted.WireShape = defaults.ProviderHosted.WireShape
	}
	if !capability.Disabled && !capability.ProviderHosted.Enabled && !capability.Client.Enabled {
		capability.ProviderHosted.Enabled = defaults.ProviderHosted.Enabled
		capability.Client.Enabled = defaults.Client.Enabled
	}
	if capability.Client.Provider == "" {
		capability.Client.Provider = ClientProviderBrave
	}
	return capability
}

func DefaultCapability(providerID string) Capability {
	switch modelcatalog.APIKind(modelcatalog.NormalizeProvider(providerID)) {
	case modelcatalog.APIOpenAIChat:
		return Capability{ProviderHosted: ProviderHostedConfig{
			Enabled:             true,
			WireShape:           WireShapeOpenAIChatWebSearchOptions,
			SupportedWireShapes: []string{WireShapeOpenAIChatWebSearchOptions},
		}, Client: ClientConfig{Provider: ClientProviderBrave}}
	case modelcatalog.APIAnthropicMessages:
		return Capability{ProviderHosted: ProviderHostedConfig{
			Enabled:             true,
			WireShape:           WireShapeAnthropicServerWebSearch,
			SupportedWireShapes: []string{WireShapeAnthropicServerWebSearch},
		}, Client: ClientConfig{Provider: ClientProviderBrave}}
	default:
		return Capability{Client: ClientConfig{Provider: ClientProviderBrave}}
	}
}

func ValidateWireShape(shape string) error {
	switch strings.TrimSpace(shape) {
	case WireShapeOpenAIChatWebSearchOptions, WireShapeAnthropicServerWebSearch:
		return nil
	default:
		return fmt.Errorf("unsupported hosted web_search wire shape %q", shape)
	}
}

func DisableProviderHosted(capability Capability) Capability {
	capability.ProviderHosted.Enabled = false
	if capability.Client.Provider == "" {
		capability.Client.Provider = ClientProviderBrave
	}
	return capability
}
