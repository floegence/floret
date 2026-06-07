package searchcap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/catalog"
)

const (
	ToolWebSearch = "web_search"

	WireShapeOpenAIChatWebSearchOptions HostedWireShape = "openai_chat_web_search_options"
	WireShapeAnthropicServerWebSearch   HostedWireShape = "anthropic_server_web_search"

	ExternalProviderBrave = "brave"
)

type WebSearchSource string

const (
	WebSearchProviderHosted WebSearchSource = "provider_hosted"
	WebSearchExternalBrave  WebSearchSource = "external_brave"
	WebSearchDisabled       WebSearchSource = "disabled"
)

type ResolveStatus string

const (
	ResolveReady       ResolveStatus = "ready"
	ResolveUnavailable ResolveStatus = "unavailable"
	ResolveInvalid     ResolveStatus = "invalid"
)

type HostedWireShape string

type HostedConfig struct {
	WireShape HostedWireShape `json:"wire_shape,omitempty"`
}

type BraveConfig struct {
	Provider string `json:"provider,omitempty"`
}

type Capability struct {
	Source WebSearchSource `json:"source,omitempty"`
	Hosted HostedConfig    `json:"hosted,omitempty"`
	Brave  BraveConfig     `json:"brave,omitempty"`
}

func (c *Capability) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*c = Capability{}
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range raw {
		switch key {
		case "source", "hosted", "brave":
		case "provider_hosted", "client", "disabled":
			return fmt.Errorf("legacy web_search configuration key %q is unsupported; use source, hosted, and brave", key)
		default:
			return fmt.Errorf("unsupported web_search configuration key %q", key)
		}
	}
	type capabilityJSON Capability
	var decoded capabilityJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = Capability(decoded)
	return nil
}

type ResolveInput struct {
	Provider       string
	Capability     Capability
	BraveAvailable bool
}

type Resolved struct {
	Source             WebSearchSource                 `json:"source"`
	Status             ResolveStatus                   `json:"status"`
	Available          bool                            `json:"available"`
	LocalToolNames     []string                        `json:"local_tool_names,omitempty"`
	HostedTools        []provider.HostedToolDefinition `json:"hosted_tools,omitempty"`
	UnavailableReasons []string                        `json:"unavailable_reasons,omitempty"`
	WireShape          HostedWireShape                 `json:"wire_shape,omitempty"`
	ExternalProvider   string                          `json:"external_provider,omitempty"`
}

func AvailableWireShapes() []HostedWireShape {
	return []HostedWireShape{
		WireShapeOpenAIChatWebSearchOptions,
		WireShapeAnthropicServerWebSearch,
	}
}

func SupportedWireShapes(providerID string) []HostedWireShape {
	capability := catalog.WebSearch(providerID)
	out := make([]HostedWireShape, 0, len(capability.HostedWireShapes))
	for _, shape := range capability.HostedWireShapes {
		if hostedShape := HostedWireShape(strings.TrimSpace(shape)); hostedShape != "" {
			out = append(out, hostedShape)
		}
	}
	return out
}

// IMPORTANT: Web search source selection must be derived from provider profile
// capability plus explicit wire shape support. Do not add provider-name special
// cases, hidden fallback, or runtime probing here; callers must surface
// unavailable capability state instead of exposing guaranteed-failing tools.
func Resolve(input ResolveInput) (Resolved, error) {
	if err := ValidateCapability(input.Provider, input.Capability); err != nil {
		source := input.Capability.Source
		if source == "" {
			source = WebSearchDisabled
		}
		return Resolved{Source: source, Status: ResolveInvalid}, err
	}
	capability := NormalizeCapability(input.Provider, input.Capability)
	out := Resolved{Source: capability.Source, Status: ResolveUnavailable}
	switch capability.Source {
	case WebSearchProviderHosted:
		shape := capability.Hosted.WireShape
		out.WireShape = shape
		out.HostedTools = []provider.HostedToolDefinition{{
			Name: ToolWebSearch,
			Type: ToolWebSearch,
			Options: map[string]any{
				"wire_shape": string(shape),
			},
		}}
		out.Status = ResolveReady
		out.Available = true
	case WebSearchExternalBrave:
		out.ExternalProvider = ExternalProviderBrave
		if !input.BraveAvailable {
			out.UnavailableReasons = append(out.UnavailableReasons, "Brave Search API key is not configured")
			break
		}
		out.LocalToolNames = []string{ToolWebSearch}
		out.Status = ResolveReady
		out.Available = true
	case WebSearchDisabled:
		out.UnavailableReasons = append(out.UnavailableReasons, "web search disabled")
	default:
		return Resolved{Source: capability.Source, Status: ResolveInvalid}, fmt.Errorf("unsupported web_search source %q", capability.Source)
	}
	if !out.Available && len(out.UnavailableReasons) == 0 {
		out.UnavailableReasons = append(out.UnavailableReasons, "web search is unavailable")
	}
	return out, nil
}

func NormalizeCapability(providerID string, capability Capability) Capability {
	source := capability.Source
	if source == "" {
		source = WebSearchDisabled
	}
	switch source {
	case WebSearchProviderHosted:
		shape := capability.Hosted.WireShape
		if shape == "" {
			shape = defaultHostedWireShape(providerID)
		}
		return Capability{Source: WebSearchProviderHosted, Hosted: HostedConfig{WireShape: shape}}
	case WebSearchExternalBrave:
		provider := strings.TrimSpace(capability.Brave.Provider)
		if provider == "" {
			provider = ExternalProviderBrave
		}
		return Capability{Source: WebSearchExternalBrave, Brave: BraveConfig{Provider: provider}}
	case WebSearchDisabled:
		return Capability{Source: WebSearchDisabled}
	default:
		return Capability{Source: source}
	}
}

func DefaultCapability(providerID string) Capability {
	return NormalizeCapability(providerID, Capability{})
}

func ProviderPresetCapability(providerID string) Capability {
	capability := catalog.WebSearch(providerID)
	source := WebSearchSource(strings.TrimSpace(capability.DefaultSource))
	switch source {
	case WebSearchProviderHosted:
		return Capability{
			Source: WebSearchProviderHosted,
			Hosted: HostedConfig{WireShape: HostedWireShape(strings.TrimSpace(capability.HostedWireShape))},
		}
	case WebSearchExternalBrave:
		return Capability{Source: WebSearchExternalBrave, Brave: BraveConfig{Provider: ExternalProviderBrave}}
	default:
		return Capability{Source: WebSearchDisabled}
	}
}

func ValidateCapability(providerID string, capability Capability) error {
	if err := ValidateRawCapability(providerID, capability); err != nil {
		return err
	}
	capability = NormalizeCapability(providerID, capability)
	switch capability.Source {
	case WebSearchProviderHosted:
		shape := capability.Hosted.WireShape
		if shape == "" {
			return fmt.Errorf("provider web_search is selected but no hosted wire shape is configured")
		}
		if err := ValidateWireShape(shape); err != nil {
			return err
		}
		if !slices.Contains(SupportedWireShapes(providerID), shape) {
			return fmt.Errorf("provider web_search wire shape %q is not supported by this profile", shape)
		}
	case WebSearchExternalBrave:
		provider := strings.TrimSpace(capability.Brave.Provider)
		if provider == "" {
			provider = ExternalProviderBrave
		}
		if provider != ExternalProviderBrave {
			return fmt.Errorf("unsupported external web_search provider %q", provider)
		}
	case WebSearchDisabled:
	default:
		return fmt.Errorf("unsupported web_search source %q", capability.Source)
	}
	return nil
}

func ValidateRawCapability(providerID string, capability Capability) error {
	source := capability.Source
	if source == "" {
		source = WebSearchDisabled
	}
	switch source {
	case WebSearchProviderHosted:
		if strings.TrimSpace(capability.Brave.Provider) != "" {
			return fmt.Errorf("provider-hosted web_search cannot include external Brave configuration")
		}
	case WebSearchExternalBrave:
		if capability.Hosted.WireShape != "" {
			return fmt.Errorf("external Brave web_search cannot include hosted wire shape configuration")
		}
		provider := strings.TrimSpace(capability.Brave.Provider)
		if provider != "" && provider != ExternalProviderBrave {
			return fmt.Errorf("unsupported external web_search provider %q", provider)
		}
	case WebSearchDisabled:
		if capability.Hosted.WireShape != "" || strings.TrimSpace(capability.Brave.Provider) != "" {
			return fmt.Errorf("disabled web_search cannot include hosted or external configuration")
		}
	default:
		return fmt.Errorf("unsupported web_search source %q", source)
	}
	return nil
}

func ValidateWireShape(shape HostedWireShape) error {
	switch shape {
	case WireShapeOpenAIChatWebSearchOptions, WireShapeAnthropicServerWebSearch:
		return nil
	default:
		return fmt.Errorf("unsupported hosted web_search wire shape %q", shape)
	}
}

func defaultHostedWireShape(providerID string) HostedWireShape {
	return HostedWireShape(strings.TrimSpace(catalog.WebSearch(providerID).HostedWireShape))
}
