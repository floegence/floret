package provider

import "github.com/floegence/floret/v7/internal/provider/catalog"

// DeepSeekCapabilities declares reviewed model support; it does not enable tools.
type DeepSeekCapabilities struct {
	IndependentSearch        bool
	HostedWebSearchWireShape string
}

// DeepSeekModelCapabilities returns reviewed support for the exact selected wire model.
// IMPORTANT: Web search source selection must be derived from provider profile
// capability plus explicit wire shape support. Do not add provider-name special
// cases, substitute unrequested sources, or probe at runtime here; callers must surface
// unavailable capability state instead of exposing guaranteed-failing tools.
func DeepSeekModelCapabilities(model string) (DeepSeekCapabilities, bool) {
	if _, ok := catalog.FindModel(catalog.ProviderDeepSeek, model); !ok {
		return DeepSeekCapabilities{}, false
	}
	switch model {
	case "deepseek-v4-pro":
		return DeepSeekCapabilities{HostedWebSearchWireShape: "deepseek_responses_web_search"}, true
	case "deepseek-flash", "deepseek-v4-flash", "deepseek-v4-flash-vision-exp":
		return DeepSeekCapabilities{IndependentSearch: true}, true
	default:
		return DeepSeekCapabilities{}, false
	}
}
