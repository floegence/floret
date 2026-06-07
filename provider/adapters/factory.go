package adapters

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/catalog"
)

func NewProvider(cfg config.Config) (provider.Provider, error) {
	resolved, err := config.Resolve(cfg, nil)
	if err != nil {
		return nil, err
	}
	model, _ := catalog.FindModel(resolved.Provider, resolved.Model)
	switch catalog.APIKind(resolved.Provider) {
	case catalog.APIFake:
		return FakeProvider{Response: cfg.FakeResponse}, nil
	case catalog.APIOpenAIChat:
		modelID := resolved.Model
		if model.OpenAIModelID != "" {
			modelID = model.OpenAIModelID
		}
		return OpenAICompatibleProvider{
			Endpoint:  strings.TrimRight(resolved.BaseURL, "/") + "/chat/completions",
			APIKey:    resolved.APIKey,
			Model:     modelID,
			CostModel: model,
			Cache:     catalog.Cache(resolved.Provider, resolved.Model),

			HTTPClient: http.DefaultClient,
		}, nil
	case catalog.APIAnthropicMessages:
		modelID := resolved.Model
		if model.AnthropicModel != "" {
			modelID = model.AnthropicModel
		}
		return AnthropicProvider{
			Endpoint:   strings.TrimRight(resolved.BaseURL, "/") + "/messages",
			APIKey:     resolved.APIKey,
			Model:      modelID,
			MaxTokens:  model.MaxTokens,
			CostModel:  model,
			Cache:      catalog.Cache(resolved.Provider, resolved.Model),
			HTTPClient: http.DefaultClient,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported provider %q", resolved.Provider)
	}
}
