package adapters

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/provider"
)

func NewProvider(cfg config.Config) (provider.Provider, error) {
	resolved, err := config.Resolve(cfg, nil)
	if err != nil {
		return nil, err
	}
	model, _ := modelcatalog.FindModel(resolved.Provider, resolved.Model)
	switch modelcatalog.APIKind(resolved.Provider) {
	case modelcatalog.APIFake:
		return FakeProvider{Response: cfg.FakeResponse}, nil
	case modelcatalog.APIOpenAIChat:
		modelID := resolved.Model
		if model.OpenAIModelID != "" {
			modelID = model.OpenAIModelID
		}
		return OpenAICompatibleProvider{
			Endpoint:  strings.TrimRight(resolved.BaseURL, "/") + "/chat/completions",
			APIKey:    resolved.APIKey,
			Model:     modelID,
			CostModel: model,
			Cache:     modelcatalog.Cache(resolved.Provider, resolved.Model),

			HTTPClient: http.DefaultClient,
		}, nil
	case modelcatalog.APIAnthropicMessages:
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
			Cache:      modelcatalog.Cache(resolved.Provider, resolved.Model),
			HTTPClient: http.DefaultClient,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported provider %q", resolved.Provider)
	}
}
