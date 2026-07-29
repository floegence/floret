package adapters

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/floegence/floret/v2/internal/provider"
	"github.com/floegence/floret/v2/internal/provider/catalog"
)

type Config struct {
	Provider             string
	Model                string
	BaseURL              string
	APIKey               string
	FakeResponse         string
	PromptCacheRetention string
}

func NewProvider(configuration Config) (provider.Provider, error) {
	providerName := catalog.NormalizeProvider(configuration.Provider)
	if !catalog.SupportsProvider(providerName) {
		return nil, fmt.Errorf("unsupported provider %q", providerName)
	}
	modelName := strings.TrimSpace(configuration.Model)
	if modelName == "" {
		if model, found := catalog.DefaultModel(providerName); found {
			modelName = model.ID
		}
	}
	baseURL := strings.TrimRight(strings.TrimSpace(configuration.BaseURL), "/")
	if baseURL == "" {
		baseURL = strings.TrimRight(catalog.DefaultBaseURL(providerName), "/")
	}
	apiKey := strings.TrimSpace(configuration.APIKey)
	if apiKey == "" {
		for _, key := range catalog.EnvKeys(providerName) {
			if value := strings.TrimSpace(os.Getenv(key)); value != "" {
				apiKey = value
				break
			}
		}
	}
	model, _ := catalog.FindModel(providerName, modelName)
	switch catalog.APIKind(providerName) {
	case catalog.APIFake:
		response := configuration.FakeResponse
		if response == "" {
			response = "ok"
		}
		return FakeProvider{Response: response}, nil
	case catalog.APIOpenAIChat:
		modelID := modelName
		if model.OpenAIModelID != "" {
			modelID = model.OpenAIModelID
		}
		return OpenAICompatibleProvider{
			Endpoint: baseURL + "/chat/completions", APIKey: apiKey, Model: modelID,
			CostModel: model, Cache: catalog.Cache(providerName, modelName), HTTPClient: http.DefaultClient,
		}, nil
	case catalog.APIAnthropicMessages:
		modelID := modelName
		if model.AnthropicModel != "" {
			modelID = model.AnthropicModel
		}
		return AnthropicProvider{
			Endpoint: baseURL + "/messages", APIKey: apiKey, Model: modelID, MaxTokens: model.MaxTokens,
			CostModel: model, Cache: catalog.Cache(providerName, modelName), HTTPClient: http.DefaultClient,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported provider %q", providerName)
	}
}
