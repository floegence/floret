package adapters

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/provider"
)

func NewProvider(cfg config.Config) (provider.Provider, error) {
	switch strings.ToLower(cfg.Provider) {
	case "", config.ProviderFake:
		return FakeProvider{Response: cfg.FakeResponse}, nil
	case config.ProviderOpenAICompatible:
		return OpenAICompatibleProvider{
			Endpoint:   strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions",
			APIKey:     cfg.APIKey,
			Model:      cfg.Model,
			HTTPClient: http.DefaultClient,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported provider %q", cfg.Provider)
	}
}
