package catalog

import (
	"slices"
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session/contextpolicy"
)

const (
	ProviderFake             = "fake"
	ProviderOpenAI           = "openai"
	ProviderAnthropic        = "anthropic"
	ProviderGoogle           = "google"
	ProviderMoonshot         = "moonshot"
	ProviderChatGLM          = "chatglm"
	ProviderDeepSeek         = "deepseek"
	ProviderQwen             = "qwen"
	ProviderOpenRouter       = "openrouter"
	ProviderXAI              = "xai"
	ProviderGroq             = "groq"
	ProviderCerebras         = "cerebras"
	ProviderMistral          = "mistral"
	ProviderTogether         = "together"
	ProviderFireworks        = "fireworks"
	ProviderVercelAIGateway  = "vercel-ai-gateway"
	ProviderOllama           = "ollama"
	ProviderOpenAICompatible = "openai-compatible"
)

const (
	APIFake              = "fake"
	APIOpenAIChat        = "openai-chat-completions"
	APIAnthropicMessages = "anthropic-messages"
)

type Cost struct {
	InputPerMTok      float64 `json:"input_per_mtok,omitempty"`
	OutputPerMTok     float64 `json:"output_per_mtok,omitempty"`
	CacheReadPerMTok  float64 `json:"cache_read_per_mtok,omitempty"`
	CacheWritePerMTok float64 `json:"cache_write_per_mtok,omitempty"`
}

type CacheCapability struct {
	PromptCacheKey        bool `json:"prompt_cache_key,omitempty"`
	PromptCacheRetention  bool `json:"prompt_cache_retention,omitempty"`
	AnthropicCacheControl bool `json:"anthropic_cache_control,omitempty"`
}

type WebSearchCapability struct {
	DefaultSource    string   `json:"default_source,omitempty"`
	HostedWireShape  string   `json:"hosted_wire_shape,omitempty"`
	HostedWireShapes []string `json:"hosted_wire_shapes,omitempty"`
}

type Model struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Provider       string          `json:"provider"`
	API            string          `json:"api"`
	ContextWindow  int64           `json:"context_window,omitempty"`
	MaxTokens      int64           `json:"max_tokens,omitempty"`
	Input          []string        `json:"input"`
	Reasoning      bool            `json:"reasoning"`
	Cost           Cost            `json:"cost,omitempty"`
	Cache          CacheCapability `json:"cache,omitempty"`
	Default        bool            `json:"default,omitempty"`
	OpenAIModelID  string          `json:"openai_model_id,omitempty"`
	AnthropicModel string          `json:"anthropic_model,omitempty"`
}

type Provider struct {
	ID             string              `json:"id"`
	Name           string              `json:"name"`
	API            string              `json:"api"`
	DefaultBaseURL string              `json:"default_base_url,omitempty"`
	DefaultModel   string              `json:"default_model,omitempty"`
	EnvKeys        []string            `json:"env_keys,omitempty"`
	Custom         bool                `json:"custom,omitempty"`
	Cache          CacheCapability     `json:"cache,omitempty"`
	WebSearch      WebSearchCapability `json:"web_search,omitempty"`
	Models         []Model             `json:"models"`
}

func Providers() []Provider {
	out := make([]Provider, len(providers))
	copy(out, providers)
	for i := range out {
		out[i].Models = modelsForProvider(out[i])
	}
	return out
}

func FindProvider(id string) (Provider, bool) {
	id = NormalizeProvider(id)
	for _, p := range providers {
		if p.ID == id {
			p.Models = modelsForProvider(p)
			return p, true
		}
	}
	return Provider{}, false
}

func Models(providerID string) []Model {
	p, ok := FindProvider(providerID)
	if !ok {
		return nil
	}
	return p.Models
}

func FindModel(providerID, modelID string) (Model, bool) {
	providerID = NormalizeProvider(providerID)
	modelID = strings.TrimSpace(modelID)
	for _, model := range Models(providerID) {
		if model.ID == modelID {
			return model, true
		}
	}
	return Model{}, false
}

func DefaultModel(providerID string) (Model, bool) {
	p, ok := FindProvider(providerID)
	if !ok {
		return Model{}, false
	}
	if p.DefaultModel != "" {
		if model, ok := FindModel(p.ID, p.DefaultModel); ok {
			return model, true
		}
	}
	if len(p.Models) > 0 {
		return p.Models[0], true
	}
	return Model{}, false
}

func NormalizeProvider(id string) string {
	id = strings.TrimSpace(strings.ToLower(id))
	id = strings.ReplaceAll(id, "_", "-")
	switch id {
	case "", "local-fake":
		return ProviderFake
	case "openai-compatible", "openai-compatible-chat", "custom":
		return ProviderOpenAICompatible
	case "zai", "z.ai", "glm":
		return ProviderChatGLM
	case "moonshotai", "moonshot-ai", "kimi":
		return ProviderMoonshot
	case "gemini", "google-generative-ai":
		return ProviderGoogle
	case "dashscope", "aliyun", "alibaba":
		return ProviderQwen
	default:
		return id
	}
}

func DefaultBaseURL(providerID string) string {
	p, ok := FindProvider(providerID)
	if !ok {
		return ""
	}
	return p.DefaultBaseURL
}

func APIKind(providerID string) string {
	p, ok := FindProvider(providerID)
	if !ok {
		return ""
	}
	return p.API
}

func EnvKeys(providerID string) []string {
	p, ok := FindProvider(providerID)
	if !ok {
		return nil
	}
	out := make([]string, len(p.EnvKeys))
	copy(out, p.EnvKeys)
	return out
}

func Cache(providerID, modelID string) CacheCapability {
	if model, ok := FindModel(providerID, modelID); ok {
		if model.Cache != (CacheCapability{}) {
			return model.Cache
		}
	}
	if provider, ok := FindProvider(providerID); ok {
		return provider.Cache
	}
	return CacheCapability{}
}

func WebSearch(providerID string) WebSearchCapability {
	if provider, ok := FindProvider(providerID); ok {
		return provider.WebSearch
	}
	return WebSearchCapability{}
}

func SupportsProvider(id string) bool {
	_, ok := FindProvider(id)
	return ok
}

func SupportsModel(providerID, modelID string) bool {
	_, ok := FindModel(providerID, modelID)
	return ok
}

func CostForUsage(model Model, usage provider.Usage) float64 {
	usage = usage.Normalized()
	return (float64(usage.InputTokens)*model.Cost.InputPerMTok +
		float64(usage.OutputTokens)*model.Cost.OutputPerMTok +
		float64(usage.CacheReadTokens)*model.Cost.CacheReadPerMTok +
		float64(usage.CacheWriteTokens)*model.Cost.CacheWritePerMTok) / 1_000_000
}

func ContextPolicy(providerID, modelID string) contextpolicy.Policy {
	policy := contextpolicy.Policy{}
	if model, ok := FindModel(providerID, modelID); ok {
		if model.ContextWindow > 0 {
			policy.ContextWindowTokens = model.ContextWindow
		}
		if model.MaxTokens > 0 {
			policy.MaxOutputTokens = model.MaxTokens
			policy.ReservedOutputTokens = minInt64(model.MaxTokens, contextpolicy.DefaultReservedOutputTokens)
		}
	}
	return contextpolicy.Normalize(policy)
}

func modelsForProvider(p Provider) []Model {
	out := make([]Model, 0, len(p.Models))
	for _, model := range p.Models {
		model.Provider = p.ID
		if model.API == "" {
			model.API = p.API
		}
		if len(model.Input) == 0 {
			model.Input = []string{"text"}
		}
		if model.ID == p.DefaultModel {
			model.Default = true
		}
		out = append(out, model)
	}
	return out
}

func providerByID(id string) *Provider {
	id = NormalizeProvider(id)
	idx := slices.IndexFunc(providers, func(p Provider) bool { return p.ID == id })
	if idx < 0 {
		return nil
	}
	return &providers[idx]
}

func RegisterForTest(p Provider) func() {
	previous := providers
	providers = append(providers, p)
	return func() { providers = previous }
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
