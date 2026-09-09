package catalog

import (
	_ "embed"
	"encoding/json"
)

// The embedded snapshot is generated explicitly; startup never fetches a catalog.
//
//go:embed models.generated.json
var generatedCatalog []byte

func init() {
	var snapshot struct {
		Providers map[string][]Model `json:"providers"`
	}
	if err := json.Unmarshal(generatedCatalog, &snapshot); err != nil {
		panic(err)
	}
	for i := range providers {
		if models, ok := snapshot.Providers[providers[i].ID]; ok {
			providers[i].Models = models
		}
	}
}

var providers = []Provider{
	{
		ID:             ProviderOpenAI,
		Name:           "OpenAI",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.openai.com/v1",
		DefaultModel:   "gpt-5.4",
		EnvKeys:        []string{"OPENAI_API_KEY"},
		Custom:         true,
		Cache:          CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
	},
	{
		ID:             ProviderAnthropic,
		Name:           "Anthropic",
		API:            APIAnthropicMessages,
		DefaultBaseURL: "https://api.anthropic.com/v1",
		DefaultModel:   "claude-sonnet-4-6",
		EnvKeys:        []string{"ANTHROPIC_API_KEY"},
		Custom:         true,
		Cache:          CacheCapability{AnthropicCacheControl: true},
		WebSearch: WebSearchCapability{
			DefaultSource:    "provider_hosted",
			HostedWireShape:  "anthropic_server_web_search",
			HostedWireShapes: []string{"anthropic_server_web_search"},
		},
	},
	{
		ID:             ProviderGoogle,
		Name:           "Google Gemini",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://generativelanguage.googleapis.com/v1beta/openai",
		DefaultModel:   "gemini-3.1-pro-preview",
		EnvKeys:        []string{"GEMINI_API_KEY", "GOOGLE_API_KEY"},
	},
	{
		ID:             ProviderMoonshot,
		Name:           "Moonshot",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.moonshot.cn/v1",
		DefaultModel:   "kimi-k2.6",
		EnvKeys:        []string{"MOONSHOT_API_KEY", "KIMI_API_KEY"},
	},
	{
		ID:             ProviderChatGLM,
		Name:           "ChatGLM / Z.ai",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.z.ai/api/paas/v4",
		DefaultModel:   "glm-5.3",
		EnvKeys:        []string{"ZAI_API_KEY", "CHATGLM_API_KEY"},
		Custom:         true,
	},
	{
		ID:             ProviderDeepSeek,
		Name:           "DeepSeek",
		API:            APIOpenAIResponses,
		WebSearch:      WebSearchCapability{DefaultSource: "provider_hosted", HostedWireShape: "deepseek_responses_web_search", HostedWireShapes: []string{"deepseek_responses_web_search"}},
		DefaultBaseURL: "https://api.deepseek.com",
		DefaultModel:   "deepseek-v4-pro",
		EnvKeys:        []string{"DEEPSEEK_API_KEY"},
	},
	{
		ID:             ProviderQwen,
		Name:           "Qwen",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
		DefaultModel:   "qwen3.6-plus",
		EnvKeys:        []string{"DASHSCOPE_API_KEY", "QWEN_API_KEY", "ALIBABA_CLOUD_API_KEY"},
	},
	{
		ID:             ProviderOpenRouter,
		Name:           "OpenRouter",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://openrouter.ai/api/v1",
		DefaultModel:   "",
		EnvKeys:        []string{"OPENROUTER_API_KEY"},
	},
	{
		ID:             ProviderXAI,
		Name:           "xAI",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.x.ai/v1",
		DefaultModel:   "grok-4.6",
		EnvKeys:        []string{"XAI_API_KEY"},
	},
	{
		ID:             ProviderGroq,
		Name:           "Groq",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.groq.com/openai/v1",
		DefaultModel:   "openai/gpt-oss-120b",
		EnvKeys:        []string{"GROQ_API_KEY"},
		Custom:         true,
	},
	{
		ID:             ProviderOllama,
		Name:           "Ollama",
		API:            APIOpenAIChat,
		DefaultBaseURL: "http://127.0.0.1:11434/v1",
		Custom:         true,
	},
	{
		ID:             ProviderOpenAICompatible,
		Name:           "OpenAI-compatible",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.example.com/v1",
		DefaultModel:   "custom-model",
		EnvKeys:        []string{"FLORET_API_KEY", "OPENAI_COMPATIBLE_API_KEY"},
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: []string{"text"}},
		},
	},
}
