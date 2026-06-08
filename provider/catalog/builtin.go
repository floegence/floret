package catalog

var text = []string{"text"}
var vision = []string{"text", "image"}

// Model capabilities are a hand-maintained catalog used to seed context policy
// defaults. Prefer provider-owned model pages for ContextWindow and MaxTokens.
// When a provider page only publishes a context/input limit, keep any MaxTokens
// value as a conservative request cap and do not describe it as an official
// output-token limit.
var providers = []Provider{
	{
		ID:           ProviderFake,
		Name:         "Fake",
		API:          APIFake,
		DefaultModel: "fake-model",
		Custom:       true,
		Models: []Model{
			{ID: "fake-model", Name: "Fake model", ContextWindow: 256000, MaxTokens: 64000, Input: text},
		},
	},
	{
		ID:             ProviderOpenAI,
		Name:           "OpenAI",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.openai.com/v1",
		DefaultModel:   "gpt-5.4",
		EnvKeys:        []string{"OPENAI_API_KEY"},
		Custom:         true,
		Cache:          CacheCapability{PromptCacheKey: true, PromptCacheRetention: true},
		// Source: OpenAI model detail pages such as
		// https://developers.openai.com/api/docs/models/gpt-5.4 list context
		// window and max output tokens.
		Models: []Model{
			{ID: "gpt-5.5", Name: "GPT-5.5", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 15, OutputPerMTok: 120}},
			{ID: "gpt-5.4", Name: "GPT-5.4", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 10, OutputPerMTok: 80}},
			{ID: "gpt-5.4-mini", Name: "GPT-5.4 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 1.25, OutputPerMTok: 10}},
			{ID: "gpt-5.4-nano", Name: "GPT-5.4 nano", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 0.25, OutputPerMTok: 2}},
			{ID: "gpt-5.2", Name: "GPT-5.2", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true},
			{ID: "gpt-5.2-mini", Name: "GPT-5.2 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true},
			{ID: "gpt-5", Name: "GPT-5", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true},
			{ID: "gpt-5-mini", Name: "GPT-5 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: true},
		},
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
		// Source: Anthropic's model overview
		// https://platform.claude.com/docs/en/about-claude/models/overview
		// publishes context windows and max output tokens for Claude 4.x/4.5+
		// models.
		Models: []Model{
			{ID: "claude-opus-4-7", Name: "Claude Opus 4.7", ContextWindow: 1000000, MaxTokens: 128000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 15, OutputPerMTok: 75}},
			{ID: "claude-sonnet-4-6", Name: "Claude Sonnet 4.6", ContextWindow: 1000000, MaxTokens: 64000, Input: vision, Reasoning: true, Cost: Cost{InputPerMTok: 3, OutputPerMTok: 15}},
		},
	},
	{
		ID:             ProviderGoogle,
		Name:           "Google Gemini",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://generativelanguage.googleapis.com/v1beta/openai",
		DefaultModel:   "gemini-3.1-pro-preview",
		EnvKeys:        []string{"GEMINI_API_KEY", "GOOGLE_API_KEY"},
		Models: []Model{
			// Source: Google Gemini model pages, for example
			// https://ai.google.dev/gemini-api/docs/models/gemini-2.5-pro, publish
			// "Input token limit" as 1,048,576 and "Output token limit" as 65,536.
			// The 3.1 Flash Preview entry is retained from the provider catalog; its
			// public model page was unavailable during audit.
			{ID: "gemini-3.1-pro-preview", Name: "Gemini 3.1 Pro Preview", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: true},
			{ID: "gemini-3.1-flash-preview", Name: "Gemini 3.1 Flash Preview", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: true},
			{ID: "gemini-2.5-pro", Name: "Gemini 2.5 Pro", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: true},
			{ID: "gemini-2.5-flash", Name: "Gemini 2.5 Flash", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: true},
		},
	},
	{
		ID:             ProviderMoonshot,
		Name:           "Moonshot",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.moonshot.cn/v1",
		DefaultModel:   "kimi-k2.6",
		EnvKeys:        []string{"MOONSHOT_API_KEY", "KIMI_API_KEY"},
		// Source: Kimi API docs at https://platform.kimi.ai/docs/overview publish
		// Kimi K2.6 as a 256K-context model. The output cap is a provider/catalog
		// request cap, not a verified public max-output value.
		Models: []Model{
			{ID: "kimi-k2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true},
			{ID: "kimi-k2.6-thinking", Name: "Kimi K2.6 Thinking", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true},
		},
	},
	{
		ID:             ProviderChatGLM,
		Name:           "ChatGLM / Z.ai",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.z.ai/api/paas/v4",
		DefaultModel:   "custom-model",
		EnvKeys:        []string{"ZAI_API_KEY", "CHATGLM_API_KEY"},
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
	},
	{
		ID:             ProviderDeepSeek,
		Name:           "DeepSeek",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.deepseek.com",
		DefaultModel:   "deepseek-v4-pro",
		EnvKeys:        []string{"DEEPSEEK_API_KEY"},
		// Source: DeepSeek Models & Pricing
		// https://api-docs.deepseek.com/quick_start/pricing lists V4 context
		// length as 1M and max output as 384K.
		Models: []Model{
			{ID: "deepseek-v4-pro", Name: "DeepSeek V4 Pro", ContextWindow: 1000000, MaxTokens: 384000, Input: text, Reasoning: true},
			{ID: "deepseek-v4-flash", Name: "DeepSeek V4 Flash", ContextWindow: 1000000, MaxTokens: 384000, Input: text, Reasoning: true},
		},
	},
	{
		ID:             ProviderQwen,
		Name:           "Qwen",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
		DefaultModel:   "qwen3.6-plus",
		EnvKeys:        []string{"DASHSCOPE_API_KEY", "QWEN_API_KEY", "ALIBABA_CLOUD_API_KEY"},
		// Source: Qwen Cloud model pages such as
		// https://www.qwencloud.com/models/qwen3.6-plus list 1M context and
		// 65.53K max output for Qwen 3.6 Plus/Flash. Store 65.53K as the exact
		// 65,536-token cap.
		Models: []Model{
			{ID: "qwen3.6-plus", Name: "Qwen 3.6 Plus", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: true},
			{ID: "qwen3.6-plus-2026-04-02", Name: "Qwen 3.6 Plus snapshot", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: true},
			{ID: "qwen3.6-flash", Name: "Qwen 3.6 Flash", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: true},
			{ID: "qwen3.6-flash-2026-04-16", Name: "Qwen 3.6 Flash snapshot", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: true},
		},
	},
	{
		ID:             ProviderOpenRouter,
		Name:           "OpenRouter",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://openrouter.ai/api/v1",
		DefaultModel:   "moonshotai/kimi-k2.6",
		EnvKeys:        []string{"OPENROUTER_API_KEY"},
		Models: []Model{
			// OpenRouter model limits mirror their upstream providers here when
			// upstream official docs publish the limit; Kimi output remains a
			// provider/catalog request cap.
			{ID: "moonshotai/kimi-k2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true, OpenAIModelID: "moonshotai/kimi-k2.6"},
			{ID: "anthropic/claude-sonnet-4.6", Name: "Claude Sonnet 4.6", ContextWindow: 1000000, MaxTokens: 64000, Input: vision, Reasoning: true, OpenAIModelID: "anthropic/claude-sonnet-4.6"},
			{ID: "openai/gpt-5.4", Name: "GPT-5.4", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: true, OpenAIModelID: "openai/gpt-5.4"},
		},
	},
	{
		ID:             ProviderXAI,
		Name:           "xAI",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.x.ai/v1",
		DefaultModel:   "grok-4.20-0309-reasoning",
		EnvKeys:        []string{"XAI_API_KEY"},
		// Source: xAI model docs at https://docs.x.ai/docs/models/grok-4.20 expose
		// maxPromptLength=1,000,000 for Grok 4.20. The output cap is a
		// provider/catalog request cap because the public page does not publish a
		// separate max-output limit.
		Models: []Model{
			{ID: "grok-4.20-0309-reasoning", Name: "Grok 4.20 Reasoning", ContextWindow: 1000000, MaxTokens: 128000, Input: vision, Reasoning: true},
			{ID: "grok-4.20", Name: "Grok 4.20", ContextWindow: 1000000, MaxTokens: 128000, Input: vision},
		},
	},
	{
		ID:             ProviderGroq,
		Name:           "Groq",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.groq.com/openai/v1",
		DefaultModel:   "custom-model",
		EnvKeys:        []string{"GROQ_API_KEY"},
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
	},
	{
		ID:             ProviderCerebras,
		Name:           "Cerebras",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.cerebras.ai/v1",
		DefaultModel:   "custom-model",
		EnvKeys:        []string{"CEREBRAS_API_KEY"},
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
	},
	{
		ID:             ProviderMistral,
		Name:           "Mistral",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.mistral.ai/v1",
		DefaultModel:   "custom-model",
		EnvKeys:        []string{"MISTRAL_API_KEY"},
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
	},
	{
		ID:             ProviderTogether,
		Name:           "Together",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.together.xyz/v1",
		DefaultModel:   "moonshotai/Kimi-K2.6",
		EnvKeys:        []string{"TOGETHER_API_KEY"},
		Models: []Model{
			{ID: "moonshotai/Kimi-K2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true, OpenAIModelID: "moonshotai/Kimi-K2.6"},
			{ID: "Qwen/Qwen3-Coder-480B-A35B-Instruct", Name: "Qwen3 Coder 480B", ContextWindow: 262144, MaxTokens: 131072, Input: text, OpenAIModelID: "Qwen/Qwen3-Coder-480B-A35B-Instruct"},
		},
	},
	{
		ID:             ProviderFireworks,
		Name:           "Fireworks",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://api.fireworks.ai/inference/v1",
		DefaultModel:   "accounts/fireworks/models/kimi-k2p6",
		EnvKeys:        []string{"FIREWORKS_API_KEY"},
		Models: []Model{
			{ID: "accounts/fireworks/models/kimi-k2p6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true},
			{ID: "accounts/fireworks/models/qwen3-coder-480b-a35b-instruct", Name: "Qwen3 Coder 480B", ContextWindow: 262144, MaxTokens: 131072, Input: text},
		},
	},
	{
		ID:             ProviderVercelAIGateway,
		Name:           "Vercel AI Gateway",
		API:            APIOpenAIChat,
		DefaultBaseURL: "https://ai-gateway.vercel.sh/v1",
		DefaultModel:   "moonshotai/kimi-k2.6",
		EnvKeys:        []string{"AI_GATEWAY_API_KEY"},
		Models: []Model{
			{ID: "moonshotai/kimi-k2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: true},
			{ID: "openai/gpt-5.4", Name: "GPT-5.4", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: true},
		},
	},
	{
		ID:             ProviderOllama,
		Name:           "Ollama",
		API:            APIOpenAIChat,
		DefaultBaseURL: "http://127.0.0.1:11434/v1",
		DefaultModel:   "custom-model",
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
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
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text},
		},
	},
}
