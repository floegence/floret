package catalog

import "github.com/floegence/floret/internal/provider"

var text = []string{"text"}
var vision = []string{"text", "image"}

var openAIReasoning = provider.ReasoningCapability{
	Kind:            provider.ReasoningKindEffort,
	SupportedLevels: []provider.ReasoningLevel{provider.ReasoningLevelMinimal, provider.ReasoningLevelLow, provider.ReasoningLevelMedium, provider.ReasoningLevelHigh, provider.ReasoningLevelXHigh},
	DefaultLevel:    provider.ReasoningLevelMedium,
	WireShape:       "openai_chat_reasoning_effort",
	ResponseReasoningFields: []string{
		"completion_tokens_details.reasoning_tokens",
	},
	SourceURLs:      []string{"https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create", "https://developers.openai.com/api/docs/guides/reasoning"},
	SourceCheckedAt: "2026-06-23",
	Fixture:         "openai_chat_reasoning_effort",
}

var anthropicReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindEffortBudget,
	SupportedLevels:  []provider.ReasoningLevel{provider.ReasoningLevelLow, provider.ReasoningLevelMedium, provider.ReasoningLevelHigh, provider.ReasoningLevelXHigh, provider.ReasoningLevelMax},
	DefaultLevel:     provider.ReasoningLevelHigh,
	DisableSupported: true,
	Budget:           provider.ReasoningBudget{MinTokens: 1024},
	WireShape:        "anthropic_output_config_effort",
	DisableShape:     "anthropic_thinking_disabled",
	BudgetShape:      "anthropic_thinking_budget_tokens",
	SourceURLs:       []string{"https://platform.claude.com/docs/en/api/messages/create", "https://platform.claude.com/docs/en/build-with-claude/extended-thinking", "https://platform.claude.com/docs/en/build-with-claude/effort"},
	SourceCheckedAt:  "2026-06-23",
	Fixture:          "anthropic_reasoning_effort_budget",
}

var gemini3OpenAICompatibleReasoning = provider.ReasoningCapability{
	Kind:            provider.ReasoningKindEffort,
	SupportedLevels: []provider.ReasoningLevel{provider.ReasoningLevelMinimal, provider.ReasoningLevelLow, provider.ReasoningLevelMedium, provider.ReasoningLevelHigh},
	DefaultLevel:    provider.ReasoningLevelMedium,
	WireShape:       "openai_chat_reasoning_effort",
	SourceURLs:      []string{"https://ai.google.dev/gemini-api/docs/generate-content/thinking", "https://ai.google.dev/gemini-api/docs/openai"},
	SourceCheckedAt: "2026-06-23",
	Fixture:         "gemini_3_thinking_level",
}

var gemini25ProBudgetReasoning = provider.ReasoningCapability{
	Kind:            provider.ReasoningKindBudget,
	DefaultEnabled:  boolPtr(true),
	Budget:          provider.ReasoningBudget{MinTokens: 128, MaxTokens: 32768},
	BudgetShape:     "gemini_thinking_budget",
	WireShape:       "gemini_openai_thinking_budget",
	SourceURLs:      []string{"https://ai.google.dev/gemini-api/docs/generate-content/thinking", "https://ai.google.dev/gemini-api/docs/openai"},
	SourceCheckedAt: "2026-06-23",
	Fixture:         "gemini_2_5_pro_thinking_budget",
}

var gemini25FlashBudgetReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindToggleBudget,
	DisableSupported: true,
	DefaultEnabled:   boolPtr(true),
	Budget:           provider.ReasoningBudget{MaxTokens: 24576},
	BudgetShape:      "gemini_thinking_budget",
	WireShape:        "gemini_openai_thinking_budget",
	DisableShape:     "gemini_thinking_budget_zero",
	SourceURLs:       []string{"https://ai.google.dev/gemini-api/docs/generate-content/thinking", "https://ai.google.dev/gemini-api/docs/openai"},
	SourceCheckedAt:  "2026-06-23",
	Fixture:          "gemini_2_5_flash_thinking_budget",
}

var kimiToggleReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindToggle,
	DisableSupported: true,
	DefaultEnabled:   boolPtr(true),
	WireShape:        "kimi_thinking_type",
	DisableShape:     "kimi_thinking_disabled",
	ResponseReasoningFields: []string{
		"reasoning_content",
	},
	HistoryReplayRequirements: []string{"reasoning_content"},
	SourceURLs:                []string{"https://platform.kimi.com/docs/api/chat", "https://platform.kimi.com/docs/guide/use-kimi-k2-thinking-model", "https://platform.kimi.com/docs/guide/kimi-k2-6-quickstart"},
	SourceCheckedAt:           "2026-06-23",
	Fixture:                   "kimi_thinking_type",
}

var deepSeekReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindEffort,
	SupportedLevels:  []provider.ReasoningLevel{provider.ReasoningLevelHigh, provider.ReasoningLevelMax},
	DefaultLevel:     provider.ReasoningLevelHigh,
	DisableSupported: true,
	WireShape:        "deepseek_reasoning_effort",
	DisableShape:     "deepseek_thinking_disabled",
	ResponseReasoningFields: []string{
		"reasoning_content",
		"completion_tokens_details.reasoning_tokens",
	},
	HistoryReplayRequirements: []string{"reasoning_content"},
	SourceURLs:                []string{"https://api-docs.deepseek.com/guides/thinking_mode", "https://api-docs.deepseek.com/api/create-chat-completion"},
	SourceCheckedAt:           "2026-06-23",
	Fixture:                   "deepseek_reasoning_effort",
}

var qwenThinkingBudgetReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindToggleBudget,
	DisableSupported: true,
	DefaultEnabled:   boolPtr(true),
	BudgetShape:      "qwen_thinking_budget",
	WireShape:        "qwen_enable_thinking",
	DisableShape:     "qwen_enable_thinking_false",
	SourceURLs:       []string{"https://help.aliyun.com/zh/model-studio/deep-thinking", "https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions", "https://help.aliyun.com/zh/model-studio/qwen-api-via-dashscope"},
	SourceCheckedAt:  "2026-06-23",
	Fixture:          "qwen_enable_thinking_budget",
}

var glmThinkingToggleReasoning = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindToggle,
	DisableSupported: true,
	DefaultEnabled:   boolPtr(true),
	WireShape:        "glm_thinking_type",
	DisableShape:     "glm_thinking_disabled",
	SourceURLs:       []string{"https://docs.z.ai/api-reference/llm/chat-completion", "https://docs.z.ai/guides/capabilities/thinking", "https://docs.z.ai/guides/capabilities/thinking-mode"},
	SourceCheckedAt:  "2026-06-23",
	Fixture:          "glm_thinking_type",
}

var glmReasoningEffort = provider.ReasoningCapability{
	Kind:             provider.ReasoningKindEffort,
	SupportedLevels:  []provider.ReasoningLevel{provider.ReasoningLevelMax, provider.ReasoningLevelXHigh, provider.ReasoningLevelHigh, provider.ReasoningLevelMedium, provider.ReasoningLevelLow, provider.ReasoningLevelMinimal, provider.ReasoningLevelOff},
	DefaultLevel:     provider.ReasoningLevelMax,
	DisableSupported: true,
	WireShape:        "glm_reasoning_effort",
	DisableShape:     "glm_reasoning_effort_none",
	SourceURLs:       []string{"https://docs.z.ai/api-reference/llm/chat-completion", "https://docs.z.ai/guides/overview/migrate-to-glm-new"},
	SourceCheckedAt:  "2026-06-23",
	Fixture:          "glm_5_2_reasoning_effort",
}

var openRouterDynamicReasoning = provider.ReasoningCapability{
	Kind:                    provider.ReasoningKindDynamic,
	DynamicProviderMetadata: true,
	WireShape:               "openrouter_reasoning_metadata",
	SourceURLs:              []string{"https://openrouter.ai/docs/api-reference/chat-completion", "https://openrouter.ai/docs/api-reference/parameters", "https://openrouter.ai/docs/guides/best-practices/reasoning-tokens", "https://openrouter.ai/api/v1/models"},
	SourceCheckedAt:         "2026-06-23",
	Fixture:                 "openrouter_model_reasoning_metadata",
}

var xAIAlwaysReasoning = provider.ReasoningCapability{
	Kind:            provider.ReasoningKindAlwaysOn,
	WireShape:       "xai_auto_reasoning",
	SourceURLs:      []string{"https://docs.x.ai/docs/guides/reasoning", "https://docs.x.ai/developers/model-capabilities/text/reasoning"},
	SourceCheckedAt: "2026-06-23",
	Fixture:         "xai_auto_reasoning",
}

var ollamaDynamicReasoning = provider.ReasoningCapability{
	Kind:                    provider.ReasoningKindDynamic,
	DynamicProviderMetadata: true,
	WireShape:               "ollama_model_family_think",
	SourceURLs:              []string{"https://docs.ollama.com/capabilities/thinking", "https://docs.ollama.com/api/chat", "https://docs.ollama.com/api/openai-compatibility"},
	SourceCheckedAt:         "2026-06-23",
	Fixture:                 "ollama_model_family_think",
}

func boolPtr(v bool) *bool {
	return &v
}

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
			{ID: "gpt-5.5", Name: "GPT-5.5", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning, Cost: Cost{InputPerMTok: 15, OutputPerMTok: 120}},
			{ID: "gpt-5.4", Name: "GPT-5.4", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning, Cost: Cost{InputPerMTok: 10, OutputPerMTok: 80}},
			{ID: "gpt-5.4-mini", Name: "GPT-5.4 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning, Cost: Cost{InputPerMTok: 1.25, OutputPerMTok: 10}},
			{ID: "gpt-5.4-nano", Name: "GPT-5.4 nano", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning, Cost: Cost{InputPerMTok: 0.25, OutputPerMTok: 2}},
			{ID: "gpt-5.2", Name: "GPT-5.2", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning},
			{ID: "gpt-5.2-mini", Name: "GPT-5.2 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning},
			{ID: "gpt-5", Name: "GPT-5", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning},
			{ID: "gpt-5-mini", Name: "GPT-5 mini", ContextWindow: 400000, MaxTokens: 128000, Input: vision, Reasoning: openAIReasoning},
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
			{ID: "claude-opus-4-7", Name: "Claude Opus 4.7", ContextWindow: 1000000, MaxTokens: 128000, Input: vision, Reasoning: anthropicReasoning, Cost: Cost{InputPerMTok: 15, OutputPerMTok: 75}},
			{ID: "claude-sonnet-4-6", Name: "Claude Sonnet 4.6", ContextWindow: 1000000, MaxTokens: 64000, Input: vision, Reasoning: anthropicReasoning, Cost: Cost{InputPerMTok: 3, OutputPerMTok: 15}},
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
			{ID: "gemini-3.1-pro-preview", Name: "Gemini 3.1 Pro Preview", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: gemini3OpenAICompatibleReasoning},
			{ID: "gemini-3.1-flash-preview", Name: "Gemini 3.1 Flash Preview", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: gemini3OpenAICompatibleReasoning},
			{ID: "gemini-2.5-pro", Name: "Gemini 2.5 Pro", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: gemini25ProBudgetReasoning},
			{ID: "gemini-2.5-flash", Name: "Gemini 2.5 Flash", ContextWindow: 1048576, MaxTokens: 65536, Input: vision, Reasoning: gemini25FlashBudgetReasoning},
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
			{ID: "kimi-k2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: kimiToggleReasoning},
			{ID: "kimi-k2.6-thinking", Name: "Kimi K2.6 Thinking", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: kimiToggleReasoning},
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
			{ID: "glm-5.2", Name: "GLM 5.2", ContextWindow: 256000, MaxTokens: 64000, Input: text, Reasoning: glmReasoningEffort},
			{ID: "glm-4.5", Name: "GLM 4.5", ContextWindow: 256000, MaxTokens: 64000, Input: text, Reasoning: glmThinkingToggleReasoning},
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
			{ID: "deepseek-v4-pro", Name: "DeepSeek V4 Pro", ContextWindow: 1000000, MaxTokens: 384000, Input: text, Reasoning: deepSeekReasoning},
			{ID: "deepseek-v4-flash", Name: "DeepSeek V4 Flash", ContextWindow: 1000000, MaxTokens: 384000, Input: text, Reasoning: deepSeekReasoning},
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
			{ID: "qwen3.6-plus", Name: "Qwen 3.6 Plus", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: qwenThinkingBudgetReasoning},
			{ID: "qwen3.6-plus-2026-04-02", Name: "Qwen 3.6 Plus snapshot", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: qwenThinkingBudgetReasoning},
			{ID: "qwen3.6-flash", Name: "Qwen 3.6 Flash", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: qwenThinkingBudgetReasoning},
			{ID: "qwen3.6-flash-2026-04-16", Name: "Qwen 3.6 Flash snapshot", ContextWindow: 1000000, MaxTokens: 65536, Input: text, Reasoning: qwenThinkingBudgetReasoning},
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
			{ID: "moonshotai/kimi-k2.6", Name: "Kimi K2.6", ContextWindow: 256000, MaxTokens: 96000, Input: text, Reasoning: openRouterDynamicReasoning, OpenAIModelID: "moonshotai/kimi-k2.6"},
			{ID: "anthropic/claude-sonnet-4.6", Name: "Claude Sonnet 4.6", ContextWindow: 1000000, MaxTokens: 64000, Input: vision, Reasoning: openRouterDynamicReasoning, OpenAIModelID: "anthropic/claude-sonnet-4.6"},
			{ID: "openai/gpt-5.4", Name: "GPT-5.4", ContextWindow: 1050000, MaxTokens: 128000, Input: vision, Reasoning: openRouterDynamicReasoning, OpenAIModelID: "openai/gpt-5.4"},
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
			{ID: "grok-4.20-0309-reasoning", Name: "Grok 4.20 Reasoning", ContextWindow: 1000000, MaxTokens: 128000, Input: vision, Reasoning: xAIAlwaysReasoning},
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
		ID:             ProviderOllama,
		Name:           "Ollama",
		API:            APIOpenAIChat,
		DefaultBaseURL: "http://127.0.0.1:11434/v1",
		DefaultModel:   "custom-model",
		Custom:         true,
		Models: []Model{
			{ID: "custom-model", Name: "Custom model", ContextWindow: 256000, Input: text, Reasoning: ollamaDynamicReasoning},
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
