package catalog

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session/contextpolicy"
)

func TestCatalogContainsFlowerProvidersAndPiStyleMetadata(t *testing.T) {
	for _, id := range []string{ProviderOpenAI, ProviderAnthropic, ProviderGoogle, ProviderMoonshot, ProviderChatGLM, ProviderDeepSeek, ProviderQwen, ProviderOpenRouter, ProviderOllama} {
		p, ok := FindProvider(id)
		if !ok {
			t.Fatalf("provider %q not found", id)
		}
		if p.DefaultModel == "" || p.API == "" || len(p.Models) == 0 {
			t.Fatalf("provider %q missing metadata: %#v", id, p)
		}
		if _, ok := DefaultModel(id); !ok {
			t.Fatalf("provider %q default model not found", id)
		}
	}
}

func TestReasoningCapabilitiesHaveOfficialProvenance(t *testing.T) {
	for _, p := range Providers() {
		for _, model := range p.Models {
			capability := model.Reasoning
			if capability.Kind == "" {
				continue
			}
			if strings.TrimSpace(capability.WireShape) == "" {
				t.Fatalf("%s/%s reasoning capability missing wire shape", p.ID, model.ID)
			}
			if strings.TrimSpace(capability.Fixture) == "" {
				t.Fatalf("%s/%s reasoning capability missing fixture", p.ID, model.ID)
			}
			if capability.SourceCheckedAt != "2026-06-23" {
				t.Fatalf("%s/%s source_checked_at = %q", p.ID, model.ID, capability.SourceCheckedAt)
			}
			if len(capability.SourceURLs) == 0 {
				t.Fatalf("%s/%s missing source urls", p.ID, model.ID)
			}
			for _, url := range capability.SourceURLs {
				if !strings.HasPrefix(url, "https://") {
					t.Fatalf("%s/%s source URL %q is not official https URL", p.ID, model.ID, url)
				}
			}
		}
	}
}

func TestReasoningCapabilitiesUseProviderSpecificValues(t *testing.T) {
	openAI, _ := FindModel(ProviderOpenAI, "gpt-5.4")
	if openAI.Reasoning.WireShape != "openai_chat_reasoning_effort" || !slices.Contains(openAI.Reasoning.SupportedLevels, provider.ReasoningLevelXHigh) {
		t.Fatalf("openai reasoning capability = %#v", openAI.Reasoning)
	}
	deepSeek, _ := FindModel(ProviderDeepSeek, "deepseek-v4-pro")
	if !slices.Equal(deepSeek.Reasoning.SupportedLevels, []provider.ReasoningLevel{provider.ReasoningLevelHigh, provider.ReasoningLevelMax}) {
		t.Fatalf("deepseek reasoning levels = %#v", deepSeek.Reasoning.SupportedLevels)
	}
	qwen, _ := FindModel(ProviderQwen, "qwen3.6-plus")
	if qwen.Reasoning.Kind != provider.ReasoningKindToggleBudget || !qwen.Reasoning.DisableSupported || len(qwen.Reasoning.SupportedLevels) != 0 {
		t.Fatalf("qwen reasoning capability = %#v", qwen.Reasoning)
	}
	openRouter, _ := FindModel(ProviderOpenRouter, "openai/gpt-5.4")
	if !openRouter.Reasoning.DynamicProviderMetadata {
		t.Fatalf("openrouter reasoning capability should be dynamic: %#v", openRouter.Reasoning)
	}
}

func TestCatalogPredefinedModelsUseSupportedContextBaseline(t *testing.T) {
	var minMaxTokens int64
	for _, provider := range Providers() {
		for _, model := range provider.Models {
			if model.ContextWindow < contextpolicy.MinSupportedContextWindowTokens {
				t.Fatalf("%s/%s context window = %d, want at least %d", provider.ID, model.ID, model.ContextWindow, contextpolicy.MinSupportedContextWindowTokens)
			}
			if model.MaxTokens > 0 && (minMaxTokens == 0 || model.MaxTokens < minMaxTokens) {
				minMaxTokens = model.MaxTokens
			}
		}
	}
	if minMaxTokens != contextpolicy.DefaultReservedOutputTokens {
		t.Fatalf("minimum predefined max tokens = %d, want reserved output default %d", minMaxTokens, contextpolicy.DefaultReservedOutputTokens)
	}
	for _, model := range []string{"deepseek-chat", "deepseek-reasoner"} {
		if SupportsModel(ProviderDeepSeek, model) {
			t.Fatalf("%s should not be a predefined DeepSeek model", model)
		}
	}
}

func TestNormalizeProviderAcceptsFlowerAndPiAliases(t *testing.T) {
	cases := map[string]string{
		"openai_compatible": ProviderOpenAICompatible,
		"moonshotai":        ProviderMoonshot,
		"z.ai":              ProviderChatGLM,
		"zai":               ProviderChatGLM,
		"dashscope":         ProviderQwen,
		"":                  ProviderFake,
	}
	for input, want := range cases {
		if got := NormalizeProvider(input); got != want {
			t.Fatalf("NormalizeProvider(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRemovedProviderPresetsAreNotSupported(t *testing.T) {
	for _, id := range []string{"cerebras", "mistral", "together", "fireworks", "vercel-ai-gateway"} {
		t.Run(id, func(t *testing.T) {
			if SupportsProvider(id) {
				t.Fatalf("provider %q should not be supported as a built-in preset", id)
			}
			if provider, ok := FindProvider(id); ok {
				t.Fatalf("provider %q unexpectedly found: %#v", id, provider)
			}
			if models := Models(id); models != nil {
				t.Fatalf("models for %q = %#v, want nil", id, models)
			}
		})
	}
}

func TestCostForUsageUsesPerMillionTokenRates(t *testing.T) {
	model := Model{Cost: Cost{InputPerMTok: 2, OutputPerMTok: 10, CacheReadPerMTok: 0.5, CacheWritePerMTok: 3}}
	got := CostForUsage(model, provider.Usage{InputTokens: 1_000_000, OutputTokens: 500_000, CacheReadTokens: 100_000, CacheWriteTokens: 10_000})
	want := 7.08
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("cost = %.4f, want %.4f", got, want)
	}
}

func TestContextPolicyUsesModelMaxTokens(t *testing.T) {
	cleanup := RegisterForTest(Provider{
		ID:           "test-provider",
		Name:         "Test Provider",
		API:          APIOpenAIChat,
		DefaultModel: "test-model",
		Models: []Model{{
			ID:            "test-model",
			Name:          "Test Model",
			ContextWindow: 98765,
			MaxTokens:     1024,
			Input:         []string{"text"},
		}},
	})
	defer cleanup()

	policy := ContextPolicy("test-provider", "test-model")
	if policy.ContextWindowTokens != 98765 {
		t.Fatalf("context window = %d, want 98765", policy.ContextWindowTokens)
	}
	if policy.MaxOutputTokens != 1024 {
		t.Fatalf("max output = %d, want model max tokens", policy.MaxOutputTokens)
	}
	if policy.ReservedOutputTokens != 1024 {
		t.Fatalf("reserved output = %d, want min(model max tokens, default)", policy.ReservedOutputTokens)
	}
	if policy.ReservedSummaryTokens != contextpolicy.DefaultReservedSummaryTokens {
		t.Fatalf("reserved summary = %d, want default", policy.ReservedSummaryTokens)
	}
}

func TestBuiltInCatalogUsesAuditedProviderCapabilities(t *testing.T) {
	cases := []struct {
		provider string
		model    string
		context  int64
		max      int64
	}{
		{provider: ProviderOpenAI, model: "gpt-5.4", context: 1050000, max: 128000},
		{provider: ProviderGoogle, model: "gemini-3.1-pro-preview", context: 1048576, max: 65536},
		{provider: ProviderGoogle, model: "gemini-2.5-flash", context: 1048576, max: 65536},
		{provider: ProviderDeepSeek, model: "deepseek-v4-pro", context: 1000000, max: 384000},
		{provider: ProviderDeepSeek, model: "deepseek-v4-flash", context: 1000000, max: 384000},
		{provider: ProviderXAI, model: "grok-4.20-0309-reasoning", context: 1000000, max: 128000},
		{provider: ProviderXAI, model: "grok-4.20", context: 1000000, max: 128000},
	}

	for _, tt := range cases {
		t.Run(tt.provider+"/"+tt.model, func(t *testing.T) {
			model, ok := FindModel(tt.provider, tt.model)
			if !ok {
				t.Fatalf("model not found")
			}
			if model.ContextWindow != tt.context || model.MaxTokens != tt.max {
				t.Fatalf("capabilities = context %d max %d, want context %d max %d", model.ContextWindow, model.MaxTokens, tt.context, tt.max)
			}
			policy := ContextPolicy(tt.provider, tt.model)
			if policy.ContextWindowTokens != tt.context || policy.MaxOutputTokens != tt.max {
				t.Fatalf("policy = context %d max %d, want context %d max %d", policy.ContextWindowTokens, policy.MaxOutputTokens, tt.context, tt.max)
			}
		})
	}
}

func TestContextPolicyScalesDefaultCompactionBudgetsAfterModelWindow(t *testing.T) {
	cleanup := RegisterForTest(Provider{
		ID:           "small-window-provider",
		Name:         "Small Window Provider",
		API:          APIOpenAIChat,
		DefaultModel: "small-model",
		Models: []Model{{
			ID:            "small-model",
			Name:          "Small Model",
			ContextWindow: 8192,
			MaxTokens:     1024,
			Input:         []string{"text"},
		}},
	})
	defer cleanup()

	policy := ContextPolicy("small-window-provider", "small-model")
	if policy.ContextWindowTokens != 8192 || policy.MaxOutputTokens != 1024 {
		t.Fatalf("model context/max output not applied before normalize: %#v", policy)
	}
	if policy.ReservedSummaryTokens != 2048 || policy.RecentTailTokens != 2048 || policy.RecentUserTokens != 2048 {
		t.Fatalf("small model defaults should scale compaction budgets: %#v", policy)
	}
}

func TestContextPolicyKeepsUnknownAndCustomModelMaxOutputUnset(t *testing.T) {
	for _, tt := range []struct {
		provider string
		model    string
	}{
		{provider: "missing-provider", model: "missing-model"},
		{provider: ProviderOpenAICompatible, model: "custom-model"},
	} {
		t.Run(tt.provider+"/"+tt.model, func(t *testing.T) {
			policy := ContextPolicy(tt.provider, tt.model)
			if policy.MaxOutputTokens != 0 {
				t.Fatalf("max output = %d, want unset", policy.MaxOutputTokens)
			}
			if policy.ReservedOutputTokens != contextpolicy.DefaultReservedOutputTokens {
				t.Fatalf("reserved output = %d, want default budget", policy.ReservedOutputTokens)
			}
		})
	}
}
