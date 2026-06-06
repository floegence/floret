package modelcatalog

import (
	"math"
	"testing"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/provider"
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
