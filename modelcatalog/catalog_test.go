package modelcatalog

import (
	"math"
	"testing"

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
