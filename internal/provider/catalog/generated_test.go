package catalog

import (
	"slices"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
)

func TestCurrentAgentCatalog(t *testing.T) {
	for _, tc := range []struct {
		provider, model string
		image           bool
	}{
		{ProviderOpenAI, "gpt-6-astra", true}, {ProviderOpenAI, "gpt-5.6-sol", true},
		{ProviderAnthropic, "claude-fable-5-1", true}, {ProviderGoogle, "gemini-3.8-flash", true},
		{ProviderMoonshot, "kimi-k3", true}, {ProviderChatGLM, "glm-5.3-flash", true},
		{ProviderDeepSeek, "deepseek-v4-flash-vision-exp", true}, {ProviderQwen, "qwen3.8-max", true},
		{ProviderXAI, "grok-4.6", true}, {ProviderGroq, "qwen/qwen3.8-27b", true},
	} {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			model, ok := FindModel(tc.provider, tc.model)
			if !ok {
				t.Fatal("current agent model missing")
			}
			if slices.Contains(model.Input, "image") != tc.image {
				t.Fatalf("input = %v", model.Input)
			}
		})
	}
	for pid, ids := range map[string][]string{
		ProviderOpenAI:   {"gpt-5.2-mini", "gpt-realtime-2.1"},
		ProviderMoonshot: {"kimi-k2.6-thinking", "kimi-k2.5"},
		ProviderGoogle:   {"gemini-3.1-flash-preview", "gemini-3.1-flash-live-preview"},
		ProviderGroq:     {"qwen/qwen3-32b", "llama-3.3-70b-versatile"},
		ProviderQwen:     {"deepseek-v4-flash-0731"},
	} {
		for _, id := range ids {
			if SupportsModel(pid, id) {
				t.Errorf("unexpected model %s/%s", pid, id)
			}
		}
	}
}

func TestCurrentReasoningBoundaries(t *testing.T) {
	for _, tc := range []struct {
		provider, model string
		allowed, denied provider.ReasoningLevel
	}{
		{ProviderOpenAI, "gpt-6-astra", provider.ReasoningLevelMax, provider.ReasoningLevelOff},
		{ProviderGoogle, "gemini-3.8-flash", provider.ReasoningLevelLow, provider.ReasoningLevelMinimal},
		{ProviderMoonshot, "kimi-k3", provider.ReasoningLevelMax, provider.ReasoningLevelOff},
		{ProviderChatGLM, "glm-5.3", provider.ReasoningLevelMax, provider.ReasoningLevelOff},
	} {
		model, ok := FindModel(tc.provider, tc.model)
		if !ok {
			t.Errorf("missing model %s/%s", tc.provider, tc.model)
			continue
		}
		if !model.Reasoning.SupportsLevel(tc.allowed) || model.Reasoning.SupportsLevel(tc.denied) {
			t.Errorf("%s/%s: invalid reasoning capability: %+v", tc.provider, tc.model, model.Reasoning)
		}
	}
}
