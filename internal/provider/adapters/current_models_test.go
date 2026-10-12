package adapters

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/catalog"
	"github.com/floegence/floret/v7/internal/session"
)

func TestGeneratedModelReasoningRequests(t *testing.T) {
	for _, tc := range []struct {
		provider, model string
		level           provider.ReasoningLevel
		effort          string
	}{
		{catalog.ProviderOpenAI, "gpt-6-astra", provider.ReasoningLevelMax, "max"},
		{catalog.ProviderOpenAI, "gpt-5.6-sol", provider.ReasoningLevelOff, "none"},
		{catalog.ProviderGoogle, "gemini-3.8-flash", provider.ReasoningLevelLow, "low"},
		{catalog.ProviderMoonshot, "kimi-k3", provider.ReasoningLevelMax, "max"},
		{catalog.ProviderChatGLM, "glm-5.3", provider.ReasoningLevelHigh, "high"},
		{catalog.ProviderQwen, "qwen3.8-max", provider.ReasoningLevelXHigh, "xhigh"},
		{catalog.ProviderXAI, "grok-4.6", provider.ReasoningLevelHigh, "high"},
		{catalog.ProviderGroq, "qwen/qwen3.8-27b", provider.ReasoningLevelOff, "none"},
	} {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			model, ok := catalog.FindModel(tc.provider, tc.model)
			if !ok {
				t.Fatal("missing catalog model")
			}
			p := OpenAICompatibleProvider{Model: tc.model, CostModel: model}
			raw, err := p.chatRequestBody(provider.Request{Messages: []session.Message{{Role: session.User, Content: "hello"}}, MaxOutputTokens: 2048, Reasoning: provider.ReasoningSelection{Level: tc.level}})
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err = json.Unmarshal(raw, &body); err != nil {
				t.Fatal(err)
			}
			if body["reasoning_effort"] != tc.effort {
				t.Fatalf("request = %s", raw)
			}
			if tc.model == "kimi-k3" || tc.provider == catalog.ProviderOpenAI {
				if body["max_completion_tokens"] != float64(2048) || body["max_tokens"] != nil {
					t.Fatalf("request output limit = %s", raw)
				}
			}
		})
	}
}

func TestQwenEffortAndBudgetAreExclusive(t *testing.T) {
	model, _ := catalog.FindModel(catalog.ProviderQwen, "qwen3.8-max")
	p := OpenAICompatibleProvider{Model: model.ID, CostModel: model}
	_, err := p.chatRequestBody(provider.Request{Reasoning: provider.ReasoningSelection{Level: provider.ReasoningLevelLow, BudgetTokens: 4096}})
	if err == nil {
		t.Fatal("combined effort and budget must fail before dispatch")
	}
}

func TestAdaptiveClaudeRequest(t *testing.T) {
	model, _ := catalog.FindModel(catalog.ProviderAnthropic, "claude-fable-5-1")
	p := AnthropicProvider{Model: model.ID, CostModel: model}
	var out anthropicRequest
	if err := p.applyAnthropicReasoning(&out, provider.Request{Reasoning: provider.ReasoningSelection{Level: provider.ReasoningLevelHigh}}, 8192); err != nil {
		t.Fatal(err)
	}
	if out.Thinking == nil || out.Thinking.Type != "adaptive" || out.OutputConfig == nil || out.OutputConfig.Effort != "high" {
		t.Fatalf("request = %+v", out)
	}
	if err := p.applyAnthropicReasoning(&out, provider.Request{Reasoning: provider.ReasoningSelection{BudgetTokens: 2048}}, 8192); err == nil {
		t.Fatal("adaptive-only model accepted a budget")
	}
}

func TestHaikuAdaptiveAndDisabledRequests(t *testing.T) {
	model, ok := catalog.FindModel(catalog.ProviderAnthropic, "claude-haiku-5-5")
	if !ok {
		t.Fatal("Haiku 5.5 is missing")
	}
	p := AnthropicProvider{Model: model.ID, CostModel: model}
	for _, level := range []provider.ReasoningLevel{provider.ReasoningLevelMedium, provider.ReasoningLevelOff} {
		var out anthropicRequest
		if err := p.applyAnthropicReasoning(&out, provider.Request{Reasoning: provider.ReasoningSelection{Level: level}}, 8192); err != nil {
			t.Fatal(err)
		}
		want := "adaptive"
		if level == provider.ReasoningLevelOff {
			want = "disabled"
		}
		if out.Thinking == nil || out.Thinking.Type != want {
			t.Fatalf("%s thinking = %#v", level, out.Thinking)
		}
	}
	if err := p.applyAnthropicReasoning(&anthropicRequest{}, provider.Request{Reasoning: provider.ReasoningSelection{Level: provider.ReasoningLevelMedium, BudgetTokens: 1024}}, 8192); err == nil {
		t.Fatal("adaptive Haiku accepted a manual thinking budget")
	}
}
