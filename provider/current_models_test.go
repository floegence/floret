package provider_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
)

func TestCurrentChatModelsThroughPublicGateway(t *testing.T) {
	for _, tc := range []struct {
		provider, model string
		level           config.ReasoningLevel
	}{
		{"chatglm", "glm-5.2", config.ReasoningLevelOff},
		{"google", "gemini-4-argon", config.ReasoningLevelMedium},
		{"google", "gemini-4-carbon", config.ReasoningLevelHigh},
	} {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				if r.URL.Path != "/v1/chat/completions" || body["model"] != tc.model || body["max_tokens"] != float64(2048) {
					t.Errorf("wrong route or model controls: %s %+v", r.URL.Path, body)
				}
				if tc.level == config.ReasoningLevelOff {
					thinking, _ := body["thinking"].(map[string]any)
					if thinking["type"] != "disabled" || body["reasoning_effort"] != nil {
						t.Errorf("invalid GLM thinking controls: %+v", body)
					}
				} else if body["reasoning_effort"] != string(tc.level) {
					t.Errorf("lost reasoning effort: %+v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"Verified"},"finish_reason":"stop"}]}`))
			}))
			defer server.Close()
			gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
				Provider: tc.provider, Model: tc.model, BaseURL: server.URL + "/v1", APIKey: "fixture",
				HTTPClient: server.Client(), StateCompatibilityKey: tc.provider + ":" + tc.model,
				Capabilities: provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{
					Kind: config.ReasoningKindEffort, SupportedLevels: []config.ReasoningLevel{tc.level}, DisableSupported: tc.level == config.ReasoningLevelOff,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := gateway.Stream(t.Context(), provider.Request{
				RunID: "catalog-run", PromptScopeID: "catalog-scope", MaxOutputTokens: 2048,
				Reasoning: config.ReasoningSelection{Level: tc.level},
				Messages:  []provider.Message{{Role: provider.RoleUser, Text: "Verify the model"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			var answer string
			var done bool
			for event := range stream {
				switch event.Type {
				case provider.EventDelta:
					answer += event.Text
				case provider.EventDone:
					done = true
				case provider.EventError:
					t.Fatal(event.Err)
				}
			}
			if answer != "Verified" || !done {
				t.Fatalf("incomplete provider output: %q done=%v", answer, done)
			}
		})
	}
}
