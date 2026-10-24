package provider_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
)

// Exercise the published constructor so downstream hosts need no private catalog import.
func TestMoonshotK31PublicGatewayPreservesReasoningAndToolHistory(t *testing.T) {
	for _, model := range []string{"kimi-k3.1-pro", "kimi-k3.1-pro-highspeed"} {
		t.Run(model, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer fixture" {
					t.Errorf("wrong route or credentials: %s", r.URL.Path)
				}
				if body["model"] != model || body["max_completion_tokens"] != float64(4096) || body["reasoning_effort"] != "high" || body["max_tokens"] != nil {
					t.Errorf("wrong controls: %+v", body)
				}
				messages, ok := body["messages"].([]any)
				if !ok || len(messages) != 6 {
					t.Errorf("incomplete canonical history: %+v", body)
					return
				}
				prior := messages[1].(map[string]any)
				call := messages[3].(map[string]any)
				result := messages[4].(map[string]any)
				if prior["content"] != "Earlier answer" || prior["reasoning_content"] != "Earlier thought" || call["reasoning_content"] != "Inspect the file" || result["tool_call_id"] != "call-read" || result["content"] != "file content" {
					t.Errorf("reasoning or tool replay lost: %+v", messages)
				}
				calls := call["tool_calls"].([]any)
				if len(calls) != 1 || calls[0].(map[string]any)["id"] != "call-read" {
					t.Errorf("tool identity lost: %+v", calls)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"choices": []any{map[string]any{"message": map[string]any{"content": "Verified", "reasoning_content": "Checked the result"}, "finish_reason": "stop"}},
					"usage":   map[string]any{"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30},
				})
			}))
			defer server.Close()
			gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
				Provider: "moonshot", Model: model, BaseURL: server.URL + "/v1", APIKey: "fixture",
				HTTPClient: server.Client(), StateCompatibilityKey: "moonshot:" + model,
				Capabilities: provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{
					Kind: config.ReasoningKindEffort, SupportedLevels: []config.ReasoningLevel{config.ReasoningLevelLow, config.ReasoningLevelHigh, config.ReasoningLevelMax}, DefaultLevel: config.ReasoningLevelMax,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := gateway.Stream(t.Context(), provider.Request{
				RunID: "moonshot-run", PromptScopeID: "moonshot-scope", MaxOutputTokens: 4096,
				Reasoning: config.ReasoningSelection{Level: config.ReasoningLevelHigh},
				Messages: []provider.Message{
					{Role: provider.RoleUser, Text: "Earlier question"},
					{Role: provider.RoleAssistant, Text: "Earlier answer", Reasoning: "Earlier thought"},
					{Role: provider.RoleUser, Text: "Inspect the file"},
					{Role: provider.RoleAssistant, Reasoning: "Inspect the file", ToolCalls: []provider.ToolCall{{ID: "call-read", Name: "read", Args: "{}"}}},
					{Role: provider.RoleTool, ToolResult: &provider.ToolResult{CallID: "call-read", ToolName: "read", Text: "file content"}},
					{Role: provider.RoleUser, Text: "Verify the result"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			var reasoning, answer string
			var done bool
			for event := range stream {
				switch event.Type {
				case provider.EventReasoning:
					reasoning += event.Text
				case provider.EventDelta:
					answer += event.Text
				case provider.EventDone:
					done = true
				case provider.EventError:
					t.Fatalf("provider failed: %v", event.Err)
				}
			}
			if reasoning != "Checked the result" || answer != "Verified" || !done {
				t.Fatalf("incomplete output: %q %q done=%v", reasoning, answer, done)
			}
		})
	}
}
