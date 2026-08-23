package provider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/floegence/floret/v5/provider"
)

func TestOpenAICompatibleGatewayUsesExplicitContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if authorization := request.Header.Get("Authorization"); authorization != "Bearer secret" {
			t.Fatalf("authorization = %q", authorization)
		}
		var body struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Model != "model-1" || len(body.Messages) != 1 || body.Messages[0].Content != "hello" {
			t.Fatalf("request body = %#v", body)
		}
		response.Header().Set("content-type", "application/json")
		_, _ = response.Write([]byte(`{"choices":[{"message":{"content":"world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	}))
	defer server.Close()

	gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
		Provider: "acme", Model: "model-1", BaseURL: server.URL + "/v1", APIKey: "secret",
		StateCompatibilityKey: "acme:model-1:v1", HTTPClient: server.Client(),
		Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	})
	if err != nil {
		t.Fatal(err)
	}
	var _ provider.Gateway = gateway
	if identity := gateway.Identity(); identity != (provider.Identity{Provider: "acme", Model: "model-1", StateCompatibilityKey: "acme:model-1:v1"}) {
		t.Fatalf("identity = %#v", identity)
	}
	events := streamGateway(t, gateway)
	if len(events) != 3 || events[0].Type != provider.EventDelta || events[0].Text != "world" || events[1].Type != provider.EventUsage || events[2].Type != provider.EventDone {
		t.Fatalf("events = %#v", events)
	}
}

func TestAnthropicGatewayUsesExplicitContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/messages" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if apiKey := request.Header.Get("x-api-key"); apiKey != "secret" {
			t.Fatalf("x-api-key = %q", apiKey)
		}
		response.Header().Set("content-type", "application/json")
		_, _ = response.Write([]byte(`{"content":[{"type":"text","text":"world"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}`))
	}))
	defer server.Close()

	gateway, err := provider.NewAnthropic(provider.AnthropicOptions{
		Provider: "anthropic", Model: "claude-test", BaseURL: server.URL + "/v1", APIKey: "secret",
		StateCompatibilityKey: "anthropic:claude-test:v1", HTTPClient: server.Client(),
		Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	})
	if err != nil {
		t.Fatal(err)
	}
	var _ provider.Gateway = gateway
	events := streamGateway(t, gateway)
	if len(events) != 3 || events[0].Type != provider.EventDelta || events[0].Text != "world" || events[1].Type != provider.EventUsage || events[2].Type != provider.EventDone {
		t.Fatalf("events = %#v", events)
	}
}

func TestOfficialGatewaysRejectImplicitConfiguration(t *testing.T) {
	validCapabilities := provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
	for name, construct := range map[string]func() error{
		"openai-compatible": func() error {
			_, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{Capabilities: validCapabilities})
			return err
		},
		"anthropic": func() error {
			_, err := provider.NewAnthropic(provider.AnthropicOptions{Capabilities: validCapabilities})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := construct(); err == nil {
				t.Fatal("incomplete configuration accepted")
			}
		})
	}
}

func streamGateway(t *testing.T, gateway provider.Gateway) []provider.Event {
	t.Helper()
	stream, err := gateway.Stream(context.Background(), provider.Request{
		RunID: "run-1", PromptScopeID: "scope-1", MaxOutputTokens: 128,
		Messages: []provider.Message{{Role: provider.RoleUser, Text: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []provider.Event
	for event := range stream {
		events = append(events, event)
	}
	return events
}
