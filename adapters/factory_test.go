package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func TestNewProviderCreatesFakeProviderThatRunsEngine(t *testing.T) {
	p, err := NewProvider(config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "fake ok"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "fake ok" {
		t.Fatalf("result = %#v", result)
	}
}

func TestOpenAICompatibleProviderSendsConfiguredModelAndReceivesAnswer(t *testing.T) {
	var seenModel string
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		var req struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		seenModel = req.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"remote ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{
		Provider: config.ProviderOpenAICompatible,
		Model:    "remote-model",
		BaseURL:  server.URL,
		APIKey:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Failed {
		t.Fatalf("status = %s, want failed until explicit task_complete", result.Status)
	}
	if seenModel != "remote-model" {
		t.Fatalf("model = %q, want configured model", seenModel)
	}
	if seenAuth != "Bearer secret" {
		t.Fatalf("auth = %q", seenAuth)
	}
}

func TestOpenAICompatibleProviderNormalizesUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}],
			"usage":{"prompt_tokens":120,"completion_tokens":30,"total_tokens":160,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":5},"completion_tokens_details":{"reasoning_tokens":10}}
		}`))
	}))
	defer server.Close()
	p, err := NewProvider(config.Config{Provider: config.ProviderOpenAICompatible, Model: "remote-model", BaseURL: server.URL, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.InputTokens != 100 || result.Metrics.Usage.OutputTokens != 30 || result.Metrics.Usage.CacheReadTokens != 20 || result.Metrics.Usage.CacheWriteTokens != 5 || result.Metrics.Usage.ReasoningTokens != 10 || result.Metrics.Usage.TotalTokens != 160 {
		t.Fatalf("usage = %#v", result.Metrics.Usage)
	}
}

func TestNewProviderUsesBuiltInOpenAIProviderPreset(t *testing.T) {
	var seenPath string
	var seenAuth string
	var seenModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		seenAuth = r.Header.Get("Authorization")
		var req struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		seenModel = req.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{Provider: "openai", Model: "gpt-5.4", BaseURL: server.URL, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/chat/completions" || seenAuth != "Bearer secret" || seenModel != "gpt-5.4" {
		t.Fatalf("path/auth/model = %q/%q/%q", seenPath, seenAuth, seenModel)
	}
}

func TestAnthropicProviderSendsMessagesRequestAndReceivesToolUse(t *testing.T) {
	var seenKey string
	var seenVersion string
	var seenPath string
	var seenBody struct {
		Model    string `json:"model"`
		System   string `json:"system"`
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenKey = r.Header.Get("x-api-key")
		seenVersion = r.Header.Get("anthropic-version")
		seenPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&seenBody); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"tool_use","id":"done","name":"task_complete","input":{"summary":"anthropic ok"}}],"stop_reason":"tool_use","usage":{"input_tokens":12,"output_tokens":3}}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{Provider: "anthropic", Model: "claude-sonnet-4-6", BaseURL: server.URL, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Tool{
		Name:        "task_complete",
		Description: "Complete the task.",
		Handler: func(context.Context, string) (string, error) {
			return "", nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "anthropic system"},
		Tools:    registry,
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != `{"summary":"anthropic ok"}` {
		t.Fatalf("result = %#v", result)
	}
	if seenPath != "/messages" || seenKey != "secret" || seenVersion == "" || seenBody.Model != "claude-sonnet-4-6" || seenBody.System != "anthropic system" {
		t.Fatalf("bad anthropic request path/key/version/body = %q/%q/%q/%#v", seenPath, seenKey, seenVersion, seenBody)
	}
	if len(seenBody.Messages) == 0 || seenBody.Messages[0].Role != "user" || len(seenBody.Tools) == 0 || seenBody.Tools[0].Name != "task_complete" {
		t.Fatalf("bad anthropic messages/tools = %#v", seenBody)
	}
}

func TestOpenAICompatibleProviderStreamsPartialToolArguments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"done\",\"type\":\"function\",\"function\":{\"name\":\"task_complete\",\"arguments\":\"{\\\"summary\\\"\"}}]}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"streamed\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n"))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", StreamResponses: true, HTTPClient: server.Client()}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != `{"summary":"streamed"}` {
		t.Fatalf("result = %#v", result)
	}
	if result.Metrics.Usage.TotalTokens != 15 {
		t.Fatalf("usage = %#v", result.Metrics.Usage)
	}
}

func TestOpenAICompatibleProviderMapsContextOverflowAndRedactsUnauthorizedKey(t *testing.T) {
	t.Run("context overflow", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "This model's maximum context length was exceeded by tokens", http.StatusBadRequest)
		}))
		defer server.Close()
		p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
		_, err := p.Stream(context.Background(), provider.Request{RunID: "r"})
		if !errors.Is(err, provider.ErrContextOverflow) {
			t.Fatalf("err = %v, want context overflow", err)
		}
	})
	t.Run("unauthorized status does not leak key", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer server.Close()
		p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "super-secret-key", Model: "remote-model", HTTPClient: server.Client()}
		_, err := p.Stream(context.Background(), provider.Request{RunID: "r"})
		if err == nil {
			t.Fatalf("expected unauthorized error")
		}
		if got := err.Error(); strings.Contains(got, "super-secret-key") {
			t.Fatalf("error leaked API key: %v", err)
		}
	})
}

func TestOpenAICompatibleProviderRendersToolResultRequestShape(t *testing.T) {
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()
	p := OpenAICompatibleProvider{Endpoint: server.URL, APIKey: "secret", Model: "remote-model", HTTPClient: server.Client()}
	_, err := p.Stream(context.Background(), provider.Request{RunID: "r", Messages: []session.Message{
		{Role: session.System, Content: "system"},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "read-1", ToolName: "read", ToolArgs: `{"path":"a.go"}`},
		{Role: session.Tool, Content: "content", ToolCallID: "read-1", ToolName: "read"},
	}, Tools: []provider.ToolDefinition{{Name: "read", Description: "Read a file"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 3 {
		t.Fatalf("messages = %#v", body.Messages)
	}
	if body.Messages[1]["role"] != "assistant" {
		t.Fatalf("assistant role missing: %#v", body.Messages[1])
	}
	toolCalls, ok := body.Messages[1]["tool_calls"].([]any)
	if !ok || len(toolCalls) != 1 {
		t.Fatalf("assistant tool_calls missing: %#v", body.Messages[1])
	}
	call := toolCalls[0].(map[string]any)
	fn := call["function"].(map[string]any)
	if call["id"] != "read-1" || fn["name"] != "read" || fn["arguments"] != `{"path":"a.go"}` {
		t.Fatalf("assistant tool call malformed: %#v", call)
	}
	if body.Messages[2]["role"] != "tool" || body.Messages[2]["tool_call_id"] != "read-1" || body.Messages[2]["name"] != "read" {
		t.Fatalf("tool result shape missing binding metadata: %#v", body.Messages[2])
	}
	if body.Messages[1]["tool_call_id"] != nil {
		t.Fatalf("assistant tool-call message should not use top-level tool_call_id: %#v", body.Messages[1])
	}
}

func TestOpenAICompatibleProviderMapsToolCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"remote done"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	p, err := NewProvider(config.Config{
		Provider: config.ProviderOpenAICompatible,
		Model:    "remote-model",
		BaseURL:  server.URL,
		APIKey:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "remote done" {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewProviderUsesProviderSpecificEnvKey(t *testing.T) {
	var seenAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"done","type":"function","function":{"name":"task_complete","arguments":"ok"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()

	t.Setenv("DEEPSEEK_API_KEY", "deepseek-secret")
	p, err := NewProvider(config.Config{Provider: "deepseek", Model: "deepseek-chat", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	result := (&engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "test"},
		Tools:    tools.NewRegistry(),
		Options:  engine.Options{RunID: "run", MaxSteps: 2},
	}).Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if seenAuth != "Bearer deepseek-secret" {
		t.Fatalf("auth = %q", seenAuth)
	}
}
