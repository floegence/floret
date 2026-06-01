package adapters

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/memory"
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
