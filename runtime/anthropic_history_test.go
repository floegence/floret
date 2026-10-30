package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestAnthropicNativeHistoryThroughRuntimeRestartForkAndLength(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, raw)
		n := len(bodies)
		mu.Unlock()
		reason := "end_turn"
		if n == 1 {
			reason = "max_tokens"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": fmt.Sprintf("msg_%d", n), "content": []any{map[string]any{"type": "thinking", "thinking": fmt.Sprintf("thinking%d", n), "signature": fmt.Sprintf("signature%d", n)}, map[string]any{"type": "text", "text": fmt.Sprintf("answer%d", n)}}, "stop_reason": reason, "usage": map[string]any{"input_tokens": 10, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0, "output_tokens": 5}})
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "history.sqlite")
	open := func() (*Host, ThreadService) {
		gateway, err := provider.NewAnthropic(provider.AnthropicOptions{Provider: "custom", Model: "fixture", BaseURL: server.URL, APIKey: "test", StateCompatibilityKey: "account", Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported}})
		if err != nil {
			t.Fatal(err)
		}
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			return NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 128000, MaxOutputTokens: 4096}}, gateway)
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	view := sendNativeTurn(t, service, created.ThreadID, "first")
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if err = host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	for i, id := range []identity.ThreadID{created.ThreadID, fork.ThreadID} {
		view = sendNativeTurn(t, service, id, fmt.Sprintf("next%d", i))
		if view.Failure != nil {
			t.Fatal(view.Failure)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 4 {
		t.Fatalf("requests=%d", len(bodies))
	}
	for i := 1; i < 3; i++ {
		if !bytes.Contains(bodies[i], []byte("signature1")) {
			t.Fatalf("request %d lost committed native output: %s", i, bodies[i])
		}
	}
	if !bytes.Contains(bodies[3], []byte("answer1answer2")) || bytes.Contains(bodies[3], []byte("signature1")) {
		t.Fatal("fork must retain canonical facts and clear source continuation state")
	}
	for i := 2; i < 3; i++ {
		if !bytes.Contains(bodies[i], []byte("signature2")) {
			t.Fatalf("request %d lost continuation native output", i)
		}
	}
}
