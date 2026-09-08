package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestDeepSeekResponsesHistorySurvivesRuntimeRestart(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input json.RawMessage                 `json:"input"`
			Tools []provider.HostedToolDefinition `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		searchDeclared := false
		for _, tool := range body.Tools {
			searchDeclared = searchDeclared || tool.Type == "web_search"
		}
		if !searchDeclared {
			t.Errorf("search not declared: %+v", body.Tools)
		}
		if count.Add(1) == 2 && !strings.Contains(string(body.Input), `"search_receipt":"opaque"`) {
			t.Errorf("opaque replay missing after restart: %s", body.Input)
		}
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response\",\"status\":\"completed\",\"output\":[{\"type\":\"web_search_call\",\"id\":\"ws1\",\"status\":\"completed\",\"search_receipt\":\"opaque\",\"action\":{\"type\":\"search\",\"query\":\"docs\"}},{\"type\":\"reasoning\",\"provider_receipt\":\"opaque\",\"content\":[{\"type\":\"reasoning_text\",\"text\":\"consider\"}]},{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}]}}\n\n")
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "restart:test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, WithAgentDynamicToolSurface(func(context.Context, ToolSurfaceRequest) (ToolSurface, error) {
		return ToolSurface{HostedToolDefinitions: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "history.db")
	open := func() (*Host, ThreadService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
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
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: UserInput{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	first := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if first.Failure != nil {
		t.Fatalf("first failed: %+v", first.Failure)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "second", Input: UserInput{Text: "again"}}); err != nil {
		t.Fatal(err)
	}
	second := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID != first.TurnID
	})
	if second.Failure != nil || count.Load() != 2 {
		t.Fatalf("restart failed: %+v requests=%d", second.Failure, count.Load())
	}
}
