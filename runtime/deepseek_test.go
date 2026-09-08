package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/tools"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestDeepSeekResponsesHistorySurvivesRuntimeRestart(t *testing.T) {
	var count atomic.Int32
	releaseSearch := make(chan struct{})
	defer close(releaseSearch)
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
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"type":"response.reasoning_text.delta","delta":"consider"}`+"\n\n")
		fmt.Fprint(w, `data: {"type":"response.output_text.delta","delta":"before"}`+"\n\n")
		fmt.Fprint(w, `data: {"type":"response.output_item.added","item":{"type":"web_search_call","id":"ws1","status":"in_progress","action":{"type":"search","queries":["Go releases","Go docs"]}}}`+"\n\n")
		w.(http.Flusher).Flush()
		if count.Load() == 1 {
			select {
			case <-releaseSearch:
			case <-r.Context().Done():
				return
			}
		}
		fmt.Fprint(w, `data: {"type":"response.output_item.done","item":{"type":"web_search_call","id":"ws1","status":"completed","action":{"type":"search","queries":["Go releases","Go docs"],"sources":[{"title":"Go","url":"https://go.dev/","snippet":"Official Go website"}]}}}`+"\n\n")
		fmt.Fprint(w, `data: {"type":"response.output_text.delta","delta":"answer"}`+"\n\n")
		fmt.Fprint(w, `data: {"type":"response.completed","response":{"id":"response","status":"completed","output":[{"type":"web_search_call","id":"ws1","status":"completed","search_receipt":"opaque","action":{"type":"search","queries":["Go releases","Go docs"]}},{"type":"reasoning","provider_receipt":"opaque","content":[{"type":"reasoning_text","text":"consider"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"beforeanswer"}]}]}}`+"\n\n")
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "secret", StateCompatibilityKey: "restart:test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}))
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
	running := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return slices.ContainsFunc(v.Items, func(item ThreadItem) bool {
			return item.Activity != nil && item.Activity.Kind == observation.ActivityKindHosted && item.Activity.Status == observation.ActivityStatusRunning
		})
	})
	if running.Activity != ThreadActivityActive {
		t.Fatal("search did not remain active")
	}
	releaseSearch <- struct{}{}
	first := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if first.Failure != nil {
		t.Fatalf("first failed: %+v", first.Failure)
	}
	assertSearch := func(view ThreadView, count int) {
		t.Helper()
		hosted := 0
		for _, item := range view.Items {
			if item.Activity == nil {
				continue
			}
			if item.Activity.Kind != observation.ActivityKindHosted {
				t.Fatalf("search became local: %+v", item)
			}
			hosted++
			if item.Live || item.Activity.Status != observation.ActivityStatusSuccess || item.Activity.Presentation == nil {
				t.Fatalf("search result incomplete: %+v", item)
			}
			payload, ok := item.Activity.Presentation.Payload.(tools.WebSearchActivityPayload)
			if !ok || payload.Query != "Go releases\nGo docs" || len(payload.Results) != 1 || payload.Operation != "search" || !payload.ResultsProvided || payload.Results[0].Snippet != "Official Go website" {
				t.Fatalf("search presentation: %+v", payload)
			}
		}
		if hosted != count {
			t.Fatalf("search count=%d want=%d: %+v", hosted, count, view.Items)
		}
	}
	assertSearch(first, 1)
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertSearch(restored, 1)

	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "second", Input: UserInput{Text: "again"}}); err != nil {
		t.Fatal(err)
	}
	second := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID != first.TurnID
	})
	if second.Failure != nil || count.Load() != 2 {
		t.Fatalf("restart failed: %+v requests=%d", second.Failure, count.Load())
	}
	assertSearch(second, 2)
}
