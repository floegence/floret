package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestProviderOriginalFailureSurvivesRestart(t *testing.T) {
	message := "  first\r\n\t" + strings.Repeat("original detail\n", 10000) + "end  "
	body, _ := json.Marshal(map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp-fixture", "status": "failed", "error": map[string]any{"code": "fixture_failure", "message": message}}})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-Id", "fixture-request")
		fmt.Fprintf(w, "data: %s\n\n", body)
	}))
	defer server.Close()
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "failure.sqlite")
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
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "fixture"}}); err != nil {
		t.Fatal(err)
	}
	first := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	check := func(v ThreadView) {
		t.Helper()
		if v.Failure == nil {
			t.Fatal("missing failure")
		}
		d := provider.FailureDetailsForError(errors.New(v.Failure.Message))
		if d == nil || d.Message != message || !bytes.Equal(d.Body, body) || d.RequestID != "fixture-request" {
			t.Fatal("canonical failure lost original evidence")
		}
	}
	check(first)
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	check(restored)
	if restored.Failure.Message != first.Failure.Message {
		t.Fatal("restart changed failure bytes")
	}
}
