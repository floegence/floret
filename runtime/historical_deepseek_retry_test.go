package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type releasedDeepSeekGateway struct {
	provider.Gateway
	statePreparations *atomic.Int32
}

func (g releasedDeepSeekGateway) Prepare(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	if req.PreviousState != nil {
		g.statePreparations.Add(1)
	}
	return g.Gateway.(provider.RequestPreparer).Prepare(ctx, req)
}

// The released writer generated this fixture with a fake provider and a fake
// confirmed effect. This test also runs in the blank-module adoption gates.
func TestReleasedDeepSeekRetryHistorySurvivesUpgrade(t *testing.T) {
	root := filepath.Join("..", "internal", "sessiontree", "testdata", "deepseek-retry-v7.107.2")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		root = "historical-deepseek-retry"
	}
	manifestRaw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Files map[string]string }
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	for name, want := range manifest.Files {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatalf("fixture %s changed", name)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "history.sqlite")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var requests, statePreparations, effects atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Input json.RawMessage }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		requests.Add(1)
		if !strings.Contains(string(body.Input), "Released writer retained reasoning.") || !strings.Contains(string(body.Input), "CONFIRMED_RELEASED_WRITE") || strings.Count(string(body.Input), `"call_id":"confirmed-call"`) != 2 {
			t.Error("released reasoning or confirmed tool exchange was lost or duplicated")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"continued\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"History retained.\"}]}]}}\n\n")
	}))
	defer server.Close()
	// The stored provider-state identity is independent of the HTTP listener.
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "fixture:v7.107.2"})
	if err != nil {
		t.Fatal(err)
	}
	write := tools.Define[struct{}](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
		effects.Add(1)
		return tools.Result{Text: "unexpected replay"}, nil
	})
	for restart := 0; restart < 2; restart++ {
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Write once.", Context: config.ContextPolicy{ContextWindowTokens: 128000}}, releasedDeepSeekGateway{Gateway: gateway, statePreparations: &statePreparations}, runtime.WithAgentTools(write), runtime.WithAgentProviderRetryPolicy(runtime.ProviderRetryPolicy{Disabled: true}))
		}))
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []identity.ThreadID{"fixture", "fixture-fork"} {
			before, err := service.View(t.Context(), id)
			if err != nil || len(before.Items) == 0 {
				t.Fatalf("historical view: %+v err=%v", before, err)
			}
			accepted, err := service.Send(t.Context(), runtime.SendInput{ThreadID: id, RequestKey: runtime.RequestKey(fmt.Sprintf("continue-%d", restart)), Input: runtime.UserInput{Text: "Continue with the confirmed result."}})
			if err != nil {
				t.Fatal(err)
			}
			waited, err := service.(runtime.ThreadWaiter).WaitThreads(t.Context(), runtime.WaitThreadsInput{ThreadIDs: []identity.ThreadID{id}, Timeout: time.Minute})
			if err != nil || waited.Reason != runtime.ThreadWaitCompleted {
				t.Fatalf("wait=%+v err=%v", waited, err)
			}
			after, err := service.View(t.Context(), id)
			if err != nil || after.TurnID != accepted.TurnID || after.Failure != nil || after.LastOutcome == nil || *after.LastOutcome != runtime.TurnOutcomeCompleted {
				t.Fatalf("continuation=%+v err=%v", after.Failure, err)
			}
			if restart == 0 && id == "fixture" && statePreparations.Load() == 0 {
				t.Fatal("released opaque state was not used by the first continuation")
			}
			for index, item := range before.Items {
				if index >= len(after.Items) || after.Items[index].ID != item.ID {
					t.Fatal("historical identity changed")
				}
			}
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 4 || statePreparations.Load() == 0 || effects.Load() != 0 {
		t.Fatalf("requests=%d state preparations=%d repeated effects=%d", requests.Load(), statePreparations.Load(), effects.Load())
	}
}
