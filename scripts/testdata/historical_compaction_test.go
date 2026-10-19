package adoption_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestPublishedHistoricalCompactionCompatibility(t *testing.T) {
	roots, err := filepath.Glob("historical-compaction/*")
	if err != nil || len(roots) == 0 {
		t.Fatalf("historical fixtures missing: %v", err)
	}
	for _, root := range roots {
		t.Run(filepath.Base(root), func(t *testing.T) { qualifyHistoricalCompaction(t, root) })
	}
}

func qualifyHistoricalCompaction(t *testing.T, root string) {
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Files map[string]string }
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	for name, expected := range manifest.Files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatalf("fixture %s changed", name)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "history.sqlite")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "compatibility", Model: "fake", StateCompatibilityKey: "compatibility"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
			florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
			florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
			florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}})
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Compatibility"}, SystemPrompt: "Continue.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
		}))
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"thread", "fork", "nested"} {
			threadID := identity.ThreadID(id)
			before, err := service.View(t.Context(), threadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(before.Items) == 0 {
				t.Fatal("historical conversation disappeared")
			}
			accepted, err := service.Send(t.Context(), runtime.SendInput{ThreadID: threadID, Input: runtime.UserInput{Text: "Continue."}, RequestKey: runtime.RequestKey(fmt.Sprintf("%s-%d", id, restart))})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				view, err := service.View(t.Context(), threadID)
				if err != nil {
					t.Fatal(err)
				}
				if view.Activity == runtime.ThreadActivityIdle && view.TurnID == accepted.TurnID {
					if view.Failure != nil || view.LastOutcome == nil || *view.LastOutcome != runtime.TurnOutcomeCompleted {
						t.Fatalf("historical continuation failed: %+v", view.Failure)
					}
					if len(view.Items) <= len(before.Items) {
						t.Fatal("new response missing")
					}
					for i, item := range before.Items {
						if view.Items[i].ID != item.ID {
							t.Fatal("historical identity changed")
						}
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("historical continuation remained busy")
				}
				time.Sleep(time.Millisecond)
			}
		}
		if len(gateway.Requests()) != 3 {
			t.Fatal("provider did not receive every continuation")
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}
