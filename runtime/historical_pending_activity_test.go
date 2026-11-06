package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/floegence/floret/v7/threadquery"
	"strings"
)

func TestReleasedPendingActivityColdWindowUpgradeAndContinuation(t *testing.T) {
	root := filepath.Join("..", "internal", "sessiontree", "testdata", "pending-activity-v7.107.4")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		root = "historical-pending-activity"
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Files map[string]string }
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	for file, want := range manifest.Files {
		raw, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(raw)
		if hex.EncodeToString(hash[:]) != want {
			t.Fatalf("fixture changed: %s", file)
		}
	}
	for _, action := range []string{"send", "retry", "fork"} {
		t.Run(action, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, "history.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "history.sqlite")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			var target identity.ThreadID = "pending-history"
			for restart := range 2 {
				gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "host-work", StateCompatibilityKey: "host-work"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "History retained."}, {Type: provider.EventDone, Reason: "stop"}}})
				host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
					return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Inspect.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
				}))
				if err != nil {
					t.Fatal(err)
				}

				scope := threadquery.Scope{Authority: "fixture", ThreadIDs: []identity.ThreadID{target}}
				subscription, err := service.(runtime.ThreadWindowSource).SubscribeWindowUpdates(t.Context(), scope)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				if err := subscription.SetWindow(ctx, scope, runtime.ThreadWindowInput{ThreadID: target, Limit: 50, IncludeSummary: true, ActivityFields: true, PreviewBytes: 1024}); err != nil {
					t.Fatal(err)
				}
				for {
					update, err := subscription.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if update.Window != nil {
						break
					}
				}
				subscription.Close()
				cancel()
				summary, err := service.(runtime.ThreadSummaryReader).Summary(t.Context(), target)
				if err != nil || summary.CreatedAt.IsZero() {
					t.Fatalf("cold summary: %v", err)
				}
				view, err := service.View(t.Context(), target)
				if err != nil {
					t.Fatal(err)
				}
				tools, thoughts := 0, 0
				for _, item := range view.Items {
					if item.Activity != nil {
						tools++
					}
					if item.Kind == runtime.ThreadItemThinking {
						thoughts++
					}
				}
				if restart == 0 && (tools != 5 || thoughts != 5) {
					t.Fatalf("lost original membership: tools=%d thoughts=%d", tools, thoughts)
				}
				if len(gateway.Requests()) != 0 {
					t.Fatal("history read caused execution")
				}

				if restart == 0 {
					if action == "fork" {
						fork, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: target, RequestKey: "fixture-fork"})
						if err != nil {
							t.Fatal(err)
						}
						target = fork.ThreadID
					}
					if action == "retry" {
						_, err = service.Retry(t.Context(), runtime.RetryInput{ThreadID: target, SourceTurnID: view.TurnID, RequestKey: "fixture-retry"})
					} else {
						_, err = service.Send(t.Context(), runtime.SendInput{ThreadID: target, RequestKey: "fixture-send", Input: runtime.UserInput{Text: "Report retained facts without repeating effects."}})
					}
					if err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(5 * time.Second)
					for {
						current, err := service.View(t.Context(), target)
						if err != nil {
							t.Fatal(err)
						}
						if current.Failure != nil {
							t.Fatal(current.Failure)
						}
						if current.Activity == runtime.ThreadActivityIdle && len(gateway.Requests()) == 1 {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("continuation did not settle")
						}
						time.Sleep(time.Millisecond)
					}
				}
				for _, request := range gateway.Requests() {
					results := 0
					for _, message := range request.Messages {
						if message.ToolResult != nil && strings.Contains(message.ToolResult.Text, "CONFIRMED_PENDING_FIXTURE_RESULT") {
							results++
						}
					}
					if action != "retry" && results != 5 {
						t.Fatalf("continuation lost confirmed tool results: %d", results)
					}
				}

				if err := host.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
