package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestReleasedCorrectionFailureUpgradeAndRecovery(t *testing.T) {
	root := filepath.Join("..", "internal", "sessiontree", "testdata", "tool-correction-v7.69.1")
	// The identical public-API test also runs from blank adoption modules.
	if _, err := os.Stat(root); os.IsNotExist(err) {
		root = "historical-correction"
	}
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Files map[string]string }
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for name, want := range manifest.Files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatalf("fixture %s changed", name)
		}
	}
	for _, action := range []string{"retry", "send", "fork"} {
		t.Run(action, func(t *testing.T) {
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
					florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained"}, {Type: provider.EventDone, Reason: "stop"}}},
					florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained"}, {Type: provider.EventDone, Reason: "stop"}}},
					florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained"}, {Type: provider.EventDone, Reason: "stop"}}},
					florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained"}, {Type: provider.EventDone, Reason: "stop"}}})
				service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
					return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Compatibility"}, SystemPrompt: "Continue.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
				}))
				if err != nil {
					t.Fatal(err)
				}
				summaries, err := service.List(t.Context(), runtime.ThreadScope{})
				if err != nil {
					t.Fatal(err)
				}
				for _, summary := range summaries {
					if restart == 0 && summary.Failure != nil && summary.Failure.Reason != runtime.ThreadTurnFailureToolArgumentCorrectionExhausted {
						t.Fatalf("historical list reason: %+v", summary.Failure)
					}
				}
				for _, id := range []identity.ThreadID{"ordinary", "control", "fork", "nested"} {
					before, err := service.View(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					if len(before.Items) == 0 || len(before.Interactions) != 0 {
						t.Fatalf("lost history or orphan interaction: %+v", before)
					}
					if restart == 0 && before.Failure != nil && before.Failure.Reason != runtime.ThreadTurnFailureToolArgumentCorrectionExhausted {
						t.Fatalf("historical view reason: %+v", before.Failure)
					}
					target := id
					if action == "fork" && restart == 0 {
						fork, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: id, RequestKey: runtime.RequestKey("fork-qualify-" + id.String())})
						if err != nil {
							t.Fatal(err)
						}
						target = fork.ThreadID
					}
					key := runtime.RequestKey(fmt.Sprintf("%s-%s-%d", action, id, restart))
					var accepted runtime.ThreadView
					if action == "retry" && restart == 0 {
						accepted, err = service.Retry(t.Context(), runtime.RetryInput{ThreadID: target, SourceTurnID: before.TurnID, RequestKey: key})
					} else {
						accepted, err = service.Send(t.Context(), runtime.SendInput{ThreadID: target, RequestKey: key, Input: runtime.UserInput{Text: "Continue with confirmed results."}})
					}
					if err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(10 * time.Second)
					for {
						view, err := service.View(t.Context(), target)
						if err != nil {
							t.Fatal(err)
						}
						if view.Activity == runtime.ThreadActivityIdle && view.TurnID == accepted.TurnID && view.LastOutcome != nil {
							if view.Failure != nil || *view.LastOutcome != runtime.TurnOutcomeCompleted {
								t.Fatalf("recovery failed: %+v", view.Failure)
							}
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("recovery remained busy")
						}
						time.Sleep(time.Millisecond)
					}
				}
				for _, request := range gateway.Requests() {
					found := false
					calls := map[string]int{}
					results := map[string]int{}
					for _, message := range request.Messages {
						for _, call := range message.ToolCalls {
							calls[call.ID]++
						}
						if message.ToolResult != nil {
							results[message.ToolResult.CallID]++
							if strings.Contains(message.ToolResult.Text, "CONFIRMED_RELEASED_RESULT") {
								found = true
							}
						}
					}
					if !found {
						t.Fatal("confirmed released result lost")
					}
					for id, count := range calls {
						if count != 1 || results[id] != 1 {
							t.Fatalf("incomplete tool exchange %s: calls=%d results=%d", id, count, results[id])
						}
					}
				}
				if len(gateway.Requests()) != 4 {
					t.Fatalf("requests=%d", len(gateway.Requests()))
				}
				if err := host.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
