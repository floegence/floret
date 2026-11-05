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
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestDeepSeekRetryPreservesInterruptedHistoryAndConfirmedEffect(t *testing.T) {
	for _, fragment := range []string{"reasoning", "text"} {
		t.Run(fragment, func(t *testing.T) {
			const retained = "Retained interrupted output."
			var requests, effects atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct{ Input json.RawMessage }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				n := requests.Add(1)
				if n > 1 && !strings.Contains(string(body.Input), retained) {
					t.Error("continuation lost interrupted output")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				switch n {
				case 1:
					typ := "response.reasoning_text.delta"
					if fragment == "text" {
						typ = "response.output_text.delta"
					}
					fmt.Fprintf(w, "data: {\"type\":%q,\"delta\":%q}\n\n", typ, retained)
					fmt.Fprint(w, "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"failed\",\"error\":{\"code\":\"fixture_failure\",\"message\":\"fixture failure\"}}}\n\n")
				case 2:
					fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"call-response\",\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"id\":\"item\",\"call_id\":\"call\",\"name\":\"write\",\"arguments\":\"{}\"}]}}\n\n")
				default:
					if strings.Count(string(body.Input), `"call_id":"call"`) != 2 || !strings.Contains(string(body.Input), "confirmed once") {
						t.Error("continuation lost or duplicated the tool exchange")
					}
					fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Completed.\"}]}]}}\n\n")
				}
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			write := tools.Define[struct{}](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
				effects.Add(1)
				return tools.Result{Text: "confirmed once"}, nil
			})
			authorize := EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
			})
			agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Write once.", Context: config.ContextPolicy{ContextWindowTokens: 128000}}, gateway, WithAgentTools(write), WithAgentEffectAuthorization(authorize), WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "history.sqlite")
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			settle := func(service ThreadService, id identity.ThreadID) ThreadView {
				t.Helper()
				waited, err := service.(ThreadWaiter).WaitThreads(t.Context(), WaitThreadsInput{ThreadIDs: []identity.ThreadID{id}, Timeout: time.Minute})
				if err != nil || waited.Reason != ThreadWaitCompleted {
					t.Fatalf("wait=%+v err=%v", waited, err)
				}
				view, err := service.View(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				return view
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Write once."}}); err != nil {
				t.Fatal(err)
			}
			failed := settle(service, created.ThreadID)
			if failed.Failure == nil || requests.Load() != 1 {
				t.Fatal("expected one failed provider attempt")
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
			if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: failed.TurnID, RequestKey: "retry"}); err != nil {
				t.Fatal(err)
			}
			completed := settle(service, created.ThreadID)
			if completed.Failure != nil || completed.LastOutcome == nil || *completed.LastOutcome != TurnOutcomeCompleted || requests.Load() != 3 || effects.Load() != 1 {
				t.Fatalf("retry failure=%+v requests=%d effects=%d", completed.Failure, requests.Load(), effects.Load())
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = open()
			fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: fork.ThreadID, RequestKey: "continue", Input: UserInput{Text: "Continue."}}); err != nil {
				t.Fatal(err)
			}
			continued := settle(service, fork.ThreadID)
			if continued.Failure != nil || continued.LastOutcome == nil || *continued.LastOutcome != TurnOutcomeCompleted || requests.Load() != 4 || effects.Load() != 1 {
				t.Fatalf("fork failure=%+v requests=%d effects=%d", continued.Failure, requests.Load(), effects.Load())
			}
		})
	}
}
