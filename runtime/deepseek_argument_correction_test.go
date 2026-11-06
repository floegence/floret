package runtime_test

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
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestDeepSeekMalformedArgumentsRecoverWithoutReplayingEffects(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		t.Run(fmt.Sprintf("exhaust=%v", exhaust), func(t *testing.T) {
			const invalid = `{"reason_code":"user_decision_required","required_from_user": restart permission,"evidence_refs":[],"questions":[]}`
			const valid = `{"reason_code":"user_decision_required","required_from_user":["Choose when to restart."],"evidence_refs":[],"questions":[{"id":"restart","header":"Restart","question":"When should the runtime restart?","is_secret":false,"response_mode":"write"}]}`
			var requests, effects, authorizations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var raw struct {
					Input []json.RawMessage `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
					t.Error(err)
					return
				}
				n := requests.Add(1)
				if n > 2 {
					var rejected, feedback, confirmed int
					for _, item := range raw.Input {
						var value struct {
							Type      string `json:"type"`
							CallID    string `json:"call_id"`
							Arguments string `json:"arguments"`
							Output    string `json:"output"`
						}
						if err := json.Unmarshal(item, &value); err != nil {
							t.Error(err)
							continue
						}
						if value.Type == "function_call" && value.CallID == "ask-2" {
							rejected++
							if value.Arguments != invalid {
								t.Error("malformed arguments were rewritten")
							}
						}
						if value.Type == "function_call_output" && value.CallID == "ask-2" && strings.Contains(value.Output, "not executed") {
							feedback++
						}
						if value.Type == "function_call_output" && value.CallID == "write-once" && value.Output == "confirmed once" {
							confirmed++
						}
					}
					if rejected != 1 || feedback != 1 || confirmed != 1 {
						t.Errorf("history pair=%d/%d confirmed=%d", rejected, feedback, confirmed)
					}
				}
				w.Header().Set("Content-Type", "text/event-stream")
				emit := func(value any) { b, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", b) }
				lastInvalid := int32(2)
				if exhaust {
					lastInvalid = 4
				}
				var output []any
				if n <= lastInvalid+1 {
					name, id, args := "ask_user", fmt.Sprintf("ask-%d", n), valid
					if n == 1 {
						name, id, args = "write", "write-once", `{}`
					} else if n <= lastInvalid {
						args = invalid
					}
					item := map[string]any{"type": "function_call", "id": "item-" + id, "call_id": id, "name": name, "arguments": args, "status": "completed"}
					emit(map[string]any{"type": "response.output_item.added", "item": map[string]any{"type": "function_call", "id": "item-" + id, "call_id": id, "name": name}, "output_index": 0})
					emit(map[string]any{"type": "response.output_item.done", "item": item, "output_index": 0})
					output = []any{item}
				} else {
					output = []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "Confirmed results retained."}}}}
				}
				emit(map[string]any{"type": "response.completed", "response": map[string]any{"id": fmt.Sprintf("response-%d", n), "status": "completed", "output": output}})
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-flash", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "argument-correction"})
			if err != nil {
				t.Fatal(err)
			}
			write := tools.Define[struct{}](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
				effects.Add(1)
				return tools.Result{Text: "confirmed once"}, nil
			})
			authorize := runtime.EffectAuthorizationGateFunc(func(ctx context.Context, request runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				authorizations.Add(1)
				return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
			})
			agent, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Write once, then ask.", Context: config.ContextPolicy{ContextWindowTokens: 128000}}, gateway, runtime.WithAgentTools(write), runtime.WithAgentEffectAuthorization(authorize), runtime.WithAgentProviderRetryPolicy(runtime.ProviderRetryPolicy{Disabled: true}))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "history.sqlite")
			open := func() (*runtime.Host, runtime.ThreadService) {
				h, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
				s, err := h.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return h, s
			}
			settle := func(s runtime.ThreadService, id identity.ThreadID) runtime.ThreadView {
				t.Helper()
				deadline := time.Now().Add(10 * time.Second)
				for {
					v, err := s.View(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					if v.Activity != runtime.ThreadActivityActive {
						return v
					}
					for _, interaction := range v.Interactions {
						if !interaction.Resolved {
							return v
						}
					}
					if time.Now().After(deadline) {
						t.Fatal("turn did not settle")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			h, s := open()
			created, err := s.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "Write once, then ask when to restart."}}); err != nil {
				t.Fatal(err)
			}
			v := settle(s, created.ThreadID)
			if exhaust {
				if v.Failure == nil || v.Failure.Reason != runtime.ThreadTurnFailureToolArgumentCorrectionExhausted || requests.Load() != 4 || len(v.Interactions) != 0 {
					t.Fatalf("exhaustion=%+v requests=%d", v.Failure, requests.Load())
				}
				if err = h.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
				h, s = open()
				if _, err = s.Retry(t.Context(), runtime.RetryInput{ThreadID: created.ThreadID, SourceTurnID: v.TurnID, RequestKey: "retry"}); err != nil {
					t.Fatal(err)
				}
				v = settle(s, created.ThreadID)
			}
			if v.Failure != nil || len(v.Interactions) != 1 {
				t.Fatalf("corrected question missing: failure=%+v interactions=%+v", v.Failure, v.Interactions)
			}
			raw, _ := json.Marshal(v.Items)
			if strings.Contains(string(raw), "restart permission") || strings.Contains(string(raw), "invalid JSON") {
				t.Fatal("correction feedback leaked into public items")
			}
			if err = h.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			h, s = open()
			if _, err = s.Respond(t.Context(), runtime.RespondInput{ThreadID: created.ThreadID, InteractionID: v.Interactions[0].ID, RequestKey: "answer", Answers: []runtime.InteractionAnswer{{Input: map[string]string{"restart": "later"}}}}); err != nil {
				t.Fatal(err)
			}
			v = settle(s, created.ThreadID)
			if v.Failure != nil || v.LastOutcome == nil || *v.LastOutcome != runtime.TurnOutcomeCompleted {
				t.Fatalf("resume failure=%+v", v.Failure)
			}
			if err = h.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, s = open()
			fork, err := s.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Send(t.Context(), runtime.SendInput{ThreadID: fork.ThreadID, RequestKey: "continue", Input: runtime.UserInput{Text: "Continue with confirmed results."}}); err != nil {
				t.Fatal(err)
			}
			v = settle(s, fork.ThreadID)
			if v.Failure != nil || v.LastOutcome == nil || *v.LastOutcome != runtime.TurnOutcomeCompleted || effects.Load() != 1 || authorizations.Load() != 1 {
				t.Fatalf("fork failure=%+v effects=%d authorizations=%d", v.Failure, effects.Load(), authorizations.Load())
			}
		})
	}
}
