package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

// Exercise the HTTP adapter: a scripted Gateway cannot detect divergence
// between provider-native response state and the canonical call history.
func TestDeepSeekMixedControlHistory(t *testing.T) {
	for _, mode := range []string{"ordinary_first", "control_first", "reasoning_only", "multiple_tools", "invalid_control", "tool_input_restart", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var requests, effects atomic.Int32
			question := `{"reason_code":"missing_external_input","required_from_user":["choice"],"evidence_refs":[],"questions":[{"id":"choice","header":"Choice","question":"Continue?","response_mode":"write","is_secret":false}]}`
			call := func(id, name, args string) map[string]any {
				return map[string]any{"type": "function_call", "id": "item-" + id, "call_id": id, "name": name, "arguments": args}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Input []map[string]json.RawMessage `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				n := requests.Add(1)
				var output []map[string]any
				if n == 1 {
					args := question
					if mode == "invalid_control" {
						args = `{}`
					}
					output = []map[string]any{call("work", "inspect", `{}`), call("mixed-ask", "ask_user", args)}
					if mode == "control_first" {
						slices.Reverse(output)
					}
					if mode == "multiple_tools" {
						output = append(output, call("work-2", "inspect", `{}`))
					}
					output = append([]map[string]any{{"type": "reasoning", "receipt": "preserve-original", "content": []map[string]string{{"type": "reasoning_text", "text": "Inspect before asking."}}}}, output...)
					if mode != "reasoning_only" {
						output = append([]map[string]any{{"type": "message", "role": "assistant", "content": []map[string]string{{"type": "output_text", "text": "Inspecting."}}}}, output...)
					}
				} else {
					counts := map[string]int{}
					var order []string
					for _, item := range body.Input {
						var kind, id string
						_ = json.Unmarshal(item["type"], &kind)
						_ = json.Unmarshal(item["call_id"], &id)
						if kind == "function_call" {
							counts[id]++
							order = append(order, id)
						}
						if kind == "function_call_output" {
							counts[id]--
							if id == "mixed-ask" && !strings.Contains(string(item["output"]), "separately") {
								t.Errorf("missing explicit mixed-call correction: %s", item["output"])
							}
						}
					}
					for id, count := range counts {
						if count != 0 {
							t.Errorf("unpaired call %s: %d", id, count)
						}
					}
					first := "work"
					if mode == "control_first" {
						first = "mixed-ask"
					}
					if len(order) < 2 || order[0] != first {
						t.Errorf("call order changed: %v", order)
					}
					raw, _ := json.Marshal(body.Input)
					if !strings.Contains(string(raw), "preserve-original") {
						t.Error("native reasoning was lost")
					}
					if n == 2 && mode != "tool_input_restart" {
						output = []map[string]any{call("ask-next", "ask_user", question)}
					} else {
						output = []map[string]any{{"type": "message", "role": "assistant", "content": []map[string]string{{"type": "output_text", "text": "Done."}}}}
					}
				}
				raw, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"id": fmt.Sprintf("response-%d", n), "status": "completed", "output": output}})
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\n", raw)
			}))
			defer server.Close()
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{Model: "deepseek-v4-pro", BaseURL: server.URL, APIKey: "fixture", StateCompatibilityKey: "mixed:test"})
			if err != nil {
				t.Fatal(err)
			}
			inspect := tools.Define[struct{}](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(map[string]any{}, []string{}), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
				effects.Add(1)
				result := tools.Result{Text: "Observed once."}
				if mode == "tool_input_restart" {
					result.InputRequired = &tools.InputRequest{Summary: "Complete verification", Questions: []tools.InputQuestion{{ID: "choice", Prompt: "Continue?", Kind: "write"}}}
				}
				return result, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(inspect), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, e AuthorizedEffect) (EffectDispatchResult, error) {
				expectedIndex, expectedSize := 0, 2
				if mode == "control_first" {
					expectedIndex = 1
				}
				if mode == "multiple_tools" {
					expectedSize = 3
					if r.ToolCallID == "work-2" {
						expectedIndex = 2
					}
				}
				if r.BatchIndex != expectedIndex || r.BatchSize != expectedSize {
					t.Errorf("authorization batch=%d/%d want=%d/%d", r.BatchIndex, r.BatchSize, expectedIndex, expectedSize)
				}
				if r.ToolName == "ask_user" {
					t.Error("control correction acquired effect authorization")
				}
				return e(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "fixture", AuditReference: "fixture", AuditHash: "fixture", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/history.db"
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, svc
			}
			host, svc := open()
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			created, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Inspect and ask."}})
			if err != nil {
				t.Fatal(err)
			}
			waiting := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return len(v.Interactions) > 0 || v.LastOutcome != nil })
			if waiting.Failure != nil || len(waiting.Interactions) != 1 {
				t.Fatalf("missing interaction: failure=%+v interactions=%+v", waiting.Failure, waiting.Interactions)
			}
			interaction := waiting.Interactions[0]
			if interaction.ToolCallID == "mixed-ask" {
				t.Fatal("mixed control opened an interaction")
			}
			if mode == "tool_input_restart" {
				if requests.Load() != 1 {
					t.Fatal("tool input did not pause provider")
				}
				if err := host.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				host, svc = open()
			}
			if mode == "cancel" {
				_, err = svc.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel"})
			} else {
				_, err = svc.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: interaction.ID, RequestKey: "answer", Answers: []InteractionAnswer{{Input: map[string]string{"choice": "Continue"}}}})
			}
			if err != nil {
				t.Fatal(err)
			}
			final := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			want := int32(1)
			if mode == "multiple_tools" {
				want = 2
			}
			if final.Failure != nil || effects.Load() != want {
				t.Fatalf("failure=%+v effects=%d want=%d", final.Failure, effects.Load(), want)
			}
		})
	}
}
