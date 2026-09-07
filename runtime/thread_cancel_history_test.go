package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

const cancelHistoryAskArgs = `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`

func TestCanceledAskUserHistoryPassesStrictHTTPAfterRestartForkAndRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Role       string `json:"role"`
				ToolCallID string `json:"tool_call_id"`
				Content    string `json:"content"`
				ToolCalls  []struct {
					ID string `json:"id"`
				} `json:"tool_calls"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		pending := map[string]bool{}
		calls, results := 0, 0
		for _, message := range body.Messages {
			if message.Role == "tool" {
				if !pending[message.ToolCallID] {
					t.Errorf("orphan or duplicate result %q", message.ToolCallID)
					http.Error(w, "tool messages must respond to preceding tool_calls", http.StatusBadRequest)
					return
				}
				delete(pending, message.ToolCallID)
				results++
				if !strings.Contains(message.Content, `"outcome":"cancelled"`) {
					t.Errorf("unexpected result %q", message.Content)
				}
				continue
			}
			if len(pending) != 0 {
				t.Error("incomplete exchange before next message")
				http.Error(w, "missing tool results", 400)
				return
			}
			for _, call := range message.ToolCalls {
				if pending[call.ID] {
					t.Error("duplicate call")
					http.Error(w, "duplicate call", 400)
					return
				}
				pending[call.ID] = true
				calls++
			}
		}
		if len(pending) != 0 {
			t.Error("missing results")
			http.Error(w, "missing results", 400)
			return
		}
		step := requests.Add(1)
		wantPairs := 1
		if step == 1 || step == 4 {
			wantPairs = 0
		}
		if calls != wantPairs || results != wantPairs {
			t.Errorf("request %d calls=%d results=%d want=%d", step, calls, results, wantPairs)
		}
		w.Header().Set("Content-Type", "application/json")
		if step == 1 {
			args, _ := json.Marshal(cancelHistoryAskArgs)
			_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"tool_calls":[{"id":"ask-cancel-http","type":"function","function":{"name":"ask_user","arguments":%s}}]},"finish_reason":"tool_calls"}]}`, args)
			return
		}
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"content":"continued"},"finish_reason":"stop"}]}`)
	}))
	t.Cleanup(server.Close)
	gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
		Provider: "deepseek-test", Model: "strict", BaseURL: server.URL + "/v1", APIKey: "test",
		StateCompatibilityKey: "strict:v1", HTTPClient: server.Client(), Capabilities: provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/cancel.sqlite"
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
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "ask"}, RequestKey: "send"})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
	for _, key := range []RequestKey{"stop", "stop", "stop-again"} {
		if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: key}); err != nil {
			t.Fatal(err)
		}
	}
	canceled := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Attention.InputCount == 0 })
	if countThreadItems(canceled.Items, ThreadItemTool) != 0 {
		t.Fatal("control cancellation produced ordinary tool activity")
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []identity.ThreadID{created.ThreadID, fork.ThreadID} {
		view, err := service.View(t.Context(), threadID)
		if err != nil || len(view.Interactions) != 1 || !view.Interactions[0].Resolved || view.Interactions[0].ID != waiting.Interactions[0].ID {
			t.Fatalf("restored interaction=%#v err=%v", view.Interactions, err)
		}
		if _, err := service.Send(t.Context(), SendInput{ThreadID: threadID, Input: UserInput{Text: "continue"}, RequestKey: "continue"}); err != nil {
			t.Fatal(err)
		}
		view = waitThreadView(t, service, threadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.TurnID != waiting.TurnID })
		if view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCompleted {
			t.Fatalf("continuation failed: %#v", view.Failure)
		}
	}
	if requests.Load() != 3 {
		t.Fatalf("continuation requests=%d", requests.Load())
	}
}

func TestAnsweredAskUserThenToolStopPreservesSingleAnswer(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprintf("respond_cancel_race_%v", race), func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "cancel", StateCompatibilityKey: "test:cancel"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: cancelHistoryAskArgs}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "slow", Name: "slow", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
			)
			started := make(chan struct{})
			var startedOnce sync.Once
			slow := tools.Define[map[string]any](tools.Definition{Name: "slow", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
				startedOnce.Do(func() { close(started) })
				<-ctx.Done()
				return tools.Result{}, ctx.Err()
			})
			agent, err := testAgent(gateway, WithAgentTools(slow))
			if err != nil {
				t.Fatal(err)
			}
			host, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "ask"}, RequestKey: "send"}); err != nil {
				t.Fatal(err)
			}
			waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
			var respondErr error
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, respondErr = service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: "ask", Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}, RequestKey: "answer"})
			}()
			if !race {
				select {
				case <-started:
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
			}
			if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"}); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			if !race && respondErr != nil {
				t.Fatal(respondErr)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Attention.InputCount == 0 })
			if len(view.Interactions) != 1 || !view.Interactions[0].Resolved {
				t.Fatalf("interactions=%#v", view.Interactions)
			}
			if !race && view.Interactions[0].Resolution.Input["q"] != "yes" {
				t.Fatal("answer was replaced by cancellation")
			}
			entries, err := host.store.repo.Path(t.Context(), created.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			messages, err := sessiontree.BuildContextChecked(entries, sessiontree.ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := session.ValidateToolHistory(messages); err != nil {
				t.Fatal(err)
			}
			results := 0
			for _, message := range messages {
				if message.Role == session.Tool && message.ToolCallID == "ask" {
					results++
				}
			}
			if results != 1 {
				t.Fatalf("ask results=%d", results)
			}
		})
	}
}
