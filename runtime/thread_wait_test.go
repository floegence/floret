package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"testing"
	"testing/synctest"
	"time"
)

func TestSubscriptionRetainsCloseCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sub := &WorkspaceSubscription{views: make(chan ThreadView, 1), done: make(chan struct{})}
		go func() { <-ctx.Done(); sub.closeWithError(ctx.Err()) }()
		time.Sleep(2 * time.Second)
		for i := 0; i < 100; i++ {
			if _, err := sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err=%v", err)
			}
		}
		explicit := &WorkspaceSubscription{views: make(chan ThreadView, 1), done: make(chan struct{})}
		explicit.Close()
		if _, err := explicit.Next(context.Background()); !errors.Is(err, ErrHostClosed) {
			t.Fatalf("explicit close=%v", err)
		}
	})
}
func TestSelectWaitThreads(t *testing.T) {
	completed := TurnOutcomeCompleted
	for _, tc := range []struct {
		name   string
		items  []ThreadSummary
		ids    []identity.ThreadID
		reason ThreadWaitReason
	}{
		{name: "empty", reason: ThreadWaitCompleted},
		{name: "missing", ids: []identity.ThreadID{"missing"}, reason: ThreadWaitMissing},
		{name: "running", items: []ThreadSummary{{ID: "child", Activity: ThreadActivityActive}}, ids: []identity.ThreadID{"child"}},
		{name: "completed", items: []ThreadSummary{{ID: "child", Activity: ThreadActivityIdle, LastOutcome: &completed}}, ids: []identity.ThreadID{"child"}, reason: ThreadWaitCompleted},
		{name: "attention", items: []ThreadSummary{{ID: "child", Activity: ThreadActivityActive, Attention: AttentionSummary{InputCount: 1}}}, ids: []identity.ThreadID{"child"}, reason: ThreadWaitAttentionRequired},
		{name: "missing before attention", items: []ThreadSummary{{ID: "child", Attention: AttentionSummary{ApprovalCount: 1}}}, ids: []identity.ThreadID{"child", "missing"}, reason: ThreadWaitMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectWaitThreads(tc.items, tc.ids)
			if got.Reason != tc.reason {
				t.Fatalf("result=%#v", got)
			}
		})
	}
}

func TestWaitThreadsTimeoutCancellationAndHostClose(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel", "host_close", "delete", "complete", "pressure"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				host, typed := testThreadService(t, newBlockingThreadGateway())
				service := typed.(*threadRuntimeService)
				parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
				if err != nil {
					t.Fatal(err)
				}
				child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: parent.ThreadID, TaskName: "child", HostProfileRef: "default", RequestKey: "child"})
				if err != nil {
					t.Fatal(err)
				}
				actor := service.runtime(child.ThreadID)
				actor.mu.Lock()
				actor.state.view.Activity = ThreadActivityActive
				actor.mu.Unlock()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				type result struct {
					value ThreadWaitResult
					err   error
				}
				done := make(chan result, 1)
				go func() {
					v, e := service.WaitThreads(ctx, WaitThreadsInput{Scope: ThreadScope{ParentID: &parent.ThreadID}, Timeout: time.Minute})
					done <- result{v, e}
				}()
				synctest.Wait()
				switch mode {
				case "cancel":
					cancel()
				case "host_close":
					service.close()
				case "delete":
					actor.mu.Lock()
					actor.state.view.Activity = ThreadActivityIdle
					actor.mu.Unlock()
					if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: child.ThreadID, RequestKey: "delete"}); err != nil {
						t.Fatal(err)
					}
				case "complete", "input", "approval", "pressure":
					actor.mu.Lock()
					if mode == "complete" {
						outcome := TurnOutcomeCompleted
						actor.state.view.Activity = ThreadActivityIdle
						actor.state.view.LastOutcome = &outcome
					}
					if mode == "input" {
						actor.state.view.Attention.InputCount = 1
					}
					if mode == "approval" {
						actor.state.view.Attention.ApprovalCount = 1
					}
					actor.state.view.ViewVersion++
					view := cloneThreadRuntimeView(actor.state.view)
					actor.mu.Unlock()
					service.publish(view)
					if mode == "pressure" {
						for i := 0; i < 200; i++ {
							view.ViewVersion++
							service.publish(view)
						}
					}
				}
				if mode == "timeout" || mode == "pressure" {
					time.Sleep(time.Minute)
				}
				got := <-done
				switch mode {
				case "cancel":
					if !errors.Is(got.err, context.Canceled) {
						t.Fatalf("err=%v", got.err)
					}
				case "host_close":
					if !errors.Is(got.err, ErrHostClosed) {
						t.Fatalf("err=%v", got.err)
					}
				default:
					want := ThreadWaitTimedOut
					if mode == "delete" {
						want = ThreadWaitMissing
					}
					if mode == "complete" {
						want = ThreadWaitCompleted
					}
					if mode == "input" || mode == "approval" {
						want = ThreadWaitAttentionRequired
					}
					if got.err != nil || got.value.Reason != want {
						t.Fatalf("result=%#v err=%v", got.value, got.err)
					}
					if mode != "delete" && (len(got.value.Threads) != 1 || got.value.Threads[0].ID != child.ThreadID) {
						t.Fatalf("targets=%#v", got.value)
					}
				}
				host.Shutdown(context.Background())
			})
		})
	}
}
func TestThreadFailureReasonCanonicalProjection(t *testing.T) {
	for _, tc := range []struct {
		message string
		reason  ThreadTurnFailureReason
	}{
		{"agent loop repeated identical tool calls", ThreadTurnFailureRepeatedToolCalls},
		{"agent loop made no progress", ThreadTurnFailureNoProgress},
		{"provider returned duplicate tool call id", ThreadTurnFailureDuplicateToolCallID},
		{"stop hook requested too many continuations", ThreadTurnFailureStopHookLoop},
		{"upstream: agent loop repeated identical tool calls", ""},
	} {
		original := &ThreadTurnFailure{Code: ThreadTurnFailureEngineContract, Message: tc.message}
		projected := cloneThreadTurnFailure(original)
		if projected.Reason != tc.reason || original.Reason != "" {
			t.Fatalf("failure=%#v", projected)
		}
		if err := projected.Validate(); err != nil {
			t.Fatal(err)
		}
		if got := classifyThreadTurnFailureReason(ThreadTurnFailureProvider, tc.message); got != "" && tc.reason != ThreadTurnFailureDuplicateToolCallID {
			t.Fatalf("provider reason=%s", got)
		}
	}
}

func TestThreadWaitFailureReasonSurvivesSQLiteRestart(t *testing.T) {
	path := t.TempDir() + "/reason.db"
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "same", Name: "missing", Args: "{}"}, {ID: "same", Name: "missing", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	open := func() (*Host, ThreadService) {
		h, e := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if e != nil {
			t.Fatal(e)
		}
		s, e := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if e != nil {
			t.Fatal(e)
		}
		return h, s
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "fail"}}); err != nil {
		t.Fatal(err)
	}
	failed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.LastOutcome != nil })
	if failed.Failure == nil || failed.Failure.Reason != ThreadTurnFailureDuplicateToolCallID {
		t.Fatalf("live=%#v", failed.Failure)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil || restored.Failure == nil || *restored.Failure != *failed.Failure {
		t.Fatalf("restored=%#v err=%v", restored.Failure, err)
	}
	summaries, err := service.List(t.Context(), ThreadScope{})
	if err != nil || len(summaries) != 1 || summaries[0].Failure.Reason != failed.Failure.Reason {
		t.Fatalf("summaries=%#v err=%v", summaries, err)
	}
}
