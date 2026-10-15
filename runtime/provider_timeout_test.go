package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type stalledArgumentsGateway struct {
	*florettest.ScriptedGateway
	calls atomic.Int32
}

func (g *stalledArgumentsGateway) Stream(ctx context.Context, req provider.Request) (<-chan provider.Event, error) {
	if g.calls.Add(1) > 1 {
		return g.ScriptedGateway.Stream(ctx, req)
	}
	ch := make(chan provider.Event)
	go func() {
		defer close(ch)
		for _, ev := range []provider.Event{{Type: provider.EventToolCallStart, ToolCallStream: &provider.ToolCallStream{ID: "draft", Name: "write"}}, {Type: provider.EventToolCallDelta, Text: `{"value":`, ToolCallStream: &provider.ToolCallStream{ID: "draft", Name: "write"}}} {
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done()
	}()
	return ch, nil
}
func TestProviderTimeoutSettlesAndSurvivesRestart(t *testing.T) {
	gateway := &stalledArgumentsGateway{ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "stalled", StateCompatibilityKey: "stalled:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})}
	var executed atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "write", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil)}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		executed.Add(1)
		return tools.Result{Text: "unexpected"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}), WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{IdleTimeout: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/timeout.sqlite"
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
	h, s := open()
	created, err := s.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure == nil || view.Failure.Code != ThreadTurnFailureProvider || view.Failure.ProviderTimeout == nil || view.Failure.ProviderTimeout.Stage != "output_idle" || view.Cancellation != nil || len(view.ToolCallPreviews) != 0 || executed.Load() != 0 {
		t.Fatalf("view=%+v executed=%d", view, executed.Load())
	}
	if err := h.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, s = open()
	defer h.Shutdown(context.Background())
	restored, err := s.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Failure == nil || restored.Failure.ProviderTimeout == nil || restored.Failure.ProviderTimeout.AttemptID != view.Failure.ProviderTimeout.AttemptID {
		t.Fatalf("lost timeout: %+v", restored.Failure)
	}
	_, err = s.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: restored.TurnID, RequestKey: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	recovered := waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && *v.LastOutcome == TurnOutcomeCompleted
	})
	if recovered.Failure != nil || executed.Load() != 0 {
		t.Fatal("unsafe recovery")
	}
}

func TestProviderTimeoutPolicyAndAutomaticRecovery(t *testing.T) {
	if _, err := testAgent(florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "test", StateCompatibilityKey: "v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}), WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{IdleTimeout: -time.Second})); err == nil {
		t.Fatal("negative policy accepted")
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "timeout", StateCompatibilityKey: "v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{WaitForCancellation: true}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	retry := DefaultProviderRetryPolicy()
	retry.BaseDelay = time.Microsecond
	retry.MaxDelay = time.Microsecond
	retry.Jitter = 0
	agent, err := testAgent(gateway, WithAgentProviderRetryPolicy(retry), WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{FirstOutputTimeout: 30 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || *view.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("view=%+v", view)
	}
	reqs := gateway.Requests()
	if len(reqs) != 2 || reqs[0].LogicalRequestID != reqs[1].LogicalRequestID || reqs[0].AttemptID == reqs[1].AttemptID {
		t.Fatalf("requests=%+v", reqs)
	}
	var timeout *provider.RequestTimeoutError
	err = &provider.RequestTimeoutError{Details: provider.RequestTimeoutDetails{Stage: "first_output"}}
	if !errors.As(err, &timeout) || !errors.Is(err, provider.ErrRequestTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("public timeout classification")
	}
}

func TestProviderTimeoutExhaustionAndStopRemainDistinct(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "timeout", StateCompatibilityKey: "v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{WaitForCancellation: true}, florettest.Step{WaitForCancellation: true}, florettest.Step{WaitForCancellation: true}, florettest.Step{WaitForCancellation: true})
			retry := DefaultProviderRetryPolicy()
			retry.BaseDelay = time.Microsecond
			retry.MaxDelay = time.Microsecond
			retry.Jitter = 0
			first := 15 * time.Millisecond
			if stop {
				first = time.Hour
			}
			agent, err := testAgent(gateway, WithAgentProviderRetryPolicy(retry), WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{FirstOutputTimeout: first}))
			if err != nil {
				t.Fatal(err)
			}
			_, s := testThreadServiceWithAgent(t, agent)
			created, err := s.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
				t.Fatal(err)
			}
			if stop {
				waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool { return v.RunProgress != nil && v.RunProgress.Request != nil })
				if _, err = s.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"}); err != nil {
					t.Fatal(err)
				}
			}
			view := waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if stop {
				if view.Failure != nil || *view.LastOutcome != TurnOutcomeCancelled {
					t.Fatalf("view=%+v", view)
				}
			} else if len(gateway.Requests()) != 4 || view.Failure == nil || view.Failure.ProviderTimeout == nil || view.Cancellation != nil {
				t.Fatalf("requests=%d view=%+v", len(gateway.Requests()), view)
			}
		})
	}
}

func TestProviderDeadlineExcludesLocalToolExecution(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "local", StateCompatibilityKey: "v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "local", Name: "slow", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "complete"}, {Type: provider.EventDone, Reason: "stop"}}})
	var calls atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "slow", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil)}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		calls.Add(1)
		select {
		case <-time.After(250 * time.Millisecond):
			return tools.Result{Text: "done"}, nil
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		}
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})), WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}), WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{FirstOutputTimeout: 100 * time.Millisecond, IdleTimeout: 100 * time.Millisecond, MaxAttemptDuration: 100 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	_, s := testThreadServiceWithAgent(t, agent)
	created, err := s.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || *view.LastOutcome != TurnOutcomeCompleted || calls.Load() != 1 {
		t.Fatalf("failure=%+v calls=%d", view.Failure, calls.Load())
	}
}
