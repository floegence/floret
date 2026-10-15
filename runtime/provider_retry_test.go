package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutomaticProviderRecoveryPreservesConfirmedToolsAndTurn(t *testing.T) {
	failure := provider.WithRetryAdvice(errors.New("upstream gateway error"), provider.RetryAdvice{Category: provider.RetryTransient})
	calls := func(items ...provider.ToolCall) florettest.Step {
		return florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: items}, {Type: provider.EventDone, Reason: "tool_calls"}}}
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "retry", StateCompatibilityKey: "retry:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		calls(provider.ToolCall{ID: "todo", Name: "write_todos", Args: `{}`}),
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "old partial"}, {Type: provider.EventError, Err: failure}}},
		calls(provider.ToolCall{ID: "child1", Name: "create_child", Args: `{}`}, provider.ToolCall{ID: "child2", Name: "create_child", Args: `{}`}, provider.ToolCall{ID: "child3", Name: "create_child", Args: `{}`}),
		florettest.Step{ReturnError: failure},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Recovered answer."}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var todoCount, childCount atomic.Int32
	define := func(name string, count *atomic.Int32) tools.Tool {
		return tools.Define[map[string]any](tools.Definition{Name: name, ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil)}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			count.Add(1)
			return tools.Result{Text: "confirmed"}, nil
		})
	}
	policy := DefaultProviderRetryPolicy()
	policy.BaseDelay = time.Microsecond
	policy.MaxDelay = time.Microsecond
	policy.Jitter = 0
	agent, err := testAgent(gateway, WithAgentProviderRetryPolicy(policy), WithAgentTools(define("write_todos", &todoCount), define("create_child", &childCount)), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-retry"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-retry", Input: UserInput{Text: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if view.Failure != nil || todoCount.Load() != 1 || childCount.Load() != 3 {
		t.Fatalf("failure=%+v tools=%d/%d", view.Failure, todoCount.Load(), childCount.Load())
	}
	users := 0
	for _, item := range view.Items {
		if item.Kind == ThreadItemUser {
			users++
		}
		if strings.Contains(item.Text, "old partial") {
			t.Fatal("failed attempt draft survived canonical settlement")
		}
	}
	if users != 1 {
		t.Fatalf("user messages=%d", users)
	}
	requests := gateway.Requests()
	if len(requests) != 5 {
		t.Fatalf("requests=%d", len(requests))
	}
	for i, req := range requests {
		if req.RunID != requests[0].RunID || req.TurnID != requests[0].TurnID || req.LogicalRequestID != requests[0].LogicalRequestID || req.AttemptEpoch != i+1 {
			t.Fatalf("identity drift: %+v", req)
		}
	}
	if requests[1].Step != requests[2].Step || requests[3].Step != requests[4].Step {
		t.Fatal("transient failures consumed agent steps")
	}
}
func TestRetryRetiresLiveDraftImmediatelyAndRejectsLateEvents(t *testing.T) {
	_, actor, sink, _ := liveActivityFixture(t)
	emit := func(kind observation.EventType, text string, retry *ProviderRetryProgress, epoch int) {
		sink.EmitEvent(Event{Type: kind, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Retry: retry, attemptLogicalID: "logical-live", attemptID: "attempt-1", attemptEpoch: epoch, Metadata: map[string]any{"logical_request_id": "logical-live", "attempt_id": "attempt-1", "attempt_epoch": epoch}, Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: text}})
	}
	emit(observation.EventTypeProviderRequest, "", nil, 1)
	emit(observation.EventTypeProviderDelta, "failed draft", nil, 1)
	emit(observation.EventTypeProviderRetry, "", &ProviderRetryProgress{Retry: 1, MaxRetries: 4, Reason: string(provider.RetryTransient), NextAttemptAt: time.Now().Add(time.Second)}, 1)
	if len(actor.state.view.Items) != 0 || actor.state.view.RunProgress.Retry == nil {
		t.Fatalf("draft/progress=%+v", actor.state.view)
	}
	emit(observation.EventTypeProviderDelta, "late event", nil, 1)
	if len(actor.state.view.Items) != 0 {
		t.Fatal("retired attempt was projected")
	}
}
func TestProviderRetryPolicyRejectsInvalidConfiguration(t *testing.T) {
	for _, mutate := range []func(*ProviderRetryPolicy){func(p *ProviderRetryPolicy) { p.MaxRetries = -1 }, func(p *ProviderRetryPolicy) { p.Jitter = 2 }, func(p *ProviderRetryPolicy) { p.RecoveryWindow = 0 }} {
		policy := DefaultProviderRetryPolicy()
		mutate(&policy)
		if policy.Validate() == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}

func TestProviderRetryWaitIsReadableAndStopsWithoutNewDispatch(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "retry", StateCompatibilityKey: "retry:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{ReturnError: &provider.ProviderHTTPError{StatusCode: 500}})
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-wait"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-wait", Input: UserInput{Text: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.RunProgress != nil && v.RunProgress.Retry != nil })
	if view.RunProgress.Retry.Retry != 1 || view.RunProgress.Retry.MaxRetries != 4 || view.RunProgress.Retry.NextAttemptAt.IsZero() {
		t.Fatalf("progress=%+v", view.RunProgress)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop-wait"}); err != nil {
		t.Fatal(err)
	}
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if *view.LastOutcome != TurnOutcomeCancelled || len(gateway.Requests()) != 1 {
		t.Fatalf("outcome=%+v requests=%d", view.LastOutcome, len(gateway.Requests()))
	}
}
