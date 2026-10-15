package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
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
	var service ThreadService
	define := func(name string, count *atomic.Int32) tools.Tool {
		return tools.Define[map[string]any](tools.Definition{Name: name, ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil)}, nil, nil, func(ctx context.Context, invocation tools.Invocation[map[string]any]) (tools.Result, error) {
			count.Add(1)
			if name == "create_child" {
				child, err := service.Create(ctx, CreateThreadInput{ParentThreadID: invocation.ThreadID, ParentTurnID: invocation.TurnID, TaskName: invocation.CallID, HostProfileRef: "test", RequestKey: RequestKey("create:" + invocation.CallID)})
				if err != nil {
					return tools.Result{}, err
				}
				return tools.Result{Text: child.ThreadID.String()}, nil
			}
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
	_, service = testThreadServiceWithAgent(t, agent)
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
	children, err := service.List(t.Context(), ThreadScope{ParentID: &created.ThreadID})
	if err != nil || len(children) != 3 {
		t.Fatalf("durable children=%+v err=%v", children, err)
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

type frozenReplayGateway struct {
	*florettest.ScriptedGateway
	preparations int
	replays      int
}

func (g *frozenReplayGateway) Prepare(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	g.preparations++
	if g.preparations != 1 {
		return nil, errors.New("mutable input was resolved again")
	}
	return &frozenReplayPrepared{calibrationPrepared: calibrationPrepared{request: req, tokens: 32}, gateway: g}, nil
}

type frozenReplayPrepared struct {
	calibrationPrepared
	gateway  *frozenReplayGateway
	consumed bool
}

func (p *frozenReplayPrepared) Stream(ctx context.Context) (<-chan provider.Event, error) {
	if p.consumed {
		return nil, errors.New("single-use request consumed")
	}
	p.consumed = true
	return p.gateway.Stream(ctx, p.request)
}
func (p *frozenReplayPrepared) Close() error { p.consumed = true; return nil }
func (p *frozenReplayPrepared) PrepareRetry(ctx context.Context, next provider.Request) (provider.PreparedRequest, error) {
	if !p.consumed {
		return nil, errors.New("previous handle was not closed")
	}
	p.gateway.replays++
	return &frozenReplayPrepared{calibrationPrepared: calibrationPrepared{request: next, tokens: p.tokens}, gateway: p.gateway}, nil
}
func TestExpandedGatewayRetriesFrozenPreparationAfterClose(t *testing.T) {
	g := &frozenReplayGateway{ScriptedGateway: florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "expanded", StateCompatibilityKey: "frozen:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentExpanded}, florettest.Step{ReturnError: &provider.ProviderHTTPError{StatusCode: 500}}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})}
	policy := DefaultProviderRetryPolicy()
	policy.BaseDelay = time.Microsecond
	policy.MaxDelay = time.Microsecond
	policy.Jitter = 0
	agent, err := testAgent(g, WithAgentProviderRetryPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "frozen-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "frozen-send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || g.preparations != 1 || g.replays != 1 {
		t.Fatalf("failure=%+v preparations=%d replays=%d", view.Failure, g.preparations, g.replays)
	}
	reqs := g.Requests()
	if len(reqs) != 2 || reqs[0].AttemptID == reqs[1].AttemptID || reqs[0].RunID != reqs[1].RunID {
		t.Fatalf("attempt identities: %+v", reqs)
	}
}

func TestProviderRecoveryWindowExhaustionRemainsProviderFailure(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "window", StateCompatibilityKey: "window:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{ReturnError: context.DeadlineExceeded}, florettest.Step{WaitForCancellation: true})
	policy := DefaultProviderRetryPolicy()
	policy.BaseDelay, policy.MaxDelay, policy.Jitter, policy.RecoveryWindow = time.Microsecond, time.Microsecond, 0, 50*time.Millisecond
	agent, err := testAgent(gateway, WithAgentProviderRetryPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-window"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-window", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	current := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if *current.LastOutcome != TurnOutcomeFailed || current.Failure == nil || current.Failure.Code != ThreadTurnFailureProvider || current.Cancellation != nil {
		t.Fatalf("current=%+v", current)
	}
	if got := service.(*threadRuntimeService).cancellationDiagnostics(); len(got) != 0 {
		t.Fatalf("false cancellation diagnostics=%+v", got)
	}
	if len(gateway.Requests()) != 2 {
		t.Fatalf("attempts=%d", len(gateway.Requests()))
	}
}

func TestPreparationContextErrorsDoNotCancelActiveExecution(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			host, err := Open(t.Context(), Options{Storage: storage.Memory()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			service, err := host.ThreadService(AgentFactoryFunc(func(ctx context.Context, _ AgentRequest) (*Agent, error) {
				if ctx.Err() != nil {
					t.Fatalf("execution was canceled before preparation: %v", ctx.Err())
				}
				return nil, cause
			}))
			if err != nil {
				t.Fatal(err)
			}
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-preparation-error"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-preparation-error", Input: UserInput{Text: "work"}}); err != nil {
				t.Fatal(err)
			}
			current := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if *current.LastOutcome != TurnOutcomeFailed || current.Failure == nil || current.Failure.Code != ThreadTurnFailureEngineContract || current.Cancellation != nil {
				t.Fatalf("current=%+v", current)
			}
		})
	}
}
