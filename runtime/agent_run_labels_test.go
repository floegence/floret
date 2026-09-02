package runtime

import (
	"context"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestAgentRunLabelsReachFreshAndResumedExecutions(t *testing.T) {
	const askArgs = `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:run-labels:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "effect-fresh", Name: "effect", Args: `{}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-run-labels", Name: "ask_user", Args: askArgs}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "effect-resumed", Name: "effect", Args: `{}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)

	wantCorrelation := map[string]string{"request": "request-1"}
	wantHost := map[string]string{"permission_snapshot_id": "snapshot-1", "permission_epoch": "epoch-1"}
	wantInvocationLabels := map[string]string{
		"correlation.request":         "request-1",
		"host.permission_snapshot_id": "snapshot-1",
		"host.permission_epoch":       "epoch-1",
	}
	inputCorrelation := maps.Clone(wantCorrelation)
	inputHost := maps.Clone(wantHost)
	var gateCalls atomic.Int32
	var toolCalls atomic.Int32
	effectTool := tools.Define[map[string]any](
		tools.Definition{
			Name: "effect", InputSchema: tools.StrictObject(nil, nil),
			Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, invocation tools.Invocation[map[string]any]) (tools.Result, error) {
			if !maps.Equal(invocation.Labels, wantInvocationLabels) || !maps.Equal(invocation.HostContext, wantHost) {
				t.Fatalf("tool labels=%v host=%v", invocation.Labels, invocation.HostContext)
			}
			invocation.Labels["request"] = "mutated-by-tool"
			invocation.HostContext["permission_epoch"] = "mutated-by-tool"
			toolCalls.Add(1)
			return tools.Result{Text: "effect complete"}, nil
		},
	)
	agent, err := testAgent(
		gateway,
		WithAgentTools(effectTool),
		WithAgentRunLabels(RunLabels{Correlation: inputCorrelation, Host: inputHost}),
		WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
			if !maps.Equal(request.Labels, wantInvocationLabels) || !maps.Equal(request.HostContext, wantHost) {
				t.Fatalf("authorization labels=%v host=%v", request.Labels, request.HostContext)
			}
			request.Labels["request"] = "mutated-by-gate"
			request.HostContext["permission_epoch"] = "mutated-by-gate"
			gateCalls.Add(1)
			return effect(ctx, EffectAuthorizationProof{
				EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
				ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
				PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-audit-hash", AuthorizedAt: time.Now().UTC(),
			})
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	inputCorrelation["request"] = "mutated-after-construction"
	inputHost["permission_epoch"] = "mutated-after-construction"

	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-run-labels"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "begin"}, RequestKey: "send-run-labels"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 1 && view.Interactions[0].Kind == ThreadInteractionInput && !view.Interactions[0].Resolved
	})
	if _, err := service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID,
		Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}, RequestKey: "respond-run-labels",
	}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if completed.Failure != nil || *completed.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("completed=%#v", completed)
	}
	if gateCalls.Load() != 2 || toolCalls.Load() != 2 {
		t.Fatalf("gate calls=%d tool calls=%d, want 2 each", gateCalls.Load(), toolCalls.Load())
	}
	requests := gateway.Requests()
	if len(requests) != 4 {
		t.Fatalf("provider requests=%d, want 4", len(requests))
	}
	for index, request := range requests {
		if !maps.Equal(request.Labels.Correlation, wantCorrelation) || !maps.Equal(request.Labels.Host, wantHost) {
			t.Fatalf("provider request %d labels=%v host=%v", index, request.Labels.Correlation, request.Labels.Host)
		}
	}
}
