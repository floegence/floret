package runtime

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestReturnedEffectUncertaintyContinuesAndRetryPreservesResult(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "uncertain", StateCompatibilityKey: "test:uncertain"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "click-1", Name: "click", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "inspect before continuing"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued from result"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var calls atomic.Int32
	click := tools.Define[map[string]any](tools.Definition{Name: "click", InputSchema: tools.StrictObject(map[string]any{}, []string{}), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			calls.Add(1)
			return tools.Result{IsError: true, EffectOutcome: tools.EffectOutcomeUncertain, Text: "One input posted; receipt timed out."}, nil
		})
	agent, err := testAgent(gateway, WithAgentTools(click), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/uncertain.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
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
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "click"}}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted || len(final.Interactions) != 0 || calls.Load() != 1 {
		t.Fatalf("unexpected settlement: %+v calls=%d", final, calls.Load())
	}
	assertResult := func(index int) {
		t.Helper()
		requests := gateway.Requests()
		if len(requests) <= index {
			t.Fatalf("missing request %d", index)
		}
		count := 0
		for _, msg := range requests[index].Messages {
			if msg.ToolResult != nil && msg.ToolResult.CallID == "click-1" {
				count++
				if !strings.Contains(msg.ToolResult.Text, "External effect unconfirmed.") {
					t.Fatalf("uncertainty missing: %s", msg.ToolResult.Text)
				}
			}
		}
		if count != 1 {
			t.Fatalf("results=%d", count)
		}
	}
	assertResult(1)
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	if _, err := svc.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: final.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	retried := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID != final.TurnID
	})
	if retried.Failure != nil || calls.Load() != 1 {
		t.Fatalf("retry replayed uncertainty: %+v calls=%d", retried, calls.Load())
	}
	assertResult(2)
}
