package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestCorrectionExhaustionDoesNotExposeCompletedSiblingInput(t *testing.T) {
	steps := []florettest.Step{}
	for i := 0; i < 3; i++ {
		calls := []provider.ToolCall{{ID: fmt.Sprint("bad-", i), Name: "read", Args: `{"unexpected":true}`}}
		if i == 2 {
			calls = append(calls, provider.ToolCall{ID: "input", Name: "read", Args: `{}`})
		}
		steps = append(steps, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: calls}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
	calls := 0
	tool := tools.Define[map[string]any](tools.Definition{Name: "read", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		calls++
		return tools.Result{Text: "CONFIRMED", InputRequired: &tools.InputRequest{Summary: "Continue?", Questions: []tools.InputQuestion{{ID: "answer", Prompt: "Continue?", Kind: "write"}}}}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/correction.sqlite"
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	failed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if failed.Failure == nil || failed.Failure.Reason != ThreadTurnFailureToolArgumentCorrectionExhausted || len(failed.Interactions) != 0 || calls != 1 {
		t.Fatalf("failure=%+v interactions=%+v calls=%d", failed.Failure, failed.Interactions, calls)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, err = Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err = host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil || restored.Failure == nil || restored.Failure.Reason != failed.Failure.Reason || len(restored.Interactions) != 0 {
		t.Fatalf("restored=%+v err=%v", restored, err)
	}
}
