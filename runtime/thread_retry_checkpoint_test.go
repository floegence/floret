package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestRetryFailedTurnRetainsConfirmedToolResultAcrossRestart(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-once", Name: "write", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{ReturnError: errors.New("stream lost after confirmed write")},
		florettest.Step{ReturnError: errors.New("another stream loss")},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "next"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var writes atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		writes.Add(1)
		return tools.Result{Text: "EFFECT_COMMITTED"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/retry.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return h, s
	}
	host, svc := open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	created, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write once then explain"}}); err != nil {
		t.Fatal(err)
	}
	current := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	for _, key := range []RequestKey{"retry-first", "retry-second"} {
		if current.Failure == nil {
			t.Fatal("expected provider failure")
		}
		if err := host.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		host, svc = open()
		if _, err := svc.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: current.TurnID, RequestKey: key}); err != nil {
			t.Fatal(err)
		}
		current = waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
		reqs := gateway.Requests()
		req := reqs[len(reqs)-1]
		found := false
		for _, m := range req.Messages {
			if m.ToolResult != nil && strings.Contains(m.ToolResult.Text, "EFFECT_COMMITTED") {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s discarded the confirmed tool result", key)
		}
	}
	if current.Failure != nil || writes.Load() != 1 || countThreadItems(current.Items, ThreadItemUser) != 1 {
		t.Fatalf("final failure=%+v writes=%d", current.Failure, writes.Load())
	}
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "next", Input: UserInput{Text: "remember the completed work"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	reqs := gateway.Requests()
	found := false
	for _, m := range reqs[len(reqs)-1].Messages {
		if m.ToolResult != nil && strings.Contains(m.ToolResult.Text, "EFFECT_COMMITTED") {
			found = true
		}
	}
	if !found {
		t.Fatal("next turn discarded confirmed work after retry")
	}
}
