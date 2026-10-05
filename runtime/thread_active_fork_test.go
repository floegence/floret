package runtime

import (
	"context"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestActiveToolForkAcceptsFirstChildTurn(t *testing.T) {
	rootGateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "delegate", Name: "delegate", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "parent done"}, {Type: provider.EventDone, Reason: "stop"}}})
	childGateway := florettest.NewScriptedGateway(rootGateway.Identity(), rootGateway.Capabilities(), florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "child done"}, {Type: provider.EventDone, Reason: "stop"}}})
	path := t.TempDir() + "/fork.db"
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	var svc ThreadService
	var rootID identity.ThreadID
	childIDs := make(chan identity.ThreadID, 1)
	delegate := tools.Define[map[string]any](tools.Definition{Name: "delegate", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		child, err := svc.Fork(ctx, ForkThreadInput{SourceThreadID: rootID, ParentThreadID: rootID, TaskName: "Child", HostProfileRef: "worker", ForkMode: "full_history", RequestKey: "fork"})
		if err != nil {
			return tools.Result{}, err
		}
		childIDs <- child.ThreadID
		_, err = svc.Send(ctx, SendInput{ThreadID: child.ThreadID, RequestKey: "child-send", Input: UserInput{Text: "use inherited history"}})
		return tools.Result{Text: "delegated"}, err
	})
	rootAgent, err := testAgent(rootGateway, WithAgentTools(delegate), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	svc, err = host.ThreadService(AgentFactoryFunc(func(_ context.Context, r AgentRequest) (*Agent, error) {
		if r.ThreadID == rootID {
			return rootAgent, nil
		}
		return testAgent(childGateway)
	}))
	if err != nil {
		t.Fatal(err)
	}
	root, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	rootID = root.ThreadID
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: rootID, RequestKey: "start", Input: UserInput{Text: "delegate"}}); err != nil {
		t.Fatal(err)
	}
	var childID identity.ThreadID
	select {
	case childID = <-childIDs:
	case <-time.After(3 * time.Second):
		t.Fatal("no child")
	}
	child := waitThreadView(t, svc, childID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if child.Failure != nil || *child.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("child failed: %+v", child.Failure)
	}
	if len(childGateway.Requests()) != 1 {
		t.Fatal("child provider not called")
	}
	paired := false
	for _, message := range childGateway.Requests()[0].Messages {
		if message.ToolResult != nil && message.ToolResult.CallID == "delegate" && strings.Contains(message.ToolResult.Text, "not inherited") {
			paired = true
		}
	}
	if !paired {
		t.Fatal("child history did not close the inherited tool call")
	}
	parent := waitThreadView(t, svc, rootID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if parent.Failure != nil {
		t.Fatalf("fork affected parent: %+v", parent.Failure)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, err = Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []identity.ThreadID{rootID, childID} {
		entries, err := host.store.repo.Path(t.Context(), id.String(), "")
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
		for _, message := range messages {
			if message.ToolCallID == "delegate" && message.Role == session.Tool {
				if id == rootID && message.Content != "delegated" {
					t.Fatalf("parent result=%q", message.Content)
				}
				if id == childID && !strings.Contains(message.Content, "not inherited") {
					t.Fatalf("child result=%q", message.Content)
				}
			}
		}
	}
}
