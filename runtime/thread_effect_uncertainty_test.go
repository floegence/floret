package runtime

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestReturnedEffectUncertaintyContinuesAndRetryPreservesResult(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "uncertain", StateCompatibilityKey: "test:uncertain"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "click-1", Name: "click", Args: "{}"}, {ID: "inspect-1", Name: "inspect", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "inspect-2", Name: "inspect", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "verified and finished remaining work"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued from result"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var calls atomic.Int32
	click := tools.Define[map[string]any](tools.Definition{Name: "click", InputSchema: tools.StrictObject(map[string]any{}, []string{}), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			calls.Add(1)
			text := "One input posted; receipt timed out."
			return tools.Result{IsError: true, EffectOutcome: tools.EffectOutcomeUncertain, Text: text, OutputPolicy: &tools.OutputPolicy{VisibleMaxBytes: len(text) + 1, VisibleMaxLines: 1, Strategy: tools.OutputHead, PreserveFull: true}}, nil
		})
	inspect := tools.Define[map[string]any](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{Text: "artifact exists; current state verified"}, nil
		})
	agent, err := testAgent(gateway, WithAgentTools(click, inspect), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
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
	settled := func(id identity.ThreadID) ThreadView {
		t.Helper()
		actor := svc.(*threadRuntimeService).runtime(id)
		actor.mu.Lock()
		done := actor.state.executionDone
		actor.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-t.Context().Done():
				t.Fatal("execution did not settle")
			}
		}
		view, err := svc.View(t.Context(), id)
		if err != nil || view.Activity != ThreadActivityIdle || view.LastOutcome == nil {
			t.Fatalf("settled view=%+v err=%v", view, err)
		}
		return view
	}
	created, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "click"}}); err != nil {
		t.Fatal(err)
	}
	final := settled(created.ThreadID)
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
				if !strings.Contains(msg.ToolResult.Text, "External effect unconfirmed.") || !strings.Contains(msg.ToolResult.Text, "One input posted; receipt timed out.") {
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
	retried := settled(created.ThreadID)
	if retried.TurnID == final.TurnID || retried.Failure != nil || calls.Load() != 1 {
		t.Fatalf("retry replayed uncertainty: %+v calls=%d", retried, calls.Load())
	}
	assertResult(3)
}

func TestRetryContinuationNeverSelectsSavepointBeforeUncertainty(t *testing.T) {
	result := func(id string) sessiontree.CanonicalTurnPathEntry {
		return sessiontree.CanonicalTurnPathEntry{Entry: sessiontree.Entry{ID: id, Message: session.Message{ToolResult: &session.ToolResultView{EffectOutcome: tools.EffectOutcomeUncertain}}}}
	}
	save := func(id, parent string) sessiontree.CanonicalTurnPathEntry {
		return sessiontree.CanonicalTurnPathEntry{Entry: sessiontree.Entry{ID: id, ParentID: parent, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnSavePoint}}
	}
	user := sessiontree.CanonicalTurnPathEntry{Entry: sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User}}}
	terminal := sessiontree.CanonicalTurnPathEntry{Entry: sessiontree.Entry{ID: "terminal", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnFailed}}
	for _, test := range []struct {
		name, want string
		entries    []sessiontree.CanonicalTurnPathEntry
	}{
		{"before-savepoint", "terminal", []sessiontree.CanonicalTurnPathEntry{user, save("old", "user"), result("uncertain"), terminal}},
		{"after-savepoint", "uncertain", []sessiontree.CanonicalTurnPathEntry{user, save("old", "user"), result("uncertain"), save("new", "uncertain"), terminal}},
		{"later-uncertainty", "terminal", []sessiontree.CanonicalTurnPathEntry{user, result("first"), save("first-save", "first"), result("second"), terminal}},
		{"ordinary-completed", "user", []sessiontree.CanonicalTurnPathEntry{user, save("ordinary", "answer")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := retryContinuationEntryID(sessiontree.CanonicalTurn{Entries: test.entries})
			if got != test.want {
				t.Fatalf("retry source=%s want=%s", got, test.want)
			}
		})
	}
}

func TestRetryAfterRestartBetweenUncertainResultAndSavepoint(t *testing.T) {
	path := t.TempDir() + "/interrupted.db"
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "interrupted-thread"}); err != nil {
		t.Fatal(err)
	}
	input := session.Message{Role: session.User, Content: "inspect the existing result"}
	request := sessiontree.AcceptTurnRequest{ThreadID: "interrupted-thread", TurnID: "interrupted-turn", RunID: "interrupted-run", LogicalRequestID: "original", Input: input, InputRequestFingerprint: "input"}
	request.RequestFingerprint, err = sessiontree.TurnAcceptanceRequestFingerprint(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.(sessiontree.RuntimeTurnRepo).AcceptTurn(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	for _, message := range []session.Message{
		{Role: session.Assistant, ToolCallID: "original-effect", ToolName: "click", ToolArgs: "{}"},
		{Role: session.Tool, ToolCallID: "original-effect", ToolName: "click", Content: "External effect unconfirmed. Inspect current state.", ToolResult: &session.ToolResultView{Status: "error", EffectOutcome: tools.EffectOutcomeUncertain}},
	} {
		if _, err := sessiontree.AppendRunMessage(t.Context(), repo, "interrupted-thread", "interrupted-turn", "interrupted-run", message); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.(sessiontree.RuntimeTurnRepo).FinishTurn(t.Context(), sessiontree.FinishTurnRequest{ThreadID: "interrupted-thread", TurnID: "interrupted-turn", RunID: "interrupted-run", TerminalEntryID: "original-terminal", OutcomeFingerprint: "finish", Status: sessiontree.TurnFailed, FailureMessage: "savepoint unavailable", Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureStorage}}); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "retry", StateCompatibilityKey: "test:retry"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "inspect without replay"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err = Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Retry(t.Context(), RetryInput{ThreadID: identity.ThreadID("interrupted-thread"), SourceTurnID: identity.TurnID("interrupted-turn"), RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, svc, "interrupted-thread", func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("retry=%+v", final)
	}
	requests := gateway.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests=%d", len(requests))
	}
	found := false
	for _, message := range requests[0].Messages {
		if message.ToolResult != nil && message.ToolResult.CallID == "original-effect" {
			found = true
		}
	}
	if !found {
		t.Fatal("retry dropped committed uncertain result")
	}
}
