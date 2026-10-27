package runtime

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestAskUserRestartResumesAndDrainsQueuedInput(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "Opening progress before asking"},
			{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}},
			{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{}},
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-1", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "queued answer"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	path := t.TempDir() + "/ask.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			return testAgent(gateway, WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}))
		}))
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
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "ask", Input: UserInput{Text: "begin"}}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "queued", Input: UserInput{Text: "next"}}); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	restored, err := svc.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Queue) != 1 || restored.Attention.InputCount != 1 {
		t.Fatalf("restored=%+v", restored)
	}
	if _, err := svc.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID, RequestKey: "answer", Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil {
		t.Fatalf("resume failed: %+v", final.Failure)
	}
	final = waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && len(v.Queue) == 0 && v.TurnID != waiting.TurnID
	})
	if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted || len(gateway.Requests()) != 3 {
		t.Fatalf("final=%+v requests=%d", final, len(gateway.Requests()))
	}
	var intro ThreadItem
	for _, item := range final.Items {
		if item.Text == "Opening progress before asking" {
			intro = item
		}
	}
	if intro.ID == "" || !slices.Contains(final.Turns[0].IntermediateItemIDs, intro.ID) || intro.RunID == final.Turns[0].RunID {
		t.Fatalf("earlier run lost its confirmed process: item=%+v turns=%+v", intro, final.Turns)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	// The first read after restart selects one old-run item, never a full View.
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	sub, err := svc.(ThreadWindowSource).SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: created.ThreadID, ItemIDs: []string{intro.ID}}); err != nil {
		t.Fatal(err)
	}
	for {
		update := nextUpdate(t, sub)
		if update.Window == nil {
			continue
		}
		if len(update.Window.Value.Items) != 1 || update.Turns == nil || len(update.Turns.Value) != 1 || !reflect.DeepEqual(update.Turns.Value[0].IntermediateItemIDs, []string{intro.ID}) {
			t.Fatalf("cold bounded old-run membership: %+v", update)
		}
		break
	}
	reopened, err := svc.View(t.Context(), created.ThreadID)
	if err != nil || !reflect.DeepEqual(reopened.Turns, final.Turns) {
		t.Fatalf("restart classification changed: %+v %v", reopened.Turns, err)
	}
}
