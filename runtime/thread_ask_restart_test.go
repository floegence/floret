package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestAskUserRestartResumesAndDrainsQueuedInput(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
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
		svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
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
}
