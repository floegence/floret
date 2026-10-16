package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestForkPinsHistoricalAssistantItem(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "first answer"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "later answer"}, {Type: provider.EventDone, Reason: "stop"}}})
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/fork.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	root, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	var anchor string
	for _, key := range []string{"first", "later"} {
		if _, err := svc.Send(t.Context(), SendInput{ThreadID: root.ThreadID, RequestKey: RequestKey(key), Input: UserInput{Text: key}}); err != nil {
			t.Fatal(err)
		}
		view := waitThreadView(t, svc, root.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
		if key == "first" {
			for _, item := range view.Items {
				if item.Kind == ThreadItemAssistant {
					anchor = item.ID
				}
			}
		}
	}
	request := ForkThreadInput{SourceThreadID: root.ThreadID, SourceItemID: anchor, RequestKey: "historical"}
	fork, err := svc.Fork(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	history, err := svc.History(t.Context(), fork.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	texts := ""
	for _, item := range history.Items {
		texts += item.Text + "\n"
	}
	if !strings.Contains(texts, "first answer") || strings.Contains(texts, "later") {
		t.Fatalf("wrong fork history: %q", texts)
	}
	replay, err := svc.Fork(t.Context(), request)
	if err != nil || replay.ThreadID != fork.ThreadID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	request.SourceItemID = "missing"
	if _, err := svc.Fork(t.Context(), request); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed anchor: %v", err)
	}
	for i, anchor := range []string{"missing", history.Items[0].ID} {
		_, err := svc.Fork(t.Context(), ForkThreadInput{SourceThreadID: root.ThreadID, SourceItemID: anchor, RequestKey: RequestKey("invalid" + string(rune('a'+i)))})
		if err == nil {
			t.Fatal("accepted non-assistant anchor")
		}
	}
	other, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Fork(t.Context(), ForkThreadInput{SourceThreadID: other.ThreadID, SourceItemID: anchor, RequestKey: "foreign"}); err == nil {
		t.Fatal("accepted foreign anchor")
	}
	if _, err := svc.Fork(t.Context(), ForkThreadInput{SourceThreadID: identity.ThreadID(root.ThreadID), SourceEntryID: "entry", SourceItemID: anchor, RequestKey: "both"}); err == nil {
		t.Fatal("accepted conflicting anchors")
	}
}
