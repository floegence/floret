package runtime

import (
	"context"
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"testing"
	"time"
)

func TestThreadOutputTimingSurvivesColdHistoryRestartAndFork(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{Kind: config.ReasoningKindNone}}, florettest.Step{Events: []provider.Event{{Type: provider.EventReasoning, Text: "short thought"}, {Type: provider.EventDelta, Text: "answer"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/timing.db"
	open := func() (*Host, ThreadService) {
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		return h, s
	}
	host, svc := open()
	root, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Send(t.Context(), SendInput{ThreadID: root.ThreadID, RequestKey: "send", Input: UserInput{Text: "think"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, svc, root.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	expected := map[string][2]time.Time{}
	anchor := ""
	for _, item := range view.Items {
		if item.Kind == ThreadItemThinking || item.Kind == ThreadItemAssistant {
			if item.OutputStartedAt == nil || item.OutputEndedAt == nil || !item.OutputStartedAt.Equal(*item.OutputEndedAt) {
				t.Fatalf("single chunk timing missing: %+v", item)
			}
			expected[item.ID] = [2]time.Time{*item.OutputStartedAt, *item.OutputEndedAt}
			if item.Kind == ThreadItemAssistant {
				anchor = item.ID
			}
		}
	}
	if len(expected) != 2 {
		t.Fatalf("missing outputs: %+v", view)
	}
	check := func(items []ThreadItem) {
		t.Helper()
		count := 0
		for _, item := range items {
			if want, ok := expected[item.ID]; ok {
				count++
				if item.OutputStartedAt == nil || item.OutputEndedAt == nil || !item.OutputStartedAt.Equal(want[0]) || !item.OutputEndedAt.Equal(want[1]) {
					t.Fatalf("timing changed on history read: %+v", item)
				}
			}
		}
		if count != 2 {
			t.Fatalf("missing timed history: %d", count)
		}
	}

	checkWindow := func() {
		t.Helper()
		scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{root.ThreadID}}
		sub, err := svc.(*threadRuntimeService).SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		if err = sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: root.ThreadID}); err != nil {
			t.Fatal(err)
		}
		for {
			update := nextUpdate(t, sub)
			if update.Window != nil {
				check(update.Window.Value.Items)
				break
			}
		}
	}
	checkWindow()
	history, err := svc.History(t.Context(), root.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	check(history.Items)
	cloned := cloneThreadItems(view.Items)
	for i := range cloned {
		if cloned[i].OutputStartedAt != nil {
			*cloned[i].OutputStartedAt = time.Time{}
			*cloned[i].OutputEndedAt = time.Time{}
		}
	}
	check(view.Items)
	if err = host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, svc = open()
	checkWindow()
	history, err = svc.History(t.Context(), root.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	check(history.Items)
	fork, err := svc.Fork(t.Context(), ForkThreadInput{SourceThreadID: root.ThreadID, SourceItemID: anchor, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	history, err = svc.History(t.Context(), fork.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	check(history.Items)
}

func TestThreadLiveOutputTimingUsesSourceClockAndLastChunk(t *testing.T) {
	_, actor, _, _ := liveActivityFixture(t)
	start := time.Unix(1700000000, 0).UTC()
	last := start.Add(250 * time.Millisecond)
	actor.appendLiveOutput(ThreadItemThinking, Event{Timestamp: start.Add(time.Hour), outputSourceID: "reason", outputStartedAt: start, outputLastAt: last, Stream: &StreamObservation{Text: "thought"}})
	item := actor.state.view.Items[0]
	if item.OutputStartedAt == nil || !item.OutputStartedAt.Equal(start) || item.OutputEndedAt != nil {
		t.Fatalf("live source clock lost: %+v", item)
	}
	actor.finishLiveTextSegment()
	item = actor.state.view.Items[0]
	if item.OutputEndedAt == nil || !item.OutputEndedAt.Equal(last) {
		t.Fatalf("seal included delivery delay: %+v", item)
	}
	actor.state.view.Items[0].CanonicalOrdinal = 1
	actor.appendLiveOutput(ThreadItemThinking, Event{Timestamp: last.Add(time.Hour), outputSourceID: "reason", outputStartedAt: last, outputLastAt: last.Add(time.Hour), Stream: &StreamObservation{Text: "stale"}})
	item = actor.state.view.Items[0]
	if !item.OutputStartedAt.Equal(start) || item.OutputEndedAt == nil || !item.OutputEndedAt.Equal(last) {
		t.Fatal("stale event changed canonical timing")
	}
}
