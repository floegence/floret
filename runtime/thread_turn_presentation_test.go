package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestThreadTurnPresentationPreservesHistoricalOutcomesAndWaitUnion(t *testing.T) {
	start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	entry := func(turn, run string, typ sessiontree.EntryType, status sessiontree.TurnMarkerStatus, second int) sessiontree.Entry {
		return sessiontree.Entry{TurnID: turn, RunID: run, Type: typ, TurnStatus: status, CreatedAt: start.Add(time.Duration(second) * time.Second)}
	}
	entries := []sessiontree.Entry{
		entry("interrupted", "run-old", sessiontree.EntryTurnMarker, sessiontree.TurnStarted, 0),
		entry("interrupted", "run-old", sessiontree.EntryTurnMarker, sessiontree.TurnAborted, 5),
		entry("completed", "run-new", sessiontree.EntryTurnMarker, sessiontree.TurnStarted, 10),
		entry("completed", "run-new", sessiontree.EntryInteractionAsked, "", 20),
		entry("completed", "run-new", sessiontree.EntryInteractionAsked, "", 25),
		entry("completed", "run-new", sessiontree.EntryInteractionDone, "", 35),
		entry("completed", "run-new", sessiontree.EntryInteractionDone, "", 40),
		entry("completed", "run-new", sessiontree.EntryTurnMarker, sessiontree.TurnCompleted, 70),
	}
	entries[1].Metadata = map[string]string{"recoverable": "true"}
	entries[3].RequestKey, entries[5].RequestKey = "approval-a", "approval-a"
	entries[4].RequestKey, entries[6].RequestKey = "approval-b", "approval-b"
	turns, err := threadRuntimeTurnsFromEntries(entries, nil)
	if err != nil || len(turns) != 2 {
		t.Fatalf("turns=%+v error=%v", turns, err)
	}
	if turns[0].Outcome == nil || *turns[0].Outcome != TurnOutcomeInterrupted || turns[0].RunID != "run-old" {
		t.Fatalf("historical interruption lost: %+v", turns[0])
	}
	if turns[1].Outcome == nil || *turns[1].Outcome != TurnOutcomeCompleted || turns[1].ElapsedMS != 60_000 || turns[1].WaitingMS != 20_000 || turns[1].WorkDurationMS != 40_000 {
		t.Fatalf("parallel waits must be deducted once: %+v", turns[1])
	}
	if turns[1].FinalBoundaryComplete || len(turns[1].FinalItemIDs) != 0 {
		t.Fatal("legacy completion must not invent a final answer boundary")
	}
	cloned := cloneThreadRuntimeView(ThreadView{Turns: turns})
	if !reflect.DeepEqual(cloned.Turns, turns) {
		t.Fatal("cloning changed canonical turn facts")
	}
	*cloned.Turns[0].Outcome = TurnOutcomeCompleted
	if *turns[0].Outcome != TurnOutcomeInterrupted {
		t.Fatal("view callers can mutate cached outcomes")
	}
}

func TestThreadTurnPresentationFinalSegmentsSurviveRestartAndNestedFork(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		steps      []florettest.Step
		finalTexts []string
	}{
		{"length", []florettest.Step{
			{Events: []provider.Event{{Type: provider.EventDelta, Text: "first final"}, {Type: provider.EventDone, Reason: "length"}}},
			{Events: []provider.Event{{Type: provider.EventDelta, Text: "second final"}, {Type: provider.EventDone, Reason: "stop"}}},
		}, []string{"first final", "second final"}},
		{"hosted", []florettest.Step{{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "identical"},
			{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}},
			{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{}},
			{Type: provider.EventDelta, Text: "identical"}, {Type: provider.EventDone, Reason: "stop"},
		}}}, []string{"identical"}},
		{"length_then_hosted", []florettest.Step{
			{Events: []provider.Event{{Type: provider.EventDelta, Text: "discarded draft"}, {Type: provider.EventDone, Reason: "length"}}},
			{Events: []provider.Event{
				{Type: provider.EventHostedToolCall, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}},
				{Type: provider.EventHostedToolResult, HostedToolCall: &provider.ToolCall{ID: "search", Name: "web_search"}, HostedResult: &provider.HostedToolResult{}},
				{Type: provider.EventDelta, Text: "final"}, {Type: provider.EventDone, Reason: "stop"},
			}},
		}, []string{"final"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "final", StateCompatibilityKey: "final:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, scenario.steps...)
			agent, err := testAgent(gateway, WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"}))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/turns.sqlite"
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			check := func(v ThreadView) {
				t.Helper()
				if len(v.Turns) != 1 || v.Turns[0].Outcome == nil || *v.Turns[0].Outcome != TurnOutcomeCompleted || !v.Turns[0].FinalBoundaryComplete {
					t.Fatalf("turns=%+v failure=%+v", v.Turns, v.Failure)
				}
				var texts []string
				for _, id := range v.Turns[0].FinalItemIDs {
					index := threadItemIndexByID(v.Items, id)
					if index < 0 {
						t.Fatalf("missing final item %s", id)
					}
					texts = append(texts, v.Items[index].Text)
				}
				if !reflect.DeepEqual(texts, scenario.finalTexts) {
					t.Fatalf("final texts=%q want=%q", texts, scenario.finalTexts)
				}
				if !reflect.DeepEqual(v.Turns, view.Turns) {
					t.Fatalf("turn facts changed across fork/restart: %+v want %+v", v.Turns, view.Turns)
				}
			}
			check(view)
			ids := []identity.ThreadID{created.ThreadID}
			for i := 0; i < 2; i++ {
				fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: ids[len(ids)-1], RequestKey: RequestKey(fmt.Sprintf("fork-%d", i))})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, fork.ThreadID)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = open()
			for _, id := range ids {
				v, err := service.View(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				check(v)
				page, err := service.History(t.Context(), id, "", 1)
				if err != nil || !reflect.DeepEqual(page.Turns, v.Turns) || len(page.Items) != 1 {
					t.Fatalf("history=%+v error=%v", page, err)
				}
			}
		})
	}
}

func TestThreadTurnPresentationRetainsFailureAfterLaterSuccess(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "final", StateCompatibilityKey: "final:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{ReturnError: errors.New("unavailable")},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "success"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []RequestKey{"fail", "success"} {
		receipt, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: key, Input: UserInput{Text: string(key)}})
		if err != nil {
			t.Fatal(err)
		}
		waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
			return v.TurnID == receipt.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
		})
	}
	v, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Turns) != 2 || *v.Turns[0].Outcome != TurnOutcomeFailed || v.Turns[0].FinalBoundaryComplete || *v.Turns[1].Outcome != TurnOutcomeCompleted || !v.Turns[1].FinalBoundaryComplete {
		t.Fatalf("turns=%+v", v.Turns)
	}
}

func TestThreadTurnPresentationKeepsStreamedLengthSegmentIdentity(t *testing.T) {
	release := make(chan struct{})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "segments", StateCompatibilityKey: "segments:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "first"}, {Type: provider.EventDone, Reason: "length"}}},
		florettest.Step{BlockUntil: release, Events: []provider.Event{{Type: provider.EventDelta, Text: "second"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	streamed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		for _, item := range v.Items {
			if item.Text == "first" {
				return true
			}
		}
		return false
	})
	var firstID string
	for _, item := range streamed.Items {
		if item.Text == "first" {
			firstID = item.ID
		}
	}
	close(release)
	completed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if len(completed.Turns) != 1 || len(completed.Turns[0].FinalItemIDs) != 2 || completed.Turns[0].FinalItemIDs[0] != firstID || completed.Items[threadItemIndexByID(completed.Items, firstID)].Text != "first" {
		t.Fatalf("streamed=%+v completed=%+v turns=%+v", streamed.Items, completed.Items, completed.Turns)
	}
}
