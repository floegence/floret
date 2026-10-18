package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
)

func goalCall(id, args string) florettest.Step {
	return florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: "goal", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
}
func goalService(t *testing.T, steps ...florettest.Step) (ThreadService, *florettest.ScriptedGateway) {
	t.Helper()
	g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goals", StateCompatibilityKey: "test:goals"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
	agent, err := testAgent(g, WithAgentGoals())
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	return service, g
}
func TestGoalMutationDoesNotRunAndPreservesText(t *testing.T) {
	service, g := goalService(t)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalController)
	text := "  " + strings.Repeat("Clear goal text. ", 1000) + "  "
	in := SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: text}
	set, err := controller.SetGoal(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if set.Goal.Text != text || set.Goal.Version != 1 || len(g.Requests()) != 0 {
		t.Fatalf("result=%+v requests=%d", set, len(g.Requests()))
	}
	replay, err := controller.SetGoal(t.Context(), in)
	if err != nil || !replay.Replayed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	conflict, err := controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "stale", Text: "other"})
	if err != nil || !conflict.Conflict || conflict.Goal.Text != text {
		t.Fatalf("conflict=%+v err=%v", conflict, err)
	}
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || view.Goal == nil || view.Goal.Text != text {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	deleted, err := controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "delete", ExpectedVersion: 1})
	if err != nil || deleted.Goal.Status != "absent" || deleted.Goal.Version != 2 {
		t.Fatalf("delete=%+v err=%v", deleted, err)
	}
}
func TestGoalNaturalStopReflectsWithSameToolsAndContext(t *testing.T) {
	service, g := goalService(t, parentEventDone(), goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"The requested result is present and verified."}`), parentEventDone())
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Deliver the verified result"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish the task"}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || view.Goal == nil || view.Goal.Status != "completed" {
		t.Fatalf("view=%+v goal=%+v failure=%+v", view, view.Goal, view.Failure)
	}
	requests := g.Requests()
	if len(requests) != 3 {
		t.Fatalf("requests=%d", len(requests))
	}
	for i := 1; i < len(requests); i++ {
		a, _ := json.Marshal(requests[0].Tools)
		b, _ := json.Marshal(requests[i].Tools)
		if string(a) != string(b) {
			t.Fatal("tool definitions changed during reflection")
		}
	}
	for _, item := range view.Items {
		if item.Kind == ThreadItemUser && strings.Contains(item.Text, "Check the current Goal") {
			t.Fatal("reflection displayed as user message")
		}
	}
	if requests[0].Messages[0].Text != requests[1].Messages[0].Text {
		t.Fatal("base prompt changed")
	}
}
func TestGoalBlockedAndLoopRemainRecoverable(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{true: "blocked", false: "review_limit"}[blocked], func(t *testing.T) {
			steps := []florettest.Step{parentEventDone(), parentEventDone(), parentEventDone()}
			if blocked {
				steps = []florettest.Step{goalCall("blocked", `{"operation":"review","expected_version":1,"outcome":"blocked","evidence":"External credentials require user action."}`), parentEventDone()}
			}
			service, g := goalService(t, steps...)
			thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Deliver result"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish"}})
			if err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if view.Goal == nil || view.Goal.Status != "active" || blocked && view.Failure != nil || !blocked && view.Failure == nil {
				t.Fatalf("goal=%+v failure=%+v requests=%d", view.Goal, view.Failure, len(g.Requests()))
			}
			if _, err := service.(ThreadGoalController).DeleteGoal(context.Background(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "clear", ExpectedVersion: 1}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGoalUpdateDuringRequestUsesNextRequest(t *testing.T) {
	gate := make(chan struct{})
	first := parentEventDone()
	first.BlockUntil = gate
	service, g := goalService(t, first, goalCall("review", `{"operation":"review","expected_version":2,"outcome":"completed","evidence":"Updated result is verified."}`), parentEventDone())
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalController)
	if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Original result"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish"}}); err != nil {
		t.Fatal(err)
	}
	if err = g.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "update", ExpectedVersion: 1, Text: "Updated result"}); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || view.Goal == nil || view.Goal.Text != "Updated result" {
		t.Fatalf("view=%+v err=%v", view.Goal, err)
	}
	close(gate)
	view = waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || view.Goal.Status != "completed" {
		t.Fatalf("goal=%+v failure=%+v", view.Goal, view.Failure)
	}
	for i, req := range g.Requests() {
		raw, _ := json.Marshal(req.Messages)
		if i == 0 && strings.Contains(string(raw), "Updated result") || i > 0 && !strings.Contains(string(raw), "Updated result") {
			t.Fatalf("incorrect goal snapshot in request %d", i)
		}
	}
}
func TestGoalDeletionCancelsReflectionWithoutStopping(t *testing.T) {
	gate := make(chan struct{})
	first := parentEventDone()
	first.BlockUntil = gate
	service, g := goalService(t, first)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalController)
	if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Result"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish"}}); err != nil {
		t.Fatal(err)
	}
	if err = g.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "delete", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	close(gate)
	view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil || view.Goal.Status != "absent" || len(g.Requests()) != 1 {
		t.Fatalf("goal=%+v failure=%+v requests=%d", view.Goal, view.Failure, len(g.Requests()))
	}
}
