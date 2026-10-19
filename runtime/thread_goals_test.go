package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
	if _, err := json.Marshal(view); err != nil {
		t.Fatalf("goal-only current view must serialize before its first Turn: %v", err)
	}
	history, err := service.History(t.Context(), thread.ThreadID, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(history); err != nil {
		t.Fatalf("goal-only history must serialize before its first Turn: %v", err)
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
	foundReview := false
	for _, item := range view.Items {
		if item.Activity == nil || item.Activity.ToolName != "goal" {
			continue
		}
		raw, err := json.Marshal(item.Activity.Presentation)
		if err != nil {
			t.Fatal(err)
		}
		var activity struct {
			Payload struct {
				Goal struct{ Text, Outcome, Evidence string }
			}
		}
		if err = json.Unmarshal(raw, &activity); err != nil {
			t.Fatal(err)
		}
		if activity.Payload.Goal.Text != "Deliver the verified result" || activity.Payload.Goal.Outcome != "completed" || activity.Payload.Goal.Evidence != "The requested result is present and verified." {
			t.Fatalf("incorrect review presentation: %s", raw)
		}
		foundReview = true
	}
	if !foundReview {
		t.Fatal("missing Goal review activity")
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

func TestGoalSurvivesRegenerationAndFrozenTransportRetry(t *testing.T) {
	gate := make(chan struct{})
	failed := parentEventDone()
	failed.BlockUntil = gate
	failed.Events = nil
	failed.ReturnError = provider.WithRetryAdvice(errors.New("temporary"), provider.RetryAdvice{Category: provider.RetryTransient})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goals", StateCompatibilityKey: "test:goals"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, failed, goalCall("review", `{"operation":"review","expected_version":2,"outcome":"blocked","evidence":"External credential is unavailable."}`), parentEventDone(), parentEventDone())
	retry := DefaultProviderRetryPolicy()
	retry.BaseDelay = time.Microsecond
	retry.MaxDelay = time.Microsecond
	retry.Jitter = 0
	agent, err := testAgent(gateway, WithAgentGoals(), WithAgentProviderRetryPolicy(retry))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalController)
	if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Original"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish"}}); err != nil {
		t.Fatal(err)
	}
	if err = gateway.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "update", ExpectedVersion: 1, Text: "Updated"}); err != nil {
		t.Fatal(err)
	}
	close(gate)
	view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	requests := gateway.Requests()
	firstInput, _ := json.Marshal(struct {
		Messages any
		Tools    any
	}{requests[0].Messages, requests[0].Tools})
	retriedInput, _ := json.Marshal(struct {
		Messages any
		Tools    any
	}{requests[1].Messages, requests[1].Tools})
	if string(firstInput) != string(retriedInput) || requests[0].LogicalRequestID != requests[1].LogicalRequestID || requests[0].AttemptID == requests[1].AttemptID {
		t.Fatal("transport retry changed frozen input or attempt identity")
	}
	if _, err = controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "delete", ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Retry(t.Context(), RetryInput{ThreadID: thread.ThreadID, SourceTurnID: view.TurnID, RequestKey: "regenerate"}); err != nil {
		t.Fatal(err)
	}
	regenerated := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.TurnID != view.TurnID })
	if regenerated.Failure != nil || regenerated.Goal.Status != "absent" {
		t.Fatalf("goal=%+v failure=%+v", regenerated.Goal, regenerated.Failure)
	}
}

func TestOutputOnlyUsageRetainsValidRuntimeEvents(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "usage", StateCompatibilityKey: "usage:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventUsage, Usage: provider.Usage{Available: true, OutputTokens: 1, Source: "native"}}, {Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	sink := goalValidatingEventSink{t: t}
	agent, err := testAgent(gateway, WithAgentEventSink(sink))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Finish"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
}

type goalValidatingEventSink struct{ t *testing.T }

func (s goalValidatingEventSink) EmitEvent(event Event) {
	if err := event.Validate(); err != nil {
		s.t.Error(err)
	}
}
