package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
)

func TestGoalSubmissionAfterCompletionPreservesContext(t *testing.T) {
	service, g := goalService(t, goalCall("review1", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"First result verified."}`), parentEventDone(), goalCall("review2", `{"operation":"review","expected_version":2,"outcome":"completed","evidence":"Second result verified."}`), parentEventDone())
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	c := service.(ThreadGoalSubmitter)
	first, err := c.SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "first", Text: "First requested result"})
	if err != nil || first.Execution != GoalExecutionStarted {
		t.Fatalf("result=%+v err=%v", first, err)
	}
	old := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	second, err := c.SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "second", Text: "Second requested result", ExpectedVersion: 1})
	if err != nil || second.Execution != GoalExecutionStarted {
		t.Fatalf("result=%+v err=%v", second, err)
	}
	v := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool {
		return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID != old.TurnID
	})
	if v.Failure != nil || v.Goal.Status != "completed" || len(g.Requests()) != 4 {
		t.Fatalf("view=%+v requests=%d", v, len(g.Requests()))
	}
	found := false
	for _, m := range g.Requests()[2].Messages {
		if strings.Contains(m.Text, "First requested result") {
			found = true
		}
	}
	if !found {
		t.Fatal("previous context missing")
	}
	conflict, err := c.SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "stale", Text: "Stale result"})
	if err != nil || !conflict.Conflict || conflict.Execution != "" {
		t.Fatalf("conflict=%+v err=%v", conflict, err)
	}
}

func TestGoalSubmissionPreservesWaitingQuestion(t *testing.T) {
	first := florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["target"],"evidence_refs":[],"questions":[{"id":"q","header":"Target","question":"Which target?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
	service, g := goalService(t, first)
	th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: th.ThreadID, RequestKey: "send", Input: UserInput{Text: "Do the task"}}); err != nil {
		t.Fatal(err)
	}
	before := waitThreadView(t, service, th.ThreadID, func(v ThreadView) bool { return len(v.Interactions) > 0 && !v.Interactions[0].Resolved })
	r, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Updated objective"})
	if err != nil || r.Execution != GoalExecutionUpdated {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	after, _ := service.View(t.Context(), th.ThreadID)
	if len(g.Requests()) != 1 || len(after.Queue) != 0 || after.Interactions[0].Resolved || after.Interactions[0].ID != before.Interactions[0].ID {
		t.Fatalf("view=%+v", after)
	}
}

func TestGoalSubmissionPreparationFailureRetainsGoalAndDoesNotRetry(t *testing.T) {
	service, _ := goalService(t)
	runtime := service.(*threadRuntimeService)
	runtime.factory = AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("model unavailable") })
	th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	in := SetGoalInput{ThreadID: th.ThreadID, RequestKey: "submit", Text: "Recoverable objective"}
	r, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), in)
	if err != nil || r.Execution != GoalExecutionStarted {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	v := waitThreadView(t, service, th.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Failure != nil })
	if v.Goal.Text != in.Text || v.Goal.Status != "active" {
		t.Fatalf("goal=%+v", v.Goal)
	}
	replay, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), in)
	if err != nil || !replay.Replayed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func TestGoalSubmissionStartsIdleAndNeverReplaysAfterStop(t *testing.T) {
	gate := make(chan struct{})
	step := parentEventDone()
	step.BlockUntil = gate
	service, gateway := goalService(t, step)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalSubmitter)
	in := SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "submit", Text: "Deliver the requested result"}
	first, err := controller.SubmitGoal(t.Context(), in)
	if err != nil || first.Execution != GoalExecutionStarted || first.Goal.Version != 1 {
		t.Fatalf("result=%+v err=%v", first, err)
	}
	if err := gateway.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: thread.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	replayed, err := controller.SubmitGoal(t.Context(), in)
	if err != nil || !replayed.Replayed || replayed.Execution != GoalExecutionStarted {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || view.Activity != ThreadActivityIdle || len(view.Queue) != 0 || len(gateway.Requests()) != 1 {
		t.Fatalf("view=%+v err=%v requests=%d", view, err, len(gateway.Requests()))
	}
}

func TestGoalSubmissionDuringExecutionUpdatesWithoutQueueOrWakeup(t *testing.T) {
	gate := make(chan struct{})
	step := parentEventDone()
	step.BlockUntil = gate
	service, gateway := goalService(t, step)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Existing task"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	in := SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "update", Text: "Updated result"}
	controller := service.(ThreadGoalSubmitter)
	result, err := controller.SubmitGoal(t.Context(), in)
	if err != nil || result.Execution != GoalExecutionUpdated {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || len(view.Queue) != 0 || view.Goal.Text != in.Text {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: thread.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitThreadView(t, service, identity.ThreadID(thread.ThreadID), func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	result, err = controller.SubmitGoal(t.Context(), in)
	if err != nil || !result.Replayed || result.Execution != GoalExecutionUpdated || len(gateway.Requests()) != 1 {
		t.Fatalf("replay=%+v err=%v", result, err)
	}
}

func TestGoalSubmissionPublicRequestKeyMatchesFactoryAndRestartIdentity(t *testing.T) {
	service, _ := goalService(t, goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Verified result."}`), parentEventDone())
	runtime := service.(*threadRuntimeService)
	original := runtime.factory
	var got AgentRequest
	runtime.factory = AgentFactoryFunc(func(ctx context.Context, req AgentRequest) (*Agent, error) {
		got = req
		return original.Agent(ctx, req)
	})
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	in := SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "authorize", Text: "Authorized result"}
	key := GoalSubmissionRequestKey(in.RequestKey)
	if key == in.RequestKey || key != GoalSubmissionRequestKey(in.RequestKey) {
		t.Fatal("execution identity is not stable and separate")
	}
	if _, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if got.RequestKey != string(key) || got.CanonicalTurnInput.Text != in.Text {
		t.Fatalf("factory=%+v key=%s", got, key)
	}
}

func TestGoalSubmissionKeepsActiveViewAndJournalInOrder(t *testing.T) {
	gate := make(chan struct{})
	step := parentEventDone()
	step.BlockUntil = gate
	service, gateway := goalService(t, step)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = service.Cancel(context.Background(), CancelInput{ThreadID: thread.ThreadID, RequestKey: "cleanup"})
		close(gate)
	})
	if _, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "submit", Text: "Original result"}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadGoalController)
	if _, err := controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "edit", ExpectedVersion: 1, Text: "Edited result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.(ThreadGoalSubmitter).SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "update", ExpectedVersion: 2, Text: "Updated result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "delete", ExpectedVersion: 3}); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || view.Goal.Version != 4 || view.Goal.Status != "absent" || view.Activity != ThreadActivityActive || len(view.Queue) != 0 {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	canonical, err := service.(*threadRuntimeService).loadCanonicalThreadView(t.Context(), thread.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Items) != len(canonical.Items) {
		t.Fatalf("current has %d items, journal has %d", len(view.Items), len(canonical.Items))
	}
	for i, item := range canonical.Items {
		if view.Items[i].ID != item.ID || view.Items[i].Ordinal != item.Ordinal {
			t.Fatalf("item %d current=%+v canonical=%+v", i, view.Items[i], item)
		}
	}
	if len(gateway.Requests()) != 1 {
		t.Fatalf("requests=%d", len(gateway.Requests()))
	}
}
