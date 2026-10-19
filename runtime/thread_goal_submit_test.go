package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
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

type goalStreamingGateway struct {
	events   chan provider.Event
	requests atomic.Int32
}

func (*goalStreamingGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "goal-stream", StateCompatibilityKey: "test:goal-stream"}
}
func (*goalStreamingGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}
func (g *goalStreamingGateway) Stream(ctx context.Context, _ provider.Request) (<-chan provider.Event, error) {
	g.requests.Add(1)
	out := make(chan provider.Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-g.events:
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
				if event.Type == provider.EventDone {
					return
				}
			}
		}
	}()
	return out, nil
}

func TestGoalMutationsPreserveStreamingTextAndCanonicalHistory(t *testing.T) {
	gateway := &goalStreamingGateway{events: make(chan provider.Event, 4)}
	agent, err := testAgent(gateway, WithAgentGoals())
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	submitter := service.(ThreadGoalSubmitter)
	controller := service.(ThreadGoalController)
	if _, err := submitter.SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "submit", Text: "Original result"}); err != nil {
		t.Fatal(err)
	}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "Working"}
	before := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool {
		return len(v.Items) > 0 && v.Items[len(v.Items)-1].Live && v.Items[len(v.Items)-1].Text == "Working"
	})
	liveID := before.Items[len(before.Items)-1].ID
	assertCurrent := func(version int64, text, status string) {
		t.Helper()
		v, err := service.View(t.Context(), thread.ThreadID)
		if err != nil || v.Goal.Version != version || v.Goal.Text != text || v.Goal.Status != status || v.Activity != ThreadActivityActive || len(v.Queue) != 0 {
			t.Fatalf("view=%+v err=%v", v, err)
		}
		canonical, err := typed.loadCanonicalThreadView(t.Context(), thread.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if len(v.Items) != len(canonical.Items)+1 {
			t.Fatalf("current=%+v canonical=%+v", v.Items, canonical.Items)
		}
		for i, item := range canonical.Items {
			if v.Items[i].ID != item.ID || v.Items[i].Ordinal != item.Ordinal || v.Items[i].CanonicalOrdinal != item.CanonicalOrdinal {
				t.Fatalf("item %d current=%+v canonical=%+v", i, v.Items[i], item)
			}
		}
		live := v.Items[len(v.Items)-1]
		if live.ID != liveID || live.Text != "Working" || !live.Live || live.CanonicalOrdinal != 0 || live.Ordinal != uint64(len(v.Items)) {
			t.Fatalf("live=%+v", live)
		}
	}
	if _, err := controller.SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "edit", ExpectedVersion: 1, Text: "Edited result"}); err != nil {
		t.Fatal(err)
	}
	assertCurrent(2, "Edited result", "active")
	if _, err := submitter.SubmitGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "update", ExpectedVersion: 2, Text: "Updated result"}); err != nil {
		t.Fatal(err)
	}
	assertCurrent(3, "Updated result", "active")
	if _, err := controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "delete", ExpectedVersion: 3}); err != nil {
		t.Fatal(err)
	}
	assertCurrent(4, "", "absent")
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: " and verified"}
	waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool {
		i := threadItemIndexByID(v.Items, liveID)
		return i >= 0 && v.Items[i].Text == "Working and verified"
	})
	gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	done := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	canonical, err := typed.loadCanonicalThreadView(t.Context(), thread.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(done.Items) != len(canonical.Items) || done.Goal.Version != 4 || done.Goal.Status != "absent" {
		t.Fatalf("terminal=%+v canonical=%+v", done, canonical)
	}
	assistants := 0
	for i, item := range done.Items {
		if item.ID != canonical.Items[i].ID || item.Ordinal != uint64(i+1) || item.Live {
			t.Fatalf("terminal item=%+v canonical=%+v", item, canonical.Items[i])
		}
		if item.Kind == ThreadItemAssistant {
			assistants++
			if item.ID != liveID || item.Text != "Working and verified" {
				t.Fatalf("assistant=%+v", item)
			}
		}
	}
	if assistants != 1 || gateway.requests.Load() != 1 {
		t.Fatalf("assistants=%d requests=%d", assistants, gateway.requests.Load())
	}
}

func TestCanonicalReconciliationRejectsMissingOrReorderedDurableItems(t *testing.T) {
	first := ThreadItem{ID: "first", Ordinal: 1, CanonicalOrdinal: 2, Kind: ThreadItemUser, TurnID: "turn", RunID: "run"}
	second := ThreadItem{ID: "second", Ordinal: 2, CanonicalOrdinal: 4, Kind: ThreadItemAssistant, TurnID: "turn", RunID: "run"}
	current := []ThreadItem{first, second}
	for _, test := range []struct {
		name      string
		canonical []ThreadItem
	}{
		{"missing durable item", []ThreadItem{first}},
		{"changed identity", []ThreadItem{first, {ID: "replacement", Ordinal: 2, CanonicalOrdinal: 4, Kind: ThreadItemAssistant, TurnID: "turn", RunID: "run"}}},
		{"reordered durable items", []ThreadItem{{ID: "second", Ordinal: 1, CanonicalOrdinal: 4, Kind: ThreadItemAssistant, TurnID: "turn", RunID: "run"}, {ID: "first", Ordinal: 2, CanonicalOrdinal: 2, Kind: ThreadItemUser, TurnID: "turn", RunID: "run"}}},
		{"changed canonical position", []ThreadItem{first, {ID: "second", Ordinal: 2, CanonicalOrdinal: 5, Kind: ThreadItemAssistant, TurnID: "turn", RunID: "run"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, ok := reconcileCanonicalThreadItems(current, test.canonical, false); ok {
				t.Fatal("incompatible canonical snapshot was accepted")
			}
		})
	}
}

func TestGoalMutationPublishesCanonicalFactsAcrossConcurrentDelta(t *testing.T) {
	service, _ := goalService(t)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), thread.ThreadID); err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(thread.ThreadID)
	started, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	typed.host.store.repo = &blockingGoalSnapshotRepo{Repo: typed.host.store.repo, GoalRepo: typed.host.store.repo.(sessiontree.GoalRepo), started: started, release: release}
	finished := make(chan error, 1)
	go func() {
		_, err := service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "edit", Text: "Keep all facts"})
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("mutation did not read canonical path")
	}
	applied := make(chan struct{})
	go func() {
		_ = actor.apply(t.Context(), func() error {
			actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "assistant:live:1", Kind: ThreadItemAssistant, Text: "new delta", Live: true})
			actor.state.view.ViewVersion++
			return nil
		})
		close(applied)
	}()
	releaseOnce()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	<-applied
	view, _ := service.View(t.Context(), thread.ThreadID)
	if view.Goal.Version != 1 || len(view.Items) != 2 || view.Items[0].Kind != ThreadItemGoal || view.Items[1].Text != "new delta" {
		t.Fatalf("view=%+v", view)
	}
}

type blockingGoalSnapshotRepo struct {
	sessiontree.Repo
	sessiontree.GoalRepo
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingGoalSnapshotRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	entries, err := r.Repo.Path(ctx, threadID, leafID)
	r.once.Do(func() { close(r.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.release:
	}
	return entries, err
}
