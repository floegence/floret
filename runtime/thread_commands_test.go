package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
)

func TestThreadCommandAckCreateTitleAndForkDoNotHydrateResult(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	service := full.(*threadRuntimeService)
	commands := full.(ThreadCommandService)
	create := CreateThreadInput{RequestKey: "ack-create"}
	created, err := commands.CreateAck(t.Context(), create)
	if err != nil || created.ThreadID == "" || created.RequestKey != create.RequestKey || created.ViewVersion != 0 {
		t.Fatalf("create ack=%+v err=%v", created, err)
	}
	if len(service.runtimes) != 0 {
		t.Fatal("create acknowledgment hydrated a presentation actor")
	}
	replayed, err := commands.CreateAck(t.Context(), create)
	if err != nil || replayed != created {
		t.Fatalf("create replay=%+v err=%v", replayed, err)
	}
	title := SetTitleInput{ThreadID: created.ThreadID, Title: "Named without a view", RequestKey: "ack-title"}
	if ack, err := commands.SetTitleAck(t.Context(), title); err != nil || ack.ThreadID != created.ThreadID {
		t.Fatalf("title ack=%+v err=%v", ack, err)
	}
	meta, err := host.store.repo.Thread(t.Context(), created.ThreadID.String())
	if err != nil || meta.Title != title.Title || len(service.runtimes) != 0 {
		t.Fatalf("title=%q actors=%d err=%v", meta.Title, len(service.runtimes), err)
	}
	fork := ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "ack-fork"}
	forked, err := commands.ForkAck(t.Context(), fork)
	if err != nil || forked.ThreadID == "" || forked.ThreadID == created.ThreadID || len(service.runtimes) != 0 {
		t.Fatalf("fork ack=%+v actors=%d err=%v", forked, len(service.runtimes), err)
	}
	view, err := full.Fork(t.Context(), fork)
	if err != nil || view.ThreadID != forked.ThreadID || view.Items == nil {
		t.Fatalf("full replay=%+v err=%v", view, err)
	}
	view, err = full.Create(t.Context(), create)
	if err != nil || view.ThreadID != created.ThreadID {
		t.Fatalf("full create replay=%+v err=%v", view, err)
	}
	title.Title = "different"
	if _, err := commands.SetTitleAck(t.Context(), title); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting title=%v", err)
	}
}

func TestThreadCommandAckSendAndQueueShareCanonicalCommands(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, full := testThreadService(t, gateway)
	commands := full.(ThreadCommandService)
	created, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: "ack-create-queue"})
	if err != nil {
		t.Fatal(err)
	}
	send := SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "ack-send"}
	started, err := commands.SendAck(t.Context(), send)
	if err != nil || started.TurnID == "" || started.RunID == "" {
		t.Fatalf("send ack=%+v err=%v", started, err)
	}
	<-gateway.started
	replay, err := full.Send(t.Context(), send)
	if err != nil || replay.TurnID != started.TurnID || countThreadItems(replay.Items, ThreadItemUser) != 1 {
		t.Fatalf("send replay=%+v err=%v", replay, err)
	}
	send.Input.Text = "conflicting"
	if _, err := commands.SendAck(t.Context(), send); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("send conflict=%v", err)
	}
	for _, key := range []RequestKey{"a", "b", "c"} {
		if queued, err := commands.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: string(key)}, RequestKey: key}); err != nil {
			t.Fatal(err)
		} else if queued.TurnID != "" || queued.RunID != "" {
			t.Fatalf("queued send claimed another request's execution: %+v", queued)
		}
	}
	edit := EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:b", ExpectedText: "b", Text: "edited", RequestKey: "ack-edit"}
	if _, err := commands.EditQueuedAck(t.Context(), edit); err != nil {
		t.Fatal(err)
	}
	if _, err := full.(ThreadQueueController).EditQueued(t.Context(), edit); err != nil {
		t.Fatalf("edit replay=%v", err)
	}
	reorder := ReorderQueueInput{ThreadID: created.ThreadID, OrderedItemIDs: []string{"queue:c", "queue:b", "queue:a"}, RequestKey: "ack-reorder"}
	if _, err := commands.ReorderQueueAck(t.Context(), reorder); err != nil {
		t.Fatal(err)
	}
	if _, err := full.ReorderQueue(t.Context(), reorder); err != nil {
		t.Fatalf("reorder replay=%v", err)
	}
	remove := DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:a", RequestKey: "ack-remove"}
	if _, err := commands.DeleteQueuedAck(t.Context(), remove); err != nil {
		t.Fatal(err)
	}
	if _, err := full.DeleteQueued(t.Context(), remove); err != nil {
		t.Fatalf("delete replay=%v", err)
	}
	view, err := full.View(t.Context(), created.ThreadID)
	if err != nil || len(view.Queue) != 2 || view.Queue[0].Input.Text != "c" || view.Queue[1].Input.Text != "edited" {
		t.Fatalf("queue=%+v err=%v", view.Queue, err)
	}
	canonical, err := hydrateCanonicalQueue(t.Context(), host.store.repo, created.ThreadID)
	if err != nil || !reflect.DeepEqual(canonical, view.Queue) {
		t.Fatalf("canonical=%+v err=%v", canonical, err)
	}
	stop := CancelInput{ThreadID: created.ThreadID, RequestKey: "ack-stop"}
	if _, err := commands.CancelAck(t.Context(), stop); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, full, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	promote := PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:b", RequestKey: "ack-promote"}
	ack, err := commands.PromoteQueuedAck(t.Context(), promote)
	if err != nil || ack.TurnID == started.TurnID {
		t.Fatalf("promote=%+v err=%v", ack, err)
	}
	view, err = full.PromoteQueued(t.Context(), promote)
	if err != nil || view.TurnID != ack.TurnID || len(view.Queue) != 1 {
		t.Fatalf("promote replay=%+v err=%v", view, err)
	}
	original, err := commands.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "ack-send"})
	if err != nil || original.TurnID != started.TurnID || original.RunID != started.RunID {
		t.Fatalf("older send acknowledged the latest execution: %+v err=%v", original, err)
	}
	promotedSend, err := commands.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "b"}, RequestKey: "b"})
	if err != nil || promotedSend.TurnID != ack.TurnID || promotedSend.RunID != ack.RunID {
		t.Fatalf("promoted send lost original request identity: %+v err=%v", promotedSend, err)
	}
	_, err = commands.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "chosen", Input: UserInput{Text: "chosen"}})
	if err != nil {
		t.Fatal(err)
	}
	now := PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:chosen", RequestKey: "ack-now"}
	chosen, err := commands.SendQueuedNowAck(t.Context(), now)
	if err != nil || chosen.TurnID == ack.TurnID {
		t.Fatalf("now=%+v err=%v", chosen, err)
	}
	view, err = full.(ThreadQueueController).SendQueuedNow(t.Context(), now)
	if err != nil || view.TurnID != chosen.TurnID || view.Cancellation != nil {
		t.Fatalf("now replay=%+v err=%v", view, err)
	}
}

func TestThreadCommandAckRespondAndRetryPreserveProviderHistory(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{ReturnError: errors.New("deterministic provider failure")},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	agent, err := testAgent(gateway, WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}))
	if err != nil {
		t.Fatal(err)
	}
	_, full := testThreadServiceWithAgent(t, agent)
	commands := full.(ThreadCommandService)
	created, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: "ack-ask-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := commands.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "begin"}, RequestKey: "ack-ask-send"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, full, created.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
	response := RespondInput{ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID, RequestKey: "ack-answer", Answers: []InteractionAnswer{{Input: map[string]string{"q": "retain this answer"}}}}
	if ack, err := commands.RespondAck(t.Context(), response); err != nil || ack.ThreadID != created.ThreadID {
		t.Fatalf("respond=%+v err=%v", ack, err)
	}
	failed := waitThreadView(t, full, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Failure != nil })
	if _, err := full.Respond(t.Context(), response); err != nil {
		t.Fatalf("respond replay=%v", err)
	}
	retry := RetryInput{ThreadID: created.ThreadID, SourceTurnID: failed.TurnID, RequestKey: "ack-retry"}
	if ack, err := commands.RetryAck(t.Context(), retry); err != nil || ack.ThreadID != created.ThreadID {
		t.Fatalf("retry=%+v err=%v", ack, err)
	}
	waitThreadView(t, full, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Failure == nil })
	if _, err := full.Retry(t.Context(), retry); err != nil {
		t.Fatalf("retry replay=%v", err)
	}
	requests := gateway.Requests()
	if len(requests) != 3 {
		t.Fatalf("provider requests=%d", len(requests))
	}
	encoded, err := json.Marshal(requests[2].Messages)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "retain this answer") {
		t.Fatal("retry lost the canonical answer")
	}
}

func TestThreadCommandAckResultDoesNotCopyPresentation(t *testing.T) {
	source := ThreadView{ThreadID: identity.ThreadID("thread-ack"), ViewVersion: 7, Items: []ThreadItem{{ID: "item", Text: strings.Repeat("x", 1<<20)}}}
	ack := threadResult(source, threadAckResult)
	if ack.ThreadID != source.ThreadID || ack.ViewVersion != source.ViewVersion || ack.Items != nil || ack.Turns != nil || ack.Interactions != nil {
		t.Fatalf("unexpected ack projection=%+v", ack)
	}
	full := threadResult(source, threadFullResult)
	full.Items[0].Text = "detached"
	if source.Items[0].Text == full.Items[0].Text {
		t.Fatal("full result aliases actor items")
	}
	if _, err := commandAck(source, "key", context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestThreadCommandAckReturnsCanonicalRequestKey(t *testing.T) {
	_, service := testThreadService(t, newBlockingThreadGateway())
	commands := service.(ThreadCommandService)
	created, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: " \tcanonical-create\n"})
	if err != nil || created.RequestKey != "canonical-create" {
		t.Fatalf("ack=%+v err=%v", created, err)
	}
	replay, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: "canonical-create"})
	if err != nil || replay != created {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if ack, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: " \n"}); err == nil || ack != (ThreadCommandAck{}) {
		t.Fatalf("invalid ack=%+v err=%v", ack, err)
	}
}

func TestThreadCommandAckGoalBeforeFirstReadPreservesIdentity(t *testing.T) {
	_, service := testThreadService(t, newBlockingThreadGateway())
	commands := service.(ThreadCommandService)
	created, err := commands.CreateAck(t.Context(), CreateThreadInput{RequestKey: "goal-before-read"})
	if err != nil {
		t.Fatal(err)
	}
	goals := service.(ThreadGoalController)
	if _, err := goals.SetGoal(t.Context(), SetGoalInput{ThreadID: created.ThreadID, RequestKey: "set-before-read", Text: "Preserve the empty thread"}); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if view.ThreadID != created.ThreadID || view.Activity != ThreadActivityIdle || view.TurnID != "" || view.RunID != "" || view.Goal == nil || view.Goal.Version != 1 || len(view.Items) != 1 {
		t.Fatalf("goal-only identity=%q activity=%q turns=%q/%q goal=%+v items=%d", view.ThreadID, view.Activity, view.TurnID, view.RunID, view.Goal, len(view.Items))
	}
	if _, err := json.Marshal(view); err != nil {
		t.Fatal(err)
	}
	if _, err := goals.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: created.ThreadID, RequestKey: "delete-before-turn", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	view, err = service.View(t.Context(), created.ThreadID)
	if err != nil || view.ThreadID != created.ThreadID || view.Activity != ThreadActivityIdle || view.Goal == nil || view.Goal.Status != "absent" {
		t.Fatalf("delete identity=%q activity=%q goal=%+v err=%v", view.ThreadID, view.Activity, view.Goal, err)
	}
}
