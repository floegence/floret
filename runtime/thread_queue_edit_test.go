package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestEditQueuedPreservesCanonicalInputAndOrder(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-edit"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "active"})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: key}, RequestKey: RequestKey(key)})
		if err != nil {
			t.Fatal(err)
		}
	}
	editor := service.(ThreadQueueController)
	command := EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:b", ExpectedText: "b", Text: "updated", RequestKey: "edit-b"}
	changed, err := editor.EditQueued(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed.Queue) != 3 || changed.Queue[0].Input.Text != "a" || changed.Queue[1].Input.Text != "updated" || changed.Queue[2].Input.Text != "c" {
		t.Fatalf("unexpected queue: %#v", changed.Queue)
	}
	canonical, err := hydrateCanonicalQueue(t.Context(), host.store.repo, created.ThreadID)
	if err != nil || !reflect.DeepEqual(canonical, changed.Queue) {
		t.Fatalf("canonical=%#v err=%v", canonical, err)
	}
	replay, err := editor.EditQueued(t.Context(), command)
	if err != nil || replay.ViewVersion != changed.ViewVersion {
		t.Fatalf("replayed edit: %#v %v", replay, err)
	}
	command.RequestKey = "stale-edit"
	if _, err := editor.EditQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("stale edit err=%v", err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop-edit"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	promoted, err := service.PromoteQueued(t.Context(), PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:b", RequestKey: "promote-b"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range promoted.Items {
		if item.Kind == ThreadItemUser && item.Text == "updated" {
			found = true
		}
	}
	if !found {
		t.Fatalf("edited text missing from canonical turn: %#v", promoted.Items)
	}
	command.ExpectedText = "updated"
	command.Text = "too late"
	command.RequestKey = "late-edit"
	if _, err := editor.EditQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("admitted edit err=%v", err)
	}
}

func TestEditQueuedKeepsAttachmentsAndSurvivesHydration(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-attachments-edit"})
	if err != nil {
		t.Fatal(err)
	}
	input := UserInput{Text: "original", Attachments: []MessageAttachment{{Name: "note.txt", MIMEType: "text/plain", ResourceRef: "host:note"}}}
	_, err = service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "attachment-edit", Input: input}}})
	if err != nil {
		t.Fatal(err)
	}
	cmd := EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:attachment-edit", ExpectedText: "original", Text: "edited", RequestKey: "edit-attachment"}
	updated, err := service.(ThreadQueueController).EditQueued(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updated.Queue[0].Input.Attachments, input.Attachments) {
		t.Fatal("attachments changed")
	}
	// A fresh service reads the journal rather than the first actor's memory.
	reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution not expected") }))
	if err != nil {
		t.Fatal(err)
	}
	view, err := reopened.View(t.Context(), created.ThreadID)
	if err != nil || !reflect.DeepEqual(view.Queue, updated.Queue) {
		t.Fatalf("rehydrated queue=%#v err=%v", view.Queue, err)
	}
	if _, err := reopened.(ThreadQueueController).EditQueued(t.Context(), cmd); err != nil {
		t.Fatalf("replayed after hydration: %v", err)
	}
}

func TestSendQueuedNowStopsAndStartsChosenMessage(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-send-now"})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "active-now"})
	if err != nil {
		t.Fatal(err)
	}
	<-gateway.started
	for _, key := range []string{"first", "chosen", "last"} {
		if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: key}, RequestKey: RequestKey(key)}); err != nil {
			t.Fatal(err)
		}
	}
	command := PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:chosen", RequestKey: "send-chosen-now"}
	view, err := service.(ThreadQueueController).SendQueuedNow(t.Context(), command)
	if err != nil || view.Activity != ThreadActivityActive || view.TurnID == started.TurnID {
		t.Fatalf("send now=%#v err=%v", view, err)
	}
	if len(view.Queue) != 2 || view.Queue[0].Input.Text != "first" || view.Queue[1].Input.Text != "last" {
		t.Fatalf("queue=%#v", view.Queue)
	}
	found := false
	for _, item := range view.Items {
		if item.Kind == ThreadItemUser && item.Text == "chosen" {
			found = true
		}
	}
	if !found {
		t.Fatal("chosen input was not admitted")
	}
	replay, err := service.(ThreadQueueController).SendQueuedNow(t.Context(), command)
	if err != nil || replay.TurnID != view.TurnID || replay.Cancellation != nil {
		t.Fatalf("replay interrupted new turn: %#v %v", replay, err)
	}
}
