package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSendReplayPreservesOriginalInputAfterQueueEditAndPromotion(t *testing.T) {
	_, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-replay"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "active"}); err != nil {
		t.Fatal(err)
	}
	original := SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "original"}, RequestKey: "queued"}
	if _, err := service.Send(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	if _, err := service.(ThreadQueueController).EditQueued(t.Context(), EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", ExpectedText: "original", Text: "edited", RequestKey: "edit"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	view, err := service.PromoteQueued(t.Context(), PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", RequestKey: "promote"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Send(t.Context(), original)
	if err != nil || replay.TurnID != view.TurnID {
		t.Fatalf("original send replay after promotion: %v", err)
	}
	got, found, lookupErr := service.(ThreadSendReader).LookupSend(t.Context(), LookupSendInput{ThreadID: created.ThreadID, RequestKey: original.RequestKey})
	if lookupErr != nil || !found || got.Text != "original" {
		t.Fatalf("promoted lookup: %#v %v %v", got, found, lookupErr)
	}
	original.Input.Text = "edited"
	if _, err := service.Send(t.Context(), original); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed original request: %v", err)
	}
}

func TestLookupSendReadsImmutableJournalAcrossQueueChanges(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-lookup"})
	if err != nil {
		t.Fatal(err)
	}
	reader := service.(ThreadSendReader)
	if _, found, err := reader.LookupSend(t.Context(), LookupSendInput{ThreadID: created.ThreadID, RequestKey: "missing"}); err != nil || found {
		t.Fatalf("missing: %v %v", found, err)
	}
	if _, _, err := reader.LookupSend(t.Context(), LookupSendInput{ThreadID: created.ThreadID}); err == nil {
		t.Fatal("empty key accepted")
	}
	input := UserInput{Text: "original", Attachments: []MessageAttachment{{Name: "note.txt", MIMEType: "text/plain", ResourceRef: "host:note"}}, Context: []MessageContextItem{{Kind: "facts", Title: "Facts", Text: "frozen"}}}
	_, err = service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "lookup", Input: input}}})
	if err != nil {
		t.Fatal(err)
	}
	lookup := LookupSendInput{ThreadID: created.ThreadID, RequestKey: "lookup"}
	assertOriginal := func(reader ThreadSendReader) {
		t.Helper()
		got, found, err := reader.LookupSend(t.Context(), lookup)
		if err != nil || !found || !reflect.DeepEqual(got, input) {
			t.Fatalf("original input: %#v %v %v", got, found, err)
		}
		got.Attachments[0].Name = "mutated"
	}
	assertOriginal(reader)
	if _, err := service.(ThreadQueueController).EditQueued(t.Context(), EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:lookup", ExpectedText: "original", Text: "edited", RequestKey: "edit-lookup"}); err != nil {
		t.Fatal(err)
	}
	assertOriginal(reader)
	if _, err := service.DeleteQueued(t.Context(), DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:lookup", RequestKey: "delete-lookup"}); err != nil {
		t.Fatal(err)
	}
	assertOriginal(reader)
	reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	assertOriginal(reopened.(ThreadSendReader))
	if err := reopened.Delete(t.Context(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete-thread"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopened.(ThreadSendReader).LookupSend(t.Context(), lookup); !errors.Is(err, ErrThreadDeleted) && !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("deleted thread: %v", err)
	}
}
