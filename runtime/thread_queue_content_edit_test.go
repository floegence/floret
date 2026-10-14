package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestEditQueuedReplacesCompleteInputAtomically(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-content-edit"})
	if err != nil {
		t.Fatal(err)
	}
	original := UserInput{Text: "same text", Attachments: []MessageAttachment{{Name: "old.png", MIMEType: "image/png", ResourceRef: "host:old"}}, References: []MessageReference{{ReferenceID: "old", Kind: MessageReferenceFile, Label: "old.go", Text: "old snapshot", ResourceRef: "host:old-file"}}, Context: []MessageContextItem{{Kind: "cwd", Title: "Directory", Text: "/project"}}}
	if _, err := service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "first", Input: UserInput{Text: "first"}}, {RequestKey: "content", Input: original}, {RequestKey: "last", Input: UserInput{Text: "last"}}}}); err != nil {
		t.Fatal(err)
	}
	replacement := UserInput{Text: original.Text, Attachments: []MessageAttachment{{Name: "new.png", MIMEType: "image/png", ResourceRef: "host:new"}}, References: []MessageReference{{ReferenceID: "new", Kind: MessageReferenceDirectory, Label: "src", Text: "new snapshot", ResourceRef: "host:new-dir"}}, Context: original.Context}
	command := EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:content", ExpectedInput: &original, Input: &replacement, RequestKey: "edit-content"}
	updated, err := service.(ThreadQueueController).EditQueued(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Queue) != 3 || updated.Queue[0].ID != "queue:first" || updated.Queue[2].ID != "queue:last" || !reflect.DeepEqual(updated.Queue[1].Input, replacement) {
		t.Fatalf("queue=%#v", updated.Queue)
	}
	if updated.Queue[1].RequestKey != "content" {
		t.Fatal("original identity changed")
	}
	canonical, err := hydrateCanonicalQueue(t.Context(), host.store.repo, created.ThreadID)
	if err != nil || !reflect.DeepEqual(canonical, updated.Queue) {
		t.Fatalf("canonical=%#v err=%v", canonical, err)
	}
	// A caller changing its submitted slices must not mutate the actor's input.
	replacement.Attachments[0].Name = "caller mutation"
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil || view.Queue[1].Input.Attachments[0].Name != "new.png" {
		t.Fatalf("aliased input: %#v %v", view.Queue, err)
	}
	replacement.Attachments[0].Name = "new.png"
	reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution not expected") }))
	if err != nil {
		t.Fatal(err)
	}
	view, err = reopened.View(t.Context(), created.ThreadID)
	if err != nil || !reflect.DeepEqual(view.Queue, updated.Queue) {
		t.Fatalf("rehydrated=%#v err=%v", view.Queue, err)
	}
	if _, err := reopened.(ThreadQueueController).EditQueued(t.Context(), command); err != nil {
		t.Fatalf("replay=%v", err)
	}
	command.RequestKey = "stale-content"
	if _, err := reopened.(ThreadQueueController).EditQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("same-text stale content err=%v", err)
	}
	if _, err := reopened.DeleteQueued(t.Context(), DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: command.QueueItemID, RequestKey: "delete-content"}); err != nil {
		t.Fatal(err)
	}
	command.ExpectedInput = &replacement
	command.RequestKey = "deleted-content"
	if _, err := reopened.(ThreadQueueController).EditQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("deleted edit err=%v", err)
	}
}

func TestEditQueuedRejectsInvalidOrAmbiguousReplacementWithoutMutation(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-invalid-edit"})
	if err != nil {
		t.Fatal(err)
	}
	original := UserInput{Text: "original"}
	if _, err := service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "original", Input: original}}}); err != nil {
		t.Fatal(err)
	}
	empty := UserInput{}
	valid := UserInput{Text: "replacement"}
	commands := []EditQueuedInput{
		{ExpectedInput: &original}, {Input: &valid},
		{ExpectedInput: &original, Input: &empty},
		{ExpectedInput: &original, Input: &valid, Text: "ambiguous"},
	}
	before, _ := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	for _, command := range commands {
		command.ThreadID = created.ThreadID
		command.QueueItemID = "queue:original"
		command.RequestKey = "invalid-edit"
		if _, err := service.(ThreadQueueController).EditQueued(t.Context(), command); err == nil {
			t.Fatalf("accepted invalid command %#v", command)
		}
	}
	after, _ := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid edit wrote canonical facts")
	}
	view, _ := service.View(t.Context(), created.ThreadID)
	if !reflect.DeepEqual(view.Queue[0].Input, original) {
		t.Fatal("invalid edit changed input")
	}
}
