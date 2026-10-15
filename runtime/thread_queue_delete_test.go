package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDeleteQueuedExpectedInput(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-delete-expected"})
	if err != nil {
		t.Fatal(err)
	}
	original := UserInput{Text: "draft", References: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "Quote", Text: "original"}}}
	if _, err := service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "draft", Input: original}, {RequestKey: "other", Input: UserInput{Text: "other"}}}}); err != nil {
		t.Fatal(err)
	}
	stale := original
	stale.References = []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "Quote", Text: "changed"}}
	hash := func(input UserInput) string {
		value, _ := stableFingerprint(input)
		return strings.TrimPrefix(value, "sha256:")
	}
	command := DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:draft", RequestKey: "take-back", ExpectedInputSHA256: hash(stale)}
	if _, err := service.DeleteQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("stale delete = %v", err)
	}
	current, _ := service.View(t.Context(), created.ThreadID)
	if len(current.Queue) != 2 {
		t.Fatal("stale delete mutated queue")
	}
	for _, malformed := range []string{"invalid", strings.Repeat("A", 64)} {
		command.ExpectedInputSHA256 = malformed
		if _, err := service.DeleteQueued(t.Context(), command); err == nil {
			t.Fatal("accepted malformed input hash")
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	command.ExpectedInputSHA256 = hash(original)
	if _, err := service.DeleteQueued(canceled, command); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	command.ExpectedInputSHA256 = hash(original)
	current, err = service.DeleteQueued(t.Context(), command)
	if err != nil || len(current.Queue) != 1 || current.Queue[0].ID != "queue:other" {
		t.Fatalf("delete=%#v err=%v", current.Queue, err)
	}
	reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution not expected") }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.DeleteQueued(t.Context(), command); err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	command.ExpectedInputSHA256 = hash(stale)
	if _, err := reopened.DeleteQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed retry = %v", err)
	}
	command.RequestKey = "another-take-back"
	command.ExpectedInputSHA256 = hash(original)
	if _, err := reopened.DeleteQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("missing delete = %v", err)
	}
}
