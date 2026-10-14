package session

import (
	"strings"
	"testing"
)

func TestThreadReferenceFactsStayCanonicalAndCannotAuthorizeResources(t *testing.T) {
	reference := MessageReference{ReferenceID: "source", Kind: MessageReferenceThread, ReferencedThreadID: "thread-source", Label: "Frozen title", Text: "Captured status: completed"}
	if err := reference.Validate(); err != nil {
		t.Fatal(err)
	}
	input := Message{Role: User, EntryID: "input", References: []MessageReference{reference}}
	projected, _, err := ProjectProviderHistory([]Message{input}, "input")
	if err != nil || len(projected) != 1 {
		t.Fatalf("%#v %v", projected, err)
	}
	if !strings.Contains(projected[0].Content, `"referenced_thread_id":"thread-source"`) || !strings.Contains(projected[0].Content, "Frozen title") || !HasRetryEligibleDurableInput(input) {
		t.Fatalf("missing canonical facts: %s", projected[0].Content)
	}
	for _, invalid := range []MessageReference{
		{ReferenceID: "a", Kind: MessageReferenceThread, Label: "a", Text: "snapshot"},
		{ReferenceID: "a", Kind: MessageReferenceThread, Label: "a", Text: "snapshot", ReferencedThreadID: "thread", ResourceRef: "secret"},
		{ReferenceID: "a", Kind: MessageReferenceText, Label: "a", Text: "text", ReferencedThreadID: "thread"},
		{ReferenceID: "a", Kind: MessageReferenceThread, Label: "a", ReferencedThreadID: "thread"},
	} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid reference accepted: %#v", invalid)
		}
	}
	if input.References[0] != reference {
		t.Fatal("projection mutated submitted reference")
	}
}
