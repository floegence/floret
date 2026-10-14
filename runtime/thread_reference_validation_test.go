package runtime

import "testing"

func TestMessageReferenceValidatePreservesReferencedThreadIdentity(t *testing.T) {
	reference := MessageReference{ReferenceID: "source", Kind: MessageReferenceThread, ReferencedThreadID: "source-thread", Label: "Source", Text: "Captured facts"}
	if err := reference.Validate(); err != nil {
		t.Fatalf("valid thread reference: %v", err)
	}
	reference.ReferencedThreadID = ""
	if err := reference.Validate(); err == nil {
		t.Fatal("accepted missing thread identity")
	}
	reference.ReferencedThreadID = "source-thread"
	reference.Kind = MessageReferenceText
	if err := reference.Validate(); err == nil {
		t.Fatal("accepted thread identity on text reference")
	}
}
