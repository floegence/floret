package session

import "testing"

func TestMemoryStoreAppendMessagesReplaceAndIsolation(t *testing.T) {
	store := NewMemoryStore()
	if err := store.AppendTranscript("a", Message{Role: User, Content: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTranscript("b", Message{Role: User, Content: "other"}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Transcript("a")
	if err != nil {
		t.Fatal(err)
	}
	got[0].Content = "mutated"
	again, err := store.Transcript("a")
	if err != nil {
		t.Fatal(err)
	}
	if again[0].Content != "one" {
		t.Fatalf("messages returned internal slice: %#v", again)
	}
	if err := store.ReplaceTranscript("a", []Message{{Role: Assistant, Content: "new"}}); err != nil {
		t.Fatal(err)
	}
	a, _ := store.Transcript("a")
	b, _ := store.Transcript("b")
	if len(a) != 1 || a[0].Content != "new" {
		t.Fatalf("replace failed: %#v", a)
	}
	if len(b) != 1 || b[0].Content != "other" {
		t.Fatalf("runs are not isolated: %#v", b)
	}
}

func TestCloneMessageDeepCopiesReferences(t *testing.T) {
	original := Message{
		Role: User,
		References: []MessageReference{{
			ReferenceID: "context:turn-1:0",
			Kind:        MessageReferenceFile,
			Label:       "config.yaml",
			Text:        "/workspace/config.yaml",
			ResourceRef: "host-resource:v1:config",
		}},
	}

	cloned := CloneMessage(original)
	cloned.References[0].Label = "mutated"
	cloned.References[0].ResourceRef = "mutated"
	if original.References[0].Label != "config.yaml" || original.References[0].ResourceRef != "host-resource:v1:config" {
		t.Fatalf("CloneMessage aliased references: original=%#v cloned=%#v", original, cloned)
	}
}

func TestCloneMessageDeepCopiesAttachmentTextStats(t *testing.T) {
	original := Message{Attachments: []MessageAttachment{{
		ResourceRef: "resource",
		Name:        "notes.txt",
		MIMEType:    "text/plain",
		SizeBytes:   5,
		TextStats:   &MessageAttachmentTextStats{UnicodeCodePointCount: 5, LogicalLineCount: 1},
	}}}
	cloned := CloneMessage(original)
	cloned.Attachments[0].TextStats.UnicodeCodePointCount = 99
	if original.Attachments[0].TextStats.UnicodeCodePointCount != 5 {
		t.Fatalf("CloneMessage aliased attachment text stats: original=%#v cloned=%#v", original, cloned)
	}
}
