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
