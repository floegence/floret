package session

import (
	"strings"
	"testing"
)

func TestProviderHistoryRetainsSubmittedReferencesAcrossTurns(t *testing.T) {
	history := []Message{
		{Role: User, EntryID: "selected", Content: "Check this device", References: []MessageReference{{ReferenceID: "device", Kind: MessageReferenceFile, Label: "udesk26", Text: "Target: ssh:host:udesk26", ResourceRef: "private-locator"}}},
		{Role: Assistant, EntryID: "reply", Content: "Checking the device."},
		{Role: User, EntryID: "followup", Content: "What about its memory?"},
	}
	first, _, err := ProjectProviderHistory(history[:1], "selected")
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := ProjectProviderHistory(history, "followup")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(first[0].Content, "ssh:host:udesk26") || first[0].Content != next[0].Content {
		t.Fatalf("submitted reference lost or changed: first=%#v next=%#v", first, next)
	}
	if strings.Contains(first[0].Content, "private-locator") || len(first[0].References) != 0 {
		t.Fatalf("opaque locator entered provider history: %#v", first)
	}
	if history[0].References[0].ResourceRef != "private-locator" || history[0].Content != "Check this device" {
		t.Fatal("projection mutated canonical input")
	}
}

func TestProviderHistoryKeepsReferenceOnlyInputAndRetryEligibility(t *testing.T) {
	input := Message{Role: User, EntryID: "selected", References: []MessageReference{{ReferenceID: "file", Kind: MessageReferenceFile, Label: "README.md", Text: "README.md", ResourceRef: "private-locator", Truncated: true}}}
	projected, insertAt, err := ProjectProviderHistory([]Message{input}, "selected")
	if err != nil || len(projected) != 1 || insertAt != 1 || !HasRetryEligibleDurableInput(input) {
		t.Fatalf("reference-only input lost: projected=%#v insertAt=%d err=%v", projected, insertAt, err)
	}
	if !strings.Contains(projected[0].Content, "README.md") || !strings.Contains(projected[0].Content, "truncated") {
		t.Fatalf("reference facts missing: %#v", projected)
	}
}
