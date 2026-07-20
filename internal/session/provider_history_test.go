package session

import (
	"testing"
)

func TestProjectProviderHistoryRequiresExactSupplementalAnchor(t *testing.T) {
	history := []Message{
		{Role: User, Content: "old", EntryID: "old"},
		{Role: User, Content: "current", EntryID: "anchor", References: []MessageReference{{
			ReferenceID: "context:0", Kind: MessageReferenceText, Label: "quote", Text: "display",
		}}},
	}
	projected, insertAt, err := ProjectProviderHistory(history, "anchor")
	if err != nil || insertAt != 2 || len(projected) != 2 || len(projected[1].References) != 0 {
		t.Fatalf("projection=%#v insertAt=%d err=%v", projected, insertAt, err)
	}

	for name, input := range map[string][]Message{
		"missing": history[:1],
		"duplicate": {
			history[0],
			history[1],
			{Role: Assistant, Content: "corrupt duplicate", EntryID: "anchor"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ProjectProviderHistory(input, "anchor"); err == nil {
				t.Fatal("invalid supplemental anchor succeeded")
			}
		})
	}
}

func TestProjectProviderHistoryReplacesReferenceOnlyAnchorInPlace(t *testing.T) {
	history := []Message{
		{Role: User, Content: "old", EntryID: "old"},
		{Role: User, EntryID: "anchor", References: []MessageReference{{
			ReferenceID: "context:0", Kind: MessageReferenceText, Label: "quote", Text: "display",
		}}},
		{Role: Assistant, Content: "after", EntryID: "after"},
	}
	projected, insertAt, err := ProjectProviderHistory(history, "anchor")
	if err != nil || insertAt != 1 || len(projected) != 2 || projected[1].EntryID != "after" {
		t.Fatalf("projection=%#v insertAt=%d err=%v", projected, insertAt, err)
	}
}
