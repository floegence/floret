package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestWindowPreviewsBindLeafAndBoundResidence(t *testing.T) {
	service := &threadRuntimeService{}
	actor := &threadRuntimeState{threadID: "thread", service: service}
	item := ThreadItem{ID: "source", Kind: ThreadItemAssistant, CanonicalOrdinal: 1, textLeafID: "leaf", Text: "original", textBytes: 8}
	read := func(want string) {
		t.Helper()
		text, err := actor.readWindowPreview(t.Context(), item, 8)
		if err != nil || text != want {
			t.Fatalf("text=%q err=%v", text, err)
		}
	}
	read("original")
	item.Text = "replaced"
	read("original")
	item.textLeafID = "other"
	read("replaced")
	item.textLeafID = "leaf"
	item.Live = true
	read("replaced")
	item.Live = false
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := actor.readWindowPreview(ctx, item, 8); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for index := range windowPreviewSlots {
		item.ID = fmt.Sprint(index)
		read("replaced")
	}
	if len(service.windowPreviews.values) != windowPreviewSlots {
		t.Fatal("unbounded window previews")
	}
	item.ID = "source"
	read("replaced")
}
