package runtime

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestThreadItemWindowBoundariesAndStableAnchors(t *testing.T) {
	actor := &threadRuntimeState{}
	for i := range 500 {
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i), Text: "text"})
	}
	for _, direction := range []threadWindowDirection{threadWindowBefore, threadWindowAfter, threadWindowAround} {
		for _, anchor := range []string{"0", "1", "250", "499"} {
			page, err := actor.itemWindow(threadItemWindowInput{anchor: anchor, direction: direction, limit: 150})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.items) > 150 || page.hasBefore != (page.first > 0) || page.hasAfter != (page.last < 500) {
				t.Fatal("wrong page boundary")
			}
			for i, item := range page.items {
				if item.ID != fmt.Sprint(page.first+i) || item.Ordinal != uint64(page.first+i+1) {
					t.Fatal("item order changed")
				}
			}
			if len(page.items) > 0 {
				page.items[0].ID = "caller-mutation"
				if actor.state.view.Items[page.first].ID == "caller-mutation" {
					t.Fatal("page aliases actor")
				}
			}
		}
	}
	tail, err := actor.itemWindow(threadItemWindowInput{direction: threadWindowBefore, limit: 150})
	if err != nil || tail.first != 350 || tail.last != 500 {
		t.Fatalf("tail=%+v err=%v", tail, err)
	}
	actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: "500"})
	before, err := actor.itemWindow(threadItemWindowInput{anchor: "350", direction: threadWindowBefore, limit: 150})
	if err != nil || before.first != 200 || before.last != 350 {
		t.Fatal("append invalidated stable history anchor")
	}
	if _, err := actor.itemWindow(threadItemWindowInput{anchor: "unavailable", direction: threadWindowAround, limit: 150}); !errors.Is(err, errThreadWindowAnchorUnavailable) {
		t.Fatal(err)
	}
}

func TestBoundedThreadTextPreservesUTF8AndExactPrefix(t *testing.T) {
	text := strings.Repeat("中é🌸\r\n", 10000)
	for limit := 0; limit < 100; limit++ {
		preview, partial := boundedThreadText(text, limit)
		if !partial || len(preview) > limit || !utf8.ValidString(preview) || !strings.HasPrefix(text, preview) {
			t.Fatalf("limit=%d", limit)
		}
	}
}

func BenchmarkActorItemWindowFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			actor := &threadRuntimeState{}
			for i := range count {
				actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i), Text: "history"})
			}
			actor.ensureItemIndex()
			input := threadItemWindowInput{anchor: fmt.Sprint(count / 2), direction: threadWindowAround, limit: 150}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				page, err := actor.itemWindow(input)
				if err != nil || len(page.items) != 150 {
					b.Fatal(err)
				}
			}
		})
	}
}
