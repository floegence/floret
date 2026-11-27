package sessiontree

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestRangePageKeepsExclusiveAnchorAndExceptions(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"one", "two", "three", "four", "five"} {
		if _, err := memory.Append(t.Context(), Entry{ID: name, Type: EntryToolCall, ThreadID: "thread", TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: name}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	for _, reader := range []ItemRangePageReader{memory, repo} {
		for _, direction := range []PathWindowDirection{PathWindowBefore, PathWindowAfter} {
			input := ItemRangePageOptions{ItemMembershipRangeOptions: ItemMembershipRangeOptions{ThreadID: "thread", LeafID: "five", AnchorID: "tool:turn:three", Kinds: []string{"tool"}}, Direction: direction, Limit: 2}
			spans, err := reader.ItemRangePage(t.Context(), input)
			first := uint64(1)
			if direction == PathWindowAfter {
				first = 4
			}
			if err != nil || len(spans) != 1 || !spans[0].Matched || spans[0].FirstOrdinal != first || spans[0].LastOrdinal != first+1 || spans[0].Activity.Running != 2 {
				t.Fatalf("%s: %+v %v", direction, spans, err)
			}
			input.SettledActivities = true
			spans, err = reader.ItemRangePage(t.Context(), input)
			if err != nil || len(spans) != 2 || spans[0].Matched || spans[1].Matched || spans[0].FirstOrdinal != first || spans[1].LastOrdinal != first+1 || spans[1].Counts.Tool != 1 {
				t.Fatalf("exceptions: %+v %v", spans, err)
			}
			input.LeafID = "stale"
			if _, err := reader.ItemRangePage(t.Context(), input); !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("stale leaf: %v", err)
			}
		}
	}
}

func TestRangePageTreeMatchesCanonicalSpans(t *testing.T) {
	ctx := t.Context()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	var leaf string
	for index := 0; index < 65; index++ {
		leaf = fmt.Sprintf("entry-%02d", index)
		turn := fmt.Sprintf("turn-%d", index/19)
		entry := Entry{ID: leaf, ThreadID: "thread", TurnID: turn, RunID: turn, Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: leaf}}
		id := "tool:" + turn + ":" + leaf
		if index%13 == 0 {
			entry.Type = EntryAssistantMessage
			entry.Message = session.Message{Role: session.Assistant, Content: "answer"}
			id = leaf
		}
		if _, err := memory.Append(ctx, entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 12, 14, 19, 32, 63, 64} {
		for _, direction := range []PathWindowDirection{PathWindowBefore, PathWindowAfter} {
			for _, limit := range []int{1, 2, 4, 32} {
				query := ItemRangePageOptions{ItemMembershipRangeOptions: ItemMembershipRangeOptions{ThreadID: "thread", LeafID: leaf, AnchorID: ids[index], Kinds: []string{"tool"}}, Direction: direction, Limit: limit}
				want, err := memory.ItemRangePage(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.ItemRangePage(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("%+v: %d != %d", query, len(got), len(want))
				}
				for position := range got {
					left, right := got[position], want[position]
					if left.First.ID != right.First.ID || left.Last.ID != right.Last.ID {
						t.Fatalf("%+v: changed endpoints", query)
					}
					left.First, left.Last = right.First, right.Last
					if !reflect.DeepEqual(left, right) {
						t.Fatalf("%+v: %+v != %+v", query, left, right)
					}
				}
			}
		}
	}
}
