package sessiontree

import (
	"errors"
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
