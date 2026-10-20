package runtime

import (
	"fmt"
	"reflect"
	"testing"
)

func TestThreadMutationIndexPreservesOrderedPrecedence(t *testing.T) {
	actor := &threadRuntimeState{}
	expected := []ThreadItemChange{}
	mark := func(id string, kind ThreadItemChangeKind, appendText bool) {
		if appendText {
			actor.markAppend(id, 7, "text")
		} else {
			actor.markItem(id, kind)
		}
		found := false
		for i := range expected {
			change := &expected[i]
			if change.ItemID != id {
				continue
			}
			found = true
			if appendText || change.Kind == ThreadItemAdded && kind != ThreadItemRemoved {
				break
			}
			change.Kind, change.Append = kind, nil
			break
		}
		if !found {
			if appendText {
				expected = append(expected, ThreadItemChange{ItemID: id, Kind: ThreadItemAppended, Append: &ThreadTextAppend{OffsetBytes: 7, Text: "text"}})
			} else {
				expected = append(expected, ThreadItemChange{ItemID: id, Kind: kind})
			}
		}
	}
	for i := range 1000 {
		mark(fmt.Sprint(i), ThreadItemAdded, false)
	}
	for i := range 3000 {
		mark(fmt.Sprint(i%1100), []ThreadItemChangeKind{ThreadItemAdded, ThreadItemReplaced, ThreadItemRemoved}[i%3], i%5 == 0)
	}
	if !reflect.DeepEqual(actor.mutations.items, expected) {
		t.Fatal("indexed mutations changed ordered update precedence")
	}
	if len(actor.mutations.itemIndex) != len(expected) {
		t.Fatal("batch index lost stable identities")
	}
}

func BenchmarkThreadMutationBatch(b *testing.B) {
	for _, size := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			ids := make([]string, size)
			for i := range ids {
				ids[i] = fmt.Sprint(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				actor := &threadRuntimeState{}
				for _, id := range ids {
					actor.markItem(id, ThreadItemAdded)
				}
			}
		})
	}
}
