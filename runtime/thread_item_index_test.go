package runtime

import (
	"fmt"
	"testing"
)

func TestThreadItemIndexExtendsWithoutRebuildingHistory(t *testing.T) {
	actor := &threadRuntimeState{}
	for i := range 1000 {
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i)})
	}
	if actor.itemIndexByID("999") != 999 {
		t.Fatal("initial index missing")
	}
	previous := actor.itemIndex
	actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "new"})
	if actor.itemIndexByID("new") != 1000 {
		t.Fatal("appended row missing")
	}
	previous["index-owner-probe"] = -2
	if _, ok := actor.itemIndex["index-owner-probe"]; !ok {
		t.Fatal("append rebuilt unchanged historical index")
	}
	delete(previous, "index-owner-probe")
	actor.state.view.Items = actor.state.view.Items[1:]
	actor.invalidateItemIndex()
	if actor.itemIndexByID("0") != -1 || actor.itemIndexByID("1") != 0 {
		t.Fatal("removal retained stale positions")
	}
	actor.state.view.Items[0], actor.state.view.Items[1] = actor.state.view.Items[1], actor.state.view.Items[0]
	actor.invalidateItemIndex()
	if actor.itemIndexByID("1") != 1 {
		t.Fatal("reorder retained stale positions")
	}
}

func BenchmarkThreadItemIndexAppend(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			actor := &threadRuntimeState{}
			for i := range count {
				actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i)})
			}
			actor.itemIndexByID("0")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "new"})
				if actor.itemIndexByID("new") != count {
					b.Fatal("missing appended row")
				}
				delete(actor.itemIndex, "new")
				actor.state.view.Items[count] = ThreadItem{}
				actor.state.view.Items = actor.state.view.Items[:count]
				actor.indexedItems = count
			}
		})
	}
}
