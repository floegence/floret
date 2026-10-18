package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
)

func TestParentEventTurnOwnershipAcrossLateDeliveryForkPaginationAndRestart(t *testing.T) {
	path := t.TempDir() + "/ownership.sqlite"
	host, service, _ := parentEventHost(t, true, storage.SQLite(path))
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	emit := func(name string) {
		child := parentEventChild(t, host, parent.ThreadID, name)
		parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	}
	send := func(key string) identity.TurnID {
		accepted, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: RequestKey(key), Input: UserInput{Text: key}})
		if err != nil {
			t.Fatal(err)
		}
		view := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool {
			return v.Activity == ThreadActivityIdle && v.LastOutcome != nil && v.TurnID == accepted.TurnID
		})
		return view.TurnID
	}
	emit("before")
	first := send("first")
	emit("late")
	second := send("second")
	if second == first {
		t.Fatal("next turn did not start")
	}
	emit("next")
	events := service.(ThreadParentEventService)
	page, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("page=%+v %v", page, err)
	}
	want := []identity.TurnID{"", first, second}
	for i, event := range page.Events {
		if event.ParentTurnID != want[i] {
			t.Fatalf("event %d owner=%s want=%s", i, event.ParentTurnID, want[i])
		}
	}
	raw, _ := json.Marshal(page.Events[0])
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(raw, &wire)
	if _, ok := wire["parent_turn_id"]; ok {
		t.Fatal("unowned event must omit owner")
	}
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: parent.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range fork.ParentEvents.Events {
		if event != page.Events[i] {
			t.Fatal("fork changed ownership or recipient")
		}
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, restarted, _ := parentEventHost(t, true, storage.SQLite(path))
	reader := restarted.(ThreadParentEventService)
	after := ""
	for i := range want {
		got, err := reader.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: fork.ThreadID, FirstEventID: page.FirstEventID, LastEventID: page.LastEventID, AfterEventID: after, Limit: 1})
		if err != nil || len(got.Events) != 1 || got.Events[0].ParentTurnID != want[i] {
			t.Fatalf("page %d=%+v %v", i, got, err)
		}
		after = got.NextAfterEventID
	}
}
