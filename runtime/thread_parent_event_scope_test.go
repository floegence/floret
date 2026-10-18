package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
)

func assertParentEventScope(t *testing.T, page ParentEventPage, threadID identity.ThreadID) {
	t.Helper()
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		ThreadID identity.ThreadID `json:"thread_id"`
		Events   json.RawMessage   `json:"events"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.ThreadID != threadID || len(wire.Events) == 0 || string(wire.Events) == "null" {
		t.Fatalf("page must identify query scope and preserve event array: %s, want %s", encoded, threadID)
	}
}

func TestParentEventPageScopeAcrossForksAndRestart(t *testing.T) {
	path := t.TempDir() + "/scope.sqlite"
	host, service, gateway := parentEventHost(t, true, storage.SQLite(path))
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	assertParentEventScope(t, *parent.ParentEvents, parent.ThreadID)
	child := parentEventChild(t, host, parent.ThreadID, "original-child")
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	events := service.(ThreadParentEventService)
	original, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
	if err != nil || len(original.Events) != 1 {
		t.Fatalf("original=%+v %v", original, err)
	}
	assertParentEventScope(t, original, parent.ThreadID)
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: parent.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	assertParentEventScope(t, *fork.ParentEvents, fork.ThreadID)
	if fork.ParentEvents.Events[0] != original.Events[0] {
		t.Fatal("fork rewrote historical event")
	}
	own := parentEventChild(t, host, fork.ThreadID, "fork-child")
	parentEventFinish(t, host, own, sessiontree.TurnCompleted)
	mixed, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: fork.ThreadID})
	if err != nil || len(mixed.Events) != 2 {
		t.Fatalf("mixed=%+v %v", mixed, err)
	}
	assertParentEventScope(t, mixed, fork.ThreadID)
	if mixed.Events[0].ParentThreadID != parent.ThreadID || mixed.Events[1].ParentThreadID != fork.ThreadID {
		t.Fatal("event recipients lost")
	}
	nested, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: fork.ThreadID, RequestKey: "nested"})
	if err != nil {
		t.Fatal(err)
	}
	assertParentEventScope(t, *nested.ParentEvents, nested.ThreadID)
	later := parentEventChild(t, host, parent.ThreadID, "later-original-child")
	parentEventFinish(t, host, later, sessiontree.TurnCompleted)
	laterFork := parentEventChild(t, host, fork.ThreadID, "later-fork-child")
	parentEventFinish(t, host, laterFork, sessiontree.TurnCompleted)

	for _, query := range []ListParentEventsInput{
		{ThreadID: nested.ThreadID},
		{ThreadID: nested.ThreadID, SourceThreadIDs: []identity.ThreadID{child}},
		{ThreadID: nested.ThreadID, SourceThreadIDs: []identity.ThreadID{later}},
		{ThreadID: nested.ThreadID, PendingOnly: true},
	} {
		page, err := events.ListParentEvents(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		assertParentEventScope(t, page, nested.ThreadID)
		for _, event := range page.Events {
			if event.SourceThreadID == later || event.SourceThreadID == laterFork {
				t.Fatal("post-fork ancestor event leaked")
			}
		}
		if len(query.SourceThreadIDs) > 0 && query.SourceThreadIDs[0] == later && len(page.Events) != 0 {
			t.Fatal("filtered page not empty")
		}
	}
	full, err := events.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: nested.ThreadID, FirstEventID: mixed.FirstEventID, LastEventID: mixed.LastEventID, Limit: 1})
	if err != nil || len(full.Events) != 1 || full.NextAfterEventID == "" {
		t.Fatalf("first=%+v %v", full, err)
	}
	assertParentEventScope(t, full, nested.ThreadID)
	tail, err := events.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: nested.ThreadID, FirstEventID: mixed.FirstEventID, LastEventID: mixed.LastEventID, AfterEventID: full.NextAfterEventID, Limit: 1})
	if err != nil || len(tail.Events) != 1 || tail.NextAfterEventID != "" {
		t.Fatalf("tail=%+v %v", tail, err)
	}
	assertParentEventScope(t, tail, nested.ThreadID)
	single, err := events.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: nested.ThreadID, EventID: original.Events[0].ID})
	if err != nil || len(single.Events) != 1 || single.Events[0] != original.Events[0] {
		t.Fatalf("single=%+v %v", single, err)
	}
	assertParentEventScope(t, single, nested.ThreadID)
	waited, err := service.(ThreadWaiter).WaitThreads(t.Context(), WaitThreadsInput{Scope: ThreadScope{ParentID: &fork.ThreadID}, ThreadIDs: []identity.ThreadID{own}, PendingParentEvents: true, Timeout: time.Second})
	if err != nil || waited.ParentEvents == nil || waited.Reason != ThreadWaitEvents {
		t.Fatalf("wait=%+v %v", waited, err)
	}
	assertParentEventScope(t, *waited.ParentEvents, fork.ThreadID)
	clone := cloneThreadRuntimeView(nested)
	assertParentEventScope(t, *clone.ParentEvents, nested.ThreadID)
	clone.ParentEvents.Events[0].ParentThreadID = "changed"
	if nested.ParentEvents.Events[0].ParentThreadID != parent.ThreadID {
		t.Fatal("clone changed source")
	}
	if len(gateway.Requests()) != 0 {
		t.Fatal("passive fork/read/wait woke provider")
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, reopened, restartedGateway := parentEventHost(t, true, storage.SQLite(path))
	for range 2 {
		view, err := reopened.View(t.Context(), nested.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		assertParentEventScope(t, *view.ParentEvents, nested.ThreadID)
		if len(view.ParentEvents.Events) != 2 || view.ParentEvents.Events[0] != mixed.Events[0] || view.ParentEvents.Events[1] != mixed.Events[1] {
			t.Fatalf("restart rewrote inherited events: %+v", view.ParentEvents)
		}
	}
	if len(restartedGateway.Requests()) != 0 {
		t.Fatal("restart woke provider")
	}
}
