package sessiontree

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32PreparationRestoresOnlyUnresolvedParentEventsOnce(t *testing.T) {
	for _, current := range []bool{false, true} {
		t.Run(map[bool]string{false: "v31", true: "v32"}[current], func(t *testing.T) {
			legacy, backend := parentEventRepoFixture(t)
			legacy.domainMemory.parentEvents = false
			for _, id := range []string{"pending", "resolved"} {
				if _, err := legacy.AppendRuntimeFacts(t.Context(), "child", []Entry{{ID: "interaction-requested:" + id, ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)}}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := legacy.AppendRuntimeFacts(t.Context(), "child", []Entry{{ID: "interaction-resolved:resolved", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionDone}}); err != nil {
				t.Fatal(err)
			}
			var repo *BackendRepo
			open := func(enabled bool) {
				t.Helper()
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					var err error
					repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, enabled)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			if current {
				open(false)
				for {
					more, err := repo.MigrateNextThread(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					if !more {
						break
					}
				}
			}
			for range 2 {
				open(true)
				if err := repo.PrepareQueryThread(t.Context(), "parent"); err != nil {
					t.Fatal(err)
				}
				if err := repo.PrepareQueryThread(t.Context(), "child"); err != nil {
					t.Fatal(err)
				}
				page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
				if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "approval_required" || page.Events[0].InteractionID != "pending" {
					t.Fatalf("recovered events = %+v, %v", page, err)
				}
				if ready, err := repo.ThreadPrepared(t.Context(), "parent"); err != nil || !ready {
					t.Fatalf("parent readiness after recovery = %v, %v", ready, err)
				}
			}
		})
	}
}

func TestV32ParentEventRecoveryPreservesJournalOrder(t *testing.T) {
	legacy, backend := parentEventRepoFixture(t)
	legacy.domainMemory.parentEvents = false
	for _, entry := range []Entry{
		{ID: "a-start", ThreadID: "child", TurnID: "a-turn", RunID: "a-run", Type: EntryTurnMarker, TurnStatus: TurnStarted, RequestKey: "a-request", RequestFingerprint: "a-request"},
		{ID: "interaction-requested:first", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)},
		{ID: "interaction-requested:second", ThreadID: "child", TurnID: "a-turn", RunID: "a-run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)},
		{ID: "interaction-requested:third", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)},
		{ID: "short", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)},
	} {
		if _, err := legacy.AppendRuntimeFacts(t.Context(), "child", []Entry{entry}); err != nil {
			t.Fatal(err)
		}
	}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareQueryThread(t.Context(), "child"); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("recovered events = %+v, %v", page, err)
	}
	for i, want := range []string{"first", "second", "third"} {
		if page.Events[i].InteractionID != want {
			t.Fatalf("event %d = %q, want %q", i, page.Events[i].InteractionID, want)
		}
	}
}
