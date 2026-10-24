package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
	"testing"
)

func TestCurrentQueueSourcesSkipConsumedHistory(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	appendFact := func(entry Entry) {
		t.Helper()
		entry.ThreadID = "thread"
		if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 200 {
		id := fmt.Sprintf("queued-%d", i)
		payload, _ := json.Marshal(map[string]any{"id": id, "input": map[string]string{"text": "retained source"}})
		appendFact(Entry{ID: "add-" + id, Type: EntryQueueAdded, Payload: payload})
		payload, _ = json.Marshal(id)
		appendFact(Entry{ID: "remove-" + id, Type: EntryQueuePromoted, Payload: payload})
	}
	payload, _ := json.Marshal(map[string]any{"id": "pending", "input": map[string]string{"text": "show me"}})
	appendFact(Entry{ID: "pending-source", Type: EntryQueueAdded, Payload: payload})
	counted := &currentStartupBackend{Backend: backend}
	repo.backend = counted
	got, err := repo.CurrentQueueSources(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Pending) != 1 || got.Pending[0].ID != "pending-source" || len(got.Restored) != 0 {
		t.Fatalf("unexpected sources: %+v", got)
	}
	if counted.gets > 32 || counted.bodies != 0 || counted.journalRows != 0 {
		t.Fatalf("history sized reads: %+v", counted)
	}
	// Corrupt the exact current locator; an index is not permission to trust it.
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(backendEntryIndexNamespace, backendDomainKey("entry", "thread", "pending-source"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CurrentQueueSources(t.Context(), "thread"); err == nil {
		t.Fatal("missing canonical source accepted")
	}
}

func TestCurrentQueueSourcesPreserveOrderRestoreAndRestart(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	add := func(id, source, text string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"id": id, "input": map[string]string{"text": text}})
		if _, err := repo.Append(t.Context(), Entry{ID: source, ThreadID: "thread", Type: EntryQueueAdded, Payload: raw}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	remove := func(source, id string, restore bool) {
		t.Helper()
		raw, _ := json.Marshal(id)
		entry := Entry{ID: source, ThreadID: "thread", Type: EntryQueueDeleted, Payload: raw}
		if restore {
			entry.Metadata = map[string]string{"restored_from_backup": "true"}
		}
		if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	add("a", "a-first", "first")
	add("b", "b", "second")
	add("a", "a-last", "latest restore input")
	raw, _ := json.Marshal([]string{"b", "a"})
	if _, err := repo.Append(t.Context(), Entry{ID: "order", ThreadID: "thread", Type: EntryQueueReordered, Payload: raw}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.CurrentQueueSources(t.Context(), "thread")
	if err != nil || len(got.Pending) != 2 || got.Pending[0].ID != "b" || got.Pending[1].ID != "a-first" {
		t.Fatalf("order lost: %+v %v", got, err)
	}
	remove("restore-a", "a", true)
	remove("restore-a-again", "a", true)
	want, err := repo.CurrentQueueSources(t.Context(), "thread")
	if err != nil || len(want.Restored) != 2 || want.Restored[0].Added.ID != "a-last" {
		t.Fatalf("restore lost: %+v %v", want, err)
	}
	// Journal queue facts survive a path switch; they are not active-path messages.
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	got, err = repo.CurrentQueueSources(t.Context(), "thread")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("restart/path changed queue: %+v %v", got, err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Delete(sparseSourcesNamespace, currentQueueKey("thread")) }); err != nil {
		t.Fatal(err)
	}
	got, err = repo.CurrentQueueSources(t.Context(), "thread")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("rebuild changed queue: %+v %v", got, err)
	}
}

func TestCurrentQueueSourceWritesRollBack(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			before := cloneMigrationRecords(backend.records)
			failure := errors.New("injected queue commit failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error {
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(failure)
				}
				return failure
			}}
			func() {
				if mode == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("panic not propagated")
						}
					}()
				}
				_, err := repo.Append(ctx, Entry{ID: "pending", ThreadID: "thread", Type: EntryQueueAdded, Payload: []byte("{}")}, AppendOptions{})
				if err == nil {
					t.Fatal("failed append succeeded")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed transaction changed canonical/index records")
			}
		})
	}
}
