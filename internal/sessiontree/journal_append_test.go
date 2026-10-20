package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestJournalAppendCheckpointRestoresTailAndReleasesReferences(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendPathTestEntry(t, repo, "root", "")
	before, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := repo.checkpointJournalAppendLocked("thread")
	for i := range 5 {
		_, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "discard"}}, AppendOptions{ID: fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	tail := repo.entries["thread"][1:]
	checkpoint.restore(repo)
	after, err := repo.EncodeMemoryState()
	if err != nil || string(before) != string(after) {
		t.Fatalf("checkpoint changed canonical state: %v", err)
	}
	for _, entry := range tail {
		if !reflect.DeepEqual(entry, Entry{}) {
			t.Fatal("rollback retained appended payload")
		}
	}
	if page, err := repo.PathPage(t.Context(), "thread", "", "", 1); err != nil || page.Entries[0].ID != "root" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestJournalAppendFailureInsideBackendTransactionCanContinue(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		_, err := memory.AppendRuntimeFacts(t.Context(), "thread", []Entry{
			{ID: "rolled-back", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"one"}`)},
			{ID: "invalid", ParentID: "missing", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"two"}`)},
		})
		if !errors.Is(err, ErrInvalidParent) {
			return fmt.Errorf("partial append error: %v", err)
		}
		_, err = memory.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "kept", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"kept"}`)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil || len(entries) != 1 || entries[0].ID != "kept" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	if _, err := repo.Entry(t.Context(), "thread", "rolled-back"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("stale index=%v", err)
	}
}

func BenchmarkRuntimeFactsFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(b.Context(), ThreadMeta{ID: "thread"}); err != nil {
				b.Fatal(err)
			}
			for i := range count {
				appendPathTestEntry(b, repo, fmt.Sprint(i), "")
			}
			facts := []Entry{{ID: "new", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"new"}`)}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				checkpoint := repo.checkpointJournalAppendLocked("thread")
				if _, err := repo.AppendRuntimeFacts(context.Background(), "thread", facts); err != nil {
					b.Fatal(err)
				}
				checkpoint.restore(repo)
			}
		})
	}
}

func TestJournalAppendCheckpointRestoresTurnMarkers(t *testing.T) {
	for _, backendTransaction := range []bool{false, true} {
		t.Run(fmt.Sprint(backendTransaction), func(t *testing.T) {
			memory := NewMemoryRepo()
			var backendRepo *BackendRepo
			if backendTransaction {
				var err error
				backendRepo, err = NewBackendRepo(t.Context(), newMigrationTestBackend(), time.Now)
				if err != nil {
					t.Fatal(err)
				}
				memory = backendRepo.domainMemory
			}
			exercise := func(memory *MemoryRepo) error {
				if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
					return err
				}
				started, err := memory.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "kept", RunID: "run-kept", Type: EntryTurnMarker, TurnStatus: TurnStarted}, AppendOptions{ID: "kept-started"})
				if err != nil {
					return err
				}
				for _, turnID := range []string{"new", "kept"} {
					facts := []Entry{}
					if turnID == "new" {
						facts = append(facts, Entry{ID: "new-started", TurnID: turnID, RunID: "run-new", Type: EntryTurnMarker, TurnStatus: TurnStarted})
					}
					facts = append(facts,
						Entry{ID: turnID + "-terminal", TurnID: turnID, RunID: "run-" + turnID, Type: EntryTurnMarker, TurnStatus: TurnCompleted},
						Entry{ID: turnID + "-invalid", ParentID: "missing", Type: EntryQueueAdded},
					)
					if _, err := memory.AppendRuntimeFacts(t.Context(), "thread", facts); !errors.Is(err, ErrInvalidParent) {
						return fmt.Errorf("failed batch: %w", err)
					}
					if _, found := memory.turnTerminalLocked("thread", turnID); found {
						return fmt.Errorf("rolled-back terminal retained for %s", turnID)
					}
					if got := memory.hasTurnStartedLocked("thread", turnID); got != (turnID == "kept") {
						return fmt.Errorf("incorrect start marker for %s", turnID)
					}
				}
				if marker := memory.turnMarkers["thread"]["kept"]; marker.started != 1 || memory.entries["thread"][marker.started-1].ID != started.ID {
					return errors.New("lost retained start marker")
				}
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "kept", RunID: "run-kept", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "continued"}}, AppendOptions{ID: "continued"}); err != nil {
					return err
				}
				return nil
			}
			if backendRepo != nil {
				if err := backendRepo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error { return exercise(memory) }); err != nil {
					t.Fatal(err)
				}
				if entries, err := backendRepo.Entries(t.Context(), "thread"); err != nil || len(entries) != 2 || entries[1].ID != "continued" {
					t.Fatalf("entries=%+v err=%v", entries, err)
				}
			} else if err := exercise(memory); err != nil {
				t.Fatal(err)
			}
		})
	}
}
