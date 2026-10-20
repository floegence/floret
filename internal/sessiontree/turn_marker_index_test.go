package sessiontree

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func assertRuntimeLocatorEquivalence(t *testing.T, repo *MemoryRepo, threadID string) {
	t.Helper()
	entries := repo.entries[threadID]
	wantTurn, wantActive := runtimeActiveTurn(entries)
	turn, active := repo.runtimeActiveTurnLocked(threadID)
	if turn != wantTurn || active != wantActive {
		t.Fatalf("active=(%s,%v), want=(%s,%v)", turn, active, wantTurn, wantActive)
	}
	for _, id := range []string{"one", "two", "three", "missing"} {
		wantRun, wantErr := activeRunIdentity(entries, id)
		run, err := repo.activeRunIdentityLocked(threadID, id)
		if run != wantRun || !errors.Is(err, wantErr) {
			t.Fatalf("identity %s=(%s,%v), want=(%s,%v)", id, run, err, wantRun, wantErr)
		}
		if got, want := repo.turnCancellationRequestedLocked(threadID, id), turnCancellationRequested(entries, id); got != want {
			t.Fatalf("cancellation %s=%v, want=%v", id, got, want)
		}
		terminal, found := repo.turnTerminalLocked(threadID, id)
		wantTerminal, wantFound := runtimeTurnTerminal(entries, id)
		if found != wantFound || terminal.ID != wantTerminal.ID {
			t.Fatalf("terminal %s=%s,%v want=%s,%v", id, terminal.ID, found, wantTerminal.ID, wantFound)
		}
	}
}

func TestRuntimeControlLocatorsMatchJournalAndRollback(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(91, 17))
	statuses := []TurnMarkerStatus{TurnStarted, TurnSavePoint, TurnWaiting, TurnCompleted, TurnFailed, TurnAborted}
	for i := range 1000 {
		checkpoint := repo.checkpointJournalAppendLocked("thread")
		for j := range 1 + random.IntN(4) {
			turn := []string{"one", "two", "three"}[random.IntN(3)]
			kind := EntryTurnMarker
			if random.IntN(4) == 0 {
				kind = EntryCancelRequested
			}
			run := fmt.Sprintf("run-%d", random.IntN(5))
			if random.IntN(5) == 0 {
				run = ""
			}
			repo.appendIndexedEntriesLocked("thread", Entry{ID: fmt.Sprintf("%d-%d", i, j), ThreadID: "thread", TurnID: turn, RunID: run, Type: kind, TurnStatus: statuses[random.IntN(len(statuses))]})
			assertRuntimeLocatorEquivalence(t, repo, "thread")
		}
		if random.IntN(3) == 0 {
			checkpoint.restore(repo)
		}
		if i%31 == 0 {
			repo.rebuildTurnMarkersLocked("thread")
		}
		assertRuntimeLocatorEquivalence(t, repo, "thread")
	}
}

func TestRuntimeControlLocatorsRestoreAfterFailedBackendCommit(t *testing.T) {
	backend := &domainCommitBoundaryBackend{Backend: newMigrationTestBackend()}
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := providerContextFixture(t, repo)
	injected := errors.New("control commit rejected")
	backend.afterCallback = func() error { return injected }
	err = repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		_, err := memory.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "replacement-run", Type: EntryCancelRequested}, AppendOptions{ID: "failed-stop"})
		return err
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if repo.domainMemory.turnCancellationRequestedLocked("thread", "turn") {
		t.Fatal("failed stop remained installed")
	}
	if run, err := repo.domainMemory.activeRunIdentityLocked("thread", "turn"); err != nil || run != "run" {
		t.Fatalf("identity=%s,%v", run, err)
	}
	backend.afterCallback = nil
	if _, err := repo.CommitProviderContext(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRuntimeControlLocators(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := NewMemoryRepo()
			_, _ = repo.CreateThread(b.Context(), ThreadMeta{ID: "thread"})
			repo.appendIndexedEntriesLocked("thread", Entry{ID: "started", ThreadID: "thread", TurnID: "one", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnStarted})
			for i := range count {
				repo.appendIndexedEntriesLocked("thread", Entry{ID: fmt.Sprint(i), ThreadID: "thread", TurnID: "one", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "history"}})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if turn, active := repo.runtimeActiveTurnLocked("thread"); !active || turn != "one" {
					b.Fatal("active identity changed")
				}
				if run, err := repo.activeRunIdentityLocked("thread", "one"); err != nil || run != "run" {
					b.Fatal("run identity changed")
				}
				if repo.turnCancellationRequestedLocked("thread", "one") {
					b.Fatal("unexpected cancellation")
				}
			}
		})
	}
}
