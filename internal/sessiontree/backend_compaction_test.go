package sessiontree

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32CompactionCommitRollbackAndRestart(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	request := compactionCommitFixture(t, memory)
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("compaction commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.CommitContextCompaction(t.Context(), request); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("compaction state survived rollback")
	}
	repo.backend = backend
	committed, err := repo.CommitContextCompaction(t.Context(), request)
	if err != nil || len(committed.Facts) != 4 || len(committed.Messages) == 0 || committed.Facts[3].TurnStatus != TurnCompleted {
		t.Fatalf("commit=%+v, %v", committed, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("compaction loaded a MemoryRepo")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil || !reflect.DeepEqual(messages, committed.Messages) {
		t.Fatalf("restart context mismatch: %v", err)
	}
	if _, err := repo.ProviderState(t.Context(), "thread"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatalf("provider state=%v", err)
	}
}
