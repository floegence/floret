package sessiontree

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32CreateThreadAndInitialEntryShareOneCommit(t *testing.T) {
	backend := newMigrationTestBackend()
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, err = NewFreshCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("create commit failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	initial := Entry{Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "initial durable message"}}
	if _, _, err := repo.CreateThreadWithInitialEntry(t.Context(), ThreadMeta{}, initial); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial thread survived rollback")
	}
	repo.backend = backend
	created, entry, err := repo.CreateThreadWithInitialEntry(t.Context(), ThreadMeta{}, initial)
	if err != nil || created.ID != "thread-1" || created.LeafID != entry.ID {
		t.Fatalf("create=%+v, %+v, %v", created, entry, err)
	}
	child, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: created.ID, TaskName: "Review output", HostProfileRef: "test"})
	if err != nil || child.Title != "Review output" || child.TitleSource != ThreadTitleSourceHost {
		t.Fatalf("child=%+v, %v", child, err)
	}
	if repo.threadMemory != nil || repo.domainMemory != nil {
		t.Fatal("creation installed a MemoryRepo")
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: created.ID}); !errors.Is(err, ErrThreadExists) {
		t.Fatalf("duplicate=%v", err)
	}
}

func TestWarmChildInventoryReadsOnlyMatchingThreadMetadata(t *testing.T) {
	backend := newMigrationTestBackend()
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) (err error) {
		repo, err = NewFreshCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	create := func(meta ThreadMeta) {
		t.Helper()
		if _, err := repo.CreateThread(t.Context(), meta); err != nil {
			t.Fatal(err)
		}
	}
	list := func(parent string, count int) []ThreadMeta {
		t.Helper()
		rows, err := repo.ListThreads(t.Context(), ListThreadsOptions{ParentThreadID: parent})
		if err != nil || len(rows) != count {
			t.Fatalf("parent=%s count=%d err=%v", parent, len(rows), err)
		}
		return rows
	}
	create(ThreadMeta{ID: "root"})
	create(ThreadMeta{ID: "other"})
	list("root", 0)
	repo.backend = noScanCanonicalBackend{backend}
	list("other", 0)
	repo.backend = backend
	create(ThreadMeta{ID: "child", ParentThreadID: "root", TaskName: "worker", HostProfileRef: "test"})
	list("root", 1)
	repo.backend = noScanCanonicalBackend{backend}
	if rows := list("root", 1); rows[0].ID != "child" {
		t.Fatal(rows)
	}
	list("other", 0)
}
