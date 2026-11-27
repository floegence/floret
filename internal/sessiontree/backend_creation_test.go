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
	repo.backend = hierarchyReadGuard{backend, []string{"other"}}
	list("other", 0)
	repo.backend = backend
	create(ThreadMeta{ID: "child", ParentThreadID: "root", TaskName: "worker", HostProfileRef: "test"})
	list("root", 1)
	repo.backend = hierarchyReadGuard{backend, []string{"root", "child", "other"}}
	if rows := list("root", 1); rows[0].ID != "child" {
		t.Fatal(rows)
	}
	list("other", 0)
}

func TestScopedSubtreeKeepsSiblingOrderWithoutReadingUnrelatedRoots(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	var repo *BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) (err error) {
		repo, err = NewFreshCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct{ id, parent string }{{"root", ""}, {"other", ""}, {"older", "root"}, {"newer", "root"}, {"leaf", "older"}, {"foreign", "other"}} {
		meta := ThreadMeta{ID: row.id, ParentThreadID: row.parent, CreatedAt: time.Unix(int64(i+1), 0)}
		if row.parent != "" {
			meta.TaskName, meta.HostProfileRef = row.id, "test"
		}
		if _, err := repo.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) (err error) {
		repo, _, err = NewCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return err
	}); err != nil || repo == nil {
		t.Fatalf("cold subtree: %v", err)
	}
	repo.backend = hierarchyReadGuard{backend, []string{"root", "newer", "older", "leaf", "missing"}}
	for root, want := range map[string][]string{"root": {"root", "newer", "older", "leaf"}, "older": {"older", "leaf"}, "leaf": {"leaf"}} {
		got, err := SubtreeThreadIDs(ctx, repo, root)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("subtree %s: %v %v", root, got, err)
		}
	}
	if _, err := SubtreeThreadIDs(ctx, repo, "missing"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("missing subtree: %v", err)
	}
}

// Only the lookup itself is guarded: opening a historically older format can
// legitimately need a one-time migration before the current owner is ready.
func TestColdEmptyChildLookupDoesNotEnumerateThreads(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	var repo *BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) (err error) {
		repo, err = NewFreshCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"oldest", "unrelated"} {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) (err error) {
		repo, _, err = NewCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return err
	}); err != nil || repo == nil {
		t.Fatalf("reopen: %v", err)
	}
	repo.backend = hierarchyReadGuard{backend, []string{"oldest"}}
	rows, err := repo.ListThreads(ctx, ListThreadsOptions{ParentThreadID: "oldest"})
	if err != nil || len(rows) != 0 {
		t.Fatalf("cold child lookup: %v %v", rows, err)
	}
}
