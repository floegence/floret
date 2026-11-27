package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

type onlyChildRangeTx struct {
	spi.ReadTx
	parent  string
	allowed map[string]bool
}

func expectChildBackfillCheckpoint(expected map[string]map[string][]byte) {
	expected[childMembershipNamespace] = map[string][]byte{string(childMembershipStateKey): []byte(`{"phase":"backfill"}`)}
}

func TestChildMembershipRestartIncludesWritesOnBothSidesOfCheckpoint(t *testing.T) {
	ctx := t.Context()
	backend := outputMigrationFixture(t, backendDomainV42Version)
	const parent = "historical"
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		for i := range backendThreadEntryPageSize + 3 {
			id := fmt.Sprintf("root-%06d", i)
			meta, err := prepareNewThreadMeta(ThreadMeta{ID: id}, time.Now)
			if err != nil {
				return err
			}
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, meta); err != nil {
				return err
			}
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordRootThread, id), backendDomainRecordRootThread, id, "", 0, backendDomainRootThread{ThreadID: id}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	open := func() *BackendRepo {
		t.Helper()
		var repo *BackendRepo
		if err := backend.Update(ctx, func(tx spi.WriteTx) (err error) {
			repo, _, err = NewCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
			return err
		}); err != nil || repo == nil {
			t.Fatalf("open: %v", err)
		}
		return repo
	}
	repo := open()
	if more, err := repo.MigrateNextThread(ctx); err != nil || !more || !repo.HasPendingMigration() {
		t.Fatalf("first batch: %t %v", more, err)
	}
	ids := []string{"a", "after-the-previously-committed-checkpoint"}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		s, err := readChildMembershipState(tx)
		if err != nil {
			return err
		}
		key := func(id string) []byte { return backendDomainKey(backendDomainRecordThread, id) }
		if s.Phase != "backfill" || bytes.Compare(key(ids[0]), key(s.AfterID)) >= 0 || bytes.Compare(key(ids[1]), key(s.AfterID)) <= 0 {
			t.Fatalf("IDs no longer straddle checkpoint: %+v", s)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: id, ParentThreadID: parent, TaskName: id, HostProfileRef: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(repo *BackendRepo) {
		t.Helper()
		rows, err := repo.ListThreads(ctx, ListThreadsOptions{ParentThreadID: parent, IncludeArchived: true})
		if err != nil || len(rows) != 2 || rows[0].ID == rows[1].ID {
			t.Fatalf("children: %+v, %v", rows, err)
		}
		for _, row := range rows {
			if row.ID != ids[0] && row.ID != ids[1] {
				t.Fatalf("unrelated child: %s", row.ID)
			}
		}
	}
	check(repo)
	// A fork origin does not establish ownership. Delete a separate tree
	// with one fork destination on each side of the backfill cursor.
	const separate = "independent"
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: parent, NewThreadID: separate}); err != nil {
		t.Fatal(err)
	}
	const deleted = "deleted"
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: deleted}); err != nil {
		t.Fatal(err)
	}
	wantDeleted := []string{deleted, "b", "owned-fork-after-checkpoint"}
	for _, child := range wantDeleted[1:] {
		if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: parent, NewThreadID: child, DestinationMeta: &ForkDestinationMeta{ParentThreadID: deleted, TaskName: child, HostProfileRef: "test"}}); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(wantDeleted)
	remove := func(replay bool) {
		t.Helper()
		res, err := repo.DeleteRootTreeWithRequest(ctx, deleted, "delete-test", "fingerprint")
		slices.Sort(res.ThreadIDs)
		if err != nil || res.Replayed != replay || !slices.Equal(res.ThreadIDs, wantDeleted) {
			t.Fatalf("delete: %+v %v", res, err)
		}
	}
	remove(false)
	repo = open()
	if !repo.HasPendingMigration() {
		t.Fatal("restart lost pending checkpoint")
	}
	if _, err := repo.MigrateNextThread(ctx); err != nil || repo.HasPendingMigration() {
		t.Fatalf("final batch: %v", err)
	}
	repo = open()
	remove(true)
	if ids, err := SubtreeThreadIDs(ctx, repo, separate); err != nil || !slices.Equal(ids, []string{separate}) {
		t.Fatalf("independent fork: %v %v", ids, err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		return visitIndexedChildren(ctx, tx, deleted, func(ThreadMeta) error { return errors.New("deleted edge survived restart") })
	}); err != nil {
		t.Fatal(err)
	}
	repo.backend = hierarchyReadGuard{backend, []string{parent, ids[0], ids[1]}}
	check(repo)
}

type hierarchyReadGuard struct {
	spi.Backend
	ids []string
}
type hierarchyGuardTx struct {
	spi.ReadTx
	ids []string
}

func (b hierarchyReadGuard) View(ctx context.Context, visit func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return visit(hierarchyGuardTx{tx, b.ids}) })
}

func (tx hierarchyGuardTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == childMembershipNamespace && bytes.Equal(key, childMembershipStateKey) || ns == backendDomainV32Namespace && bytes.Equal(key, backendDomainKey(backendDomainRecordManifest)) {
		return tx.ReadTx.Get(ns, key)
	}
	for _, id := range tx.ids {
		for _, kind := range []string{backendDomainRecordThread, backendDomainRecordRootThread, backendDomainRecordTombstone} {
			if ns == backendDomainV32Namespace && bytes.Equal(key, backendDomainKey(kind, id)) {
				return tx.ReadTx.Get(ns, key)
			}
		}
	}
	return nil, errors.New("unrelated hierarchy point read")
}

func (tx hierarchyGuardTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	for _, id := range tx.ids {
		if bytes.Equal(req.Start, backendDomainKey(id)) {
			return (onlyChildRangeTx{ReadTx: tx.ReadTx, parent: id}).Scan(req)
		}
	}
	return spi.ScanPage{}, errors.New("unrelated hierarchy scan")
}

func TestChildMembershipBackfillKeepsPublishedHistoryAndRollsBackAPage(t *testing.T) {
	ctx := t.Context()
	backend := outputMigrationFixture(t, backendDomainV42Version)
	const parent = "historical"
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		for i := range backendThreadEntryPageSize + 2 {
			id := fmt.Sprintf("child-%05d", i)
			meta, err := prepareNewThreadMeta(ThreadMeta{ID: id, ParentThreadID: parent, TaskName: id, HostProfileRef: "test"}, time.Now)
			if err != nil {
				return err
			}
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, meta); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	repo := &BackendRepo{}
	rollback := errors.New("interrupt page")
	var after string
	var more bool
	step := func(tx spi.WriteTx) error {
		var err error
		after, more, err = repo.backfillChildMembershipPage(ctx, tx, after)
		return err
	}
	if err := backend.Update(ctx, step); err != nil || !more || after == "" {
		t.Fatalf("first page: %q %t %v", after, more, err)
	}
	checkpoint := after
	first := cloneMigrationRecords(backend.records)
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := step(tx); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) || !reflect.DeepEqual(first, backend.records) {
		t.Fatalf("partial second page: %v", err)
	}
	after = checkpoint
	if err := backend.Update(ctx, step); err != nil || more {
		t.Fatalf("resumed page: %q %t %v", after, more, err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		n := 0
		err := visitIndexedChildren(ctx, tx, parent, func(meta ThreadMeta) error { n++; return nil })
		if err != nil || n != backendThreadEntryPageSize+2 {
			t.Fatalf("backfilled children: %d %v", n, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	afterRecords := cloneMigrationRecords(backend.records)
	delete(afterRecords, childMembershipNamespace)
	if !reflect.DeepEqual(before, afterRecords) {
		t.Fatal("metadata backfill rewrote historical records")
	}
}

func (tx onlyChildRangeTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	prefix := backendDomainKey(tx.parent)
	if req.Namespace != childMembershipNamespace || !bytes.Equal(req.Start, prefix) || !bytes.Equal(req.End, backendKeyRangeEnd(prefix)) || req.Limit <= 0 || req.Limit > backendThreadEntryPageSize {
		return spi.ScanPage{}, errors.New("unscoped child scan")
	}
	return tx.ReadTx.Scan(req)
}

func (tx onlyChildRangeTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace != backendDomainV32Namespace || !tx.allowed[string(key)] {
		return nil, errors.New("unrelated child metadata read")
	}
	return tx.ReadTx.Get(namespace, key)
}

func TestIndexedChildMembershipIsScopedCompleteAndAtomic(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	const parent = "parent"
	allowed := make(map[string]bool)
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		for i := range backendThreadEntryPageSize + 3 {
			p := parent
			if i == 0 {
				p = "parent-also"
			}
			id := fmt.Sprintf("child-%05d", i)
			meta, err := prepareNewThreadMeta(ThreadMeta{ID: id, ParentThreadID: p, TaskName: id, HostProfileRef: "test"}, time.Now)
			if err != nil {
				return err
			}
			key := backendDomainKey(backendDomainRecordThread, id)
			if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, key, backendDomainRecordThread, id, "", 0, meta); err != nil {
				return err
			}
			if err := changeChildMembership(tx, p, id, true); err != nil {
				return err
			}
			if p == parent {
				allowed[string(key)] = true
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	read := func(ctx context.Context, tx spi.ReadTx, p string) (int, error) {
		n := 0
		err := visitIndexedChildren(ctx, onlyChildRangeTx{tx, p, allowed}, p, func(meta ThreadMeta) error { n++; return nil })
		return n, err
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		for p, want := range map[string]int{parent: len(allowed), "empty": 0} {
			n, err := read(ctx, tx, p)
			if err != nil || n != want {
				t.Fatalf("%s: %d, %v", p, n, err)
			}
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := read(cancelled, tx, parent); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("abort test transaction")
	for label, damage := range map[string]func(spi.WriteTx) error{
		"missing count": func(tx spi.WriteTx) error { return tx.Delete(childMembershipNamespace, backendDomainKey(parent)) },
		"missing edge": func(tx spi.WriteTx) error {
			return tx.Delete(childMembershipNamespace, backendDomainKey(parent, "child-00001"))
		},
		"wrong location": func(tx spi.WriteTx) error {
			return tx.Put(childMembershipNamespace, backendDomainKey(parent, "child-00001"), []byte("child-00000"))
		},
	} {
		err := backend.Update(ctx, func(tx spi.WriteTx) error {
			if err := damage(tx); err != nil {
				return err
			}
			if _, err := read(ctx, tx, parent); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("%s: %v", label, err)
			}
			return rollback
		})
		if !errors.Is(err, rollback) || !reflect.DeepEqual(before, backend.records) {
			t.Fatalf("%s rollback: %v", label, err)
		}
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := changeChildMembership(tx, parent, "child-00001", false); err != nil {
			return err
		}
		if n, err := read(ctx, tx, parent); err != nil || n != len(allowed)-1 {
			t.Fatalf("removed child: %d, %v", n, err)
		}
		return changeChildMembership(tx, "parent-also", "child-00000", false)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		if n, err := read(ctx, tx, "parent-also"); err != nil || n != 0 {
			t.Fatalf("last child removed: %d, %v", n, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
