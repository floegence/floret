package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

type workPointReadBackend struct{ spi.Backend }

func (backend workPointReadBackend) Update(ctx context.Context, visit func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return visit(workPointReadTx{tx}) })
}

type workPointReadTx struct{ spi.WriteTx }

func (tx workPointReadTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	if req.Namespace == sparseSourcesNamespace {
		return spi.ScanPage{}, errors.New("single-work mutation scanned thread inventory")
	}
	return tx.WriteTx.Scan(req)
}

func TestHostWorkMutationsNeedNoInventoryScan(t *testing.T) {
	for _, hostFirst := range []bool{false, true} {
		backend, repo, attempt := canonicalSettlementFixture(t, true)
		repo.backend = workPointReadBackend{backend}
		work := HostWork{ID: "selected", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
		if _, err := repo.BindHostWork(t.Context(), work); err != nil {
			t.Fatal(err)
		}
		settle := func() {
			if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ID: work.ID, ThreadID: "thread", ExecutorID: "executor", State: "completed", Notice: "exited"}); err != nil {
				t.Fatal(err)
			}
		}
		if hostFirst {
			settle()
		}
		request := canonicalEffectRequest(attempt)
		request.Result.Message.ToolResult.ObservedHostWork = work.ID
		first, err := repo.FinishEffectDispatch(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if !hostFirst {
			settle()
		}
		replay, err := repo.FinishEffectDispatch(t.Context(), request)
		if err != nil || !replay.Replayed || replay.Result.ID != first.Result.ID {
			t.Fatalf("replay: %+v %v", replay, err)
		}
	}
}

type failedWorkRead struct{ spi.ReadTx }

func TestColdHostWorkRecoveryReadBudget(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	for index := range 48 {
		work := HostWork{ID: fmt.Sprint("work-", index), ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
		if _, err := repo.BindHostWork(t.Context(), work); err != nil {
			t.Fatal(err)
		}
		if index != 4 {
			if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ThreadID: "thread", ID: work.ID, ExecutorID: work.ExecutorID, State: "completed", Notice: "done"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	counted := &sparseReadBackend{Backend: backend}
	repo.backend = counted
	works, err := repo.PendingHostWorks(t.Context(), "thread")
	if err != nil || len(works) != 1 || works[0].ID != "work-4" || counted.gets > 72 || counted.scans > 1 {
		t.Fatalf("recovery works=%d gets=%d scans=%d err=%v", len(works), counted.gets, counted.scans, err)
	}
	t.Logf("recovery: %d pending, %d gets, %d scans", len(works), counted.gets, counted.scans)
	repo.backend = backend
	all, err := repo.HostWorks(t.Context(), "thread")
	if err != nil || len(all) != 48 {
		t.Fatalf("full inventory=%d: %v", len(all), err)
	}
}

func TestColdHostWorkRecoveryRebuildAndBranch(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "work", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
	ctx := t.Context()
	if _, err := repo.BindHostWork(ctx, work); err != nil {
		t.Fatal(err)
	}
	request := canonicalEffectRequest(attempt)
	request.Result.Message.ToolResult.ObservedHostWork = work.ID
	if _, err := repo.FinishEffectDispatch(ctx, request); err != nil {
		t.Fatal(err)
	}
	check := func(want int) {
		t.Helper()
		works, err := repo.PendingHostWorks(ctx, "thread")
		if err != nil || len(works) != want || want == 1 && works[0].ID != work.ID {
			t.Fatalf("want=%d works=%v err=%v", want, works, err)
		}
	}
	drop := func() {
		t.Helper()
		if err := backend.Update(ctx, func(tx spi.WriteTx) error { return tx.Delete(sparseSourcesNamespace, pendingWorkKey("thread")) }); err != nil {
			t.Fatal(err)
		}
	}
	check(1)
	drop()
	check(1)
	settlement := HostWorkSettlement{ThreadID: "thread", ID: work.ID, ExecutorID: work.ExecutorID, State: "completed", Notice: "done"}
	injected := errors.New("rollback")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return injected }}
	if _, err := repo.SettleHostWork(ctx, settlement); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	repo.backend = backend
	check(1)
	if _, err := repo.SettleHostWork(ctx, settlement); err != nil {
		t.Fatal(err)
	}
	check(0)
	drop()
	check(0)
	fork, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", EntryID: hostWorkEntryID(work.ID, false), NewThreadID: "fork", Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := repo.PendingHostWorks(ctx, fork.ID); err != nil || len(got) != 0 {
		t.Fatalf("inherited recovery: %v %v", got, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.PendingHostWorks(cancelled, "thread"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(sparseSourcesNamespace, pendingWorkKey("thread"), []byte(`{"Events":2,"IDs":[]}`))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PendingHostWorks(ctx, "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("damaged locator: %v", err)
	}
}

func TestSQLiteColdHostWorkRecoveryAtIncidentScale(t *testing.T) {
	ctx := t.Context()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(filepath.Join(t.TempDir(), "work.sqlite"))))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	var repo *BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		repo, err = NewFreshCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "long"}); err != nil {
		t.Fatal(err)
	}
	var batch []Entry
	for index := range 955 {
		work := HostWork{ID: fmt.Sprint("work-", index), ExecutorID: "previous", Invocation: EffectInvocationIdentity{ThreadID: "long", TurnID: "turn", RunID: "run", ToolCallID: fmt.Sprint(index), ToolName: "shell", ArgumentHash: StableHash("{}")}, State: "bound"}
		for _, final := range []bool{false, true} {
			if final {
				work.State, work.Notice = "completed", "done"
			}
			raw, err := json.Marshal(work)
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, Entry{ID: hostWorkEntryID(work.ID, final), ThreadID: "long", TurnID: "turn", RunID: "run", Type: EntryHostWork, Payload: raw})
		}
		if len(batch) >= 128 || index == 954 {
			if _, err := repo.AppendRuntimeFacts(ctx, "long", batch); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
	}
	for _, mode := range []string{"inventory", "missing", "prepared"} {
		if mode == "missing" {
			if err := backend.Update(ctx, func(tx spi.WriteTx) error { return tx.Delete(sparseSourcesNamespace, pendingWorkKey("long")) }); err != nil {
				t.Fatal(err)
			}
		}
		repo = reopenCurrentStartupRepo(t, backend, false)
		read, want := repo.PendingHostWorks, 0
		if mode == "inventory" {
			read, want = repo.HostWorks, 955
		}
		start := time.Now()
		works, err := read(ctx, "long")
		t.Logf("%s: %v, %d works", mode, time.Since(start), len(works))
		if err != nil || len(works) != want {
			t.Fatalf("%s: %v", mode, err)
		}
	}
}

func TestHostWorkAdmissionDoesNotRescanActiveHistory(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	admit := func(want bool) {
		t.Helper()
		entry, err := repo.AdmitHostWork(t.Context(), "thread", "turn", "run")
		if err != nil || (entry.ID != "") != want {
			t.Fatalf("admission %q: %v (want notice %v)", entry.ID, err, want)
		}
	}
	work := HostWork{ID: "selected", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
	if _, err := repo.BindHostWork(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	admit(false)
	repo.backend = workPointReadBackend{backend}
	admit(false)
	if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ID: work.ID, ThreadID: "thread", ExecutorID: "executor", State: "completed", Notice: "exited"}); err != nil {
		t.Fatal(err)
	}
	admit(true)
	admit(false)
}

func TestHostWorkAdmissionWindowFollowsCommittedFacts(t *testing.T) {
	for _, observed := range []bool{false, true} {
		backend, repo, attempt := canonicalSettlementFixture(t, true)
		work := HostWork{ID: "selected", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
		if _, err := repo.BindHostWork(t.Context(), work); err != nil {
			t.Fatal(err)
		}
		admit := func() (Entry, error) { return repo.AdmitHostWork(t.Context(), "thread", "turn", "run") }
		if entry, err := admit(); err != nil || entry.ID != "" {
			t.Fatalf("warm: %q %v", entry.ID, err)
		}
		if observed {
			request := canonicalEffectRequest(attempt)
			request.Result.Message.ToolResult.ObservedHostWork = work.ID
			if _, err := repo.FinishEffectDispatch(t.Context(), request); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ID: work.ID, ThreadID: "thread", ExecutorID: "executor", State: "completed", Notice: "exited"}); err != nil {
			t.Fatal(err)
		}
		injected := errors.New("rollback admission")
		repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return injected }}
		if _, err := admit(); !errors.Is(err, injected) {
			t.Fatalf("rollback: %v", err)
		}
		repo.backend = workPointReadBackend{backend}
		entry, err := admit()
		if err != nil || (entry.ID == "") != observed {
			t.Fatalf("observed=%v entry=%q: %v", observed, entry.ID, err)
		}
		if entry, err = admit(); err != nil || entry.ID != "" {
			t.Fatalf("duplicate: %q %v", entry.ID, err)
		}
	}
}

func (failedWorkRead) Get(string, []byte) ([]byte, error) { return nil, context.DeadlineExceeded }

func TestAncestorReadDeadlineIsNotCorruptAuthority(t *testing.T) {
	_, err := entryAncestor(t.Context(), failedWorkRead{}, "thread", entryLocator{Depth: 2, Parent: "parent"}, 1)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ancestor deadline: %v", err)
	}
}

func BenchmarkSQLiteHostWorkIncidentScale(b *testing.B) {
	ctx := b.Context()
	source := storagebridge.Source(storage.SQLite(filepath.Join(b.TempDir(), "incident.sqlite")))
	backend, err := storagebridge.Open(ctx, source)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	var repo *BackendRepo
	err = backend.Update(ctx, func(tx spi.WriteTx) error {
		var initErr error
		repo, initErr = NewFreshCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		return initErr
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err = repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		b.Fatal(err)
	}
	rows := make([]Entry, 16000)
	for index := range rows {
		entry := Entry{ThreadID: "thread", ID: fmt.Sprintf("fact-%d", index), Type: EntryCustom}
		if index%5 == 0 {
			work := HostWork{ID: entry.ID, State: "bound", ExecutorID: "executor", Invocation: EffectInvocationIdentity{ThreadID: "thread", TurnID: "history", RunID: "run", ToolCallID: entry.ID, ToolName: "shell", ArgumentHash: StableHash("{}")}}
			entry.ID, entry.Type, entry.TurnID, entry.RunID = hostWorkEntryID(work.ID, false), EntryHostWork, "history", "run"
			entry.Payload, err = json.Marshal(work)
			if err != nil {
				b.Fatal(err)
			}
		}
		rows[index] = entry
	}
	for start := 0; start < len(rows); start += 128 {
		if _, err = repo.AppendRuntimeFacts(ctx, "thread", rows[start:min(start+128, len(rows))]); err != nil {
			b.Fatal(err)
		}
	}
	if err = repo.prepareSparseSources(ctx, "thread"); err != nil {
		b.Fatal(err)
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		b.Fatal(err)
	}
	if err = backend.View(ctx, func(tx spi.ReadTx) error {
		cov, err := readSparseCoverage(tx, "thread")
		if err == nil && (cov.Through != 16000 || cov.HostWork != 3200) {
			b.Fatalf("coverage=%+v", cov)
		}
		return err
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		err = backend.View(ctx, func(tx spi.ReadTx) error {
			counted := &canonicalReadCountingTx{ReadTx: tx}
			_, err := hostWorkOnBranch(ctx, counted, "thread", meta.LeafID, "fact-15995", false)
			if counted.gets > 20 {
				b.Errorf("selected binding needed %d reads", counted.gets)
			}
			b.ReportMetric(float64(counted.gets), "gets/op")
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
