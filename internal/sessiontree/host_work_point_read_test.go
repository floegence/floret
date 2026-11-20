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
