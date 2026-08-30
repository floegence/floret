package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v6/storage"
	"github.com/floegence/floret/v6/storage/spi"
)

func TestBackendDomainV7SubAgentCreateWriteDoesNotGrowWithUnrelatedHistory(t *testing.T) {
	measure := func(historyEntries int) (int, int64) {
		backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()
		measured := &measuringBackend{Backend: backend}
		now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
		repo, err := NewBackendRepo(t.Context(), measured, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < historyEntries; index++ {
			if _, err := repo.Append(t.Context(), Entry{
				ThreadID: "root", Type: EntryUserMessage,
				Message: session.Message{Role: session.User, Content: strings.Repeat("history", 4096)},
			}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		measured.reset()
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{
			ID: "child", ParentThreadID: "root", TaskName: "research", HostProfileRef: "default",
			CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		return measured.usage()
	}

	smallPuts, smallBytes := measure(1)
	largePuts, largeBytes := measure(80)
	if smallPuts != largePuts || smallBytes != largeBytes {
		t.Fatalf("same SubAgent create wrote (%d,%d) with small history and (%d,%d) with large history", smallPuts, smallBytes, largePuts, largeBytes)
	}
	if largePuts > 2 {
		t.Fatalf("SubAgent create wrote %d records, want only affected records", largePuts)
	}
}

func TestBackendDomainV7RejectsFutureRecordWithoutMutation(t *testing.T) {
	backend := newMigrationTestBackend()
	manifest, err := encodeBackendDomainV7Record(backendDomainRecordManifest, "", "", 0, backendDomainV7Manifest{
		Version: backendDomainV7Version + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainV7Namespace, backendDomainV7Key(backendDomainRecordManifest), manifest)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("future v7 record error=%v, want ErrAuthorityCorrupt", err)
	}
	if got := cloneMigrationRecords(backend.records); !equalMigrationRecords(got, before) {
		t.Fatal("future v7 rejection mutated durable records")
	}
}

func TestBackendDomainV7WriteFailureDoesNotCommitStagedMemory(t *testing.T) {
	underlying, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	injected := errors.New("v7 write failed")
	backend := &toggleFailBackend{Backend: underlying, err: injected}
	now := time.Date(2026, 8, 29, 12, 30, 0, 0, time.UTC)
	repo, err := NewBackendRepo(t.Context(), backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	backend.setFail(true)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{
		ID: "child", ParentThreadID: "root", TaskName: "research", HostProfileRef: "default",
		CreatedAt: now, UpdatedAt: now,
	}); !errors.Is(err, injected) {
		t.Fatalf("CreateThread error=%v, want injected failure", err)
	}
	if _, err := repo.Thread(t.Context(), "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("failed staged child remained in memory: %v", err)
	}
	backend.setFail(false)
	reopened, err := NewBackendRepo(t.Context(), backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Thread(t.Context(), "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("failed staged child reached durable state: %v", err)
	}
}

func TestBackendDomainV7ConcurrentSubAgentCreateSendAndFinalize(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	repo, err := NewBackendRepo(t.Context(), backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "root", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	errorsByChild := make(chan error, 4)
	for index := 0; index < 4; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			childID := fmt.Sprintf("child-%d", index)
			turnID := fmt.Sprintf("turn-%d", index)
			runID := fmt.Sprintf("run-%d", index)
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{
				ID: childID, ParentThreadID: "root", TaskName: fmt.Sprintf("task-%d", index), HostProfileRef: "default",
				CreatedAt: now, UpdatedAt: now,
			}); err != nil {
				errorsByChild <- err
				return
			}
			if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{
				ThreadID: childID, TurnID: turnID, RunID: runID, LogicalRequestID: fmt.Sprintf("request-%d", index),
				RequestFingerprint: fmt.Sprintf("accept-%d", index), InputRequestFingerprint: fmt.Sprintf("input-%d", index),
				Input: session.Message{Role: session.User, Content: "research"}, Now: now,
			}); err != nil {
				errorsByChild <- err
				return
			}
			if _, err := repo.FinishTurn(t.Context(), FinishTurnRequest{
				ThreadID: childID, TurnID: turnID, RunID: runID, TerminalEntryID: fmt.Sprintf("terminal-%d", index),
				Status: TurnCompleted, OutcomeFingerprint: fmt.Sprintf("outcome-%d", index), Now: now.Add(time.Second),
			}); err != nil {
				errorsByChild <- err
			}
		}()
	}
	group.Wait()
	close(errorsByChild)
	for err := range errorsByChild {
		if err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := NewBackendRepo(t.Context(), backend, func() time.Time { return now.Add(2 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	threads, err := reopened.ListThreads(t.Context(), ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	children := make([]ThreadMeta, 0, 4)
	for _, thread := range threads {
		if thread.ParentThreadID == "root" {
			children = append(children, thread)
		}
	}
	if len(children) != 4 {
		t.Fatalf("reopened SubAgents=%d, want 4", len(children))
	}
	for _, child := range children {
		entries, err := reopened.Entries(t.Context(), child.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 || entries[len(entries)-1].TurnStatus != TurnCompleted {
			t.Fatalf("child %q did not retain completed terminal", child.ID)
		}
	}
}

func TestCreateChildInitializesCanonicalTaskTitle(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	repo := newMemoryRepo(func() time.Time { return now })
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	child, err := repo.CreateThread(t.Context(), ThreadMeta{
		ID: "child", ParentThreadID: "root", TaskName: "AI research", HostProfileRef: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	if child.Title != child.TaskName || child.TitleStatus != ThreadTitleReady || child.TitleSource != ThreadTitleSourceHost || child.TitleGeneration != 1 || !child.TitleUpdatedAt.Equal(now) {
		t.Fatalf("child title=%#v", child)
	}
}

type measuringBackend struct {
	spi.Backend
	mu       sync.Mutex
	puts     int
	putBytes int64
}

func (backend *measuringBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(measuringWriteTx{WriteTx: tx, backend: backend})
	})
}

func (backend *measuringBackend) reset() {
	backend.mu.Lock()
	backend.puts, backend.putBytes = 0, 0
	backend.mu.Unlock()
}

func (backend *measuringBackend) usage() (int, int64) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.puts, backend.putBytes
}

type measuringWriteTx struct {
	spi.WriteTx
	backend *measuringBackend
}

type toggleFailBackend struct {
	spi.Backend
	mu   sync.Mutex
	fail bool
	err  error
}

func (backend *toggleFailBackend) setFail(fail bool) {
	backend.mu.Lock()
	backend.fail = fail
	backend.mu.Unlock()
}

func (backend *toggleFailBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(toggleFailWriteTx{WriteTx: tx, backend: backend})
	})
}

type toggleFailWriteTx struct {
	spi.WriteTx
	backend *toggleFailBackend
}

func (tx toggleFailWriteTx) Put(namespace string, key, value []byte) error {
	tx.backend.mu.Lock()
	fail, err := tx.backend.fail, tx.backend.err
	tx.backend.mu.Unlock()
	if fail && namespace == backendDomainV7Namespace {
		return err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func (tx measuringWriteTx) Put(namespace string, key, value []byte) error {
	if namespace == backendDomainV7Namespace {
		tx.backend.mu.Lock()
		tx.backend.puts++
		tx.backend.putBytes += int64(len(key) + len(value))
		tx.backend.mu.Unlock()
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func equalMigrationRecords(left, right map[string]map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for namespace, records := range left {
		other, ok := right[namespace]
		if !ok || len(records) != len(other) {
			return false
		}
		for key, value := range records {
			if string(value) != string(other[key]) {
				return false
			}
		}
	}
	return true
}
