package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/provider/cache"
	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/sessiontree"
	"github.com/floegence/floret/v6/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v6/storage"
	"github.com/floegence/floret/v6/storage/spi"
)

func TestProviderRequestCheckpointSurvivesReopenWithoutTurnTerminal(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 31, 1, 0, 0, 0, time.UTC)
	kernel, err := NewBackendKernel(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	segment := cache.Segment{ID: "segment", PromptScopeID: "thread", Provider: "deepseek", Model: "flash", Fingerprint: "fingerprint"}
	if err := kernel.AppendSegment(ctx, segment); err != nil {
		t.Fatal(err)
	}
	request := cache.ProviderRequestRecord{ID: "request", PromptScopeID: "thread", RunID: "run", TurnID: "turn", Provider: "deepseek", Model: "flash", SegmentIDs: []string{"segment"}}
	if err := kernel.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	requests, err := reopened.ProviderRequests(ctx, "thread")
	if err != nil || len(requests) != 1 || requests[0].ID != "request" {
		t.Fatalf("reopened requests=%#v err=%v", requests, err)
	}
	segments, err := reopened.Segments(ctx, "thread", "deepseek", "flash")
	if err != nil || len(segments) != 1 || segments[0].ID != "segment" {
		t.Fatalf("reopened segments=%#v err=%v", segments, err)
	}
}

func TestProviderRequestCheckpointRollsBackMemoryOnWriteFailureAndCancellation(t *testing.T) {
	ctx := context.Background()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &checkpointFailBackend{Backend: inner}
	now := time.Date(2026, 8, 31, 2, 0, 0, 0, time.UTC)
	kernel, err := NewBackendKernel(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := cache.ProviderRequestRecord{ID: "request", PromptScopeID: "thread", RunID: "run", TurnID: "turn", Provider: "deepseek", Model: "flash"}
	writeErr := errors.New("checkpoint write failed")
	backend.failNext(writeErr)
	if err := kernel.AppendProviderRequest(ctx, request); !errors.Is(err, writeErr) {
		t.Fatalf("write failure=%v, want %v", err, writeErr)
	}
	assertProviderRequestCount(t, ctx, kernel, 0)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := kernel.AppendProviderRequest(cancelled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled checkpoint=%v, want context.Canceled", err)
	}
	assertProviderRequestCount(t, ctx, kernel, 0)

	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	assertProviderRequestCount(t, ctx, reopened, 0)
}

func assertProviderRequestCount(t *testing.T, ctx context.Context, kernel *BackendKernel, want int) {
	t.Helper()
	requests, err := kernel.ProviderRequests(ctx, "thread")
	if err != nil || len(requests) != want {
		t.Fatalf("provider requests=%#v err=%v, want count %d", requests, err, want)
	}
}

type checkpointFailBackend struct {
	spi.Backend
	mu      sync.Mutex
	nextErr error
}

func (backend *checkpointFailBackend) failNext(err error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.nextErr = err
}

func (backend *checkpointFailBackend) Update(ctx context.Context, update func(spi.WriteTx) error) error {
	backend.mu.Lock()
	err := backend.nextErr
	backend.nextErr = nil
	backend.mu.Unlock()
	if err != nil {
		return err
	}
	return backend.Backend.Update(ctx, update)
}

func TestFinishTurnCommitsCanonicalSegmentedStateWithoutRecoveryJournal(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 25, 2, 0, 0, 0, time.UTC)
	kernel, err := NewBackendKernel(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.AcceptTurn(ctx, sessiontree.AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "turn-fingerprint", InputRequestFingerprint: "input-fingerprint",
		Input: session.Message{Role: session.User, Content: "hello"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if count := recoveryJournalRecordCount(t, ctx, backend); count != 0 {
		t.Fatalf("active turn wrote %d obsolete recovery journal frames", count)
	}
	if _, err := kernel.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "completed-fingerprint", Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if count := recoveryJournalRecordCount(t, ctx, backend); count != 0 {
		t.Fatalf("terminal checkpoint left %d recovery journal frames", count)
	}
	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(2 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	meta, err := reopened.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != "terminal" {
		t.Fatalf("reopened leaf=%q, want terminal", meta.LeafID)
	}
}

func recoveryJournalRecordCount(t *testing.T, ctx context.Context, backend spi.Backend) int {
	t.Helper()
	count := 0
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		var after []byte
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: "floret.domain.sessiontree.journal.v1", After: after, Limit: 256})
			if err != nil {
				return err
			}
			count += len(page.Records)
			if !page.HasMore {
				return nil
			}
			after = page.Next
		}
	}); err != nil {
		t.Fatal(err)
	}
	return count
}
