package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestProviderCheckpointWritesOnlyNewScopeRecords(t *testing.T) {
	ctx := t.Context()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &promptCountingBackend{Backend: inner}
	kernel, err := NewBackendKernel(ctx, backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := cache.ProviderRequestRecord{ID: "request", PromptScopeID: "active"}
	backend.bytes = 0
	if err := kernel.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}
	want := backend.bytes
	if err := kernel.CheckpointProviderRequest(ctx, []cache.Segment{{ID: "large", PromptScopeID: "unrelated", Raw: strings.Repeat("x", 1<<20)}}, nil, cache.ProviderRequestRecord{ID: "other", PromptScopeID: "unrelated"}); err != nil {
		t.Fatal(err)
	}
	backend.bytes = 0
	if err := kernel.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}
	if backend.bytes != want {
		t.Fatalf("checkpoint wrote %d bytes with unrelated history, want %d", backend.bytes, want)
	}
	requests, err := kernel.ProviderRequests(ctx, "active")
	if err != nil || len(requests) != 2 {
		t.Fatalf("duplicate attempt history: count=%d err=%v", len(requests), err)
	}
	backend.bytes = 0
	if err := kernel.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if backend.bytes != 0 {
		t.Fatalf("unchanged cache rewrote %d bytes", backend.bytes)
	}
}

type promptCountingBackend struct {
	spi.Backend
	bytes int
}

func (backend *promptCountingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(promptCountingTx{WriteTx: tx, backend: backend}) })
}

type promptCountingTx struct {
	spi.WriteTx
	backend *promptCountingBackend
}

func TestPromptPendingResponseSurvivesFailedCommitAndFlushesOnlyOnce(t *testing.T) {
	inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &promptCommitFailureBackend{Backend: inner}
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := kernel.AppendProviderResponse(t.Context(), cache.ProviderResponseRecord{RequestID: "previous", PromptScopeID: "active", InputTokens: 42}); err != nil {
		t.Fatal(err)
	}
	backend.fail = true
	request := cache.ProviderRequestRecord{ID: "new", PromptScopeID: "active"}
	if err := kernel.AppendProviderRequest(t.Context(), request); err == nil {
		t.Fatal("commit failure succeeded")
	}
	values, err := kernel.ProviderResponses(t.Context(), "active")
	if err != nil || len(values) != 1 {
		t.Fatalf("pending lost: %v", err)
	}
	requests, err := kernel.ProviderRequests(t.Context(), "active")
	if err != nil || len(requests) != 0 {
		t.Fatal("failed candidate published")
	}
	backend.fail = false
	if err := kernel.AppendProviderRequest(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(kernel.pending) != 0 {
		t.Fatal("committed observations remained pending")
	}
	reopened, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	values, err = reopened.ProviderResponses(t.Context(), "active")
	if err != nil || len(values) != 1 || values[0].InputTokens != 42 {
		t.Fatal("response duplicated or lost")
	}
}

type promptCommitFailureBackend struct {
	spi.Backend
	fail bool
}

func (backend *promptCommitFailureBackend) Update(ctx context.Context, update func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := update(tx); err != nil {
			return err
		}
		if backend.fail {
			return errors.New("injected commit failure")
		}
		return nil
	})
}

func (tx promptCountingTx) Put(namespace string, key, value []byte) error {
	if namespace == "floret.domain" || strings.HasPrefix(namespace, "floret.domain.prompt") {
		tx.backend.bytes += len(key) + len(value)
	}
	return tx.WriteTx.Put(namespace, key, value)
}

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
	toolset := cache.ToolsetSnapshot{ID: "toolset", PromptScopeID: "thread", Provider: "deepseek", Model: "flash", Fingerprint: "toolset-fingerprint"}
	prepared := cache.NewPreparedStore(kernel)
	if err := prepared.AppendSegment(ctx, segment); err != nil {
		t.Fatal(err)
	}
	if err := prepared.AppendToolset(ctx, toolset); err != nil {
		t.Fatal(err)
	}
	request := cache.ProviderRequestRecord{
		ID: "request", PromptScopeID: "thread", RunID: "run", TurnID: "turn", Provider: "deepseek", Model: "flash", SegmentIDs: []string{"segment"},
		TurnSurface: cache.TurnSurfaceSnapshot{Provider: "deepseek", Model: "flash", AdapterVersion: cache.Version, StateCompatibilityKey: "deepseek:flash:v1", Hash: "surface-hash"},
	}
	if err := prepared.AppendProviderRequest(ctx, request); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	requests, err := reopened.ProviderRequests(ctx, "thread")
	if err != nil || len(requests) != 1 || requests[0].ID != "request" || requests[0].TurnSurface.Hash != "surface-hash" {
		t.Fatalf("reopened requests=%#v err=%v", requests, err)
	}
	segments, err := reopened.Segments(ctx, "thread", "deepseek", "flash")
	if err != nil || len(segments) != 1 || segments[0].ID != "segment" {
		t.Fatalf("reopened segments=%#v err=%v", segments, err)
	}
	if active, found, err := reopened.ActiveToolset(ctx, "thread", "deepseek", "flash"); err != nil || !found || active.ID != "toolset" {
		t.Fatalf("reopened toolset=%#v found=%v err=%v", active, found, err)
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
	failed := cache.NewPreparedStore(kernel)
	if err := failed.AppendSegment(ctx, cache.Segment{ID: "failed-segment", PromptScopeID: "thread", Provider: "deepseek", Model: "flash"}); err != nil {
		t.Fatal(err)
	}
	if err := failed.AppendToolset(ctx, cache.ToolsetSnapshot{ID: "failed-toolset", PromptScopeID: "thread", Provider: "deepseek", Model: "flash"}); err != nil {
		t.Fatal(err)
	}
	backend.failNext(writeErr)
	if err := failed.AppendProviderRequest(ctx, request); !errors.Is(err, writeErr) {
		t.Fatalf("write failure=%v, want %v", err, writeErr)
	}
	assertProviderRequestCount(t, ctx, kernel, 0)
	assertPromptPreparationCount(t, ctx, kernel, 0, false)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	cancelledPreparation := cache.NewPreparedStore(kernel)
	if err := cancelledPreparation.AppendSegment(ctx, cache.Segment{ID: "cancelled-segment", PromptScopeID: "thread", Provider: "deepseek", Model: "flash"}); err != nil {
		t.Fatal(err)
	}
	if err := cancelledPreparation.AppendToolset(ctx, cache.ToolsetSnapshot{ID: "cancelled-toolset", PromptScopeID: "thread", Provider: "deepseek", Model: "flash"}); err != nil {
		t.Fatal(err)
	}
	if err := cancelledPreparation.AppendProviderRequest(cancelled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled checkpoint=%v, want context.Canceled", err)
	}
	assertProviderRequestCount(t, ctx, kernel, 0)
	assertPromptPreparationCount(t, ctx, kernel, 0, false)

	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	assertProviderRequestCount(t, ctx, reopened, 0)
	assertPromptPreparationCount(t, ctx, reopened, 0, false)
}

func assertProviderRequestCount(t *testing.T, ctx context.Context, kernel *BackendKernel, want int) {
	t.Helper()
	requests, err := kernel.ProviderRequests(ctx, "thread")
	if err != nil || len(requests) != want {
		t.Fatalf("provider requests=%#v err=%v, want count %d", requests, err, want)
	}
}

func assertPromptPreparationCount(t *testing.T, ctx context.Context, kernel *BackendKernel, wantSegments int, wantToolset bool) {
	t.Helper()
	segments, err := kernel.Segments(ctx, "thread", "deepseek", "flash")
	if err != nil || len(segments) != wantSegments {
		t.Fatalf("segments=%#v err=%v, want count %d", segments, err, wantSegments)
	}
	_, found, err := kernel.ActiveToolset(ctx, "thread", "deepseek", "flash")
	if err != nil || found != wantToolset {
		t.Fatalf("active toolset found=%v err=%v, want found=%v", found, err, wantToolset)
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
