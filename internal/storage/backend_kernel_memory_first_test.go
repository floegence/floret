package storage_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
	kernelstorage "github.com/floegence/floret/v3/internal/storage"
	"github.com/floegence/floret/v3/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

type countingBackend struct {
	inner   spi.Backend
	updates atomic.Int32
}

func (backend *countingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.inner.View(ctx, read)
}

func (backend *countingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	backend.updates.Add(1)
	return backend.inner.Update(ctx, mutate)
}

func (backend *countingBackend) Close() error { return backend.inner.Close() }

func TestPromptObservationStaysInMemoryUntilSemanticCheckpoint(t *testing.T) {
	ctx := context.Background()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	backend := &countingBackend{inner: inner}
	defer backend.Close()
	kernel, err := kernelstorage.NewBackendKernel(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	backend.updates.Store(0)

	record := cache.ProviderRequestRecord{
		ID: "request-1", PromptScopeID: "thread-1", RunID: "run-1", ThreadID: "thread-1", TurnID: "turn-1",
		Step: 1, LogicalRequestID: "logical-1", Attempt: 1, Provider: "test", Model: "model", CreatedAt: time.Now(),
	}
	if err := kernel.AppendProviderRequest(ctx, record); err != nil {
		t.Fatal(err)
	}
	if writes := backend.updates.Load(); writes != 0 {
		t.Fatalf("prompt observation performed %d backend updates, want memory-only hot path", writes)
	}
	requests, err := kernel.ProviderRequests(ctx, record.PromptScopeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].ID != record.ID {
		t.Fatalf("in-memory provider requests = %#v", requests)
	}
}

func TestTurnAdmissionStaysInMemoryUntilSemanticCheckpoint(t *testing.T) {
	ctx := context.Background()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	backend := &countingBackend{inner: inner}
	defer backend.Close()
	kernel, err := kernelstorage.NewBackendKernel(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-admission"}); err != nil {
		t.Fatal(err)
	}
	backend.updates.Store(0)

	admission, err := kernel.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread-admission", TurnID: "turn-admission", RunID: "run-admission", OwnerID: "owner-admission",
		Input: session.Message{Role: session.User, Content: "hello"}, RequestFingerprint: "admission-fingerprint",
	})
	if err != nil {
		t.Fatal(err)
	}
	if admission.UserMessage.ID == "" || admission.Lease.ThreadID != "thread-admission" {
		t.Fatalf("admission = %#v", admission)
	}
	if writes := backend.updates.Load(); writes != 0 {
		t.Fatalf("turn admission performed %d backend updates, want memory-only hot path", writes)
	}
	read, found, err := kernel.ReadTurnAdmission(ctx, "thread-admission", "turn-admission", "run-admission")
	if err != nil || !found || read.UserMessage.ID != admission.UserMessage.ID {
		t.Fatalf("read admission = %#v found=%v err=%v", read, found, err)
	}
}
