package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func newSizedMutationRepo(tb testing.TB, unrelatedThreads int) *BackendRepo {
	tb.Helper()
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(tb.TempDir()+"/domain.sqlite")))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = backend.Close() })
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
	if err != nil {
		tb.Fatal(err)
	}
	if err := repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error {
		for index := 0; index <= unrelatedThreads; index++ {
			threadID, count, content := fmt.Sprintf("history-%03d", index), 67, strings.Repeat("unrelated ", 32)
			if index == 0 {
				threadID, count, content = "active", 395, strings.Repeat("context ", 490)
			}
			if _, err := memory.CreateThread(ctx, ThreadMeta{ID: threadID, CreatedAt: now}); err != nil {
				return err
			}
			for ordinal := 0; ordinal < count; ordinal++ {
				if _, err := memory.Append(ctx, Entry{ThreadID: threadID, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: content}}, AppendOptions{ID: fmt.Sprintf("%s-%04d", threadID, ordinal), Now: now}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return repo
}

func TestBackendDomainMutationAllocationsDoNotGrowWithUnrelatedHistory(t *testing.T) {
	measure := func(unrelated int) (float64, float64) {
		repo := newSizedMutationRepo(t, unrelated)
		meta, err := repo.Thread(t.Context(), "active")
		if err != nil {
			t.Fatal(err)
		}
		metadata := testing.AllocsPerRun(20, func() {
			meta.UpdatedAt = meta.UpdatedAt.Add(time.Nanosecond)
			if err := repo.UpdateThread(t.Context(), meta); err != nil {
				t.Fatal(err)
			}
		})
		rollback := errors.New("fixed-target rollback")
		repo.backend = &domainCommitBoundaryBackend{Backend: repo.backend, afterCallback: func() error { return rollback }}
		queue := testing.AllocsPerRun(20, func() {
			if _, err := repo.Append(t.Context(), Entry{ID: "fixed-queue", ThreadID: "active", Type: EntryQueueAdded, Payload: json.RawMessage(`{"id":"fixed"}`)}, AppendOptions{}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
		return metadata, queue
	}
	small, smallQueue := measure(0)
	large, largeQueue := measure(150)
	t.Logf("fixed 395-entry thread update: %.0f allocations with one thread, %.0f with 151 threads and 10,445 entries", small, large)
	if large > small+24 {
		t.Fatalf("unrelated history increased mutation allocations from %.0f to %.0f", small, large)
	}
	t.Logf("fixed 395-entry queue append: %.0f allocations with one thread, %.0f with 151 threads and 10,445 entries", smallQueue, largeQueue)
	if largeQueue > smallQueue+24 {
		t.Fatalf("unrelated history increased queue admission allocations from %.0f to %.0f", smallQueue, largeQueue)
	}
}

func BenchmarkBackendDomainMutationWithUnrelatedHistory(b *testing.B) {
	for _, unrelated := range []int{0, 150} {
		b.Run(fmt.Sprintf("threads-%d", unrelated+1), func(b *testing.B) {
			repo := newSizedMutationRepo(b, unrelated)
			meta, err := repo.Thread(b.Context(), "active")
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				meta.UpdatedAt = meta.UpdatedAt.Add(time.Nanosecond)
				if err := repo.UpdateThread(b.Context(), meta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBackendQueueAdmissionWithUnrelatedHistory(b *testing.B) {
	for _, unrelated := range []int{0, 150} {
		b.Run(fmt.Sprintf("threads-%d", unrelated+1), func(b *testing.B) {
			repo := newSizedMutationRepo(b, unrelated)
			// Roll back after preparing the complete write so every sample uses
			// exactly the same 395-entry target, independent of benchmark N.
			rollback := errors.New("benchmark rollback")
			repo.backend = &domainCommitBoundaryBackend{Backend: repo.backend, afterCallback: func() error { return rollback }}
			entry := Entry{ID: "queue_added:fixed", ThreadID: "active", Type: EntryQueueAdded, RequestKey: "fixed", RequestFingerprint: "fixed", Payload: json.RawMessage(`{"id":"fixed","request_key":"fixed","input":{"text":"Please inspect the image"}}`)}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := repo.Append(b.Context(), entry, AppendOptions{}); !errors.Is(err, rollback) {
					b.Fatal(err)
				}
			}
		})
	}
}

type domainCommitBoundaryBackend struct {
	spi.Backend
	afterCallback func() error
}

func (backend *domainCommitBoundaryBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := callback(tx); err != nil {
			return err
		}
		if backend.afterCallback != nil {
			return backend.afterCallback()
		}
		return nil
	})
}

func TestBackendDomainMutationRestoresMemoryBeforeCommitAndOnFailure(t *testing.T) {
	for _, mode := range []string{"commit_failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			underlying, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/rollback.sqlite")))
			if err != nil {
				t.Fatal(err)
			}
			defer underlying.Close()
			backend := &domainCommitBoundaryBackend{Backend: underlying}
			repo, err := NewBackendRepo(ctx, backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "active"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(ctx, Entry{ThreadID: "active", Type: EntryUserMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: "original"}}, AppendOptions{ID: "original"}); err != nil {
				t.Fatal(err)
			}
			before, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("backend commit failed")
			backend.afterCallback = func() error {
				current, err := repo.domainMemory.EncodeMemoryState()
				if err != nil {
					return err
				}
				if string(current) != string(before) {
					t.Fatal("staged memory became authoritative before backend commit")
				}
				if mode == "cancel" {
					cancel()
					return nil
				}
				return injected
			}
			var panicked bool
			func() {
				defer func() {
					if recover() != nil {
						panicked = true
					}
				}()
				err = repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error {
					if _, err := memory.Append(ctx, Entry{ThreadID: "active", Type: EntryUserMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.User, Content: "staged"}}, AppendOptions{ID: "staged"}); err != nil {
						return err
					}
					if _, err := memory.CompareAndSwapAgentTodoState(ctx, AgentTodoState{ThreadID: "active"}, 0); err != nil {
						return err
					}
					if mode == "panic" {
						panic("mutation panic")
					}
					return nil
				})
			}()
			if mode == "panic" && !panicked {
				t.Fatal("mutation panic was not observed")
			}
			if mode == "commit_failure" && !errors.Is(err, injected) {
				t.Fatalf("commit error=%v", err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error=%v", err)
			}
			after, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Fatal("failed transaction changed in-memory authority")
			}
			if _, err := repo.Entry(t.Context(), "active", "staged"); !errors.Is(err, ErrEntryNotFound) {
				t.Fatalf("failed entry retained an index: %v", err)
			}
			if got := repo.domainMemory.turnEntryOrdinals["active"]["turn"]; !reflect.DeepEqual(got, []int{0}) || repo.domainMemory.turnEntryCounts["active"]["turn"] != 1 {
				t.Fatalf("failed append changed canonical turn indexes: %v", got)
			}
			backend.afterCallback = nil
			reopened, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := reopened.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, persisted) {
				t.Fatal("failed transaction changed persisted authority")
			}
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "active", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retry"}}, AppendOptions{ID: "staged"}); err != nil {
				t.Fatalf("same-identity retry failed after rollback: %v", err)
			}
		})
	}
}
