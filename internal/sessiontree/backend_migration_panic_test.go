package sessiontree

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type preparationPanicBackend struct {
	spi.Backend
	read  bool
	write bool
}

func (backend *preparationPanicBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error {
		if backend.read {
			panic("injected preparation read panic")
		}
		return read(tx)
	})
}

func (backend *preparationPanicBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		if backend.write {
			panic("injected preparation commit panic")
		}
		return nil
	})
}

func TestV32PreparationPanicReleasesOwnersAndAllowsRetry(t *testing.T) {
	for _, mode := range []string{"background_stage", "foreground_read", "foreground_stage", "index_reset"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.Append(t.Context(), Entry{ID: "entry", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			base, repo := openV31ReviewFixture(t, memory)
			if mode == "index_reset" {
				if err := repo.PrepareThread(t.Context(), "history"); err != nil {
					t.Fatal(err)
				}
			}
			backend := &preparationPanicBackend{Backend: base, read: mode == "foreground_read", write: mode != "foreground_read"}
			repo.backend = backend
			func() {
				defer func() {
					if recover() == nil {
						t.Error("preparation did not propagate the injected panic")
					}
				}()
				switch mode {
				case "background_stage":
					_, _ = repo.MigrateNextThread(t.Context())
				case "index_reset":
					_ = repo.PrepareQueryThread(t.Context(), "history")
				default:
					_ = repo.PrepareThread(t.Context(), "history")
				}
			}()
			if !repo.mu.TryLock() {
				t.Fatal("panic leaked the repository lock")
			}
			repo.mu.Unlock()
			backend.read, backend.write = false, false
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := repo.PrepareThread(ctx, "history"); err != nil {
				t.Fatalf("retry migration: %v", err)
			}
			if err := repo.PrepareQueryThread(ctx, "history"); err != nil {
				t.Fatalf("retry index: %v", err)
			}
			entry, err := repo.Entry(ctx, "history", "entry")
			if err != nil || entry.Message.Content != "retained fact" {
				t.Fatalf("retained entry = %+v, %v", entry, err)
			}
			if ready, err := repo.ThreadPrepared(ctx, "history"); err != nil || !ready {
				t.Fatalf("retry readiness = %v, %v", ready, err)
			}
		})
	}
}
