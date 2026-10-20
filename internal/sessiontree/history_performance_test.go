package sessiontree

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

// newFixedHistorySQLiteRepo exercises the production backend and native exact
// index reader. The benchmark rolls back only after the complete write callback,
// keeping the measured history identical across iterations.
func newFixedHistorySQLiteRepo(tb testing.TB, count int) *BackendRepo {
	tb.Helper()
	backend, err := storagebridge.Open(tb.Context(), storagebridge.Source(publicstorage.SQLite(tb.TempDir()+"/history.sqlite")))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = backend.Close() })
	repo, err := NewBackendRepo(tb.Context(), backend, time.Now)
	if err != nil {
		tb.Fatal(err)
	}
	if err := repo.UpdateDomain(tb.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		if _, err := memory.CreateThread(tb.Context(), ThreadMeta{ID: "history"}); err != nil {
			return err
		}
		if _, err := memory.Append(tb.Context(), Entry{ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnStarted}, AppendOptions{ID: "started"}); err != nil {
			return err
		}
		for i := range count {
			if _, err := memory.Append(tb.Context(), Entry{ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "fixed historical segment", ContentSourceID: fmt.Sprintf("source-%d", i)}}, AppendOptions{ID: fmt.Sprintf("entry-%d", i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return repo
}

func BenchmarkSQLiteFixedHistoryAppend(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			rollback := errors.New("fixed-history measurement rollback")
			repo.backend = &domainCommitBoundaryBackend{Backend: repo.backend, afterCallback: func() error { return rollback }}
			entry := Entry{ID: "new-segment", ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "new segment", ContentSourceID: "new-source"}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := repo.Append(b.Context(), entry, AppendOptions{}); !errors.Is(err, rollback) {
					b.Fatalf("append: %v", err)
				}
			}
		})
	}
}

func BenchmarkSQLiteFixedHistoryProviderCommit(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			rollback := errors.New("fixed-history provider rollback")
			repo.backend = &domainCommitBoundaryBackend{Backend: repo.backend, afterCallback: func() error { return rollback }}
			request := ProviderContextCommit{ThreadID: "history", TurnID: "turn", RunID: "run", Entries: []Entry{
				{ID: "new-segment", ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "new segment", ContentSourceID: "new-source"}},
			}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := repo.CommitProviderContext(b.Context(), request); !errors.Is(err, rollback) {
					b.Fatalf("provider commit: %v", err)
				}
			}
		})
	}
}

// Each sample reads the same small canonical field through the real backend;
// unrelated history must affect neither body I/O nor allocated result size.
func BenchmarkSQLiteFixedHistoryBodyRead(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				part, err := repo.ReadCanonicalBody(b.Context(), "history", "entry-0", "content", 0, canonicalBodyBlockBytes)
				if err != nil || part.Text != "fixed historical segment" {
					b.Fatalf("read: %v", err)
				}
			}
		})
	}
}
