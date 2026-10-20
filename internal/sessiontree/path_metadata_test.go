package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestPathMetadataPreservesCanonicalLocatorsWithoutBodyReads(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Repeat("中文🌸\r\n", 10000)
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			return err
		}
		for i := range 300 {
			if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: source}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for restart := range 2 {
		complete, err := repo.PathPage(t.Context(), "thread", "", "200", 150)
		if err != nil {
			t.Fatal(err)
		}
		ref := repo.domainMemory.entries["thread"][0].bodyFields["content"]
		releaseCachedBody(ref)
		repo.backend = failingBodyReadBackend{Backend: backend, failure: errors.New("metadata read a body")}
		window, err := repo.PathMetadataWindow(t.Context(), PathWindowOptions{ThreadID: "thread", AnchorID: "150", Direction: PathWindowAround, Limit: 150})
		if err != nil || len(window.Entries) != 150 || window.FirstEntryID != "75" || window.LastEntryID != "224" {
			t.Fatalf("window hydrated a body or changed boundaries: %v", err)
		}
		metadata, err := repo.PathMetadataPage(t.Context(), "thread", "", "200", 150)
		if err != nil {
			t.Fatal(err)
		}
		if len(metadata.Entries) != 150 || metadata.HasMore != complete.HasMore || metadata.NextEntryID != complete.NextEntryID || metadata.NewestOrdinal != complete.NewestOrdinal {
			t.Fatal("different page boundary")
		}
		for i, item := range metadata.Entries {
			want := complete.Entries[i]
			want.Message.Content = ""
			want.Raw = ""
			if !reflect.DeepEqual(item.Entry, want) || len(item.Bodies) != 1 || item.Bodies[0] != (CanonicalField{Field: "content", SHA256: stableHash(source), Bytes: len(source)}) {
				t.Fatalf("metadata differs at %d", i)
			}
		}
		metadata.Entries[0].Entry.Message.References = append(metadata.Entries[0].Entry.Message.References, session.MessageReference{Kind: "text", Text: "detached"})
		if len(repo.domainMemory.entries["thread"][199].Message.References) != 0 {
			t.Fatal("metadata mutated canonical owner")
		}
		if _, err := repo.PathMetadataPage(t.Context(), "thread", "100", "200", 150); !errors.Is(err, ErrEntryNotFound) {
			t.Fatalf("inactive cursor accepted: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := repo.PathMetadataPage(ctx, "thread", "", "", 150); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
		repo.backend = backend
		if restart == 0 {
			repo, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func BenchmarkSQLiteFixedHistoryMetadataPage(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			before := fmt.Sprintf("entry-%d", count/2)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				page, err := repo.PathMetadataPage(b.Context(), "history", "", before, 150)
				if err != nil || len(page.Entries) != 150 {
					b.Fatalf("metadata page: %v", err)
				}
			}
		})
	}
}
