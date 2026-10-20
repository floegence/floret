package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestMetadataWindowMatchesCanonicalBranchIntervals(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(51, 97))
	journal := []Entry{}
	for i := range 256 {
		parent := ""
		if i > 0 {
			parent = journal[random.IntN(i)].ID
		}
		journal = append(journal, appendPathTestEntry(t, repo, fmt.Sprint(i), parent))
	}
	for _, leaf := range journal {
		if err := repo.MoveLeaf(t.Context(), "thread", leaf.ID); err != nil {
			t.Fatal(err)
		}
		path, err := repo.Path(t.Context(), "thread", leaf.ID)
		if err != nil {
			t.Fatal(err)
		}
		for anchorIndex, anchor := range path {
			for _, direction := range []PathWindowDirection{PathWindowBefore, PathWindowAfter, PathWindowAround} {
				for _, limit := range []int{1, 4, 150} {
					page, err := repo.PathMetadataWindow(t.Context(), PathWindowOptions{ThreadID: "thread", LeafID: leaf.ID, AnchorID: anchor.ID, Direction: direction, Limit: limit})
					if err != nil {
						t.Fatal(err)
					}
					var first, last int
					switch direction {
					case PathWindowBefore:
						last = anchorIndex
						first = max(0, last-limit)
					case PathWindowAfter:
						first = anchorIndex + 1
						last = min(len(path), first+limit)
					case PathWindowAround:
						first = max(0, anchorIndex-limit/2)
						last = min(len(path), first+limit)
						first = max(0, last-limit)
					}
					want := path[first:last]
					if len(page.Entries) != len(want) || page.HasBefore != (first > 0) || page.HasAfter != (last < len(path)) || page.ThroughOrdinal != int64(len(path)) {
						t.Fatalf("bounds direction=%s anchor=%d limit=%d page=%+v", direction, anchorIndex, limit, page)
					}
					for i, metadata := range page.Entries {
						entry := want[i]
						entry.Message.Content = ""
						entry.Raw = ""
						if !reflect.DeepEqual(entry, metadata.Entry) {
							t.Fatalf("metadata differs: %+v %+v", entry, metadata.Entry)
						}
					}
					if len(want) > 0 && (page.FirstEntryID != want[0].ID || page.LastEntryID != want[len(want)-1].ID || page.FirstOrdinal != int64(first+1) || page.LastOrdinal != int64(last)) {
						t.Fatal("unstable source boundary")
					}
				}
			}
		}
	}
}

func TestMetadataWindowFreezesAppendAndInvalidatesChangedPath(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			return err
		}
		for i := range 40 {
			appendPathTestEntry(t, memory, fmt.Sprint(i), "")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	options := PathWindowOptions{ThreadID: "thread", LeafID: "39", AnchorID: "20", Direction: PathWindowAround, Limit: 15}
	first, err := repo.PathMetadataWindow(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "appended"}}, AppendOptions{ID: "40"}); err != nil {
		t.Fatal(err)
	}
	for restart := range 2 {
		if restart > 0 {
			repo, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
		page, err := repo.PathMetadataWindow(t.Context(), options)
		// Go monotonic-clock metadata is deliberately not durable. Compare exact
		// source/wire facts, including wall timestamps, rather than time internals.
		before, _ := json.Marshal(first)
		after, _ := json.Marshal(page)
		if err != nil || string(before) != string(after) {
			t.Fatalf("append/restart=%d changed canonical window: %v", restart, err)
		}

	}
	if err := repo.MoveLeaf(t.Context(), "thread", "10"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PathMetadataWindow(t.Context(), options); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive leaf accepted: %v", err)
	}
	options.LeafID = ""
	options.AnchorID = "20"
	if _, err := repo.PathMetadataWindow(t.Context(), options); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive anchor accepted: %v", err)
	}
	options.AnchorID = "5"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.PathMetadataWindow(ctx, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
	for _, mutate := range []func(*PathWindowOptions){func(o *PathWindowOptions) { o.Limit = 0 }, func(o *PathWindowOptions) { o.Limit = 257 }, func(o *PathWindowOptions) { o.Direction = "wrong" }, func(o *PathWindowOptions) { o.AnchorID = "" }} {
		invalid := options
		mutate(&invalid)
		if _, err := repo.PathMetadataWindow(t.Context(), invalid); !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatalf("invalid window accepted: %+v %v", invalid, err)
		}
	}
}

func BenchmarkSQLiteFixedHistoryMetadataWindow(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			options := PathWindowOptions{ThreadID: "history", AnchorID: fmt.Sprintf("entry-%d", count/2), Direction: PathWindowAround, Limit: 150}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				page, err := repo.PathMetadataWindow(b.Context(), options)
				if err != nil || len(page.Entries) != 150 {
					b.Fatalf("window: %v", err)
				}
			}
		})
	}
}
