package sessiontree

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestTurnRequestLocatorsMatchJournalAcrossRollbackAndRebuild(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(72, 17))
	keys := []string{"one", "two", "three", "missing", ""}
	assert := func() {
		t.Helper()
		for _, key := range keys {
			var want Entry
			for _, entry := range repo.entries["thread"] {
				if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.RequestKey == key {
					want = entry
				}
			}
			got, found, err := repo.FindTurnRequest(t.Context(), "thread", key)
			if err != nil || found != (want.ID != "") || !reflect.DeepEqual(got, want) {
				t.Fatalf("key=%q got=%s found=%v err=%v want=%s", key, got.ID, found, err, want.ID)
			}
		}
	}
	for i := range 500 {
		checkpoint := repo.checkpointJournalAppendLocked("thread")
		for j := range 1 + random.IntN(3) {
			status := TurnStarted
			if random.IntN(3) == 0 {
				status = TurnCompleted
			}
			kind := EntryTurnMarker
			if random.IntN(3) == 0 {
				kind = EntryCustom
			}
			entry := Entry{ID: fmt.Sprintf("%d-%d", i, j), ThreadID: "thread", TurnID: fmt.Sprint(i), RunID: "run", Type: kind, TurnStatus: status, RequestKey: keys[random.IntN(4)], Metadata: map[string]string{"original": "yes"}}
			entry.Raw = rawForEntry(entry)
			entry.RawHash = stableHash(entry.Raw)
			repo.appendIndexedEntriesLocked("thread", entry)
			assert()
		}
		if random.IntN(3) == 0 {
			checkpoint.restore(repo)
		}
		if i%17 == 0 {
			repo.rebuildTurnMarkersLocked("thread")
		}
		assert()
	}
}

func TestTurnRequestLocatorsRestoreAfterRejectedCommitAndRestart(t *testing.T) {
	backend := &domainCommitBoundaryBackend{Backend: newMigrationTestBackend()}
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendMarker := func(memory *MemoryRepo, id string) error {
		_, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryTurnMarker, TurnID: id, RunID: "run", TurnStatus: TurnStarted, RequestKey: "same"}, AppendOptions{ID: id})
		return err
	}
	if err = repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error { return appendMarker(memory, "original") }); err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("request locator rollback")
	backend.afterCallback = func() error { return rejected }
	if err = repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error { return appendMarker(memory, "rejected") }); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	got, found, err := repo.FindTurnRequest(t.Context(), "thread", "same")
	if err != nil || !found || got.ID != "original" {
		t.Fatalf("lookup after rollback=%s %v %v", got.ID, found, err)
	}
	got.Metadata = map[string]string{"mutated": "yes"}
	backend.afterCallback = nil
	repo, err = NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err = repo.FindTurnRequest(t.Context(), "thread", "same")
	if err != nil || !found || got.ID != "original" || got.Metadata["mutated"] != "" {
		t.Fatalf("lookup after restart=%+v %v %v", got, found, err)
	}
}

func TestTurnRequestLocatorsRejectCorruptAuthority(t *testing.T) {
	for _, corrupt := range []struct {
		name  string
		apply func(*MemoryRepo)
	}{
		{"negative ordinal", func(repo *MemoryRepo) { repo.turnRequests["thread"]["key"] = []int{-1} }},
		{"past end ordinal", func(repo *MemoryRepo) { repo.turnRequests["thread"]["key"] = []int{1} }},
		{"wrong request key", func(repo *MemoryRepo) { repo.entries["thread"][0].RequestKey = "other" }},
		{"wrong entry locator", func(repo *MemoryRepo) { repo.entryOrdinals["thread"]["started"] = 1 }},
		{"invalid raw hash", func(repo *MemoryRepo) { repo.entries["thread"][0].RawHash = "invalid" }},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnStarted, RequestKey: "key"}, AppendOptions{ID: "started"}); err != nil {
				t.Fatal(err)
			}
			corrupt.apply(repo)
			if _, found, err := repo.FindTurnRequest(t.Context(), "thread", "key"); !found || !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("found=%v err=%v", found, err)
			}
		})
	}
}

func BenchmarkSQLiteFixedHistoryFindTurnRequest(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, found, err := repo.FindTurnRequest(b.Context(), "history", "missing"); err != nil || found {
					b.Fatalf("find missing: %v %v", found, err)
				}
			}
		})
	}
}
