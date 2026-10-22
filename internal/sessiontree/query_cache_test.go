package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestQueryCacheOverlayPublishesOnlyAfterBackendCommit(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	queryFixture(t, repo)
	previous := repo.queryThreads["a"]
	before := make(map[string]*queryRecord, len(previous.records))
	for key, record := range previous.records {
		before[key] = record
	}
	rollback := errors.New("reject backend commit")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	entry := Entry{ThreadID: "a", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "transactional tail"}}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "overlay-tail"}); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if repo.queryThreads["a"] != previous || !reflect.DeepEqual(previous.records, before) {
		t.Fatal("failed commit modified accepted projection")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "overlay-tail"}); err != nil {
		t.Fatal(err)
	}
	current := repo.queryThreads["a"]
	if current.complete || len(current.records) != 0 {
		t.Fatal("commit retained historical projection records")
	}
	assertQueryProjectionMatches(t, repo, "a")
}

func TestQueryCacheAppendAndReplacementMatchFullProjection(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	queryFixture(t, repo)
	for i := range 4 {
		entry := Entry{ThreadID: "a", Type: EntryAssistantMessage, TurnID: "next", RunID: "newrun", Message: session.Message{Role: session.Assistant, Content: fmt.Sprint(i), ContentSourceID: "shared-output"}}
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprintf("new-%d", i)}); err != nil {
			t.Fatal(err)
		}
		assertQueryProjectionMatches(t, repo, "a")
	}
	meta, err := repo.Thread(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	meta.LeafID = "a1"
	if err := repo.UpdateThread(t.Context(), meta); err != nil {
		t.Fatal(err)
	}
	assertQueryProjectionMatches(t, repo, "a")
}

func TestQueryCacheFixedTailDoesNotCopyHistoricalRecords(t *testing.T) {
	var allocationCounts []float64
	for _, count := range []int{1096, 109600} {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		for i := range count {
			appendPathTestEntry(t, repo, fmt.Sprint(i), "")
		}
		previous, err := repo.projectQueryThread(t.Context(), "thread")
		if err != nil {
			t.Fatal(err)
		}
		appendPathTestEntry(t, repo, "tail", "")
		allocationCounts = append(allocationCounts, testing.AllocsPerRun(10, func() {
			state, changed, err := repo.extendQueryThread(context.Background(), "thread", previous, func(key string) (*queryRecord, error) { return previous.records[key], nil })
			if err != nil || len(state.records) != 2 || len(changed) != 2 {
				t.Fatalf("unbounded overlay: %v", err)
			}
		}))
	}
	if allocationCounts[1] > allocationCounts[0]+2 {
		t.Fatalf("historical map copied: %v", allocationCounts)
	}
}

func BenchmarkQueryCacheFixedTail(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(b.Context(), ThreadMeta{ID: "thread"}); err != nil {
				b.Fatal(err)
			}
			for i := range count {
				appendPathTestEntry(b, repo, fmt.Sprint(i), "")
			}
			previous, err := repo.projectQueryThread(b.Context(), "thread")
			if err != nil {
				b.Fatal(err)
			}
			appendPathTestEntry(b, repo, "tail", "")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, err := repo.extendQueryThread(b.Context(), "thread", previous, func(key string) (*queryRecord, error) { return previous.records[key], nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func assertQueryProjectionMatches(t *testing.T, repo *BackendRepo, id string) {
	t.Helper()
	want, err := repo.domainMemory.projectQueryThread(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	current := repo.queryThreads[id]
	if current == nil || len(current.records) != 0 || current.complete {
		t.Fatal("commit retained a historical record cache")
	}
	if current.previous != nil && (len(current.previous.parts) != 0 || current.previous.firstSource != "" || current.previous.lastSource != "" || len(current.previous.activities) != 0) {
		t.Fatal("continuation retained a historical body")
	}
	if err := repo.backend.View(t.Context(), func(tx spi.ReadTx) error {
		documents, err := queryMetadataReader(tx).listDocuments(queryMetadataSelection{ThreadIDs: []string{id}})
		if err != nil {
			return err
		}
		got := map[string]*queryRecord{}
		for _, document := range documents {
			record, err := repo.domainMemory.materializeQueryDocument(document)
			if err != nil {
				return err
			}
			got[record.key] = record
		}
		if !reflect.DeepEqual(got, want.records) {
			t.Fatal("incremental index differs from complete canonical projection")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContinuationSurvivesManyThreadsRestartAndLargeBodies(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/continuation.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		id := fmt.Sprintf("thread-%d", i)
		if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{ID: "user"}); err != nil {
			t.Fatal(err)
		}
	}
	// This exceeded the old complete-projection cache's per-thread limit.
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread-0", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("a", 17<<20), ContentSourceID: "large"}}, AppendOptions{ID: "large"}); err != nil {
		t.Fatal(err)
	}
	if len(repo.queryThreads) != 32 {
		t.Fatalf("continuations=%d", len(repo.queryThreads))
	}
	for id, state := range repo.queryThreads {
		if len(state.records) != 0 || state.complete || state.previous != nil && len(state.previous.parts) != 0 {
			t.Fatalf("%s retained history", id)
		}
	}
	reopened, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.queryThreads) != 32 {
		t.Fatalf("restart continuations=%d", len(reopened.queryThreads))
	}
	for i := range 32 {
		id := fmt.Sprintf("thread-%d", i)
		if _, err := reopened.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "tail after restart"}}, AppendOptions{ID: "tail"}); err != nil {
			t.Fatal(err)
		}
		assertQueryProjectionMatches(t, reopened, id)
	}
}

func TestQueryContinuationMarkerRollbackDoesNotAdvanceLifecycle(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	queryFixture(t, repo)
	previous := repo.queryThreads["a"]
	started := maps.Clone(previous.lifecycle.started)
	terminal := maps.Clone(previous.lifecycle.terminal)
	rollback := errors.New("reject marker commit")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	entry := Entry{ThreadID: "a", Type: EntryTurnMarker, TurnID: "rollback-turn", RunID: "rollback-run", TurnStatus: TurnStarted, Metadata: map[string]string{}}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "rollback-marker"}); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if repo.queryThreads["a"] != previous || !reflect.DeepEqual(started, previous.lifecycle.started) || !reflect.DeepEqual(terminal, previous.lifecycle.terminal) {
		t.Fatal("failed marker mutated accepted lifecycle")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "rollback-marker"}); err != nil {
		t.Fatal(err)
	}
	assertQueryProjectionMatches(t, repo, "a")
}
