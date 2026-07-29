package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v2/internal/sessiontree"
)

func TestPublicThreadReadModelPathsReturnValidDTOsAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(*testing.T, func(*Store))
	}{
		{
			name: "memory",
			run: func(t *testing.T, verify func(*Store)) {
				verify(NewMemoryStore())
			},
		},
		{
			name: "sqlite_reopen",
			run: func(t *testing.T, verify func(*Store)) {
				path := filepath.Join(t.TempDir(), "floret.db")
				store, err := openSQLiteStoreForTest(path)
				if err != nil {
					t.Fatal(err)
				}
				verify(store)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := openSQLiteStoreForTest(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				maintenance, err := newTestMaintenanceHost(t, reopened)
				if err != nil {
					t.Fatal(err)
				}
				for _, threadID := range []ThreadID{"source", "fork"} {
					snapshot, err := maintenance.ReadThread(ctx, threadID)
					if err != nil {
						t.Fatalf("reopened ReadThread(%q): %v", threadID, err)
					}
					assertValidThreadSnapshot(t, snapshot)
					overview, err := maintenance.ReadThreadOverview(ctx, threadID)
					if err != nil {
						t.Fatalf("reopened ReadThreadOverview(%q): %v", threadID, err)
					}
					assertValidThreadSnapshot(t, overview.Thread)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, func(store *Store) {
				maintenance, err := newTestMaintenanceHost(t, store)
				if err != nil {
					t.Fatal(err)
				}
				created, err := maintenance.CreateThread(ctx, testCreateThreadRequest("source"))
				if err != nil {
					t.Fatalf("CreateThread: %v", err)
				}
				assertValidThreadSummary(t, created)
				replayed, err := maintenance.CreateThread(ctx, testCreateThreadRequest("source"))
				if err != nil {
					t.Fatalf("replayed CreateThread: %v", err)
				}
				assertValidThreadSummary(t, replayed)

				read, err := maintenance.ReadThread(ctx, "source")
				if err != nil {
					t.Fatalf("ReadThread: %v", err)
				}
				assertValidThreadSnapshot(t, read)
				titled, err := maintenance.SetThreadTitle(ctx, SetThreadTitleRequest{ThreadID: "source", Title: "Canonical title"})
				if err != nil {
					t.Fatalf("SetThreadTitle: %v", err)
				}
				assertValidThreadSnapshot(t, titled)
				forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{
					OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork",
				})
				if err != nil {
					t.Fatalf("ForkThread: %v", err)
				}
				assertValidThreadSummary(t, forked.Thread)
				overview, err := maintenance.ReadThreadOverview(ctx, "source")
				if err != nil {
					t.Fatalf("ReadThreadOverview: %v", err)
				}
				assertValidThreadSnapshot(t, overview.Thread)
			})
		})
	}
}

func TestPublicThreadReadModelPathsRejectCorruptProjection(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	repo := &corruptThreadProjectionRepo{MemoryRepo: store.repo.(*sessiontree.MemoryRepo)}
	store.repo = repo
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.CreateThread(ctx, testCreateThreadRequest("thread")); err != nil {
		t.Fatal(err)
	}
	repo.corrupt = true
	if _, err := maintenance.ReadThread(ctx, "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ReadThread corrupt projection err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := maintenance.ReadThreadOverview(ctx, "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ReadThreadOverview corrupt projection err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestPublicThreadReadModelCheckedConvertersRejectCorruption(t *testing.T) {
	snapshot := ThreadSnapshot{ID: "thread", Phase: ThreadPhaseIdle, Status: ThreadStatusIdle, CanAppendMessage: true}
	if _, err := validateThreadSnapshotResult(snapshot); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("snapshot converter err=%v, want ErrAuthorityCorrupt", err)
	}
	summary := ThreadSummary{ID: "thread", Phase: ThreadPhaseIdle, Status: ThreadStatusIdle, CanAppendMessage: true}
	if _, err := validateThreadSummaryResult(summary); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("summary converter err=%v, want ErrAuthorityCorrupt", err)
	}
}

type corruptThreadProjectionRepo struct {
	*sessiontree.MemoryRepo
	corrupt bool
}

func (r *corruptThreadProjectionRepo) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := r.MemoryRepo.Thread(ctx, threadID)
	if err == nil && r.corrupt {
		meta.TitleStatus = sessiontree.ThreadTitleReady
	}
	return meta, err
}

func assertValidThreadSnapshot(t *testing.T, snapshot ThreadSnapshot) {
	t.Helper()
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("invalid public thread snapshot %#v: %v", snapshot, err)
	}
}

func assertValidThreadSummary(t *testing.T, summary ThreadSummary) {
	t.Helper()
	if err := summary.Validate(); err != nil {
		t.Fatalf("invalid public thread summary %#v: %v", summary, err)
	}
}
