package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/sessiontree"
)

type compactionAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Append(context.Context, sessiontree.Entry, sessiontree.AppendOptions) (sessiontree.Entry, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	Path(context.Context, string, string) ([]sessiontree.Entry, error)
	Entries(context.Context, string) ([]sessiontree.Entry, error)
	BeginCompaction(context.Context, sessiontree.BeginCompactionRequest) (sessiontree.BeginCompactionResult, error)
	TakeOverCompaction(context.Context, sessiontree.TakeOverCompactionRequest) (sessiontree.BeginCompactionResult, error)
	FinishCompaction(context.Context, sessiontree.FinishCompactionRequest) (sessiontree.FinishCompactionResult, error)
	RenewTurnLease(context.Context, sessiontree.TurnLease) (sessiontree.TurnLease, error)
}

type mutableAuthorityClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableAuthorityClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableAuthorityClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

func TestSQLiteCompactionAuthorityMatchesMemory(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(filepath.Join(t.TempDir(), "compaction.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for backend, repo := range map[string]compactionAuthorityTestRepo{"memory": memory, "sqlite": store} {
		t.Run(backend, func(t *testing.T) {
			request := seedCompactionAuthority(t, repo, fixed)
			begun, err := repo.BeginCompaction(context.Background(), request)
			if err != nil || !begun.Owner || begun.Replayed || begun.Operation.State != sessiontree.CompactionOperationPrepared || begun.Operation.Lease.Validate() != nil {
				t.Fatalf("begin = %#v err=%v", begun, err)
			}
			renewed, err := repo.RenewTurnLease(context.Background(), begun.Operation.Lease)
			if err != nil || renewed.Heartbeat != begun.Operation.Lease.Heartbeat+1 {
				t.Fatalf("renewed = %#v err=%v", renewed, err)
			}
			begun.Operation.Lease = renewed
			replayedBegin, err := repo.BeginCompaction(context.Background(), request)
			if err != nil || !replayedBegin.Replayed || replayedBegin.Owner || replayedBegin.TakeoverEligible || !sessiontree.SameTurnLease(replayedBegin.Operation.Lease, renewed) {
				t.Fatalf("begin replay = %#v err=%v", replayedBegin, err)
			}
			entry, err := sessiontree.CompactionEntry("thread", "", compaction.Result{
				CompactionID: "compaction-1", OperationID: "operation-1", RequestID: request.RequestID, Source: request.Source,
				CompactionGeneration: 1, CompactionWindowID: "window-1", SummarySchemaVersion: compaction.SummarySchemaVersion,
				Summary: "summary", Trigger: compaction.TriggerManual, Reason: compaction.ReasonManual, Phase: compaction.PhaseInstall,
			})
			if err != nil {
				t.Fatal(err)
			}
			finished, err := repo.FinishCompaction(context.Background(), sessiontree.FinishCompactionRequest{
				Lease: renewed, RequestID: request.RequestID, RequestFingerprint: request.RequestFingerprint,
				OutcomeFingerprint: "outcome-1", Result: &entry, Now: fixed,
			})
			if err != nil || finished.Replayed || finished.Operation.State != sessiontree.CompactionOperationCompleted || finished.Entry == nil {
				t.Fatalf("finish = %#v err=%v", finished, err)
			}
			replayedFinish, err := repo.FinishCompaction(context.Background(), sessiontree.FinishCompactionRequest{
				Lease: renewed, RequestID: request.RequestID, RequestFingerprint: request.RequestFingerprint,
				OutcomeFingerprint: "outcome-1", Result: &entry, Now: fixed,
			})
			if err != nil || !replayedFinish.Replayed || replayedFinish.Entry == nil || replayedFinish.Entry.ID != finished.Entry.ID {
				t.Fatalf("finish replay = %#v err=%v", replayedFinish, err)
			}
			entries, err := repo.Entries(context.Background(), "thread")
			if err != nil || len(entries) != 2 {
				t.Fatalf("entries = %#v err=%v", entries, err)
			}
		})
	}
}

func TestSQLiteCompactionTakeoverFencesOldGeneration(t *testing.T) {
	policy := sessiontree.LeasePolicy{TTL: 3 * time.Second, RenewInterval: time.Second, ClockSkewAllowance: time.Second}
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			clock := &mutableAuthorityClock{now: time.Now().UTC()}
			var repo compactionAuthorityTestRepo
			if backend == "memory" {
				memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, clock.Now)
				if err != nil {
					t.Fatal(err)
				}
				repo = memory
			} else {
				store, err := Open(filepath.Join(t.TempDir(), "takeover.db"), WithLeasePolicy(policy), WithAuthorityClock(clock.Now))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				repo = store
			}
			request := seedCompactionAuthority(t, repo, clock.Now())
			begun, err := repo.BeginCompaction(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(5 * time.Second)
			inspect, err := repo.BeginCompaction(context.Background(), request)
			if err != nil || !inspect.TakeoverEligible || inspect.Owner {
				t.Fatalf("eligible replay = %#v err=%v", inspect, err)
			}
			taken, err := repo.TakeOverCompaction(context.Background(), sessiontree.TakeOverCompactionRequest{
				ThreadID: "thread", RequestID: request.RequestID, RequestFingerprint: request.RequestFingerprint,
				ExpectedLease: inspect.Operation.Lease, OwnerID: "owner-2", Now: clock.Now(),
			})
			if err != nil || !taken.Owner || taken.Operation.Lease.Generation <= begun.Operation.Lease.Generation {
				t.Fatalf("takeover = %#v err=%v", taken, err)
			}
			if _, err := repo.FinishCompaction(context.Background(), sessiontree.FinishCompactionRequest{
				Lease: begun.Operation.Lease, RequestID: request.RequestID, RequestFingerprint: request.RequestFingerprint,
				OutcomeFingerprint: "old-outcome", ErrorCode: "old", ErrorMessage: "old owner", Now: clock.Now(),
			}); !errors.Is(err, sessiontree.ErrStaleAuthority) {
				t.Fatalf("old finish err=%v, want ErrStaleAuthority", err)
			}
			finished, err := repo.FinishCompaction(context.Background(), sessiontree.FinishCompactionRequest{
				Lease: taken.Operation.Lease, RequestID: request.RequestID, RequestFingerprint: request.RequestFingerprint,
				OutcomeFingerprint: "new-outcome", ErrorCode: "provider_failed", ErrorMessage: "failed", Now: clock.Now(),
			})
			if err != nil || finished.Operation.State != sessiontree.CompactionOperationFailed {
				t.Fatalf("takeover finish = %#v err=%v", finished, err)
			}
		})
	}
}

func seedCompactionAuthority(t *testing.T, repo compactionAuthorityTestRepo, now time.Time) sessiontree.BeginCompactionRequest {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "history"},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	return sessiontree.BeginCompactionRequest{
		ThreadID: "thread", RequestID: "request-1", RequestFingerprint: "request-fingerprint",
		Source: "test", SourceLeafID: meta.LeafID, ActivePathHash: sessiontree.ActivePathHash(path),
		SummarySchemaVersion: compaction.SummarySchemaVersion, PromptIdentity: "prompt-identity",
		RequestPayloadHash: "payload-hash", OwnerID: "owner-1", Now: now,
	}
}
