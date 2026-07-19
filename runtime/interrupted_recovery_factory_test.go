package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
)

func TestInterruptedTurnRecoveryFactoryRefreshesOnlyItsExactTarget(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
			now := time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC)
			store := newInterruptedRecoveryTestStore(t, backend, policy, func() time.Time { return now })
			capabilities := mustTestCapabilities(t, store)
			createTestRoot(t, ctx, capabilities, "thread")
			createTestRoot(t, ctx, capabilities, "idle")

			if _, err := capabilities.interrupted.BindThread(ctx, "idle"); !errors.Is(err, ErrInterruptedTurnNotFound) {
				t.Fatalf("idle bind err=%v, want ErrInterruptedTurnNotFound", err)
			}

			admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn-1", "run-1", "owner-1")
			factory, err := capabilities.interrupted.BindThread(ctx, "thread")
			if err != nil {
				t.Fatalf("BindThread: %v", err)
			}
			factory.state.mu.Lock()
			boundLease := factory.state.latestLease
			factory.state.mu.Unlock()
			if boundLease.TurnID != "turn-1" || boundLease.OwnerID != "owner-1" || boundLease.Generation != admission.Lease.Generation {
				t.Fatalf("factory target=%#v, want exact admitted authority %#v", boundLease, admission.Lease)
			}
			events := &runtimeEventRecorder{}
			oldHost, err := factory.NewHost(ctx, events)
			if err != nil {
				t.Fatalf("NewHost initial: %v", err)
			}

			now = now.Add(time.Second)
			renewed, err := store.repo.(sessiontree.TurnLeaseRepo).RenewTurnLease(ctx, admission.Lease)
			if err != nil {
				t.Fatalf("RenewTurnLease: %v", err)
			}
			before := threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if _, err := oldHost.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrStaleAuthority) {
				t.Fatalf("old handle recovery err=%v, want ErrStaleAuthority", err)
			}
			assertRecoveryFailureHadNoSideEffects(t, ctx, store, "thread", before, events)

			refreshedHost, err := factory.NewHost(ctx, events)
			if err != nil {
				t.Fatalf("NewHost after renewal: %v", err)
			}
			if !sessiontree.SameTurnLease(refreshedHost.expectedLease, renewed) {
				t.Fatalf("refreshed proof=%#v, want %#v", refreshedHost.expectedLease, renewed)
			}
			before = threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if _, err := refreshedHost.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrThreadBusy) {
				t.Fatalf("fresh recovery err=%v, want ErrThreadBusy", err)
			}
			assertRecoveryFailureHadNoSideEffects(t, ctx, store, "thread", before, events)

			now = renewed.ExpiresAt.Add(policy.ClockSkewAllowance)
			before = threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if _, err := refreshedHost.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrThreadBusy) {
				t.Fatalf("expired-fenced recovery err=%v, want ErrThreadBusy", err)
			}
			assertRecoveryFailureHadNoSideEffects(t, ctx, store, "thread", before, events)

			now = now.Add(time.Nanosecond)
			result, err := refreshedHost.RecoverInterruptedTurn(ctx)
			if err != nil {
				t.Fatalf("takeover-eligible recovery: %v", err)
			}
			if result.ThreadID != "thread" || result.TurnID != "turn-1" || result.RunID != "run-1" || result.Status != TurnStatusCancelled {
				t.Fatalf("recovery result=%#v", result)
			}
			if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
				t.Fatalf("resolved factory err=%v, want ErrRecoveryTargetResolved", err)
			}

			future := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn-2", "run-2", "owner-2")
			if future.Lease.Generation <= admission.Lease.Generation {
				t.Fatalf("future generation=%d, want greater than target generation=%d", future.Lease.Generation, admission.Lease.Generation)
			}
			if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
				t.Fatalf("factory followed future turn: %v", err)
			}
			active, ok, err := store.repo.(sessiontree.TurnLeaseRepo).ActiveTurnLease(ctx, "thread")
			if err != nil || !ok || !sessiontree.SameTurnLease(active, future.Lease) {
				t.Fatalf("future lease changed: active=%#v ok=%v err=%v", active, ok, err)
			}
		})
	}
}

func TestInterruptedTurnRecoverySuccessorClassification(t *testing.T) {
	now := time.Date(2026, time.July, 20, 9, 0, 0, 0, time.UTC)
	target := sessiontree.TurnLease{
		ThreadID: "thread", Purpose: sessiontree.TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner",
		Generation: 3, Heartbeat: 2, AcquiredAt: now, RenewedAt: now.Add(time.Second), ExpiresAt: now.Add(31 * time.Second),
	}
	tests := []struct {
		name    string
		mutate  func(sessiontree.TurnLease) sessiontree.TurnLease
		wantErr error
	}{
		{name: "same proof", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { return in }},
		{name: "renewed", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease {
			in.Heartbeat++
			in.RenewedAt = in.RenewedAt.Add(time.Second)
			in.ExpiresAt = in.ExpiresAt.Add(time.Second)
			return in
		}},
		{name: "renewed at same authority time", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.Heartbeat++; return in }},
		{name: "generation rollback", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.Generation--; return in }, wantErr: ErrAuthorityCorrupt},
		{name: "heartbeat rollback", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.Heartbeat--; return in }, wantErr: ErrAuthorityCorrupt},
		{name: "same generation turn drift", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.TurnID = "other"; return in }, wantErr: ErrAuthorityCorrupt},
		{name: "same generation owner drift", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.OwnerID = "other"; return in }, wantErr: ErrAuthorityCorrupt},
		{name: "same generation purpose drift", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease {
			in.Purpose = sessiontree.TurnLeasePurposeMutation
			in.TurnID = ""
			in.MutationID = "mutation"
			in.MutationKind = sessiontree.CompactionMutationKind
			return in
		}, wantErr: ErrAuthorityCorrupt},
		{name: "malformed higher generation", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease { in.Generation++; in.OwnerID = ""; return in }, wantErr: ErrAuthorityCorrupt},
		{name: "valid higher generation", mutate: func(in sessiontree.TurnLease) sessiontree.TurnLease {
			in.Generation++
			in.Heartbeat = 0
			in.TurnID = "future"
			in.OwnerID = "future-owner"
			in.AcquiredAt = in.AcquiredAt.Add(time.Minute)
			in.RenewedAt = in.AcquiredAt
			in.ExpiresAt = in.AcquiredAt.Add(30 * time.Second)
			return in
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := runtimeHostError(sessiontree.ValidateInterruptedTurnLeaseSuccessor(target, testCase.mutate(target)))
			if !errors.Is(err, testCase.wantErr) || (testCase.wantErr == nil && err != nil) {
				t.Fatalf("error=%v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFactorySharesMonotonicStateAcrossCopies(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 9, 30, 0, 0, time.UTC)
	store := newInterruptedRecoveryTestStore(t, "memory", sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	capabilities := mustTestCapabilities(t, store)
	createTestRoot(t, ctx, capabilities, "thread")
	admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
	factory, err := capabilities.interrupted.BindThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	base := store.repo.(*sessiontree.MemoryRepo)
	override := &interruptedRecoveryInspectionRepo{MemoryRepo: base}
	store.repo = override
	renewed := admission.Lease
	for range 5 {
		renewed, err = store.repo.(sessiontree.TurnLeaseRepo).RenewTurnLease(ctx, renewed)
		if err != nil {
			t.Fatal(err)
		}
	}
	high, err := base.InspectThreadAuthority(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.NewHost(ctx, nil); err != nil {
		t.Fatalf("observe high heartbeat: %v", err)
	}

	copied := *factory
	rollback := cloneRecoveryAuthoritySnapshot(high)
	rollback.Lease.Heartbeat--
	override.set(rollback)
	before := threadEntriesForRecoveryTest(t, ctx, store, "thread")
	if _, err := copied.NewHost(ctx, nil); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("copied factory heartbeat rollback err=%v, want ErrAuthorityCorrupt", err)
	}
	after := threadEntriesForRecoveryTest(t, ctx, store, "thread")
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rollback validation changed journal: before=%#v after=%#v", before, after)
	}

	missingWithGenerationRollback := cloneRecoveryAuthoritySnapshot(high)
	missingWithGenerationRollback.Lease = nil
	missingWithGenerationRollback.LeaseGeneration = high.LeaseGeneration - 1
	override.set(missingWithGenerationRollback)
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("missing lease with generation rollback err=%v, want ErrAuthorityCorrupt", err)
	}

	override.clear()
	if _, err := store.repo.(sessiontree.TurnAuthorityRepo).FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: renewed, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "future", "future-run", "future-owner")
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
		t.Fatalf("replacement err=%v, want ErrRecoveryTargetResolved", err)
	}
	override.set(high)
	if _, err := copied.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
		t.Fatalf("resolved copied factory reissued authority: %v", err)
	}
}

func TestInterruptedTurnRecoveryFactoryFailsAfterStoreClose(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store := newInterruptedRecoveryTestStore(t, backend, sessiontree.DefaultLeasePolicy, time.Now)
			capabilities := mustTestCapabilities(t, store)
			createTestRoot(t, ctx, capabilities, "thread")
			admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
			factory, err := capabilities.interrupted.BindThread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			host, err := factory.NewHost(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("closed factory err=%v, want ErrStoreClosed", err)
			}
			if _, err := host.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("closed handle err=%v, want ErrStoreClosed", err)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFactoryValidatesResolutionBeforeLatching(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 9, 40, 0, 0, time.UTC)
	store := newInterruptedRecoveryTestStore(t, "memory", sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	capabilities := mustTestCapabilities(t, store)
	createTestRoot(t, ctx, capabilities, "thread")
	admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
	factory, err := capabilities.interrupted.BindThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	base := store.repo.(*sessiontree.MemoryRepo)
	override := &interruptedRecoveryInspectionRepo{MemoryRepo: base, resolutionErr: sessiontree.ErrAuthorityCorrupt}
	store.repo = override
	if _, err := base.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admission.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	before := threadEntriesForRecoveryTest(t, ctx, store, "thread")
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("invalid resolution err=%v, want ErrAuthorityCorrupt", err)
	}
	override.setResolutionError(nil)
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
		t.Fatalf("factory latched invalid resolution: %v", err)
	}
	after := threadEntriesForRecoveryTest(t, ctx, store, "thread")
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("resolution validation changed journal: before=%#v after=%#v", before, after)
	}
}

func TestInterruptedTurnRecoveryFactoryPreservesConcurrentDelete(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 9, 42, 0, 0, time.UTC)
	store := newInterruptedRecoveryTestStore(t, "memory", sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	capabilities := mustTestCapabilities(t, store)
	createTestRoot(t, ctx, capabilities, "thread")
	admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
	factory, err := capabilities.interrupted.BindThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	base := store.repo.(*sessiontree.MemoryRepo)
	override := &interruptedRecoveryInspectionRepo{MemoryRepo: base, resolutionErr: sessiontree.ErrThreadDeleted}
	store.repo = override
	if _, err := base.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admission.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("concurrent delete err=%v, want ErrThreadDeleted", err)
	}
	override.setResolutionError(nil)
	if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
		t.Fatalf("delete race latched factory: %v", err)
	}
}

func TestInterruptedTurnRecoveryHandleMapsDurableRequestConflictToCorruption(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 9, 44, 0, 0, time.UTC)
	store := newInterruptedRecoveryTestStore(t, "memory", sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	capabilities := mustTestCapabilities(t, store)
	createTestRoot(t, ctx, capabilities, "thread")
	admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
	factory, err := capabilities.interrupted.BindThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	base := store.repo.(*sessiontree.MemoryRepo)
	store.repo = &interruptedRecoveryInspectionRepo{MemoryRepo: base, recoveryErr: sessiontree.ErrRequestConflict}
	host, err := factory.NewHost(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrAuthorityCorrupt) || errors.Is(err, ErrRequestConflict) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want only ErrAuthorityCorrupt", err)
	}
}

func TestInterruptedTurnRecoveryHandleTreatsConcurrentOwnerFinishAsResolved(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.July, 20, 9, 45, 0, 0, time.UTC)
			store := newInterruptedRecoveryTestStore(t, backend, sessiontree.DefaultLeasePolicy, func() time.Time { return now })
			capabilities := mustTestCapabilities(t, store)
			createTestRoot(t, ctx, capabilities, "thread")
			admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
			factory, err := capabilities.interrupted.BindThread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			host, err := factory.NewHost(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			turnRepo := store.repo.(sessiontree.TurnAuthorityRepo)
			if _, err := turnRepo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
				Lease: admission.Lease, RunID: "run", TerminalEntryID: "normal-terminal",
				Status: sessiontree.TurnCompleted, OutcomeFingerprint: "normal-finish", Now: now.Add(time.Second),
			}); err != nil {
				t.Fatalf("FinishTurn: %v", err)
			}
			before := threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if _, err := host.RecoverInterruptedTurn(ctx); !errors.Is(err, ErrRecoveryTargetResolved) {
				t.Fatalf("recovery after owner finish err=%v, want ErrRecoveryTargetResolved", err)
			}
			after := threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("resolved recovery changed journal: before=%#v after=%#v", before, after)
			}
			if _, err := factory.NewHost(ctx, nil); !errors.Is(err, ErrRecoveryTargetResolved) {
				t.Fatalf("factory was not permanently resolved: %v", err)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFactoryConcurrentRefreshHasZeroSideEffects(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			clock := &interruptedRecoveryTestClock{now: time.Date(2026, time.July, 20, 9, 55, 0, 0, time.UTC)}
			store := newInterruptedRecoveryTestStore(t, backend, sessiontree.DefaultLeasePolicy, clock.Now)
			capabilities := mustTestCapabilities(t, store)
			createTestRoot(t, ctx, capabilities, "thread")
			admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "thread", "turn", "run", "owner")
			factory, err := capabilities.interrupted.BindThread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			before := threadEntriesForRecoveryTest(t, ctx, store, "thread")
			start := make(chan struct{})
			errs := make(chan error, 33)
			var wg sync.WaitGroup
			for range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					host, err := factory.NewHost(ctx, nil)
					if err != nil {
						errs <- err
						return
					}
					_, err = host.RecoverInterruptedTurn(ctx)
					if !errors.Is(err, ErrThreadBusy) && !errors.Is(err, ErrStaleAuthority) {
						errs <- err
					}
				}()
			}
			wg.Add(1)
			var renewed sessiontree.TurnLease
			go func() {
				defer wg.Done()
				<-start
				clock.Set(admission.Lease.RenewedAt.Add(time.Second))
				var err error
				renewed, err = store.repo.(sessiontree.TurnLeaseRepo).RenewTurnLease(ctx, admission.Lease)
				if err != nil {
					errs <- err
				}
			}()
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatalf("concurrent recovery error: %v", err)
				}
			}
			after := threadEntriesForRecoveryTest(t, ctx, store, "thread")
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("concurrent refresh changed journal: before=%#v after=%#v", before, after)
			}
			active, ok, err := store.repo.(sessiontree.TurnLeaseRepo).ActiveTurnLease(ctx, "thread")
			if err != nil || !ok || !sessiontree.SameTurnLease(active, renewed) {
				t.Fatalf("active lease=%#v ok=%v err=%v, want renewed %#v", active, ok, err, renewed)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFactoryAllowsClosingSubAgentTarget(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC)
			store := newInterruptedRecoveryTestStore(t, backend, sessiontree.DefaultLeasePolicy, func() time.Time { return now })
			capabilities := mustTestCapabilities(t, store)
			createTestRoot(t, ctx, capabilities, "parent")
			publishTestSubAgentFixture(t, ctx, store, "publication-closing-recovery", "parent", "child", "")
			admission := admitInterruptedRecoveryTestTurn(t, ctx, store, "child", "child-turn", "child-run", "child-owner")
			closeRepo, ok := store.repo.(sessiontree.SubAgentCloseAuthorityRepo)
			if !ok {
				t.Fatal("store does not implement SubAgentCloseAuthorityRepo")
			}
			if _, err := closeRepo.PrepareSubAgentClose(ctx, sessiontree.PrepareSubAgentCloseRequest{
				CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "shutdown", TargetLease: &admission.Lease,
			}); err != nil {
				t.Fatalf("PrepareSubAgentClose: %v", err)
			}
			factory, err := capabilities.interrupted.BindSubAgent(ctx, "parent", "child")
			if err != nil {
				t.Fatalf("BindSubAgent closing child: %v", err)
			}
			host, err := factory.NewHost(ctx, nil)
			if err != nil {
				t.Fatalf("NewHost closing child: %v", err)
			}
			if !sessiontree.SameTurnLease(host.expectedLease, admission.Lease) {
				t.Fatalf("closing child proof=%#v, want %#v", host.expectedLease, admission.Lease)
			}
			now = admission.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			if _, err := host.RecoverInterruptedTurn(ctx); err != nil {
				t.Fatalf("recover closing child: %v", err)
			}
			child, err := store.repo.Thread(ctx, "child")
			if err != nil {
				t.Fatal(err)
			}
			if child.Lifecycle != sessiontree.ThreadLifecycleClosing || child.CloseOperationID != "close-child" {
				t.Fatalf("closing authority changed after recovery: %#v", child)
			}
		})
	}
}

func newInterruptedRecoveryTestStore(t *testing.T, backend string, policy sessiontree.LeasePolicy, now func() time.Time) *Store {
	t.Helper()
	switch backend {
	case "memory":
		repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, now)
		if err != nil {
			t.Fatal(err)
		}
		store := NewMemoryStore()
		store.repo = repo
		store.rootAuthority = repo
		store.agentTodos = repo
		store.forkOperations = storage.NewMemoryForkOperationStore(repo)
		return store
	case "sqlite":
		repo, err := sqlite.Open(filepath.Join(t.TempDir(), "floret.db"), sqlite.WithLeasePolicy(policy), sqlite.WithAuthorityClock(now))
		if err != nil {
			t.Fatal(err)
		}
		store := &Store{
			repo: repo, prompt: repo, forkOperations: repo, agentTodos: repo, rootAuthority: repo,
			deleteCleanup: func(context.Context, []string) error { return nil }, close: repo.Close,
		}
		store.self = store
		store.initLifetime()
		t.Cleanup(func() { _ = store.Close() })
		return store
	default:
		t.Fatalf("unknown backend %q", backend)
		return nil
	}
}

func createTestRoot(t *testing.T, ctx context.Context, capabilities *testCapabilitySet, threadID ThreadID) {
	t.Helper()
	req := testCreateThreadRequest(threadID)
	host, err := capabilities.create.Bind(req.ThreadID, req.CreateIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, req); err != nil {
		t.Fatal(err)
	}
}

func admitInterruptedRecoveryTestTurn(t *testing.T, ctx context.Context, store *Store, threadID, turnID, runID, ownerID string) sessiontree.AdmitTurnResult {
	t.Helper()
	repo, ok := store.repo.(sessiontree.TurnAuthorityRepo)
	if !ok {
		t.Fatal("store does not implement TurnAuthorityRepo")
	}
	result, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: ownerID,
		Input: session.Message{Role: session.User, Content: "recover me"}, RequestFingerprint: "request-" + turnID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func threadEntriesForRecoveryTest(t *testing.T, ctx context.Context, store *Store, threadID string) []sessiontree.Entry {
	t.Helper()
	entries, err := store.repo.Entries(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func assertRecoveryFailureHadNoSideEffects(t *testing.T, ctx context.Context, store *Store, threadID string, before []sessiontree.Entry, events *runtimeEventRecorder) {
	t.Helper()
	after := threadEntriesForRecoveryTest(t, ctx, store, threadID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("recovery failure changed journal:\nbefore=%#v\nafter=%#v", before, after)
	}
	if got := events.snapshot(); len(got) != 0 {
		t.Fatalf("recovery failure emitted events: %#v", got)
	}
}

type interruptedRecoveryInspectionRepo struct {
	*sessiontree.MemoryRepo
	mu            sync.Mutex
	override      *sessiontree.ThreadAuthoritySnapshot
	resolutionErr error
	recoveryErr   error
}

type interruptedRecoveryTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *interruptedRecoveryTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *interruptedRecoveryTestClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (r *interruptedRecoveryInspectionRepo) InspectThreadAuthority(ctx context.Context, threadID string) (sessiontree.ThreadAuthoritySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.override != nil && r.override.Thread.ID == threadID {
		return cloneRecoveryAuthoritySnapshot(*r.override), nil
	}
	return r.MemoryRepo.InspectThreadAuthority(ctx, threadID)
}

func (r *interruptedRecoveryInspectionRepo) set(snapshot sessiontree.ThreadAuthoritySnapshot) {
	r.mu.Lock()
	copy := cloneRecoveryAuthoritySnapshot(snapshot)
	r.override = &copy
	r.mu.Unlock()
}

func (r *interruptedRecoveryInspectionRepo) clear() {
	r.mu.Lock()
	r.override = nil
	r.mu.Unlock()
}

func (r *interruptedRecoveryInspectionRepo) setResolutionError(err error) {
	r.mu.Lock()
	r.resolutionErr = err
	r.mu.Unlock()
}

func (r *interruptedRecoveryInspectionRepo) ValidateInterruptedTurnResolution(ctx context.Context, req sessiontree.RecoverInterruptedTurnRequest) error {
	r.mu.Lock()
	err := r.resolutionErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.MemoryRepo.ValidateInterruptedTurnResolution(ctx, req)
}

func (r *interruptedRecoveryInspectionRepo) RecoverInterruptedTurn(ctx context.Context, req sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error) {
	r.mu.Lock()
	err := r.recoveryErr
	r.mu.Unlock()
	if err != nil {
		return sessiontree.RecoverInterruptedTurnResult{}, err
	}
	return r.MemoryRepo.RecoverInterruptedTurn(ctx, req)
}

func cloneRecoveryAuthoritySnapshot(snapshot sessiontree.ThreadAuthoritySnapshot) sessiontree.ThreadAuthoritySnapshot {
	if snapshot.Lease != nil {
		lease := *snapshot.Lease
		snapshot.Lease = &lease
	}
	return snapshot
}
