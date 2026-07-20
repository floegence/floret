package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

type interruptedTurnRecoveryTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	Path(context.Context, string, string) ([]sessiontree.Entry, error)
	Append(context.Context, sessiontree.Entry, sessiontree.AppendOptions) (sessiontree.Entry, error)
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	PrepareEffectAttempt(context.Context, sessiontree.PrepareEffectAttemptRequest) (sessiontree.PrepareEffectAttemptResult, error)
	BeginEffectDispatch(context.Context, sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error)
	RecoverInterruptedTurn(context.Context, sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error)
}

func TestInterruptedTurnRecoveryPrioritizesDispatchingEffectUnknown(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			initial := time.Date(2026, 7, 21, 2, 0, 0, 0, time.UTC)
			current := initial
			policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
			repo := newInterruptedTurnRecoveryTestRepo(t, backend, policy, func() time.Time { return current })
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: initial, UpdatedAt: initial}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: initial,
			})
			if err != nil {
				t.Fatal(err)
			}
			leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
			for _, callID := range []string{"prepared", "dispatching"} {
				if _, err := repo.Append(leaseCtx, sessiontree.Entry{
					ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryToolCall,
					Message: session.Message{Role: session.Assistant, ToolCallID: callID, ToolName: "tool"},
				}, sessiontree.AppendOptions{Now: initial}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repo.PrepareEffectAttempt(ctx, effectPrepareRequest(admitted.Lease, "run", "prepared", "hash-p", "request-p", initial)); err != nil {
				t.Fatal(err)
			}
			dispatching, err := repo.PrepareEffectAttempt(ctx, effectPrepareRequest(admitted.Lease, "run", "dispatching", "hash-d", "request-d", initial))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginEffectDispatch(ctx, sessiontree.BeginEffectDispatchRequest{
				Lease: admitted.Lease, EffectAttemptID: dispatching.Attempt.EffectAttemptID, RequestFingerprint: "request-d",
				ObservedHeartbeat: admitted.Lease.Heartbeat, AuthorizationProofHash: "proof", Now: initial,
			}); err != nil {
				t.Fatal(err)
			}
			current = admitted.Lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
			recovered, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current})
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Status != sessiontree.TurnFailed || recovered.Terminal.TurnStatus != sessiontree.TurnFailed ||
				recovered.Failure == nil || recovered.Failure.Error != sessiontree.InterruptedTurnEffectOutcomeUnknownMessage {
				t.Fatalf("recovered=%#v, want canonical effect outcome unknown failure", recovered)
			}
			for _, result := range recovered.ToolResults {
				if result.Message.ToolCallID == "dispatching" {
					if result.Message.ToolResult == nil || result.Message.ToolResult.Status != "error" || !strings.Contains(strings.ToLower(result.Message.Content), "unknown") {
						t.Fatalf("dispatching recovery result=%#v, want unknown error", result.Message)
					}
					return
				}
			}
			t.Fatalf("missing dispatching recovery tool result: %#v", recovered.ToolResults)
		})
	}
}

func TestInterruptedTurnRecoveryDerivesLatestPathInsideAuthorityTransaction(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			initial := time.Date(2026, 7, 19, 15, 0, 0, 0, time.UTC)
			current := initial
			policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
			repo := newInterruptedTurnRecoveryTestRepo(t, backend, policy, func() time.Time { return current })
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: initial, UpdatedAt: initial}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: initial,
			})
			if err != nil {
				t.Fatal(err)
			}
			meta, err := repo.Thread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			diagnosticPath, err := repo.Path(ctx, "thread", meta.LeafID)
			if err != nil {
				t.Fatal(err)
			}
			diagnosticPlan, err := sessiontree.DeriveInterruptedTurnRecoveryPlan(diagnosticPath, admitted.Lease, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			if diagnosticPlan.Status != sessiontree.TurnAborted || diagnosticPlan.FailureMessage == "" {
				t.Fatalf("diagnostic plan = %#v", diagnosticPlan)
			}

			leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
			if _, err := repo.Append(leaseCtx, sessiontree.Entry{
				ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryRunFailure, Error: "provider failed",
			}, sessiontree.AppendOptions{Now: initial.Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			current = admitted.Lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
			recovered, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{
				ExpectedLease: admitted.Lease,
				Now:           current,
			})
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Replayed || recovered.RunID != "run" || recovered.Status != sessiontree.TurnFailed ||
				recovered.Failure != nil || recovered.Terminal.TurnStatus != sessiontree.TurnFailed ||
				recovered.OutcomeFingerprint == diagnosticPlan.OutcomeFingerprint {
				t.Fatalf("recovered from stale diagnostic path: %#v", recovered)
			}
		})
	}
}

func TestInterruptedTurnRecoveryReplayBindsCompleteLeaseAndParent(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			initial := time.Date(2026, 7, 19, 16, 0, 0, 0, time.UTC)
			current := initial
			policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
			repo := newInterruptedTurnRecoveryTestRepo(t, backend, policy, func() time.Time { return current })
			ctx := context.Background()
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: initial, UpdatedAt: initial}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
				ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker",
				CreatedAt: initial, UpdatedAt: initial,
			}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: initial,
			})
			if err != nil {
				t.Fatal(err)
			}
			current = admitted.Lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
			request := sessiontree.RecoverInterruptedTurnRequest{
				ExpectedLease:  admitted.Lease,
				ParentThreadID: "parent",
				Now:            current,
			}
			first, err := repo.RecoverInterruptedTurn(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := repo.RecoverInterruptedTurn(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if !replayed.Replayed || replayed.RunID != first.RunID || replayed.Status != first.Status ||
				replayed.OutcomeFingerprint != first.OutcomeFingerprint || replayed.Terminal.ID != first.Terminal.ID ||
				replayed.Generation != first.Generation {
				t.Fatalf("first=%#v replayed=%#v", first, replayed)
			}

			changedParent := request
			changedParent.ParentThreadID = "different-parent"
			if _, err := repo.RecoverInterruptedTurn(ctx, changedParent); !errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
				t.Fatalf("changed parent replay err=%v, want ErrInvalidThreadAuthority", err)
			}
			changedProof := request
			changedProof.ExpectedLease.OwnerID = "different-owner"
			if _, err := repo.RecoverInterruptedTurn(ctx, changedProof); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("changed lease replay err=%v, want ErrRequestConflict", err)
			}
		})
	}
}

func TestSQLiteInterruptedTurnRecoveryRejectsCorruptResolvedFinish(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 30, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name  string
		query string
	}{
		{name: "invalid terminal", query: `UPDATE entries SET type = 'run_failure' WHERE thread_id = 'thread' AND id = 'terminal'`},
		{name: "admission run drift", query: `UPDATE turn_admissions SET run_id = 'different-run' WHERE thread_id = 'thread' AND turn_id = 'turn'`},
		{name: "turn started reference drift", query: `UPDATE turn_admissions SET turn_started_id = 'terminal' WHERE thread_id = 'thread' AND turn_id = 'turn'`},
		{name: "generation rollback", query: `UPDATE threads SET lease_generation = 0 WHERE id = 'thread'`},
		{name: "active finished generation", query: `INSERT INTO active_turn_leases(thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at) SELECT thread_id, 'turn', turn_id, '', '', owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at FROM turn_admissions WHERE thread_id = 'thread' AND turn_id = 'turn'`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "resolved-corruption.db"), WithAuthorityClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "finish", Now: now.Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, testCase.query); err != nil {
				t.Fatal(err)
			}

			request := sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
			if err := store.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := store.RecoverInterruptedTurn(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestSQLiteInterruptedTurnResolutionRejectsRecoveryFailureLinkDrift(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 45, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "recovery-link-corruption.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	recovered, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: now})
	if err != nil || recovered.Failure == nil {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = ? WHERE thread_id = 'thread' AND id = ?`, admitted.TurnStarted.ID, recovered.Terminal.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInterruptedTurnResolution(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestSQLiteInterruptedTurnResolutionReturnsDeletedForTombstone(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 50, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "resolution-delete.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteRootTree(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInterruptedTurnResolution(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, sessiontree.ErrThreadDeleted) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrThreadDeleted", err)
	}
}

func TestSQLiteInterruptedTurnRecoveryRejectsCorruptAdmissionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 13, 30, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name  string
		query string
	}{
		{name: "missing", query: `DELETE FROM turn_admissions WHERE thread_id = 'thread' AND turn_id = 'turn'`},
		{name: "run drift", query: `UPDATE turn_admissions SET run_id = 'different-run' WHERE thread_id = 'thread' AND turn_id = 'turn'`},
		{name: "lease drift", query: `UPDATE turn_admissions SET heartbeat = heartbeat + 1 WHERE thread_id = 'thread' AND turn_id = 'turn'`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			current := now
			store, err := Open(filepath.Join(t.TempDir(), "admission-corruption.db"), WithAuthorityClock(func() time.Time { return current }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, testCase.query); err != nil {
				t.Fatal(err)
			}
			current = admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			var beforeEntries, beforeGeneration, beforeLeases int
			if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&beforeEntries); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&beforeGeneration); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_turn_leases WHERE thread_id = 'thread'`).Scan(&beforeLeases); err != nil {
				t.Fatal(err)
			}

			if _, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
			var afterEntries, afterGeneration, afterLeases int
			if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&afterEntries); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&afterGeneration); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_turn_leases WHERE thread_id = 'thread'`).Scan(&afterLeases); err != nil {
				t.Fatal(err)
			}
			if afterEntries != beforeEntries || afterGeneration != beforeGeneration || afterLeases != beforeLeases {
				t.Fatalf("corrupt admission mutated authority: entries %d->%d generation %d->%d leases %d->%d", beforeEntries, afterEntries, beforeGeneration, afterGeneration, beforeLeases, afterLeases)
			}
		})
	}
}

func TestSQLiteInterruptedTurnResolutionPreservesContextCancellation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 14, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "resolution-cancel.db"), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.ValidateInterruptedTurnResolution(cancelled, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want context.Canceled", err)
	}
}

func newInterruptedTurnRecoveryTestRepo(
	t *testing.T,
	backend string,
	policy sessiontree.LeasePolicy,
	now func() time.Time,
) interruptedTurnRecoveryTestRepo {
	t.Helper()
	if backend == "memory" {
		repo, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, now)
		if err != nil {
			t.Fatal(err)
		}
		return repo
	}
	repo, err := Open(filepath.Join(t.TempDir(), "interrupted-turn.db"), WithLeasePolicy(policy), WithAuthorityClock(now))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}
