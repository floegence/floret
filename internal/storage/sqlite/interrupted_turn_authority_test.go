package sqlite

import (
	"context"
	"errors"
	"path/filepath"
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
	RecoverInterruptedTurn(context.Context, sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error)
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
			diagnosticPlan, err := sessiontree.DeriveInterruptedTurnRecoveryPlan(diagnosticPath, admitted.Lease, "")
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
