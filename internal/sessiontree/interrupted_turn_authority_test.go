package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryInterruptedTurnRecoveryRejectsCorruptResolvedFinish(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, string)
	}{
		{
			name: "missing terminal",
			mutate: func(repo *MemoryRepo, key string) {
				finish := repo.turnFinishes[key]
				finish.TerminalEntryID = "missing-terminal"
				repo.turnFinishes[key] = finish
			},
		},
		{
			name: "admission run drift",
			mutate: func(repo *MemoryRepo, key string) {
				admission := repo.turnAdmissions[key]
				admission.RunID = "different-run"
				repo.turnAdmissions[key] = admission
			},
		},
		{
			name: "turn started reference drift",
			mutate: func(repo *MemoryRepo, key string) {
				admission := repo.turnAdmissions[key]
				admission.TurnStartedID = "terminal"
				repo.turnAdmissions[key] = admission
			},
		},
		{
			name: "generation rollback",
			mutate: func(repo *MemoryRepo, _ string) {
				repo.leaseGeneration["thread"] = 0
			},
		},
		{
			name: "active finished generation",
			mutate: func(repo *MemoryRepo, key string) {
				repo.leases["thread"] = repo.turnAdmissions[key].Lease
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
				OutcomeFingerprint: "finish", Now: now.Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(repo, turnAdmissionKey("thread", "turn"))

			request := RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
			if err := repo.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := repo.RecoverInterruptedTurn(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestInterruptedTurnRecoveryFingerprintIncludesCompleteEffectFacts(t *testing.T) {
	lease := TurnLease{
		ThreadID: "thread", Purpose: TurnLeasePurposeTurn, TurnID: "turn", OwnerID: "owner", Generation: 1,
		AcquiredAt: time.Date(2026, 7, 21, 2, 30, 0, 0, time.UTC), RenewedAt: time.Date(2026, 7, 21, 2, 30, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 7, 21, 2, 31, 0, 0, time.UTC),
	}
	prepared := InterruptedTurnRecoveryEffect{
		EffectAttemptID: "effect-1", ToolCallID: "call-1", State: EffectAttemptPrepared,
	}
	dispatching := InterruptedTurnRecoveryEffect{
		EffectAttemptID: "effect-2", ToolCallID: "call-2", State: EffectAttemptDispatching,
	}
	base, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, InterruptedTurnEffectOutcomeUnknownMessage,
		[]InterruptedTurnRecoveryEffect{prepared, dispatching},
	)
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, InterruptedTurnEffectOutcomeUnknownMessage,
		[]InterruptedTurnRecoveryEffect{dispatching, prepared},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reordered != base {
		t.Fatalf("effect fact order changed fingerprint: base=%s reordered=%s", base, reordered)
	}
	dispatching.State = EffectAttemptPrepared
	changed, err := InterruptedTurnRecoveryFingerprint(
		lease, "", "run", TurnFailed, InterruptedTurnEffectOutcomeUnknownMessage,
		[]InterruptedTurnRecoveryEffect{prepared, dispatching},
	)
	if err != nil {
		t.Fatal(err)
	}
	if changed == base {
		t.Fatal("effect state change did not change recovery fingerprint")
	}
}

func TestMemoryInterruptedTurnResolutionRejectsRecoveryFailureLinkDrift(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 45, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	recovered, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: now})
	if err != nil || recovered.Failure == nil {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	entries := repo.entries["thread"]
	for index := range entries {
		if entries[index].ID != recovered.Terminal.ID {
			continue
		}
		entries[index].ParentID = admitted.TurnStarted.ID
		entries[index].Raw = rawForEntry(entries[index])
		entries[index].RawHash = stableHash(entries[index].Raw)
	}
	repo.entries["thread"] = entries
	if err := repo.ValidateInterruptedTurnResolution(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryInterruptedTurnResolutionReturnsDeletedForTombstone(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 50, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if err := repo.ValidateInterruptedTurnResolution(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrThreadDeleted", err)
	}
}

func TestMemoryInterruptedTurnRecoveryRejectsCorruptAdmissionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, string)
	}{
		{name: "missing", mutate: func(repo *MemoryRepo, key string) { delete(repo.turnAdmissions, key) }},
		{name: "run drift", mutate: func(repo *MemoryRepo, key string) {
			admission := repo.turnAdmissions[key]
			admission.RunID = "different-run"
			repo.turnAdmissions[key] = admission
		}},
		{name: "lease drift", mutate: func(repo *MemoryRepo, key string) {
			admission := repo.turnAdmissions[key]
			admission.Lease.Heartbeat++
			repo.turnAdmissions[key] = admission
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			current := now
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return current })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: current,
			})
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(repo, turnAdmissionKey("thread", "turn"))
			current = admitted.Lease.ExpiresAt.Add(DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
			beforeEntries := len(repo.entries["thread"])
			beforeLease := repo.leases["thread"]
			beforeGeneration := repo.leaseGeneration["thread"]

			if _, err := repo.RecoverInterruptedTurn(ctx, RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
			if !SameTurnLease(repo.leases["thread"], beforeLease) || repo.leaseGeneration["thread"] != beforeGeneration ||
				len(repo.entries["thread"]) != beforeEntries {
				t.Fatalf("corrupt admission mutated authority: lease=%#v generation=%d entries=%d", repo.leases["thread"], repo.leaseGeneration["thread"], len(repo.entries["thread"]))
			}
		})
	}
}
