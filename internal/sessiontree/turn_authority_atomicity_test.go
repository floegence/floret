package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryRecoverInterruptedTurnConflictHasZeroSideEffects(t *testing.T) {
	initial := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	current := initial
	policy := LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
	repo, err := NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	lease, prepared := seedMemoryAtomicTurn(t, repo, initial, "recovery")
	leaseCtx := ContextWithTurnLease(context.Background(), lease)
	if _, err := repo.Append(leaseCtx, Entry{
		ThreadID: "recovery", TurnID: "turn-1", Type: EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call-1", ToolName: "tool"},
	}, AppendOptions{Now: initial}); err != nil {
		t.Fatal(err)
	}
	meta, err := repo.Thread(context.Background(), "recovery")
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(context.Background(), "recovery", meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := InterruptedTurnRecoveryEffects([]EffectAttempt{prepared.Attempt}, "")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DeriveInterruptedTurnRecoveryPlan(path, lease, "", effects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(leaseCtx, Entry{
		ThreadID: "recovery", TurnID: "turn-1", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "collision"},
	}, AppendOptions{ID: plan.TerminalEntryID, Now: initial}); err != nil {
		t.Fatal(err)
	}
	current = lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
	beforeEntries, err := repo.Entries(context.Background(), "recovery")
	if err != nil {
		t.Fatal(err)
	}
	beforeSequence := repo.seq
	beforeGeneration := repo.leaseGeneration["recovery"]
	beforeAttempt := repo.effectAttempts[prepared.Attempt.EffectAttemptID]

	_, err = repo.RecoverInterruptedTurn(context.Background(), RecoverInterruptedTurnRequest{
		ExpectedLease: lease,
		Now:           current,
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want ErrRequestConflict", err)
	}
	afterEntries, err := repo.Entries(context.Background(), "recovery")
	if err != nil {
		t.Fatal(err)
	}
	active, ok, err := repo.ActiveTurnLease(context.Background(), "recovery")
	if err != nil || !ok || !SameTurnLease(active, lease) {
		t.Fatalf("recovery conflict changed lease: active=%#v ok=%v err=%v", active, ok, err)
	}
	if !reflect.DeepEqual(afterEntries, beforeEntries) || repo.seq != beforeSequence || repo.leaseGeneration["recovery"] != beforeGeneration ||
		!reflect.DeepEqual(repo.effectAttempts[prepared.Attempt.EffectAttemptID], beforeAttempt) {
		t.Fatalf("recovery conflict mutated state: entries=%#v seq=%d generation=%d attempt=%#v", afterEntries, repo.seq,
			repo.leaseGeneration["recovery"], repo.effectAttempts[prepared.Attempt.EffectAttemptID])
	}
}

func TestMemoryFinishTurnConflictsHaveZeroSideEffects(t *testing.T) {
	for _, test := range []struct {
		name       string
		terminalID func(*MemoryRepo, []Entry) string
	}{
		{name: "existing terminal", terminalID: func(_ *MemoryRepo, entries []Entry) string { return entries[0].ID }},
		{name: "generated failure collision", terminalID: func(repo *MemoryRepo, _ []Entry) string {
			return fmt.Sprintf("finish-entry-%d", repo.seq+1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 7, 19, 12, 30, 0, 0, time.UTC)
			repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			lease, prepared := seedMemoryAtomicTurn(t, repo, now, "finish")
			beforeEntries, err := repo.Entries(context.Background(), "finish")
			if err != nil {
				t.Fatal(err)
			}
			beforeSequence := repo.seq
			beforeGeneration := repo.leaseGeneration["finish"]
			beforeAttempt := repo.effectAttempts[prepared.Attempt.EffectAttemptID]
			_, err = repo.FinishTurn(context.Background(), FinishTurnRequest{
				Lease: lease, RunID: "run-1", TerminalEntryID: test.terminalID(repo, beforeEntries), Status: TurnFailed,
				Metadata:       map[string]string{TurnFailureCodeMetadataKey: TurnFailureEngineContract},
				FailureMessage: "failed", OutcomeFingerprint: "finish-outcome", Now: now,
			})
			if !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("FinishTurn err=%v, want ErrRequestConflict", err)
			}
			afterEntries, err := repo.Entries(context.Background(), "finish")
			if err != nil {
				t.Fatal(err)
			}
			active, ok, err := repo.ActiveTurnLease(context.Background(), "finish")
			if err != nil || !ok || !SameTurnLease(active, lease) {
				t.Fatalf("finish conflict changed lease: active=%#v ok=%v err=%v", active, ok, err)
			}
			if !reflect.DeepEqual(afterEntries, beforeEntries) || repo.seq != beforeSequence || repo.leaseGeneration["finish"] != beforeGeneration ||
				!reflect.DeepEqual(repo.effectAttempts[prepared.Attempt.EffectAttemptID], beforeAttempt) {
				t.Fatalf("finish conflict mutated state: entries=%#v seq=%d generation=%d attempt=%#v", afterEntries, repo.seq,
					repo.leaseGeneration["finish"], repo.effectAttempts[prepared.Attempt.EffectAttemptID])
			}
		})
	}
}

func TestMemoryFinishTurnRejectsVisibleApprovalWithZeroSideEffects(t *testing.T) {
	now := time.Date(2026, 7, 21, 11, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "finish-approval", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "finish-approval", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
	if err != nil {
		t.Fatal(err)
	}
	beforeEntries, err := repo.Entries(ctx, "finish-approval")
	if err != nil {
		t.Fatal(err)
	}
	beforeSequence := repo.seq
	beforeGeneration := repo.leaseGeneration["finish-approval"]
	beforeApproval := repo.approvals[prepared.Approvals[0].ApprovalID]
	beforeAttempt := repo.effectAttempts[prepared.Effects[0].EffectAttemptID]
	beforeQueue := repo.approvalQueues[prepared.Queue.RootThreadID]

	_, err = repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("FinishTurn err=%v, want ErrRequestConflict", err)
	}
	afterEntries, err := repo.Entries(ctx, "finish-approval")
	if err != nil {
		t.Fatal(err)
	}
	active, ok, err := repo.ActiveTurnLease(ctx, "finish-approval")
	if err != nil || !ok || !SameTurnLease(active, admitted.Lease) {
		t.Fatalf("rejected finish changed lease: active=%#v ok=%v err=%v", active, ok, err)
	}
	if !reflect.DeepEqual(afterEntries, beforeEntries) || repo.seq != beforeSequence ||
		repo.leaseGeneration["finish-approval"] != beforeGeneration ||
		!reflect.DeepEqual(repo.approvals[prepared.Approvals[0].ApprovalID], beforeApproval) ||
		!reflect.DeepEqual(repo.effectAttempts[prepared.Effects[0].EffectAttemptID], beforeAttempt) ||
		!reflect.DeepEqual(repo.approvalQueues[prepared.Queue.RootThreadID], beforeQueue) {
		t.Fatalf("rejected finish mutated canonical authority")
	}
}

func TestMemoryFinishTurnReplayRejectsVisibleApprovalCorruption(t *testing.T) {
	now := time.Date(2026, 7, 21, 11, 20, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "finish-replay-approval", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "finish-replay-approval", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := "cancel"
	if _, err := repo.CancelApprovalBatch(ctx, CancelApprovalBatchRequest{
		Lease: admitted.Lease, RunID: "run", CancellationFingerprint: fingerprint, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	finishRequest := FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
		OutcomeFingerprint: "finish", Now: now,
	}
	if _, err := repo.FinishTurn(ctx, finishRequest); err != nil {
		t.Fatal(err)
	}

	repo.mu.Lock()
	repo.approvals[prepared.Approvals[0].ApprovalID] = prepared.Approvals[0]
	repo.approvalQueues[prepared.Queue.RootThreadID] = approvalQueueLedger{
		RootThreadID: prepared.Queue.RootThreadID, Generation: prepared.Queue.Generation,
		Revision: prepared.Queue.Revision, CurrentApprovalID: prepared.Approvals[0].ApprovalID,
	}
	repo.mu.Unlock()
	beforeEntries, err := repo.Entries(ctx, "finish-replay-approval")
	if err != nil {
		t.Fatal(err)
	}
	beforeSequence := repo.seq
	beforeFinish := repo.turnFinishes[turnAdmissionKey("finish-replay-approval", "turn")]
	beforeApproval := repo.approvals[prepared.Approvals[0].ApprovalID]
	beforeQueue := repo.approvalQueues[prepared.Queue.RootThreadID]

	if _, err := repo.FinishTurn(ctx, finishRequest); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("FinishTurn replay err=%v, want ErrAuthorityCorrupt", err)
	}
	afterEntries, err := repo.Entries(ctx, "finish-replay-approval")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterEntries, beforeEntries) || repo.seq != beforeSequence ||
		!reflect.DeepEqual(repo.turnFinishes[turnAdmissionKey("finish-replay-approval", "turn")], beforeFinish) ||
		!reflect.DeepEqual(repo.approvals[prepared.Approvals[0].ApprovalID], beforeApproval) ||
		!reflect.DeepEqual(repo.approvalQueues[prepared.Queue.RootThreadID], beforeQueue) {
		t.Fatal("rejected finish replay mutated corrupted authority")
	}
}

func seedMemoryAtomicTurn(t *testing.T, repo *MemoryRepo, now time.Time, threadID string) (TurnLease, PrepareEffectAttemptResult) {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: threadID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: threadID, TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "admit-" + threadID, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "effect-" + threadID, Now: now,
		Invocation: EffectInvocationIdentity{
			ThreadID: threadID, TurnID: "turn-1", RunID: "run-1", ToolCallID: "effect-call", ToolName: "tool", ArgumentHash: "arguments",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.Lease, prepared
}
