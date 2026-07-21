package sqlite

import (
	"context"
	"encoding/json"
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
	PrepareApprovalBatch(context.Context, sessiontree.PrepareApprovalBatchRequest) (sessiontree.PrepareApprovalBatchResult, error)
	ReadApprovalQueue(context.Context, string) (sessiontree.ApprovalQueue, error)
	Approval(context.Context, string) (sessiontree.ApprovalRecord, error)
	ResolveApproval(context.Context, sessiontree.ResolveApprovalRequest) (sessiontree.ResolveApprovalResult, error)
	RecoverInterruptedTurn(context.Context, sessiontree.RecoverInterruptedTurnRequest) (sessiontree.RecoverInterruptedTurnResult, error)
}

func TestInterruptedTurnRecoveryCancelsVisibleApprovalAndAdvancesRootQueue(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, state := range []sessiontree.ApprovalState{sessiontree.ApprovalRequested, sessiontree.ApprovalDecisionSubmitted} {
			t.Run(backend+"/"+string(state), func(t *testing.T) {
				initial := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
				current := initial
				policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
				repo := newInterruptedTurnRecoveryTestRepo(t, backend, policy, func() time.Time { return current })
				ctx := context.Background()
				if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "root", CreatedAt: initial, UpdatedAt: initial}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
					ID: "child", ParentThreadID: "root", ParentTurnID: "root-parent-turn", TaskName: "worker", AgentPath: "root/worker",
					CreatedAt: initial, UpdatedAt: initial,
				}); err != nil {
					t.Fatal(err)
				}
				childLease := admitInterruptedApprovalTurn(t, repo, initial, "child", "child-turn", "child-run")
				childPrepared, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(childLease, "child-call", 0, 1, initial, "child-run"))
				if err != nil {
					t.Fatal(err)
				}
				childApproval := childPrepared.Approvals[0]
				var childDecision *sessiontree.ResolveApprovalRequest
				if state == sessiontree.ApprovalDecisionSubmitted {
					request := sessiontree.ResolveApprovalRequest{
						DecisionID: "child-decision", ExpectedRootThreadID: "root", ExpectedGeneration: childPrepared.Queue.Generation,
						ExpectedRevision: childPrepared.Queue.Revision, ExpectedCurrent: childApproval.Identity(),
						ExpectedApprovalRevision: childApproval.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: initial,
					}
					submitted, err := repo.ResolveApproval(ctx, request)
					if err != nil || submitted.Approval.State != sessiontree.ApprovalDecisionSubmitted {
						t.Fatalf("submitted=%#v err=%v", submitted, err)
					}
					childApproval = submitted.Approval
					childDecision = &request
				}

				rootLease := admitInterruptedApprovalTurn(t, repo, initial, "root", "root-turn", "root-run")
				rootPrepared, err := repo.PrepareApprovalBatch(ctx, approvalPrepare(rootLease, "root-call", 0, 1, initial, "root-run"))
				if err != nil {
					t.Fatal(err)
				}
				rootApproval := rootPrepared.Approvals[0]
				before, err := repo.ReadApprovalQueue(ctx, "root")
				if err != nil || before.CurrentApprovalID != childApproval.ApprovalID || len(before.Items) != 2 {
					t.Fatalf("queue before recovery=%#v err=%v", before, err)
				}

				current = childLease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
				recovered, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{
					ExpectedLease: childLease, ParentThreadID: "root", Now: current,
				})
				if err != nil {
					t.Fatal(err)
				}
				cancelled, err := repo.Approval(ctx, childApproval.ApprovalID)
				if err != nil || cancelled.State != sessiontree.ApprovalCancelled || cancelled.Reason != sessiontree.ApprovalReasonCancelled ||
					cancelled.DecisionID == "" || cancelled.ResolvedAt.IsZero() || cancelled.Revision != childApproval.Revision+1 {
					t.Fatalf("cancelled approval=%#v err=%v", cancelled, err)
				}
				if state == sessiontree.ApprovalRequested && cancelled.DecisionID != sessiontree.InterruptedTurnRecoveryApprovalCancellationID(recovered.OutcomeFingerprint, childApproval.ApprovalID) {
					t.Fatalf("requested cancellation id=%q", cancelled.DecisionID)
				}
				if state == sessiontree.ApprovalDecisionSubmitted && cancelled.DecisionID != "child-decision" {
					t.Fatalf("submitted decision id changed to %q", cancelled.DecisionID)
				}
				if childDecision != nil {
					replayed, err := repo.ResolveApproval(ctx, *childDecision)
					if err != nil || !replayed.Replayed || replayed.Receipt.State != sessiontree.ApprovalCancelled ||
						replayed.Receipt.Reason != sessiontree.ApprovalReasonCancelled || replayed.Receipt.ResolvedAt.IsZero() ||
						replayed.Effect.State != sessiontree.EffectAttemptCancelled {
						t.Fatalf("cancelled decision replay=%#v err=%v", replayed, err)
					}
				}
				after, err := repo.ReadApprovalQueue(ctx, "root")
				if err != nil || after.CurrentApprovalID != rootApproval.ApprovalID || len(after.Items) != 1 || after.Revision != before.Revision+1 {
					t.Fatalf("queue after recovery=%#v err=%v", after, err)
				}
				resolved, err := repo.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
					DecisionID: "root-decision", ExpectedRootThreadID: "root", ExpectedGeneration: after.Generation,
					ExpectedRevision: after.Revision, ExpectedCurrent: rootApproval.Identity(),
					ExpectedApprovalRevision: rootApproval.Revision, Decision: sessiontree.ApprovalDecisionReject,
					RejectedEntry: approvalRejectedTestEntry("root-decision", rootApproval.Identity()), Now: current,
				})
				if err != nil || resolved.Approval.State != sessiontree.ApprovalRejected || len(resolved.Queue.Items) != 0 {
					t.Fatalf("root approval after recovery=%#v err=%v", resolved, err)
				}
			})
		}
	}
}

func TestSQLiteInterruptedTurnApprovalRecoverySurvivesReopen(t *testing.T) {
	initial := time.Date(2026, 7, 21, 10, 30, 0, 0, time.UTC)
	current := initial
	policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
	path := filepath.Join(t.TempDir(), "interrupted-approval.db")
	store, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return current }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, initial, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, initial))
	if err != nil {
		t.Fatal(err)
	}
	current = lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
	if _, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease, Now: current}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return current }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	approval, err := reopened.Approval(ctx, prepared.Approvals[0].ApprovalID)
	if err != nil || approval.State != sessiontree.ApprovalCancelled || approval.Reason != sessiontree.ApprovalReasonCancelled {
		t.Fatalf("reopened approval=%#v err=%v", approval, err)
	}
	queue, err := reopened.ReadApprovalQueue(ctx, "thread")
	if err != nil || queue.CurrentApprovalID != "" || len(queue.Items) != 0 {
		t.Fatalf("reopened queue=%#v err=%v", queue, err)
	}
	if err := reopened.ValidateInterruptedTurnResolution(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease}); err != nil {
		t.Fatalf("reopened recovery validation: %v", err)
	}
}

func TestSQLiteInterruptedTurnRecoveryNotifiesApprovalWaiter(t *testing.T) {
	initial := time.Date(2026, 7, 21, 10, 40, 0, 0, time.UTC)
	current := initial
	policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
	store, err := Open(filepath.Join(t.TempDir(), "interrupted-approval-waiter.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return current }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	lease := seedApprovalTurn(t, store, initial, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
	prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, initial))
	if err != nil {
		t.Fatal(err)
	}
	approvalID := prepared.Approvals[0].ApprovalID
	done := make(chan error, 1)
	go func() {
		waited, err := store.WaitApprovalDecision(ctx, approvalID)
		if err == nil && (waited.Approval.State != sessiontree.ApprovalCancelled || waited.Approval.Reason != sessiontree.ApprovalReasonCancelled) {
			err = sessiontree.ErrAuthorityCorrupt
		}
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		store.approvalSignalMu.Lock()
		_, subscribed := store.approvalSignals[approvalID]
		store.approvalSignalMu.Unlock()
		if subscribed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval waiter did not subscribe")
		}
		time.Sleep(time.Millisecond)
	}
	current = lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
	if _, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease, Now: current}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not notify approval waiter")
	}
}

func TestSQLiteInterruptedTurnRecoveryReplayRejectsApprovalAuthorityDrift(t *testing.T) {
	initial := time.Date(2026, 7, 21, 10, 45, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name      string
		submitted bool
		mutate    func(*testing.T, *Store, sessiontree.ApprovalRecord)
	}{
		{name: "requested cancellation id", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_requests SET decision_id = 'different-cancellation' WHERE approval_id = ?`, record.ApprovalID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "requested approval state", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_requests SET state = 'failed', reason = ? WHERE approval_id = ?`, sessiontree.ApprovalReasonAuthorizationUnavailable, record.ApprovalID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "queue current", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_queues SET current_approval_id = ? WHERE root_thread_id = ?`, record.ApprovalID, record.RootThreadID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "queue revision", mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_queues SET revision = 2 WHERE root_thread_id = ?`, record.RootThreadID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "submitted receipt state", submitted: true, mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET receipt_state = 'failed', reason = ? WHERE decision_id = ?`, sessiontree.ApprovalReasonAuthorizationUnavailable, record.DecisionID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "submitted receipt revision", submitted: true, mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET approval_revision = approval_revision - 1 WHERE decision_id = ?`, record.DecisionID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "submitted receipt timestamp", submitted: true, mutate: func(t *testing.T, store *Store, record sessiontree.ApprovalRecord) {
			if _, err := store.db.Exec(`UPDATE approval_decisions SET resolved_at = submitted_at WHERE decision_id = ?`, record.DecisionID); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			current := initial
			policy := sessiontree.LeasePolicy{TTL: 30 * time.Second, RenewInterval: 10 * time.Second, ClockSkewAllowance: 2 * time.Second}
			store, err := Open(filepath.Join(t.TempDir(), "interrupted-approval-corruption.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return current }))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx := context.Background()
			lease := seedApprovalTurn(t, store, initial, sessiontree.ThreadMeta{ID: "thread"}, "turn", "run")
			prepared, err := store.PrepareApprovalBatch(ctx, approvalPrepare(lease, "call", 0, 1, initial))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			if testCase.submitted {
				resolved, err := store.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
					DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
					ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
					ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: initial.Add(time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				record = resolved.Approval
			}
			if testCase.name == "queue revision" {
				if _, err := store.db.Exec(`UPDATE approval_queues SET revision = 8 WHERE root_thread_id = ?`, record.RootThreadID); err != nil {
					t.Fatal(err)
				}
			}
			current = lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
			if _, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease, Now: current}); err != nil {
				t.Fatal(err)
			}
			record, err = store.Approval(ctx, record.ApprovalID)
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, store, record)

			request := sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: lease}
			if err := store.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
			}
			if _, err := store.RecoverInterruptedTurn(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
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

func TestInterruptedTurnRecoveryDoesNotClassifyFailureText(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, failureText := range []string{"provider failed", context.Canceled.Error(), context.DeadlineExceeded.Error(), "runtime restarted"} {
			t.Run(backend+"/"+strings.ReplaceAll(failureText, " ", "_"), func(t *testing.T) {
				initial := time.Date(2026, 7, 19, 15, 30, 0, 0, time.UTC)
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
				if _, err := repo.Append(leaseCtx, sessiontree.Entry{
					ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryRunFailure, Error: failureText,
				}, sessiontree.AppendOptions{Now: initial.Add(time.Second)}); err != nil {
					t.Fatal(err)
				}
				current = admitted.Lease.ExpiresAt.Add(policy.ClockSkewAllowance + time.Nanosecond)
				recovered, err := repo.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current})
				if err != nil {
					t.Fatal(err)
				}
				if recovered.Status != sessiontree.TurnFailed || recovered.Terminal.TurnStatus != sessiontree.TurnFailed ||
					recovered.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureLegacyUnclassified {
					t.Fatalf("recovered from %q = %#v", failureText, recovered)
				}
			})
		}
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

func TestSQLiteInterruptedTurnRecoveryReplayRejectsExistingFailureTamper(t *testing.T) {
	for _, failureCase := range []struct {
		name     string
		metadata map[string]string
		wantCode string
	}{
		{name: "typed", metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider}, wantCode: sessiontree.TurnFailureProvider},
		{name: "legacy", wantCode: sessiontree.TurnFailureLegacyUnclassified},
	} {
		for _, tamperCase := range []struct {
			name   string
			mutate func(context.Context, *Store, sessiontree.Entry, sessiontree.Entry) error
		}{
			{name: "id", mutate: func(ctx context.Context, store *Store, failure, terminal sessiontree.Entry) error {
				if _, err := store.db.ExecContext(ctx, `UPDATE entries SET id = 'tampered-failure-id' WHERE thread_id = 'thread' AND id = ?`, failure.ID); err != nil {
					return err
				}
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = 'tampered-failure-id' WHERE thread_id = 'thread' AND id = ?`, terminal.ID)
				return err
			}},
			{name: "canonical ancestry", mutate: func(ctx context.Context, store *Store, failure, terminal sessiontree.Entry) error {
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET parent_id = ? WHERE thread_id = 'thread' AND id = ?`, failure.ParentID, terminal.ID)
				return err
			}},
			{name: "message", mutate: func(ctx context.Context, store *Store, failure, _ sessiontree.Entry) error {
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET error = 'tampered message' WHERE thread_id = 'thread' AND id = ?`, failure.ID)
				return err
			}},
			{name: "raw", mutate: func(ctx context.Context, store *Store, failure, _ sessiontree.Entry) error {
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET raw = raw || ' ' WHERE thread_id = 'thread' AND id = ?`, failure.ID)
				return err
			}},
			{name: "raw hash", mutate: func(ctx context.Context, store *Store, failure, _ sessiontree.Entry) error {
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET raw_hash = ? WHERE thread_id = 'thread' AND id = ?`, sessiontree.StableHash("tampered"), failure.ID)
				return err
			}},
			{name: "self consistent message", mutate: func(ctx context.Context, store *Store, failure, _ sessiontree.Entry) error {
				failure.Error = "tampered message"
				failure.Raw = sessiontree.RawForEntry(failure)
				failure.RawHash = sessiontree.StableHash(failure.Raw)
				_, err := store.db.ExecContext(ctx, `UPDATE entries SET error = ?, raw = ?, raw_hash = ? WHERE thread_id = 'thread' AND id = ?`,
					failure.Error, failure.Raw, failure.RawHash, failure.ID)
				return err
			}},
			{name: "self consistent raw and hash", mutate: func(ctx context.Context, store *Store, failure, _ sessiontree.Entry) error {
				if failure.Metadata == nil {
					failure.Metadata = map[string]string{}
				}
				failure.Metadata["tampered"] = "true"
				failure.Raw = sessiontree.RawForEntry(failure)
				failure.RawHash = sessiontree.StableHash(failure.Raw)
				metadataJSON, err := json.Marshal(failure.Metadata)
				if err != nil {
					return err
				}
				_, err = store.db.ExecContext(ctx, `UPDATE entries SET metadata_json = ?, raw = ?, raw_hash = ? WHERE thread_id = 'thread' AND id = ?`,
					string(metadataJSON), failure.Raw, failure.RawHash, failure.ID)
				return err
			}},
		} {
			t.Run(failureCase.name+"/"+tamperCase.name, func(t *testing.T) {
				ctx := context.Background()
				current := time.Date(2026, time.July, 21, 9, 45, 0, 0, time.UTC)
				store, err := Open(filepath.Join(t.TempDir(), "source-failure-tamper.db"), WithAuthorityClock(func() time.Time { return current }))
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
				failure, err := store.Append(sessiontree.ContextWithTurnLease(ctx, admitted.Lease), sessiontree.Entry{
					ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryRunFailure,
					Error: "canonical failure", Metadata: failureCase.metadata,
				}, sessiontree.AppendOptions{Now: current.Add(time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				current = admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
				request := sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease, Now: current}
				recovered, err := store.RecoverInterruptedTurn(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if recovered.Status != sessiontree.TurnFailed || recovered.Failure != nil || recovered.Terminal.ParentID != failure.ID ||
					recovered.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != failureCase.wantCode ||
					recovered.Terminal.Metadata[sessiontree.InterruptedTurnRecoverySourceFailureEntryKey] != failure.ID ||
					recovered.Terminal.Metadata[sessiontree.InterruptedTurnRecoverySourceFailureRawHashKey] != failure.RawHash {
					t.Fatalf("recovered=%#v failure=%#v", recovered, failure)
				}
				if replayed, err := store.RecoverInterruptedTurn(ctx, request); err != nil || !replayed.Replayed {
					t.Fatalf("clean replay=%#v err=%v", replayed, err)
				}
				if err := tamperCase.mutate(ctx, store, failure, recovered.Terminal); err != nil {
					t.Fatal(err)
				}
				if err := store.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
					t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
				}
				if _, err := store.RecoverInterruptedTurn(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
					t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
				}
			})
		}
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

func TestSQLiteInterruptedTurnRecoveryRejectsEffectRunDriftBeforeMutation(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, time.July, 20, 13, 45, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "effect-run-drift.db"), WithAuthorityClock(func() time.Time { return current }))
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
	prepared, err := store.PrepareEffectAttempt(ctx, effectPrepareRequest(admitted.Lease, "run", "call", "arguments", "effect-request", current))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET run_id = 'different-run' WHERE effect_attempt_id = ?`, prepared.Attempt.EffectAttemptID); err != nil {
		t.Fatal(err)
	}
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

	current = admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	if _, err := store.RecoverInterruptedTurn(ctx, sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
	}
	var afterEntries, afterGeneration, afterLeases int
	var attemptState string
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE thread_id = 'thread'`).Scan(&afterEntries); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = 'thread'`).Scan(&afterGeneration); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_turn_leases WHERE thread_id = 'thread'`).Scan(&afterLeases); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT state FROM effect_attempts WHERE effect_attempt_id = ?`, prepared.Attempt.EffectAttemptID).Scan(&attemptState); err != nil {
		t.Fatal(err)
	}
	if afterEntries != beforeEntries || afterGeneration != beforeGeneration || afterLeases != beforeLeases || attemptState != string(sessiontree.EffectAttemptPrepared) {
		t.Fatalf("effect run drift mutated authority: entries %d->%d generation %d->%d leases %d->%d attempt=%q",
			beforeEntries, afterEntries, beforeGeneration, afterGeneration, beforeLeases, afterLeases, attemptState)
	}
}

func TestSQLiteInterruptedTurnRecoveryReplayRejectsEffectRunDrift(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, time.July, 20, 13, 50, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "effect-replay-run-drift.db"), WithAuthorityClock(func() time.Time { return current }))
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
	prepared, err := store.PrepareEffectAttempt(ctx, effectPrepareRequest(admitted.Lease, "run", "call", "arguments", "effect-request", current))
	if err != nil {
		t.Fatal(err)
	}
	current = admitted.Lease.ExpiresAt.Add(sessiontree.DefaultLeasePolicy.ClockSkewAllowance + time.Nanosecond)
	request := sessiontree.RecoverInterruptedTurnRequest{ExpectedLease: admitted.Lease}
	if _, err := store.RecoverInterruptedTurn(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE effect_attempts SET run_id = 'different-run' WHERE effect_attempt_id = ?`, prepared.Attempt.EffectAttemptID); err != nil {
		t.Fatal(err)
	}

	if err := store.ValidateInterruptedTurnResolution(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("ValidateInterruptedTurnResolution err=%v, want ErrAuthorityCorrupt", err)
	}
	if _, err := store.RecoverInterruptedTurn(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("RecoverInterruptedTurn err=%v, want ErrAuthorityCorrupt", err)
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

func admitInterruptedApprovalTurn(t *testing.T, repo interruptedTurnRecoveryTestRepo, now time.Time, threadID, turnID, runID string) sessiontree.TurnLease {
	t.Helper()
	admitted, err := repo.AdmitTurn(context.Background(), sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "owner-" + threadID,
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit-" + turnID, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.Lease
}
