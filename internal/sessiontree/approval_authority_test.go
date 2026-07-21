package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryFinalizeApprovalRejectsAuthorityTamperingWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 21, 13, 0, 0, 0, time.UTC)
	type authoritySnapshot struct {
		approval ApprovalRecord
		effect   EffectAttempt
		queue    approvalQueueLedger
		decision approvalDecisionLedger
		thread   ThreadMeta
		entries  []Entry
	}
	setup := func(t *testing.T, terminal bool) (*MemoryRepo, FinalizeApprovalRequest, func() authoritySnapshot) {
		t.Helper()
		repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
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
		prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
		if err != nil {
			t.Fatal(err)
		}
		record := prepared.Approvals[0]
		submitted, err := repo.ResolveApproval(ctx, ResolveApprovalRequest{
			DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
			Decision: ApprovalDecisionApprove, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		req := FinalizeApprovalRequest{
			ResolutionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
			ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
			State: ApprovalFailed, Reason: ApprovalReasonAuthorizationUnavailable, Now: now,
			FinalizedEntry: Entry{
				ID: ApprovalFinalizationEntryID("decision", record.ApprovalID), ThreadID: "thread", TurnID: "turn",
				Type: EntryCustom, Metadata: map[string]string{"approval_state": "failed", "approval_reason": ApprovalReasonAuthorizationUnavailable},
			},
		}
		if terminal {
			if _, err := repo.FinalizeApproval(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
		snapshot := func() authoritySnapshot {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			entries := make([]Entry, len(repo.entries["thread"]))
			for index := range entries {
				entries[index] = cloneEntry(repo.entries["thread"][index])
			}
			return authoritySnapshot{
				approval: cloneApprovalRecord(repo.approvals[record.ApprovalID]),
				effect:   cloneEffectAttempt(repo.effectAttempts[record.EffectAttemptID]),
				queue:    repo.approvalQueues["thread"], decision: repo.approvalDecisions["decision"],
				thread: repo.threads["thread"], entries: entries,
			}
		}
		return repo, req, snapshot
	}
	tests := []struct {
		name     string
		terminal bool
		mutate   func(*MemoryRepo, FinalizeApprovalRequest)
	}{
		{name: "first/record identity", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			record := repo.approvals[req.ExpectedCurrent.ApprovalID]
			record.ToolName = "tampered"
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "first/record revision", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			record := repo.approvals[req.ExpectedCurrent.ApprovalID]
			record.Revision++
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "first/effect invocation", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.Invocation.ToolName = "tampered"
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/effect state", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.State = EffectAttemptCancelled
			effect.TerminalFingerprint = "tampered"
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/effect request fingerprint", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.RequestFingerprint = ""
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/effect rejection code", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.State = EffectAttemptRejected
			effect.RejectionCode = "tampered"
			effect.TerminalFingerprint = "tampered"
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/effect updated at", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.UpdatedAt = effect.UpdatedAt.Add(time.Second)
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/effect owner", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.OwnerID = ""
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "first/queue revision", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			queue := repo.approvalQueues["thread"]
			queue.Revision = 0
			repo.approvalQueues["thread"] = queue
		}},
		{name: "first/queue current", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			queue := repo.approvalQueues["thread"]
			queue.CurrentApprovalID = ""
			repo.approvalQueues["thread"] = queue
		}},
		{name: "first/receipt state", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.State = ApprovalFailed
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "first/receipt reason", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.Reason = "tampered"
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "first/receipt revisions", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.QueueRevision++
			decision.Receipt.ApprovalRevision++
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "first/receipt submitted at", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.SubmittedAt = decision.Receipt.SubmittedAt.Add(time.Second)
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "first/receipt resolved at", mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.ResolvedAt = now
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "first/requested entry", mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			entries := repo.entries["thread"]
			for index := range entries {
				if entries[index].ID == ApprovalRequestedEntryID(req.ExpectedCurrent.ApprovalID) {
					entries[index].RawHash = StableHash("tampered")
				}
			}
			repo.entries["thread"] = entries
		}},
		{name: "replay/record identity", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			record := repo.approvals[req.ExpectedCurrent.ApprovalID]
			record.ToolName = "tampered"
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "replay/record revision", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			record := repo.approvals[req.ExpectedCurrent.ApprovalID]
			record.Revision++
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "replay/effect invocation", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.Invocation.ToolName = "tampered"
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/effect state", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.State = EffectAttemptCancelled
			effect.RejectionCode = ""
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/effect request fingerprint", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.RequestFingerprint = ""
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/effect terminal fingerprint", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.TerminalFingerprint = StableHash("tampered")
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/effect rejection code", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.RejectionCode = "tampered"
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/effect updated at", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			effect := repo.effectAttempts[req.ExpectedCurrent.EffectAttemptID]
			effect.UpdatedAt = effect.UpdatedAt.Add(time.Second)
			repo.effectAttempts[effect.EffectAttemptID] = effect
		}},
		{name: "replay/queue revision", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			queue := repo.approvalQueues["thread"]
			queue.Revision = 0
			repo.approvalQueues["thread"] = queue
		}},
		{name: "replay/queue current", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			queue := repo.approvalQueues["thread"]
			queue.CurrentApprovalID = req.ExpectedCurrent.ApprovalID
			repo.approvalQueues["thread"] = queue
		}},
		{name: "replay/receipt state", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.State = ApprovalCancelled
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "replay/receipt reason", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.Reason = "tampered"
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "replay/receipt revisions", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.QueueRevision++
			decision.Receipt.ApprovalRevision++
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "replay/receipt submitted at", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.SubmittedAt = decision.Receipt.SubmittedAt.Add(time.Second)
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "replay/receipt resolved at", terminal: true, mutate: func(repo *MemoryRepo, _ FinalizeApprovalRequest) {
			decision := repo.approvalDecisions["decision"]
			decision.Receipt.ResolvedAt = decision.Receipt.ResolvedAt.Add(time.Second)
			repo.approvalDecisions["decision"] = decision
		}},
		{name: "replay/finalized entry", terminal: true, mutate: func(repo *MemoryRepo, req FinalizeApprovalRequest) {
			entries := repo.entries["thread"]
			for index := range entries {
				if entries[index].ID == req.FinalizedEntry.ID {
					entries[index].RawHash = StableHash("tampered")
				}
			}
			repo.entries["thread"] = entries
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, req, snapshot := setup(t, test.terminal)
			repo.mu.Lock()
			test.mutate(repo, req)
			repo.mu.Unlock()
			before := snapshot()
			if _, err := repo.FinalizeApproval(context.Background(), req); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("FinalizeApproval err=%v, want ErrAuthorityCorrupt", err)
			}
			if after := snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("failed finalization mutated authority:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestMemoryWaitApprovalDecisionObservesCommittedDecision(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 55, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	ctx := context.Background()
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
	prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	type waitResult struct {
		result WaitApprovalDecisionResult
		err    error
	}
	done := make(chan waitResult, 1)
	go func() {
		result, err := repo.WaitApprovalDecision(ctx, record.ApprovalID)
		done <- waitResult{result: result, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		repo.mu.Lock()
		_, subscribed := repo.approvalSignals[record.ApprovalID]
		repo.mu.Unlock()
		if subscribed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval waiter did not subscribe")
		}
		runtime.Gosched()
	}
	resolved, err := repo.ResolveApproval(ctx, ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: ApprovalDecisionApprove, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case waited := <-done:
		if waited.err != nil || waited.result.Approval.State != ApprovalDecisionSubmitted ||
			waited.result.Approval.DecisionID != resolved.Approval.DecisionID || waited.result.Queue.Revision < resolved.Queue.Revision {
			t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
		}
	case <-time.After(time.Second):
		t.Fatal("approval waiter was not notified")
	}
}

func TestMemoryFinalizeApprovalNotifiesWaiter(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 58, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	ctx := context.Background()
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
	prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	done := make(chan WaitApprovalDecisionResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := repo.WaitApprovalDecision(ctx, record.ApprovalID)
		done <- result
		errs <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		repo.mu.Lock()
		_, subscribed := repo.approvalSignals[record.ApprovalID]
		repo.mu.Unlock()
		if subscribed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("approval waiter did not subscribe")
		}
		runtime.Gosched()
	}
	if _, err := repo.FinalizeApproval(ctx, FinalizeApprovalRequest{
		ResolutionID: "timeout", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
		State: ApprovalTimedOut, Reason: ApprovalReasonTimedOut, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case waited := <-done:
		err := <-errs
		if err != nil || waited.Approval.State != ApprovalTimedOut || waited.Receipt.Decision != ApprovalDecisionApprove ||
			waited.Receipt.State != ApprovalTimedOut || waited.Receipt.Reason != ApprovalReasonTimedOut {
			t.Fatalf("waited=%#v err=%v", waited, err)
		}
	case <-time.After(time.Second):
		t.Fatal("finalization did not notify approval waiter")
	}
}

func TestMemoryWaitApprovalDecisionRejectsMissingPostDecisionReceipt(t *testing.T) {
	now := time.Date(2026, 7, 21, 8, 59, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	ctx := context.Background()
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
	prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
	if err != nil {
		t.Fatal(err)
	}
	record := prepared.Approvals[0]
	if _, err := repo.ResolveApproval(ctx, ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
		ExpectedApprovalRevision: record.Revision, Decision: ApprovalDecisionApprove, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	delete(repo.approvalDecisions, "decision")
	repo.mu.Unlock()
	if _, err := repo.WaitApprovalDecision(ctx, record.ApprovalID); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("WaitApprovalDecision err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryPrepareApprovalBatchRollsBackRequestedEntryConflict(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	ctx := context.Background()
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
	req := memoryInterruptedApprovalPrepare(admitted.Lease, now)
	second := req.Items[0]
	second.Invocation.ToolCallID = "call-2"
	second.Invocation.ArgumentHash = "args-2"
	second.BatchIndex = 1
	second.BatchSize = 2
	second.EffectRequestFingerprint = "effect-call-2"
	second.ApprovalRequestFingerprint = "approval-call-2"
	second.EffectAttemptID = ApprovalEffectAttemptID(second.Invocation)
	second.RequestedEntry.ID = ApprovalRequestedEntryID(second.EffectAttemptID)
	req.Items[0].BatchSize = 2
	req.Items = append(req.Items, second)

	collision := second.RequestedEntry
	collision.Metadata = map[string]string{"kind": "preexisting"}
	if _, err := repo.Append(ContextWithTurnLease(ctx, admitted.Lease), collision, AppendOptions{ID: collision.ID, Now: now}); err != nil {
		t.Fatal(err)
	}
	before, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PrepareApprovalBatch(ctx, req); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("PrepareApprovalBatch err=%v, want ErrRequestConflict", err)
	}
	after, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(before, after, func(left, right Entry) bool { return left.ID == right.ID && left.RawHash == right.RawHash }) {
		t.Fatalf("entries mutated: before=%#v after=%#v", before, after)
	}
	queue, err := repo.ReadApprovalQueue(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if queue.Generation != 0 || queue.Revision != 0 || queue.CurrentApprovalID != "" || len(queue.Items) != 0 {
		t.Fatalf("queue mutated: %#v", queue)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.approvals) != 0 || len(repo.approvalByEffectAttempt) != 0 || len(repo.effectAttempts) != 0 || len(repo.effectAttemptByInvocation) != 0 {
		t.Fatalf("authority mutated: approvals=%#v effects=%#v", repo.approvals, repo.effectAttempts)
	}
}

func TestMemoryApprovalDispatchReplayRejectsCorruptAuthority(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 15, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, ApprovalRecord)
	}{
		{name: "approval state", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			record.State = ApprovalFailed
			record.Reason = ApprovalReasonAuthorizationUnavailable
			record.AuthorizationProofHash = ""
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "approval revision", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			record.Revision++
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "effect state", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			effect := repo.effectAttempts[record.EffectAttemptID]
			effect.State = EffectAttemptCancelled
			effect.TerminalFingerprint = "corrupt"
			repo.effectAttempts[record.EffectAttemptID] = effect
		}},
		{name: "receipt proof", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.AuthorizationProofHash = "different-proof"
			repo.approvalDecisions[record.DecisionID] = decision
		}},
		{name: "receipt revision", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.ApprovalRevision--
			repo.approvalDecisions[record.DecisionID] = decision
		}},
		{name: "queue revision", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			queue := repo.approvalQueues[record.RootThreadID]
			queue.Revision--
			repo.approvalQueues[record.RootThreadID] = queue
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo := NewMemoryRepo()
			ctx := context.Background()
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
			prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			submitted, err := repo.ResolveApproval(ctx, ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			request := CommitApprovalDispatchRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: submitted.Approval.Revision,
				EffectAttemptID: record.EffectAttemptID, Lease: admitted.Lease, AuthorizationProofHash: "proof",
				ApprovedEntry: Entry{
					ID: ApprovalDispatchEntryID("decision", record.ApprovalID), ThreadID: "thread", TurnID: "turn",
					Type: EntryCustom, Metadata: map[string]string{"approval_state": "approved"},
				},
				Now: now.Add(2 * time.Second),
			}
			if _, err := repo.CommitApprovalDispatch(ctx, request); err != nil {
				t.Fatal(err)
			}
			repo.mu.Lock()
			record = repo.approvals[record.ApprovalID]
			testCase.mutate(repo, record)
			repo.mu.Unlock()

			if _, err := repo.CommitApprovalDispatch(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("CommitApprovalDispatch err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}

func TestMemoryResolveApprovalReplayRejectsCorruptAuthority(t *testing.T) {
	now := time.Date(2026, 7, 21, 9, 12, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*MemoryRepo, ApprovalRecord)
	}{
		{name: "receipt reason", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			decision := repo.approvalDecisions[record.DecisionID]
			decision.Receipt.Reason = ApprovalReasonPolicyDenied
			repo.approvalDecisions[record.DecisionID] = decision
		}},
		{name: "record reason", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			record.Reason = ApprovalReasonPolicyDenied
			repo.approvals[record.ApprovalID] = record
		}},
		{name: "effect rejection", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			effect := repo.effectAttempts[record.EffectAttemptID]
			effect.RejectionCode = ApprovalReasonPolicyDenied
			repo.effectAttempts[record.EffectAttemptID] = effect
		}},
		{name: "effect owner", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			effect := repo.effectAttempts[record.EffectAttemptID]
			effect.OwnerID = ""
			repo.effectAttempts[record.EffectAttemptID] = effect
		}},
		{name: "queue revision", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			queue := repo.approvalQueues[record.RootThreadID]
			queue.Revision = 0
			repo.approvalQueues[record.RootThreadID] = queue
		}},
		{name: "terminal retained as current", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			queue := repo.approvalQueues[record.RootThreadID]
			queue.CurrentApprovalID = record.ApprovalID
			repo.approvalQueues[record.RootThreadID] = queue
		}},
		{name: "rejected entry", mutate: func(repo *MemoryRepo, record ApprovalRecord) {
			entries := repo.entries[record.ThreadID]
			for index := range entries {
				if entries[index].ID == ApprovalRejectedEntryID(record.DecisionID, record.ApprovalID) {
					repo.entries[record.ThreadID] = append(entries[:index], entries[index+1:]...)
					break
				}
			}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo := NewMemoryRepo()
			ctx := context.Background()
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
			prepared, err := repo.PrepareApprovalBatch(ctx, memoryInterruptedApprovalPrepare(admitted.Lease, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			request := ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: ApprovalDecisionReject,
				RejectedEntry: Entry{
					ID: ApprovalRejectedEntryID("decision", record.ApprovalID), ThreadID: "thread", TurnID: "turn",
					Type: EntryCustom, Metadata: map[string]string{"approval_state": "rejected"},
				},
				Now: now.Add(time.Second),
			}
			resolved, err := repo.ResolveApproval(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			repo.mu.Lock()
			testCase.mutate(repo, resolved.Approval)
			repo.mu.Unlock()
			if _, err := repo.ResolveApproval(ctx, request); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ResolveApproval err=%v, want ErrAuthorityCorrupt", err)
			}
		})
	}
}
