package runtime

import (
	"testing"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
)

func TestResolveApprovalRequestRequiresExactAuthority(t *testing.T) {
	valid := ResolveApprovalRequest{
		DecisionID: "decision", ExpectedRootThreadID: "root", ExpectedGeneration: 1, ExpectedRevision: 2,
		ExpectedCurrent: ApprovalIdentity{
			ApprovalID: "approval", ThreadID: "thread", TurnID: "turn", RunID: "run",
			ToolCallID: "call", EffectAttemptID: "effect",
		},
		ExpectedApprovalRevision: 1, Decision: ApprovalDecisionApprove,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}
	for name, mutate := range map[string]func(*ResolveApprovalRequest){
		"decision": func(req *ResolveApprovalRequest) { req.DecisionID = "" },
		"root":     func(req *ResolveApprovalRequest) { req.ExpectedRootThreadID = "" },
		"revision": func(req *ResolveApprovalRequest) { req.ExpectedRevision = 0 },
		"identity": func(req *ResolveApprovalRequest) { req.ExpectedCurrent.EffectAttemptID = "" },
		"choice":   func(req *ResolveApprovalRequest) { req.Decision = "later" },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("invalid approval request passed validation")
			}
		})
	}
}

func TestApprovalDecisionReceiptValidatesLifecycle(t *testing.T) {
	now := time.Now().UTC()
	base := ApprovalDecisionReceipt{
		DecisionID: "decision", ApprovalID: "approval", RootThreadID: "root",
		Decision: ApprovalDecisionApprove, QueueGeneration: 1, QueueRevision: 2,
		ApprovalRevision: 2, SubmittedAt: now,
	}
	submitted := base
	submitted.State = string(sessiontree.ApprovalDecisionSubmitted)
	if err := submitted.Validate(); err != nil {
		t.Fatalf("submitted receipt: %v", err)
	}
	approved := base
	approved.State = string(sessiontree.ApprovalApproved)
	approved.AuthorizationProofHash = "proof"
	approved.ResolvedAt = now.Add(time.Second)
	if err := approved.Validate(); err != nil {
		t.Fatalf("approved receipt: %v", err)
	}
	rejected := base
	rejected.Decision = ApprovalDecisionReject
	rejected.State = string(sessiontree.ApprovalRejected)
	rejected.Reason = sessiontree.ApprovalReasonUserRejected
	rejected.ResolvedAt = now.Add(time.Second)
	if err := rejected.Validate(); err != nil {
		t.Fatalf("rejected receipt: %v", err)
	}
	rejected.AuthorizationProofHash = "unexpected"
	if err := rejected.Validate(); err == nil {
		t.Fatal("rejected receipt with proof passed validation")
	}
	policyRejected := base
	policyRejected.State = string(sessiontree.ApprovalRejected)
	policyRejected.Reason = sessiontree.ApprovalReasonPolicyDenied
	policyRejected.ResolvedAt = now.Add(time.Second)
	if err := policyRejected.Validate(); err != nil {
		t.Fatalf("approved decision rejected by policy: %v", err)
	}
	policyRejected.Decision = ApprovalDecisionReject
	if err := policyRejected.Validate(); err == nil {
		t.Fatal("user rejection with policy reason passed validation")
	}
}

func TestApprovalRecordValidatesCompleteLifecycle(t *testing.T) {
	now := time.Now().UTC()
	base := ApprovalRecord{
		ApprovalID: "approval", RootThreadID: "root", ThreadID: "thread", TurnID: "turn", RunID: "run",
		ToolCallID: "call", EffectAttemptID: "effect", ToolName: "write_file", ToolKind: "local",
		Step: 1, BatchSize: 1, State: string(sessiontree.ApprovalRequested), Revision: 1, QueueSequence: 1,
		RequestedAt: now, UpdatedAt: now, ArgsHash: "args", RequestFingerprint: "request",
		Resources: []ApprovalResource{{Kind: "file", Value: "notes.md"}}, Effects: []string{"write"},
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*ApprovalRecord)
	}{
		{name: "requested", mutate: func(*ApprovalRecord) {}},
		{name: "decision submitted", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalDecisionSubmitted)
			record.DecisionID = "decision"
			record.Revision++
			record.UpdatedAt = now.Add(time.Second)
		}},
		{name: "approved", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalApproved)
			record.DecisionID = "decision"
			record.AuthorizationProofHash = "proof"
			record.Revision += 2
			record.UpdatedAt = now.Add(2 * time.Second)
			record.ResolvedAt = record.UpdatedAt
		}},
		{name: "rejected", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalRejected)
			record.DecisionID = "decision"
			record.Reason = sessiontree.ApprovalReasonUserRejected
			record.Revision++
			record.UpdatedAt = now.Add(time.Second)
			record.ResolvedAt = record.UpdatedAt
		}},
		{name: "failed", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalFailed)
			record.DecisionID = "failure"
			record.Reason = sessiontree.ApprovalReasonAuthorizationUnavailable
			record.Revision++
			record.UpdatedAt = now.Add(time.Second)
			record.ResolvedAt = record.UpdatedAt
		}},
		{name: "timed out", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalTimedOut)
			record.DecisionID = "timeout"
			record.Reason = sessiontree.ApprovalReasonTimedOut
			record.Revision++
			record.UpdatedAt = now.Add(time.Second)
			record.ResolvedAt = record.UpdatedAt
		}},
		{name: "cancelled", mutate: func(record *ApprovalRecord) {
			record.State = string(sessiontree.ApprovalCancelled)
			record.DecisionID = "cancellation"
			record.Reason = sessiontree.ApprovalReasonCancelled
			record.Revision++
			record.UpdatedAt = now.Add(time.Second)
			record.ResolvedAt = record.UpdatedAt
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			record := base
			testCase.mutate(&record)
			if err := record.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestApprovalQueueRejectsTerminalRecord(t *testing.T) {
	now := time.Now().UTC()
	record := ApprovalRecord{
		ApprovalID: "approval", RootThreadID: "root", ThreadID: "thread", TurnID: "turn", RunID: "run",
		ToolCallID: "call", EffectAttemptID: "effect", ToolName: "write_file", ToolKind: "local",
		Step: 1, BatchSize: 1, State: string(sessiontree.ApprovalRejected), Revision: 2, QueueSequence: 1,
		DecisionID: "decision", Reason: sessiontree.ApprovalReasonUserRejected,
		RequestedAt: now, UpdatedAt: now.Add(time.Second), ResolvedAt: now.Add(time.Second),
		ArgsHash: "args", RequestFingerprint: "request",
	}
	queue := ApprovalQueue{
		RootThreadID: "root", Generation: 1, Revision: 2, CurrentApprovalID: record.ApprovalID,
		Items: []ApprovalRecord{record}, GeneratedAt: now.Add(time.Second),
	}
	if err := queue.Validate(); err == nil {
		t.Fatal("terminal approval passed queue validation")
	}
}
