package sessiontree

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryRootAuthorityCreateDeleteAndTombstoneReplay(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	req := CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1",
		Meta: ThreadMeta{ID: "root"},
	}
	created, err := repo.CreateRoot(ctx, req)
	if err != nil || created.Replayed || created.Thread.ID != "root" || created.Thread.Lifecycle != ThreadLifecycleOpen {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	replayed, err := repo.CreateRoot(ctx, req)
	if err != nil || !replayed.Replayed || replayed.Thread.ID != created.Thread.ID {
		t.Fatalf("replayed=%#v err=%v", replayed, err)
	}
	newIntentForLiveThread := req
	newIntentForLiveThread.CreateIntentID = "create-root-live-conflict"
	if _, err := repo.CreateRoot(ctx, newIntentForLiveThread); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("new intent for live thread err=%v, want ErrRequestConflict", err)
	}
	changed := req
	changed.ThreadID = "other"
	changed.Meta.ID = "other"
	if _, err := repo.CreateRoot(ctx, changed); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed create intent err=%v, want ErrRequestConflict", err)
	}
	deleted, err := repo.DeleteRootTree(ctx, "root")
	if err != nil || deleted.Replayed || len(deleted.ThreadIDs) != 1 || deleted.ThreadIDs[0] != "root" {
		t.Fatalf("deleted=%#v err=%v", deleted, err)
	}
	if _, err := repo.Thread(ctx, "root"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("deleted canonical read err=%v, want ErrThreadNotFound", err)
	}
	tombstone, err := repo.ThreadTombstone(ctx, "root")
	if err != nil || tombstone.CreateIntentID != "create-root" || tombstone.RootThreadID != "root" || !tombstone.DeletedAt.Equal(now) {
		t.Fatalf("tombstone=%#v err=%v", tombstone, err)
	}
	if _, err := repo.CreateRoot(ctx, req); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("exact create replay after delete err=%v, want ErrThreadDeleted", err)
	}
	newIntent := req
	newIntent.CreateIntentID = "create-root-again"
	if _, err := repo.CreateRoot(ctx, newIntent); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("new create intent after delete err=%v, want ErrThreadDeleted", err)
	}
	replayedDelete, err := repo.DeleteRootTree(ctx, "root")
	if err != nil || !replayedDelete.Replayed || len(replayedDelete.ThreadIDs) != 1 {
		t.Fatalf("delete replay=%#v err=%v", replayedDelete, err)
	}
}

func TestMemoryRootCreateReplayRejectsNonCanonicalLiveShape(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ThreadMeta)
	}{
		{name: "closed root", mutate: func(meta *ThreadMeta) { meta.Lifecycle = ThreadLifecycleClosed }},
		{name: "child ownership", mutate: func(meta *ThreadMeta) {
			meta.ParentThreadID = "parent"
			meta.ParentTurnID = "parent-turn"
			meta.TaskName = "child"
			meta.AgentPath = "child"
		}},
		{name: "fork provenance", mutate: func(meta *ThreadMeta) {
			meta.ForkedFromThreadID = "source"
			meta.ForkedFromEntryID = "source-entry"
		}},
		{name: "malformed fork operation", mutate: func(meta *ThreadMeta) { meta.ForkOperationID = "operation" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewMemoryRepo()
			req := CreateRootRequest{ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: ThreadMeta{ID: "root"}}
			if _, err := repo.CreateRoot(ctx, req); err != nil {
				t.Fatal(err)
			}
			repo.mu.Lock()
			meta := repo.threads["root"]
			test.mutate(&meta)
			repo.threads["root"] = meta
			repo.mu.Unlock()

			if _, err := repo.CreateRoot(ctx, req); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("CreateRoot replay err=%v, want ErrRequestConflict", err)
			}
		})
	}
}

func TestCreateRootRequestRejectsAllForkProvenance(t *testing.T) {
	for _, mutate := range []func(*ThreadMeta){
		func(meta *ThreadMeta) { meta.ForkedFromEntryID = "entry" },
		func(meta *ThreadMeta) { meta.ForkOperationNodeID = "root" },
	} {
		req := CreateRootRequest{ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: ThreadMeta{ID: "root"}}
		mutate(&req.Meta)
		if err := ValidateCreateRootRequest(req); err == nil {
			t.Fatalf("ValidateCreateRootRequest accepted fork provenance: %#v", req.Meta)
		}
	}
}

func TestMemoryAuthorityInspectionRejectsLeaseLedgerAndLifecycleCorruption(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 11, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "/root/child"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.InspectSubAgentThreadAuthority(ctx, "other", "child"); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("foreign child err=%v, want ErrSubAgentNotFound", err)
	}
	if _, err := repo.InspectSubAgentThreadAuthority(ctx, "root", "missing"); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("missing child err=%v, want ErrSubAgentNotFound", err)
	}
	repo.mu.Lock()
	repo.leaseGeneration["child"] = admitted.Lease.Generation + 1
	repo.mu.Unlock()
	if _, err := repo.InspectThreadAuthority(ctx, "child"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("lease generation mismatch err=%v, want ErrAuthorityCorrupt", err)
	}
	repo.mu.Lock()
	repo.leaseGeneration["child"] = admitted.Lease.Generation
	child := repo.threads["child"]
	child.Lifecycle = ThreadLifecycleClosed
	repo.threads["child"] = child
	repo.mu.Unlock()
	if _, err := repo.InspectThreadAuthority(ctx, "child"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("closed child with lease err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryRootDeleteClearsQueryableAuthorityAndRetainsRequestIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 5, 30, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateRoot(ctx, CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1", Meta: ThreadMeta{ID: "root"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "child", ParentThreadID: "root", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child",
	}); err != nil {
		t.Fatal(err)
	}

	invocation := EffectInvocationIdentity{ThreadID: "root", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", ArgumentHash: "arguments"}
	repo.mu.Lock()
	repo.entries["root"] = []Entry{{ID: "root-entry", ThreadID: "root", Type: EntryTurnMarker}}
	repo.entries["child"] = []Entry{{ID: "child-entry", ThreadID: "child", Type: EntryTurnMarker}}
	repo.todos["root"] = AgentTodoState{ThreadID: "root", Version: 1}
	repo.providerStates["root"] = ProviderStateRecord{ThreadID: "root"}
	repo.leaseGeneration["root"] = 7
	repo.turnAdmissions[turnAdmissionKey("root", "turn")] = turnAdmissionLedger{ThreadID: "root", TurnID: "turn", RunID: "run"}
	repo.turnFinishes[turnAdmissionKey("root", "turn")] = turnFinishLedger{ThreadID: "root", TurnID: "turn", RunID: "run"}
	repo.effectAttempts["effect"] = EffectAttempt{EffectAttemptID: "effect", Invocation: invocation, State: EffectAttemptCompleted}
	repo.effectAttemptByInvocation[effectInvocationKey(invocation)] = "effect"
	repo.pendingToolCompletions["root-completion"] = pendingToolCompletionLedger{CompletionRequestID: "root-completion", ThreadID: "root"}
	repo.compactionOperations["compaction"] = CompactionOperation{ThreadID: "root", RequestID: "compaction", State: CompactionOperationCompleted}
	repo.subAgentCloseOperations["close"] = SubAgentCloseOperation{CloseOperationID: "close", ParentThreadID: "root", TargetThreadID: "child", State: SubAgentCloseCompleted}
	repo.subAgentInputs["child"] = []SubAgentInputRecord{{SubAgentInputID: "input-row", ParentThreadID: "root", ChildThreadID: "child", State: SubAgentInputPending}}
	repo.subAgentInputSequence["child"] = 1
	repo.subAgentPublications["publication-retained"] = subAgentRequestLedger{ParentThreadID: "root", ChildThreadID: "child", RequestFingerprint: "publication-fingerprint", SubAgentInputID: "input-row"}
	repo.subAgentInputRequests["input-retained"] = subAgentRequestLedger{ParentThreadID: "root", ChildThreadID: "child", RequestFingerprint: "input-fingerprint", SubAgentInputID: "input-row"}
	repo.subAgentPendingToolCompletions["completion-retained"] = subAgentPendingToolCompletionLedger{
		InputRequestID: "completion-retained", RequestFingerprint: "completion-fingerprint", SettlementFingerprint: "settlement-fingerprint",
		ParentThreadID: "root", ChildThreadID: "child", SettlementEntryID: "child-entry", SubAgentInputID: "input-row",
	}
	repo.mu.Unlock()

	if _, err := repo.DeleteRootTree(ctx, "root"); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	if len(repo.threads) != 0 || len(repo.entries) != 0 || len(repo.todos) != 0 || len(repo.providerStates) != 0 ||
		len(repo.leases) != 0 || len(repo.leaseGeneration) != 0 || len(repo.turnAdmissions) != 0 || len(repo.turnFinishes) != 0 ||
		len(repo.effectAttempts) != 0 || len(repo.effectAttemptByInvocation) != 0 || len(repo.pendingToolCompletions) != 0 ||
		len(repo.compactionOperations) != 0 || len(repo.subAgentCloseOperations) != 0 || len(repo.subAgentInputs) != 0 || len(repo.subAgentInputSequence) != 0 {
		t.Fatalf("root delete retained queryable authority: threads=%d entries=%d todos=%d provider=%d leases=%d generations=%d admissions=%d finishes=%d effects=%d effect-index=%d completions=%d compactions=%d closes=%d inputs=%d input-sequences=%d",
			len(repo.threads), len(repo.entries), len(repo.todos), len(repo.providerStates), len(repo.leases), len(repo.leaseGeneration),
			len(repo.turnAdmissions), len(repo.turnFinishes), len(repo.effectAttempts), len(repo.effectAttemptByInvocation),
			len(repo.pendingToolCompletions), len(repo.compactionOperations), len(repo.subAgentCloseOperations), len(repo.subAgentInputs), len(repo.subAgentInputSequence))
	}
	if len(repo.rootCreateIntents) != 1 || len(repo.tombstones) != 2 || len(repo.subAgentPublications) != 1 ||
		len(repo.subAgentInputRequests) != 1 || len(repo.subAgentPendingToolCompletions) != 1 {
		t.Fatalf("root delete dropped retained identity: create=%d tombstones=%d publications=%d inputs=%d completions=%d",
			len(repo.rootCreateIntents), len(repo.tombstones), len(repo.subAgentPublications), len(repo.subAgentInputRequests), len(repo.subAgentPendingToolCompletions))
	}
	repo.mu.Unlock()

	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "other-root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "other-child", ParentThreadID: "other-root", ParentTurnID: "other-turn", TaskName: "other", AgentPath: "other",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PublishSubAgent(ctx, PublishSubAgentRequest{
		PublicationID: "publication-retained", RequestFingerprint: "new-publication", ParentThreadID: "other-root",
		ChildMeta: ThreadMeta{ID: "new-child", ParentThreadID: "other-root"}, Message: session.Message{Role: session.User, Content: "start"},
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reused publication identity err=%v, want ErrRequestConflict", err)
	}
	if _, _, err := repo.PublishSubAgentInput(ctx, PublishSubAgentInputRequest{
		InputRequestID: "input-retained", RequestFingerprint: "new-input", ParentThreadID: "other-root", ChildThreadID: "other-child",
		Message: session.Message{Role: session.User, Content: "continue"},
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reused input identity err=%v, want ErrRequestConflict", err)
	}
	target := PendingToolSettlementTarget{
		ThreadID: "other-child", TurnID: "other-turn", RunID: "other-run", ToolCallID: "other-call", ToolName: "tool", Handle: "tool:other",
	}
	if _, err := repo.PublishSubAgentPendingToolCompletion(ctx, PublishSubAgentPendingToolCompletionRequest{
		InputRequestID: "completion-retained", RequestFingerprint: "new-completion", SettlementFingerprint: "new-settlement",
		ParentThreadID: "other-root", ChildThreadID: "other-child", Target: target,
		Settlement: Entry{ThreadID: "other-child", TurnID: "other-turn", Type: EntryCustom,
			Message:  session.Message{Role: session.Tool, ToolCallID: "other-call", ToolName: "tool", Content: "done"},
			Metadata: map[string]string{PendingToolSettlementKindKey: PendingToolSettlementKind, PendingToolSettlementFingerprintKey: "new-settlement"}},
		Message: session.Message{Role: session.User, Content: "continue"}, Now: now,
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reused pending completion identity err=%v, want ErrRequestConflict", err)
	}
}

func TestMemorySubAgentInputReplayPrecedesClaimAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 7, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker"}); err != nil {
		t.Fatal(err)
	}
	request := PublishSubAgentInputRequest{
		InputRequestID: "input", RequestFingerprint: "fingerprint", ParentThreadID: "parent", ChildThreadID: "child",
		Message: session.Message{Role: session.User, Content: "continue"}, Now: now,
	}
	input, replayed, err := repo.PublishSubAgentInput(ctx, request)
	if err != nil || replayed {
		t.Fatalf("input=%#v replayed=%v err=%v", input, replayed, err)
	}
	admitted, err := repo.AdmitSubAgentInput(ctx, AdmitSubAgentInputRequest{
		ParentThreadID: "parent", ChildThreadID: "child", TurnID: "turn", RunID: "run", OwnerID: "owner", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, replayed, err := repo.PublishSubAgentInput(ctx, request)
	if err != nil || !replayed || got.SubAgentInputID != input.SubAgentInputID || got.State != SubAgentInputAdmitted {
		t.Fatalf("leased replay=%#v replayed=%v err=%v", got, replayed, err)
	}
	changed := request
	changed.Message.Content = "changed"
	changed.RequestFingerprint = "changed-fingerprint"
	if _, _, err := repo.PublishSubAgentInput(ctx, changed); !errors.Is(err, ErrSubAgentRequestConflict) || !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("leased changed replay err=%v, want request conflict", err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted,
		OutcomeFingerprint: "outcome", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AcquireThreadAuthorityClaim(ctx, "fork", []string{"child"}, []string{"child", "destination"}); err != nil {
		t.Fatal(err)
	}

	got, replayed, err = repo.PublishSubAgentInput(ctx, request)
	if err != nil || !replayed || got.SubAgentInputID != input.SubAgentInputID || got.State != SubAgentInputAdmitted {
		t.Fatalf("claimed replay=%#v replayed=%v err=%v", got, replayed, err)
	}
	if _, _, err := repo.PublishSubAgentInput(ctx, changed); !errors.Is(err, ErrSubAgentRequestConflict) || !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("claimed changed replay err=%v, want request conflict", err)
	}
	newRequest := request
	newRequest.InputRequestID = "new-input"
	newRequest.RequestFingerprint = "new-fingerprint"
	if _, _, err := repo.PublishSubAgentInput(ctx, newRequest); !errors.Is(err, ErrThreadAuthorityBusy) {
		t.Fatalf("new claimed input err=%v, want ErrThreadAuthorityBusy", err)
	}
}
