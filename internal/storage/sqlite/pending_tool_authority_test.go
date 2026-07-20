package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

type pendingToolAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	Entries(context.Context, string) ([]sessiontree.Entry, error)
	Append(context.Context, sessiontree.Entry, sessiontree.AppendOptions) (sessiontree.Entry, error)
	AcquireTurnLease(context.Context, sessiontree.TurnLease) (sessiontree.TurnLease, error)
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	SettlePendingToolRecovery(context.Context, sessiontree.SettlePendingToolRecoveryRequest) (sessiontree.SettlePendingToolRecoveryResult, error)
	AdmitPendingToolCompletion(context.Context, sessiontree.AdmitPendingToolCompletionRequest) (sessiontree.AdmitPendingToolCompletionResult, error)
	ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
	PublishSubAgentPendingToolCompletion(context.Context, sessiontree.PublishSubAgentPendingToolCompletionRequest) (sessiontree.PublishSubAgentPendingToolCompletionResult, error)
	AdmitSubAgentInput(context.Context, sessiontree.AdmitSubAgentInputRequest) (sessiontree.AdmitSubAgentInputResult, error)
	ListSubAgentInputs(context.Context, string, sessiontree.SubAgentInputState) ([]sessiontree.SubAgentInputRecord, error)
	PrepareEffectAttempt(context.Context, sessiontree.PrepareEffectAttemptRequest) (sessiontree.PrepareEffectAttemptResult, error)
	BeginEffectDispatch(context.Context, sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error)
	FinishEffectDispatch(context.Context, sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error)
}

func TestSQLiteSubAgentPendingToolCompletionMatchesMemory(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "subagent-pending-completion.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for backend, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(backend, func(t *testing.T) {
			request := seedSubAgentPendingToolCompletion(t, repo, fixed)
			published, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if published.Replayed || published.SettlementReplayed || published.Settlement.ID == "" ||
				published.Input.State != sessiontree.SubAgentInputPending ||
				published.Input.RequestKind != sessiontree.SubAgentRequestPendingToolCompletion ||
				published.Input.Message.Content != "continue after background work" {
				t.Fatalf("subagent pending completion = %#v", published)
			}
			if _, active, err := repo.ActiveTurnLease(context.Background(), "child"); err != nil || active {
				t.Fatalf("publication admitted child unexpectedly: active=%v err=%v", active, err)
			}
			pending, err := repo.ListSubAgentInputs(context.Background(), "child", sessiontree.SubAgentInputPending)
			if err != nil || len(pending) != 1 || pending[0].SubAgentInputID != published.Input.SubAgentInputID {
				t.Fatalf("pending child input = %#v err=%v", pending, err)
			}
			admitted, err := repo.AdmitSubAgentInput(context.Background(), sessiontree.AdmitSubAgentInputRequest{
				ParentThreadID: "parent", ChildThreadID: "child", TurnID: "turn-2", RunID: "run-2", OwnerID: "wait-owner", Now: fixed,
			})
			if err != nil || admitted.Input.SubAgentInputID != published.Input.SubAgentInputID || admitted.Input.State != sessiontree.SubAgentInputAdmitted {
				t.Fatalf("wait-owned child admission = %#v err=%v", admitted, err)
			}
			replayed, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), request)
			if err != nil || !replayed.Replayed || replayed.Input.State != sessiontree.SubAgentInputAdmitted || replayed.Input.AdmittedTurnID != "turn-2" {
				t.Fatalf("admitted completion replay = %#v err=%v", replayed, err)
			}
			conflict := request
			conflict.RequestFingerprint = "changed-completion-fingerprint"
			if _, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), conflict); !errors.Is(err, sessiontree.ErrSubAgentRequestConflict) || !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("subagent completion conflict err=%v, want durable request conflict", err)
			}
			targetConflict := request
			targetConflict.Target.EffectAttemptID = "different-effect-attempt"
			targetConflict.Settlement = pendingToolSettlementRequest(targetConflict.Target, targetConflict.SettlementFingerprint, fixed).Settlement
			if _, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), targetConflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("subagent completion target replay err=%v, want ErrRequestConflict", err)
			}
			if _, err := repo.FinishTurn(context.Background(), sessiontree.FinishTurnRequest{
				Lease: admitted.Lease, RunID: "run-2", TerminalEntryID: "turn-2-terminal", Status: sessiontree.TurnCompleted,
				OutcomeFingerprint: "turn-2-outcome", Now: fixed,
			}); err != nil {
				t.Fatal(err)
			}
			prepareSubAgentReplayClaim(t, repo, fixed)
			claimedReplay, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), request)
			if err != nil || !claimedReplay.Replayed || claimedReplay.Input.SubAgentInputID != published.Input.SubAgentInputID {
				t.Fatalf("claimed completion replay=%#v err=%v", claimedReplay, err)
			}
			if _, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), conflict); !errors.Is(err, sessiontree.ErrSubAgentRequestConflict) {
				t.Fatalf("claimed completion conflict err=%v, want durable request conflict", err)
			}
			entries, err := repo.Entries(context.Background(), "child")
			if err != nil || countPendingToolAuthoritySettlements(entries) != 1 {
				t.Fatalf("subagent settlement entries = %#v err=%v", entries, err)
			}
		})
	}
}

func TestSubAgentAdmissionRejectsHistoricalTurnIDWithoutMutation(t *testing.T) {
	fixed := time.Date(2026, 7, 20, 18, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "subagent-historical-turn.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for backend, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(backend, func(t *testing.T) {
			request := seedSubAgentPendingToolCompletion(t, repo, fixed)
			published, err := repo.PublishSubAgentPendingToolCompletion(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			before, err := repo.Entries(context.Background(), "child")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AdmitSubAgentInput(context.Background(), sessiontree.AdmitSubAgentInputRequest{
				ParentThreadID: "parent", ChildThreadID: "child", TurnID: "turn-1", RunID: "different-run", OwnerID: "owner", Now: fixed,
			}); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("historical subagent turn err=%v, want ErrRequestConflict", err)
			}
			after, err := repo.Entries(context.Background(), "child")
			if err != nil || len(after) != len(before) {
				t.Fatalf("historical subagent admission mutated journal: before=%d after=%#v err=%v", len(before), after, err)
			}
			pending, err := repo.ListSubAgentInputs(context.Background(), "child", sessiontree.SubAgentInputPending)
			if err != nil || len(pending) != 1 || pending[0].SubAgentInputID != published.Input.SubAgentInputID {
				t.Fatalf("historical subagent admission changed pending input=%#v err=%v", pending, err)
			}
			if lease, active, err := repo.ActiveTurnLease(context.Background(), "child"); err != nil || active {
				t.Fatalf("historical subagent admission lease=%#v active=%v err=%v", lease, active, err)
			}
		})
	}
}

func prepareSubAgentReplayClaim(t *testing.T, repo pendingToolAuthorityTestRepo, now time.Time) {
	t.Helper()
	ctx := context.Background()
	parent, err := repo.Thread(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := repo.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	plan := storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "claim-replay", RequestFingerprint: "claim-replay-fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: parent.ID, SourceEntryID: parent.LeafID,
			SourceLeafEntryID: parent.LeafID, DestinationThreadID: "fork-parent",
			ArtifactClosure: emptySQLiteForkArtifactClosure(t, parent.ID, "fork-parent"),
		},
		TerminalChildren: []storage.ForkOperationPlanNode{{
			NodeID: "terminal-child-1", SourceThreadID: child.ID, SourceEntryID: child.LeafID,
			SourceLeafEntryID: child.LeafID, DestinationThreadID: "fork-child",
			ArtifactClosure: emptySQLiteForkArtifactClosure(t, child.ID, "fork-child"),
			DestinationMeta: &sessiontree.ForkDestinationMeta{
				ParentThreadID: "fork-parent", ParentTurnID: child.ParentTurnID, TaskName: child.TaskName,
				TaskDescription: child.TaskDescription, AgentPath: child.AgentPath, HostProfileRef: child.HostProfileRef,
				ForkMode: child.ForkMode, Lifecycle: child.Lifecycle,
			},
		}},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	record := storage.ForkOperationRecord{
		OperationID: plan.OperationID, RequestFingerprint: plan.RequestFingerprint,
		SourceThreadIDs: storage.ForkOperationPlanSourceThreadIDs(plan), AuthorityThreadIDs: storage.ForkOperationPlanAuthorityThreadIDs(plan),
		State: storage.ForkOperationPrepared, Plan: planJSON, CreatedAt: now, UpdatedAt: now,
	}
	var operations storage.ForkOperationStore
	switch backend := repo.(type) {
	case *sessiontree.MemoryRepo:
		operations = storage.NewMemoryForkOperationStore(backend)
	case *Store:
		operations = backend
	default:
		t.Fatalf("unsupported replay-claim backend %T", repo)
	}
	if _, created, err := operations.PrepareForkOperation(ctx, record); err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
}

func TestSQLitePendingToolCompletionMatchesMemory(t *testing.T) {
	for _, settlementPrewritten := range []bool{false, true} {
		name := "writes-settlement"
		if settlementPrewritten {
			name = "accepts-identical-settlement"
		}
		t.Run(name, func(t *testing.T) {
			fixed := time.Now().UTC()
			policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
			memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
			if err != nil {
				t.Fatal(err)
			}
			sqliteStore, err := Open(filepath.Join(t.TempDir(), "pending-completion.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqliteStore.Close() })

			for backend, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
				t.Run(backend, func(t *testing.T) {
					settlement := seedPendingToolRecovery(t, repo, fixed)
					if settlementPrewritten {
						if _, err := repo.SettlePendingToolRecovery(context.Background(), settlement); err != nil {
							t.Fatal(err)
						}
					}
					request := pendingToolCompletionRequest(settlement, fixed)
					admitted, err := repo.AdmitPendingToolCompletion(context.Background(), request)
					if err != nil {
						t.Fatal(err)
					}
					if admitted.Replayed || admitted.SettlementReplayed != settlementPrewritten ||
						admitted.Settlement.ID == "" || admitted.Admission.Lease.Validate() != nil ||
						admitted.Admission.TurnStarted.ParentID != admitted.Admission.BaseLeafID ||
						admitted.Admission.UserMessage.ParentID != admitted.Admission.TurnStarted.ID ||
						admitted.Admission.UserMessage.Message.Content != "background work completed" {
						t.Fatalf("completion admission = %#v", admitted)
					}
					active, ok, err := repo.ActiveTurnLease(context.Background(), settlement.Target.ThreadID)
					if err != nil || !ok || !sessiontree.SameTurnLease(active, admitted.Admission.Lease) {
						t.Fatalf("completion active lease = %#v ok=%v err=%v", active, ok, err)
					}
					replayed, err := repo.AdmitPendingToolCompletion(context.Background(), request)
					if err != nil || !replayed.Replayed || replayed.Admission.Lease.Validate() == nil ||
						replayed.Settlement.ID != admitted.Settlement.ID || replayed.Admission.TurnStarted.ID != admitted.Admission.TurnStarted.ID {
						t.Fatalf("completion replay = %#v err=%v", replayed, err)
					}
					conflict := request
					conflict.RequestFingerprint = "changed-completion-fingerprint"
					conflict.Input.Content = "changed continuation"
					before, err := repo.Entries(context.Background(), settlement.Target.ThreadID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := repo.AdmitPendingToolCompletion(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
						t.Fatalf("completion conflict err=%v, want ErrRequestConflict", err)
					}
					after, err := repo.Entries(context.Background(), settlement.Target.ThreadID)
					if err != nil || len(after) != len(before) || countPendingToolAuthoritySettlements(after) != 1 {
						t.Fatalf("completion conflict mutated entries: before=%d after=%#v err=%v", len(before), after, err)
					}
					targetConflict := request
					targetConflict.Target.EffectAttemptID = "different-effect-attempt"
					targetConflict.Settlement = pendingToolSettlementRequest(targetConflict.Target, targetConflict.SettlementFingerprint, fixed).Settlement
					if _, err := repo.AdmitPendingToolCompletion(context.Background(), targetConflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
						t.Fatalf("completion target replay err=%v, want ErrRequestConflict", err)
					}
					settlementConflict := request
					settlementConflict.SettlementFingerprint = "different-settlement-fingerprint"
					settlementConflict.Settlement = pendingToolSettlementRequest(settlementConflict.Target, settlementConflict.SettlementFingerprint, fixed).Settlement
					if _, err := repo.AdmitPendingToolCompletion(context.Background(), settlementConflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
						t.Fatalf("completion settlement replay err=%v, want ErrRequestConflict", err)
					}
				})
			}
		})
	}
}

func TestPendingToolContinuationRejectsHistoricalTurnIDWithoutMutation(t *testing.T) {
	fixed := time.Date(2026, 7, 20, 18, 30, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "pending-historical-turn.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for backend, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(backend, func(t *testing.T) {
			settlement := seedPendingToolRecovery(t, repo, fixed)
			request := pendingToolCompletionRequest(settlement, fixed)
			request.ContinuationTurnID = "turn-1"
			request.ContinuationRunID = "different-run"
			before, err := repo.Entries(context.Background(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AdmitPendingToolCompletion(context.Background(), request); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("historical pending continuation err=%v, want ErrRequestConflict", err)
			}
			after, err := repo.Entries(context.Background(), "thread")
			if err != nil || len(after) != len(before) || countPendingToolAuthoritySettlements(after) != 0 {
				t.Fatalf("historical pending continuation mutated journal: before=%d after=%#v err=%v", len(before), after, err)
			}
			if lease, active, err := repo.ActiveTurnLease(context.Background(), "thread"); err != nil || active {
				t.Fatalf("historical pending continuation lease=%#v active=%v err=%v", lease, active, err)
			}
		})
	}
}

func TestSQLitePendingToolCompletionDualOpenersAdmitOneOwner(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	path := filepath.Join(t.TempDir(), "pending-completion-dual.db")
	first, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	request := pendingToolCompletionRequest(seedPendingToolRecovery(t, first, fixed), fixed)

	type outcome struct {
		result sessiontree.AdmitPendingToolCompletionResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, store := range []*Store{first, second} {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			<-start
			result, err := store.AdmitPendingToolCompletion(context.Background(), request)
			outcomes <- outcome{result: result, err: err}
		}(store)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	writers, replays := 0, 0
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if outcome.result.Replayed {
			replays++
			if outcome.result.Admission.Lease.Validate() == nil {
				t.Fatalf("replay leaked owner proof: %#v", outcome.result)
			}
		} else {
			writers++
			if outcome.result.Admission.Lease.Validate() != nil {
				t.Fatalf("writer missing owner proof: %#v", outcome.result)
			}
		}
	}
	if writers != 1 || replays != 1 {
		t.Fatalf("completion outcomes writers=%d replays=%d", writers, replays)
	}
	var rows int
	if err := first.db.QueryRow(`SELECT COUNT(*) FROM pending_tool_completions`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("completion ledger rows=%d err=%v", rows, err)
	}
}

func TestSQLitePendingToolRecoveryMatchesMemory(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "pending-tool.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			request := seedPendingToolRecovery(t, repo, fixed)
			request.Settlement.ID = "caller-entry-id"
			request.Settlement.ParentID = "caller-parent-id"
			request.Settlement.CreatedAt = fixed.Add(-time.Hour)
			settled, err := repo.SettlePendingToolRecovery(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if settled.Replayed || settled.Entry.Type != sessiontree.EntryCustom || settled.Entry.ParentID != "terminal-1" ||
				settled.Entry.ID == request.Settlement.ID || !settled.Entry.CreatedAt.Equal(fixed) ||
				settled.Entry.Message.ToolCallID != request.Target.ToolCallID || settled.Entry.Message.ToolName != request.Target.ToolName ||
				settled.Entry.Metadata[sessiontree.PendingToolSettlementFingerprintKey] != request.RequestFingerprint {
				t.Fatalf("settlement = %#v", settled)
			}
			replayed, err := repo.SettlePendingToolRecovery(context.Background(), request)
			if err != nil || !replayed.Replayed || replayed.Entry.ID != settled.Entry.ID {
				t.Fatalf("settlement replay = %#v err=%v", replayed, err)
			}
			conflict := pendingToolSettlementRequest(request.Target, "changed-fingerprint", fixed)
			if _, err := repo.SettlePendingToolRecovery(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("settlement conflict err=%v, want ErrRequestConflict", err)
			}
			entries, err := repo.Entries(context.Background(), request.Target.ThreadID)
			if err != nil || len(entries) != 6 {
				t.Fatalf("entries after replay/conflict = %#v err=%v", entries, err)
			}
			meta, err := repo.Thread(context.Background(), request.Target.ThreadID)
			if err != nil || meta.LeafID != settled.Entry.ID {
				t.Fatalf("thread after settlement = %#v err=%v", meta, err)
			}
		})
	}
}

func TestPendingToolRecoveryRequiresCanonicalEffectAttemptIdentity(t *testing.T) {
	fixed := time.Date(2026, 7, 19, 11, 30, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return fixed })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "pending-local-effect.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for name, repo := range map[string]pendingToolAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			request := seedLocalEffectPendingToolRecovery(t, repo, fixed)
			missingAttempt := request
			missingAttempt.Target.EffectAttemptID = ""
			missingAttempt.Settlement = pendingToolSettlementRequest(missingAttempt.Target, missingAttempt.RequestFingerprint, fixed).Settlement
			if _, err := repo.SettlePendingToolRecovery(context.Background(), missingAttempt); !errors.Is(err, sessiontree.ErrPendingToolNotPending) {
				t.Fatalf("missing effect attempt err=%v, want ErrPendingToolNotPending", err)
			}
			wrongAttempt := request
			wrongAttempt.Target.EffectAttemptID = "wrong-effect-attempt"
			wrongAttempt.Settlement = pendingToolSettlementRequest(wrongAttempt.Target, wrongAttempt.RequestFingerprint, fixed).Settlement
			if _, err := repo.SettlePendingToolRecovery(context.Background(), wrongAttempt); !errors.Is(err, sessiontree.ErrPendingToolNotPending) {
				t.Fatalf("wrong effect attempt err=%v, want ErrPendingToolNotPending", err)
			}
			settled, err := repo.SettlePendingToolRecovery(context.Background(), request)
			if err != nil || settled.Replayed {
				t.Fatalf("canonical effect settlement=%#v err=%v", settled, err)
			}
		})
	}
}

func TestSQLitePendingToolRecoveryDualOpenersSingleWriter(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	path := filepath.Join(t.TempDir(), "pending-tool-dual.db")
	first, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path, WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	request := seedPendingToolRecovery(t, first, fixed)

	type outcome struct {
		result sessiontree.SettlePendingToolRecoveryResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, store := range []*Store{first, second} {
		store := store
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := store.SettlePendingToolRecovery(context.Background(), request)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(outcomes)
	writers := 0
	replays := 0
	entryID := ""
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("concurrent settlement err=%v", outcome.err)
		}
		if outcome.result.Replayed {
			replays++
		} else {
			writers++
		}
		if entryID == "" {
			entryID = outcome.result.Entry.ID
		} else if outcome.result.Entry.ID != entryID {
			t.Fatalf("concurrent settlement IDs differ: got %q want %q", outcome.result.Entry.ID, entryID)
		}
	}
	if writers != 1 || replays != 1 {
		t.Fatalf("concurrent outcomes: writers=%d replays=%d", writers, replays)
	}
	entries, err := first.Entries(context.Background(), request.Target.ThreadID)
	if err != nil || countPendingToolAuthoritySettlements(entries) != 1 {
		t.Fatalf("settlement entries = %#v err=%v", entries, err)
	}
}

func TestSQLitePendingToolRecoveryActiveLeaseHasZeroSideEffects(t *testing.T) {
	fixed := time.Now().UTC()
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	store, err := Open(filepath.Join(t.TempDir(), "pending-tool-active.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	request := seedPendingToolRecovery(t, store, fixed)
	lease, err := store.AcquireTurnLease(context.Background(), sessiontree.TurnLease{
		ThreadID: request.Target.ThreadID, Purpose: sessiontree.TurnLeasePurposeMutation,
		MutationID: "mutation-1", MutationKind: "compaction", OwnerID: "mutation-owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	before := pendingToolAuthorityState(t, store, request.Target.ThreadID)
	if _, err := store.SettlePendingToolRecovery(context.Background(), request); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("SettlePendingToolRecovery err=%v, want ErrThreadAuthorityBusy", err)
	}
	after := pendingToolAuthorityState(t, store, request.Target.ThreadID)
	if before != after {
		t.Fatalf("active-lease rejection mutated state: before=%#v after=%#v", before, after)
	}
	active, ok, err := store.ActiveTurnLease(context.Background(), request.Target.ThreadID)
	if err != nil || !ok || !sessiontree.SameTurnLease(active, lease) {
		t.Fatalf("active mutation lease changed: %#v ok=%v err=%v", active, ok, err)
	}
}

func TestSQLitePendingToolRecoveryWrongTargetsHaveZeroSideEffects(t *testing.T) {
	fixed := time.Now().UTC()
	store, err := Open(filepath.Join(t.TempDir(), "pending-tool-targets.db"), WithAuthorityClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	valid := seedPendingToolRecovery(t, store, fixed)
	tests := []struct {
		name   string
		mutate func(*sessiontree.PendingToolSettlementTarget)
		want   error
	}{
		{name: "turn", mutate: func(target *sessiontree.PendingToolSettlementTarget) { target.TurnID = "missing-turn" }, want: sessiontree.ErrPendingToolTurnNotFound},
		{name: "run", mutate: func(target *sessiontree.PendingToolSettlementTarget) { target.RunID = "missing-run" }, want: sessiontree.ErrPendingToolRunNotFound},
		{name: "tool", mutate: func(target *sessiontree.PendingToolSettlementTarget) { target.ToolCallID = "missing-call" }, want: sessiontree.ErrPendingToolNotFound},
		{name: "handle", mutate: func(target *sessiontree.PendingToolSettlementTarget) { target.Handle = "missing-handle" }, want: sessiontree.ErrPendingToolNotPending},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := valid.Target
			test.mutate(&target)
			request := pendingToolSettlementRequest(target, "fingerprint-"+test.name, fixed)
			before := pendingToolAuthorityState(t, store, valid.Target.ThreadID)
			if _, err := store.SettlePendingToolRecovery(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("SettlePendingToolRecovery err=%v, want %v", err, test.want)
			}
			after := pendingToolAuthorityState(t, store, valid.Target.ThreadID)
			if before != after {
				t.Fatalf("wrong-target rejection mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func seedPendingToolRecovery(t *testing.T, repo pendingToolAuthorityTestRepo, now time.Time) sessiontree.SettlePendingToolRecoveryRequest {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "admit-fingerprint", Now: now,
	})
	if err != nil {
		t.Fatalf("admit local effect turn: %v", err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call-1", ToolName: "terminal"},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatalf("append local effect call: %v", err)
	}
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
		Message: session.Message{
			Role: session.Tool, ToolCallID: "call-1", ToolName: "terminal",
			ToolResult: &session.ToolResultView{Status: "running"},
			Activity:   &session.ActivityPresentation{Payload: map[string]any{"pending_handle": "terminal:job:123"}},
		},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "terminal-1",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "finish-fingerprint", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	target := sessiontree.PendingToolSettlementTarget{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1",
		ToolCallID: "call-1", ToolName: "terminal", Handle: "terminal:job:123",
	}
	return pendingToolSettlementRequest(target, "settlement-fingerprint", now)
}

func pendingToolSettlementRequest(target sessiontree.PendingToolSettlementTarget, fingerprint string, now time.Time) sessiontree.SettlePendingToolRecoveryRequest {
	return sessiontree.SettlePendingToolRecoveryRequest{
		Target: target, RequestFingerprint: fingerprint, Now: now,
		Settlement: sessiontree.Entry{
			ThreadID: target.ThreadID, TurnID: target.TurnID, Type: sessiontree.EntryCustom,
			Message: session.Message{Role: session.Tool, ToolCallID: target.ToolCallID, ToolName: target.ToolName, Content: "settled"},
			Metadata: map[string]string{
				sessiontree.PendingToolSettlementKindKey:        sessiontree.PendingToolSettlementKind,
				sessiontree.PendingToolSettlementFingerprintKey: fingerprint,
				sessiontree.PendingToolEffectAttemptIDKey:       target.EffectAttemptID,
			},
		},
	}
}

func pendingToolCompletionRequest(settlement sessiontree.SettlePendingToolRecoveryRequest, now time.Time) sessiontree.AdmitPendingToolCompletionRequest {
	return sessiontree.AdmitPendingToolCompletionRequest{
		CompletionRequestID: "completion-1", RequestFingerprint: "completion-fingerprint",
		SettlementFingerprint: settlement.RequestFingerprint, Target: settlement.Target, Settlement: settlement.Settlement,
		ContinuationTurnID: "turn-2", ContinuationRunID: "run-2", OwnerID: "completion-owner",
		Input: session.Message{Role: session.User, Content: "background work completed"}, Now: now,
	}
}

func seedLocalEffectPendingToolRecovery(t *testing.T, repo pendingToolAuthorityTestRepo, now time.Time) sessiontree.SettlePendingToolRecoveryRequest {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "local-effect", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "local-effect", TurnID: "turn-1", RunID: "run-1", OwnerID: "owner-1",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "admit-local-effect", Now: now,
	})
	if err != nil {
		t.Fatalf("prepare local effect: %v", err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "local-effect", TurnID: "turn-1", Type: sessiontree.EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call-1", ToolName: "terminal"},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatalf("append local effect call: %v", err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "effect-request", Now: now,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "local-effect", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "terminal", ArgumentHash: "arguments",
		},
	})
	if err != nil {
		t.Fatalf("prepare local effect: %v", err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, sessiontree.BeginEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-request",
		ObservedHeartbeat: admitted.Lease.Heartbeat, AuthorizationProofHash: "proof", Now: now,
	}); err != nil {
		t.Fatalf("begin local effect: %v", err)
	}
	if _, err := repo.FinishEffectDispatch(ctx, sessiontree.FinishEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-request",
		OutcomeFingerprint: "effect-outcome", Now: now,
		Result: sessiontree.Entry{ThreadID: "local-effect", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: "call-1", ToolName: "terminal",
				ToolResult: &session.ToolResultView{Status: "running"}, Activity: &session.ActivityPresentation{Payload: map[string]any{"pending_handle": "terminal:local:123"}}}},
	}); err != nil {
		t.Fatalf("finish local effect: %v", err)
	}
	if _, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "local-effect-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "turn-outcome", Now: now,
	}); err != nil {
		t.Fatalf("finish local effect turn: %v", err)
	}
	target := sessiontree.PendingToolSettlementTarget{
		ThreadID: "local-effect", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "terminal",
		Handle: "terminal:local:123", EffectAttemptID: prepared.Attempt.EffectAttemptID,
	}
	return pendingToolSettlementRequest(target, "local-effect-settlement", now)
}

func seedSubAgentPendingToolCompletion(t *testing.T, repo pendingToolAuthorityTestRepo, now time.Time) sessiontree.PublishSubAgentPendingToolCompletionRequest {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "child", TurnID: "turn-1", RunID: "run-1", OwnerID: "child-owner",
		Input: session.Message{Role: session.User, Content: "start"}, RequestFingerprint: "child-admit", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, admitted.Lease)
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "child", TurnID: "turn-1", Type: sessiontree.EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call-1", ToolName: "terminal"},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(leaseCtx, sessiontree.Entry{
		ThreadID: "child", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
		Message: session.Message{Role: session.Tool, ToolCallID: "call-1", ToolName: "terminal",
			ToolResult: &session.ToolResultView{Status: "running"}, Activity: &session.ActivityPresentation{Payload: map[string]any{"pending_handle": "terminal:job:child"}}},
	}, sessiontree.AppendOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-1", TerminalEntryID: "child-terminal-1",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "child-finish", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	target := sessiontree.PendingToolSettlementTarget{ThreadID: "child", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "terminal", Handle: "terminal:job:child"}
	settlement := pendingToolSettlementRequest(target, "child-settlement", now)
	return sessiontree.PublishSubAgentPendingToolCompletionRequest{
		InputRequestID: "child-completion-1", RequestFingerprint: "child-completion-fingerprint",
		SettlementFingerprint: settlement.RequestFingerprint, ParentThreadID: "parent", ChildThreadID: "child",
		Target: target, Settlement: settlement.Settlement,
		Message: session.Message{Role: session.User, Content: "continue after background work"}, Now: now,
	}
}

type pendingToolState struct {
	EntryCount int
	LeafID     string
	LeaseCount int
}

func pendingToolAuthorityState(t *testing.T, store *Store, threadID string) pendingToolState {
	t.Helper()
	ctx := context.Background()
	entries, err := store.Entries(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Thread(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	var leases int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_turn_leases WHERE thread_id = ?`, threadID).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	return pendingToolState{EntryCount: len(entries), LeafID: meta.LeafID, LeaseCount: leases}
}

func countPendingToolAuthoritySettlements(entries []sessiontree.Entry) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[sessiontree.PendingToolSettlementKindKey] == sessiontree.PendingToolSettlementKind {
			count++
		}
	}
	return count
}
