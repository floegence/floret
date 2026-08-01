package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/v3/identity"
	publicstorage "github.com/floegence/floret/v3/storage"
)

func TestExecuteAdmissionRecordReplaysLegacyCommittedTurnWithoutPlan(t *testing.T) {
	threadID := identity.ThreadID("thread-legacy-committed")
	turnID := identity.TurnID("turn-legacy-committed")
	runID := identity.RunID("run-legacy-committed")
	requestID := identity.LogicalRequestID("request-legacy-committed")
	turns := &Turns{thread: &Thread{host: &Host{}, id: threadID}, agent: &Agent{}}
	record := requestLedgerRecord{
		Version: requestLedgerVersion, Operation: "start_turn", Authority: threadID.String(),
		LogicalRequestID: requestID, ThreadID: threadID, TurnID: &turnID, RunID: &runID,
		State: requestStateCommitted, Revision: 3,
	}
	receipt := TurnAdmissionReceipt{
		LogicalRequestID: requestID, ThreadID: threadID, TurnID: turnID, RunID: runID,
		UserEntryID: "entry-legacy-committed", Revision: 2,
	}

	result, err := turns.executeAdmissionRecord(context.Background(), record, receipt, ExecutionContext{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.ThreadID != threadID || result.TurnID != turnID || result.RunID != runID || !result.Receipt.Replayed {
		t.Fatalf("legacy committed replay = %#v", result)
	}
}

func TestReserveStartTurnUpgradesLegacyPreparedFingerprintWithExecutionPlan(t *testing.T) {
	ctx := context.Background()
	host, err := Open(ctx, Options{Storage: publicstorage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()

	threadID := identity.ThreadID("thread-legacy-prepared")
	turnID := identity.TurnID("turn-legacy-prepared")
	runID := identity.RunID("run-legacy-prepared")
	requestID := identity.LogicalRequestID("request-legacy-prepared")
	command := StartTurnCommand{
		LogicalRequestID: requestID,
		UserMessage:      TurnInput{Text: "canonical"},
		SupplementalContext: []TurnSupplementalContextItem{{
			Kind: "host_context", Text: "legacy ephemeral value",
		}},
	}
	const agentFingerprint = "agent-fingerprint"
	legacyFingerprint, err := startTurnFingerprint(command, agentFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	plan := newTurnExecutionPlan(command, agentFingerprint)
	fingerprint, err := turnExecutionPlanFingerprint(requestID, plan)
	if err != nil {
		t.Fatal(err)
	}
	if legacyFingerprint == fingerprint {
		t.Fatal("legacy and execution-plan fingerprints must differ")
	}
	_, replayed, err := host.reserveRequest(ctx, requestLedgerRecord{
		Version: requestLedgerVersion, Operation: "start_turn", Authority: threadID.String(),
		LogicalRequestID: requestID, Fingerprint: legacyFingerprint, ThreadID: threadID,
		TurnID: &turnID, RunID: &runID, State: requestStatePrepared,
	})
	if err != nil || replayed {
		t.Fatalf("seed legacy request: replayed = %v, err = %v", replayed, err)
	}

	upgraded, replayed, err := host.reserveStartTurn(ctx, threadID, requestID, fingerprint, legacyFingerprint, &plan)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed || upgraded.ExecutionPlan == nil || upgraded.Fingerprint != fingerprint {
		t.Fatalf("upgraded request = %#v, replayed = %v", upgraded, replayed)
	}
	persisted, found, err := host.loadRequest(ctx, "start_turn", threadID.String(), requestID)
	if err != nil || !found {
		t.Fatalf("load upgraded request: found = %v, err = %v", found, err)
	}
	if persisted.ExecutionPlan == nil || persisted.Fingerprint != fingerprint {
		t.Fatalf("persisted upgraded request = %#v", persisted)
	}
}
