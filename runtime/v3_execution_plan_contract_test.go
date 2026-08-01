package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v3/identity"
	publicstorage "github.com/floegence/floret/v3/storage"
)

func TestExecuteAdmissionRecordRejectsRecordWithoutExecutionPlan(t *testing.T) {
	threadID := identity.ThreadID("thread-without-plan")
	turnID := identity.TurnID("turn-without-plan")
	runID := identity.RunID("run-without-plan")
	requestID := identity.LogicalRequestID("request-without-plan")
	turns := &turnExecutorView{thread: &Thread{host: &Host{}, id: threadID}, agent: &Agent{}}
	record := requestLedgerRecord{
		Version: requestLedgerVersion, Operation: "start_turn", Authority: threadID.String(),
		LogicalRequestID: requestID, ThreadID: threadID, TurnID: &turnID, RunID: &runID,
		State: requestStateCommitted, Revision: 3,
	}
	receipt := TurnAdmissionReceipt{
		LogicalRequestID: requestID, ThreadID: threadID, TurnID: turnID, RunID: runID,
		UserEntryID: "entry-without-plan", Revision: 2,
	}

	if _, err := turns.executeAdmissionRecord(context.Background(), record, receipt, ExecutionContext{}, true); !errors.Is(err, ErrExecutionPlanUnavailable) {
		t.Fatalf("missing execution plan error = %v, want ErrExecutionPlanUnavailable", err)
	}
}

func TestReserveStartTurnRejectsPreparedRecordWithoutExecutionPlan(t *testing.T) {
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
			Kind: "host_context", Text: "ephemeral value",
		}},
	}
	const agentFingerprint = "agent-fingerprint"
	plan := newTurnExecutionPlan(command, agentFingerprint)
	fingerprint, err := turnExecutionPlanFingerprint(requestID, plan)
	if err != nil {
		t.Fatal(err)
	}
	staleFingerprint, err := stableFingerprint(struct {
		Shape string `json:"shape"`
	}{Shape: "without_execution_plan"})
	if err != nil {
		t.Fatal(err)
	}
	_, replayed, err := host.reserveRequest(ctx, requestLedgerRecord{
		Version: requestLedgerVersion, Operation: "start_turn", Authority: threadID.String(),
		LogicalRequestID: requestID, Fingerprint: staleFingerprint, ThreadID: threadID,
		TurnID: &turnID, RunID: &runID, State: requestStatePrepared,
	})
	if err != nil || replayed {
		t.Fatalf("seed stale request: replayed = %v, err = %v", replayed, err)
	}

	if _, _, err := host.reserveStartTurn(ctx, threadID, requestID, fingerprint, &plan); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("stale request error = %v, want ErrRequestConflict", err)
	}
	persisted, found, err := host.loadRequest(ctx, "start_turn", threadID.String(), requestID)
	if err != nil || !found {
		t.Fatalf("load stale request: found = %v, err = %v", found, err)
	}
	if persisted.ExecutionPlan != nil || persisted.Fingerprint != staleFingerprint {
		t.Fatalf("persisted stale request = %#v", persisted)
	}
}
