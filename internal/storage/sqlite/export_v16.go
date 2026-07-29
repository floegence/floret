package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v2/internal/provider/cache"
	"github.com/floegence/floret/v2/internal/session/artifact"
	"github.com/floegence/floret/v2/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v2/internal/storage"
)

// V16Runner is the SQL surface required to verify and export one legacy v16
// transaction. It is intentionally limited to migration infrastructure.
type V16Runner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// V16Identity returns the only legacy schema accepted by the v2 migrator.
func V16Identity() (version, fingerprint string) {
	return schemaVersion, schemaFingerprintVersion16
}

// VerifyV16Runner validates an exact v16 schema through the package SQL
// runner contract. It is internal migration infrastructure.
func VerifyV16Runner(ctx context.Context, runner V16Runner) error {
	version, err := metaValue(ctx, runner, "schema_version")
	if err != nil {
		return err
	}
	if version != schemaVersion {
		fingerprint, _ := metaValue(ctx, runner, "schema_fingerprint")
		return unsupportedSchemaError(version, fingerprint)
	}
	return verifySchemaVersion(ctx, runner, schemaVersion)
}

// ExportedV16State is the strict logical state written by the v2 migrator.
type ExportedV16State struct {
	Session        []byte
	Prompt         []byte
	ForkOperations []internalstorage.ForkOperationRecord
	Threads        int
	Entries        int
}

// ExportV16State decodes canonical v16 thread, journal, and prompt data from
// one already-validated read snapshot.
func ExportV16State(ctx context.Context, runner V16Runner) (ExportedV16State, error) {
	threads, err := loadThreadAuthorityGraph(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	threadMap := make(map[string]sessiontree.ThreadMeta, len(threads))
	entryMap := make(map[string][]sessiontree.Entry, len(threads))
	entryOrdinals := make(map[string]map[string]int, len(threads))
	entryDepths := make(map[string]map[string]int64, len(threads))
	turnOrdinals := make(map[string]map[string][]int, len(threads))
	turnCounts := make(map[string]map[string]int, len(threads))
	leaseGenerations := make(map[string]int64, len(threads))
	leases := make(map[string]sessiontree.TurnLease)
	todos := make(map[string]sessiontree.AgentTodoState)
	providerStates := make(map[string]sessiontree.ProviderStateRecord)
	entryCount := 0
	for _, thread := range threads {
		entries, loadErr := loadEntries(ctx, runner, thread.ID)
		if loadErr != nil {
			return ExportedV16State{}, loadErr
		}
		threadMap[thread.ID], entryMap[thread.ID] = thread, entries
		entryOrdinals[thread.ID] = make(map[string]int, len(entries))
		entryDepths[thread.ID] = make(map[string]int64, len(entries))
		turnOrdinals[thread.ID] = map[string][]int{}
		turnCounts[thread.ID] = map[string]int{}
		for ordinal, entry := range entries {
			entryOrdinals[thread.ID][entry.ID] = ordinal
			entryDepths[thread.ID][entry.ID] = entry.PathDepth
			if entry.TurnID != "" {
				turnOrdinals[thread.ID][entry.TurnID] = append(turnOrdinals[thread.ID][entry.TurnID], ordinal)
				turnCounts[thread.ID][entry.TurnID]++
			}
		}
		var leaseGeneration int64
		if err := runner.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = ?`, thread.ID).Scan(&leaseGeneration); err != nil {
			return ExportedV16State{}, err
		}
		leaseGenerations[thread.ID] = leaseGeneration
		lease, found, err := loadTurnLease(ctx, runner, thread.ID)
		if err != nil {
			return ExportedV16State{}, err
		}
		if found {
			leases[thread.ID] = lease
		}
		todo, found, err := loadAgentTodoState(ctx, runner, thread.ID)
		if err != nil {
			return ExportedV16State{}, err
		}
		if found {
			todos[thread.ID] = todo
		}
		providerState, found, err := exportProviderState(ctx, runner, thread.ID)
		if err != nil {
			return ExportedV16State{}, err
		}
		if found {
			providerStates[thread.ID] = providerState
		}
		entryCount += len(entries)
	}
	authorityClaims, err := exportAuthorityClaims(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	turnAdmissions, turnFinishes, err := exportTurnLedgers(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	effectAttempts, effectByInvocation, effectSequence, err := exportEffectAttempts(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	approvalQueues, approvals, approvalByEffect, approvalDecisions, err := exportApprovals(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	rootCreateIntents, tombstones, err := exportRootAuthority(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	subAgentInputs, subAgentSequences, publications, inputRequests, err := exportSubAgentInputs(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	pendingCompletions, subAgentPendingCompletions, err := exportPendingCompletions(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	closeOperations, compactionOperations, artifacts, err := exportOperationsAndArtifacts(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	if err := rejectLegacyMetadata(ctx, runner); err != nil {
		return ExportedV16State{}, err
	}
	leasePolicy, err := exportLeasePolicy(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	sessionState, err := json.Marshal(map[string]any{
		"version": 1, "threads": threadMap, "entries": entryMap,
		"entry_ordinals": entryOrdinals, "entry_depths": entryDepths,
		"turn_entry_ordinals": turnOrdinals, "turn_entry_counts": turnCounts,
		"leases": leases, "lease_generation": leaseGenerations, "lease_policy": leasePolicy,
		"authority_claims": authorityClaims, "todos": todos, "provider_states": providerStates,
		"turn_admissions": turnAdmissions, "turn_finishes": turnFinishes,
		"effect_attempts": effectAttempts, "effect_attempt_by_invocation": effectByInvocation,
		"effect_attempt_sequence": effectSequence,
		"approval_queues":         approvalQueues, "approvals": approvals,
		"approval_by_effect_attempt": approvalByEffect, "approval_decisions": approvalDecisions,
		"root_create_intents": rootCreateIntents, "tombstones": tombstones,
		"subagent_inputs": subAgentInputs, "subagent_input_sequence": subAgentSequences,
		"subagent_publications": publications, "subagent_input_requests": inputRequests,
		"pending_tool_completions":          pendingCompletions,
		"subagent_pending_tool_completions": subAgentPendingCompletions,
		"subagent_close_operations":         closeOperations, "compaction_operations": compactionOperations,
		"artifacts": artifacts,
		"sequence":  entryCount,
	})
	if err != nil {
		return ExportedV16State{}, err
	}
	memory, err := sessiontree.DecodeMemoryState(sessionState, time.Now)
	if err != nil {
		return ExportedV16State{}, fmt.Errorf("validate exported v16 session state: %w", err)
	}
	sessionState, err = memory.EncodeMemoryState()
	if err != nil {
		return ExportedV16State{}, fmt.Errorf("encode canonical v16 session state: %w", err)
	}
	promptState, err := exportPromptState(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	forkOperations, err := exportForkOperations(ctx, runner)
	if err != nil {
		return ExportedV16State{}, err
	}
	return ExportedV16State{
		Session: sessionState, Prompt: promptState, ForkOperations: forkOperations,
		Threads: len(threads), Entries: entryCount,
	}, nil
}

type migratedRootCreateLedger struct {
	ThreadID        string
	CreateIntentID  string
	Fingerprint     string
	ContractVersion string
}

func exportRootAuthority(ctx context.Context, runner V16Runner) (map[string]migratedRootCreateLedger, map[string]sessiontree.ThreadTombstone, error) {
	intents := map[string]migratedRootCreateLedger{}
	rows, err := runner.QueryContext(ctx, `SELECT create_intent_id, thread_id, contract_version, request_fingerprint FROM root_create_intents ORDER BY create_intent_id`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var intent migratedRootCreateLedger
		if err := rows.Scan(&intent.CreateIntentID, &intent.ThreadID, &intent.ContractVersion, &intent.Fingerprint); err != nil {
			rows.Close()
			return nil, nil, err
		}
		intents[intent.CreateIntentID] = intent
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	tombstones := map[string]sessiontree.ThreadTombstone{}
	ids, err := querySingleStringColumn(ctx, runner, `SELECT thread_id FROM thread_tombstones ORDER BY thread_id`)
	if err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		tombstone, err := loadThreadTombstone(ctx, runner, id)
		if err != nil {
			return nil, nil, err
		}
		tombstones[id] = tombstone
	}
	return intents, tombstones, nil
}

type migratedSubAgentRequestLedger struct {
	ParentThreadID     string
	ChildThreadID      string
	RequestFingerprint string
	SubAgentInputID    string
	ArtifactClosure    artifact.Closure
}

func exportSubAgentInputs(ctx context.Context, runner V16Runner) (
	map[string][]sessiontree.SubAgentInputRecord,
	map[string]int64,
	map[string]migratedSubAgentRequestLedger,
	map[string]migratedSubAgentRequestLedger,
	error,
) {
	inputs := map[string][]sessiontree.SubAgentInputRecord{}
	sequences := map[string]int64{}
	inputIDs, err := querySingleStringColumn(ctx, runner, `SELECT subagent_input_id FROM subagent_inputs ORDER BY child_thread_id, sequence, subagent_input_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, inputID := range inputIDs {
		input, found, err := loadSubAgentInput(ctx, runner, inputID)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, nil, err
		}
		inputs[input.ChildThreadID] = append(inputs[input.ChildThreadID], input)
		if input.Sequence > sequences[input.ChildThreadID] {
			sequences[input.ChildThreadID] = input.Sequence
		}
	}
	publications, err := exportSubAgentRequestTable(ctx, runner, "subagent_publications", "publication_id", true)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	requests, err := exportSubAgentRequestTable(ctx, runner, "subagent_input_requests", "input_request_id", false)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return inputs, sequences, publications, requests, nil
}

func exportSubAgentRequestTable(ctx context.Context, runner V16Runner, table, idColumn string, publication bool) (map[string]migratedSubAgentRequestLedger, error) {
	ids, err := querySingleStringColumn(ctx, runner, `SELECT `+idColumn+` FROM `+table+` ORDER BY `+idColumn)
	if err != nil {
		return nil, err
	}
	result := map[string]migratedSubAgentRequestLedger{}
	for _, id := range ids {
		var ledger sqliteSubAgentRequestLedger
		var found bool
		if publication {
			ledger, found, err = loadSubAgentPublicationLedger(ctx, runner, id)
		} else {
			ledger, found, err = loadSubAgentRequestLedger(ctx, runner, table, idColumn, id)
		}
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, err
		}
		result[id] = migratedSubAgentRequestLedger{
			ParentThreadID: ledger.parentThreadID, ChildThreadID: ledger.childThreadID,
			RequestFingerprint: ledger.fingerprint, SubAgentInputID: ledger.inputID,
			ArtifactClosure: ledger.artifactClosure,
		}
	}
	return result, nil
}

type migratedPendingToolCompletion struct {
	CompletionRequestID   string
	RequestFingerprint    string
	ThreadID              string
	Target                sessiontree.PendingToolSettlementTarget
	SettlementFingerprint string
	ContinuationTurnID    string
	ContinuationRunID     string
	SettlementEntryID     string
	TurnStartedID         string
	UserMessageID         string
	BaseLeafID            string
}

func exportPendingCompletions(ctx context.Context, runner V16Runner) (
	map[string]migratedPendingToolCompletion,
	map[string]sqliteSubAgentPendingToolCompletionLedger,
	error,
) {
	pending := map[string]migratedPendingToolCompletion{}
	ids, err := querySingleStringColumn(ctx, runner, `SELECT completion_request_id FROM pending_tool_completions ORDER BY completion_request_id`)
	if err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		ledger, found, err := loadSQLitePendingToolCompletion(ctx, runner, id)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, err
		}
		pending[id] = migratedPendingToolCompletion{
			CompletionRequestID: ledger.CompletionRequestID, RequestFingerprint: ledger.RequestFingerprint,
			ThreadID: ledger.Target.ThreadID, Target: ledger.Target,
			SettlementFingerprint: ledger.SettlementFingerprint,
			ContinuationTurnID:    ledger.ContinuationTurnID, ContinuationRunID: ledger.ContinuationRunID,
			SettlementEntryID: ledger.SettlementEntryID, TurnStartedID: ledger.TurnStartedID,
			UserMessageID: ledger.UserMessageID, BaseLeafID: ledger.BaseLeafID,
		}
	}
	subAgentPending := map[string]sqliteSubAgentPendingToolCompletionLedger{}
	ids, err = querySingleStringColumn(ctx, runner, `SELECT input_request_id FROM subagent_pending_tool_completions ORDER BY input_request_id`)
	if err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		ledger, found, err := loadSQLiteSubAgentPendingToolCompletion(ctx, runner, id)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, err
		}
		subAgentPending[id] = ledger
	}
	return pending, subAgentPending, nil
}

func exportOperationsAndArtifacts(ctx context.Context, runner V16Runner) (
	map[string]sessiontree.SubAgentCloseOperation,
	map[string]sessiontree.CompactionOperation,
	map[string]artifact.Record,
	error,
) {
	closeOperations := map[string]sessiontree.SubAgentCloseOperation{}
	ids, err := querySingleStringColumn(ctx, runner, `SELECT close_operation_id FROM subagent_close_operations ORDER BY close_operation_id`)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, id := range ids {
		operation, found, err := loadSubAgentCloseOperation(ctx, runner, id)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, err
		}
		closeOperations[id] = operation
	}
	compactions := map[string]sessiontree.CompactionOperation{}
	ids, err = querySingleStringColumn(ctx, runner, `SELECT request_id FROM compaction_operations ORDER BY request_id`)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, id := range ids {
		operation, found, err := loadSQLiteCompactionOperation(ctx, runner, id)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, err
		}
		compactions[id] = operation
	}
	artifacts := map[string]artifact.Record{}
	rows, err := runner.QueryContext(ctx, `SELECT thread_id, id FROM tool_output_artifacts ORDER BY thread_id, id`)
	if err != nil {
		return nil, nil, nil, err
	}
	var artifactKeys [][2]string
	for rows.Next() {
		var key [2]string
		if err := rows.Scan(&key[0], &key[1]); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		artifactKeys = append(artifactKeys, key)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	for _, key := range artifactKeys {
		record, err := loadSQLiteArtifactRecord(ctx, runner, key[0], key[1])
		if err != nil {
			return nil, nil, nil, err
		}
		if err := validateSQLiteArtifactRecord(ctx, runner, record); err != nil {
			return nil, nil, nil, err
		}
		artifacts[key[0]+"\x00"+key[1]] = record
	}
	return closeOperations, compactions, artifacts, nil
}

func rejectLegacyMetadata(ctx context.Context, runner V16Runner) error {
	var count int
	if err := runner.QueryRowContext(ctx, `SELECT COUNT(*) FROM metadata_records`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("legacy metadata_records contains %d host-owned records", count)
	}
	return nil
}

type migratedTurnFinish struct {
	ThreadID           string
	TurnID             string
	RunID              string
	Generation         int64
	OutcomeFingerprint string
	FailureEntryID     string
	TerminalEntryID    string
}

func exportTurnLedgers(ctx context.Context, runner V16Runner) (map[string]sqliteTurnAdmissionLedger, map[string]migratedTurnFinish, error) {
	admissions := map[string]sqliteTurnAdmissionLedger{}
	rows, err := runner.QueryContext(ctx, `SELECT thread_id, turn_id FROM turn_admissions ORDER BY thread_id, turn_id`)
	if err != nil {
		return nil, nil, err
	}
	var admissionKeys [][2]string
	for rows.Next() {
		var key [2]string
		if err := rows.Scan(&key[0], &key[1]); err != nil {
			rows.Close()
			return nil, nil, err
		}
		admissionKeys = append(admissionKeys, key)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, key := range admissionKeys {
		ledger, found, err := loadSQLiteTurnAdmission(ctx, runner, key[0], key[1])
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, err
		}
		admissions[key[0]+"\x00"+key[1]] = ledger
	}
	finishes := map[string]migratedTurnFinish{}
	rows, err = runner.QueryContext(ctx, `SELECT thread_id, turn_id FROM turn_finishes ORDER BY thread_id, turn_id`)
	if err != nil {
		return nil, nil, err
	}
	var finishKeys [][2]string
	for rows.Next() {
		var key [2]string
		if err := rows.Scan(&key[0], &key[1]); err != nil {
			rows.Close()
			return nil, nil, err
		}
		finishKeys = append(finishKeys, key)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, key := range finishKeys {
		ledger, found, err := loadSQLiteTurnFinish(ctx, runner, key[0], key[1])
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, err
		}
		finishes[key[0]+"\x00"+key[1]] = migratedTurnFinish{
			ThreadID: ledger.ThreadID, TurnID: ledger.TurnID, RunID: ledger.RunID,
			Generation: ledger.Generation, OutcomeFingerprint: ledger.OutcomeFingerprint,
			FailureEntryID: ledger.FailureEntryID, TerminalEntryID: ledger.TerminalEntryID,
		}
	}
	return admissions, finishes, nil
}

func exportEffectAttempts(ctx context.Context, runner V16Runner) (map[string]sessiontree.EffectAttempt, map[string]string, int64, error) {
	rows, err := runner.QueryContext(ctx, `SELECT effect_attempt_id FROM effect_attempts ORDER BY effect_attempt_id`)
	if err != nil {
		return nil, nil, 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, nil, 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, 0, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	attempts := map[string]sessiontree.EffectAttempt{}
	byInvocation := map[string]string{}
	var sequence int64
	for _, id := range ids {
		attempt, found, err := loadEffectAttempt(ctx, runner, id)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, 0, err
		}
		attempts[id] = attempt
		invocation := attempt.Invocation
		byInvocation[strings.Join([]string{invocation.ThreadID, invocation.TurnID, invocation.RunID, invocation.ToolCallID}, "\x00")] = id
		var ordinal int64
		if _, err := fmt.Sscanf(id, "effect-%d", &ordinal); err == nil && ordinal > sequence {
			sequence = ordinal
		}
	}
	return attempts, byInvocation, sequence, nil
}

type migratedApprovalDecision struct {
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          sessiontree.ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 sessiontree.ApprovalDecision
	Receipt                  sessiontree.ApprovalDecisionReceipt
}

func exportApprovals(ctx context.Context, runner V16Runner) (
	map[string]sqliteApprovalQueueLedger,
	map[string]sessiontree.ApprovalRecord,
	map[string]string,
	map[string]migratedApprovalDecision,
	error,
) {
	queues := map[string]sqliteApprovalQueueLedger{}
	queueIDs, err := querySingleStringColumn(ctx, runner, `SELECT root_thread_id FROM approval_queues ORDER BY root_thread_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, rootID := range queueIDs {
		queue, found, err := loadSQLiteApprovalQueueLedger(ctx, runner, rootID)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, nil, err
		}
		queues[rootID] = queue
	}
	approvals := map[string]sessiontree.ApprovalRecord{}
	byEffect := map[string]string{}
	approvalIDs, err := querySingleStringColumn(ctx, runner, `SELECT approval_id FROM approval_requests ORDER BY approval_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, approvalID := range approvalIDs {
		record, found, err := loadSQLiteApproval(ctx, runner, approvalID)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, nil, err
		}
		approvals[approvalID] = record
		byEffect[record.EffectAttemptID] = approvalID
	}
	decisions := map[string]migratedApprovalDecision{}
	decisionIDs, err := querySingleStringColumn(ctx, runner, `SELECT decision_id FROM approval_decisions ORDER BY decision_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, decisionID := range decisionIDs {
		decision, found, err := loadSQLiteApprovalDecision(ctx, runner, decisionID)
		if err != nil || !found {
			if err == nil {
				err = sessiontree.ErrAuthorityCorrupt
			}
			return nil, nil, nil, nil, err
		}
		decisions[decisionID] = migratedApprovalDecision{
			ExpectedRootThreadID: decision.ExpectedRootThreadID,
			ExpectedGeneration:   decision.ExpectedGeneration, ExpectedRevision: decision.ExpectedRevision,
			ExpectedCurrent: decision.ExpectedCurrent, ExpectedApprovalRevision: decision.ExpectedApprovalRevision,
			Decision: decision.Decision, Receipt: decision.Receipt,
		}
	}
	return queues, approvals, byEffect, decisions, nil
}

func querySingleStringColumn(ctx context.Context, runner V16Runner, query string) ([]string, error) {
	rows, err := runner.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func exportAuthorityClaims(ctx context.Context, runner V16Runner) (map[string]string, error) {
	rows, err := runner.QueryContext(ctx, `SELECT thread_id, operation_id FROM thread_authority_claims ORDER BY thread_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	claims := map[string]string{}
	for rows.Next() {
		var threadID, operationID string
		if err := rows.Scan(&threadID, &operationID); err != nil {
			return nil, err
		}
		claims[threadID] = operationID
	}
	return claims, rows.Err()
}

func exportProviderState(ctx context.Context, runner V16Runner, threadID string) (sessiontree.ProviderStateRecord, bool, error) {
	var record sessiontree.ProviderStateRecord
	var rawState, updatedAt string
	err := runner.QueryRowContext(ctx, `SELECT thread_id, leaf_entry_id, compatibility_key, state_json,
		created_by_run_id, created_by_turn_id, updated_at FROM provider_states WHERE thread_id = ?`, threadID).Scan(
		&record.ThreadID, &record.LeafEntryID, &record.CompatibilityKey, &rawState,
		&record.CreatedByRunID, &record.CreatedByTurnID, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ProviderStateRecord{}, false, nil
	}
	if err != nil {
		return sessiontree.ProviderStateRecord{}, false, err
	}
	if err := json.Unmarshal([]byte(rawState), &record.State); err != nil {
		return sessiontree.ProviderStateRecord{}, false, err
	}
	record.UpdatedAt = parseTime(updatedAt)
	return record, true, nil
}

func exportForkOperations(ctx context.Context, runner V16Runner) ([]internalstorage.ForkOperationRecord, error) {
	rows, err := runner.QueryContext(ctx, `SELECT operation_id FROM fork_operations ORDER BY operation_id`)
	if err != nil {
		return nil, err
	}
	var operationIDs []string
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			rows.Close()
			return nil, err
		}
		operationIDs = append(operationIDs, operationID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	operations := make([]internalstorage.ForkOperationRecord, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		record, err := loadForkOperation(ctx, runner, operationID)
		if err != nil {
			return nil, err
		}
		operations = append(operations, record)
	}
	return operations, nil
}

func exportLeasePolicy(ctx context.Context, runner V16Runner) (sessiontree.LeasePolicy, error) {
	var policy sessiontree.LeasePolicy
	var ttl, renew, skew int64
	if err := runner.QueryRowContext(ctx, `SELECT lease_ttl_ns, renew_interval_ns, clock_skew_allowance_ns FROM authority_lease_policy WHERE singleton = 1`).Scan(&ttl, &renew, &skew); err != nil {
		return policy, err
	}
	policy.TTL, policy.RenewInterval, policy.ClockSkewAllowance = time.Duration(ttl), time.Duration(renew), time.Duration(skew)
	return policy, policy.Validate()
}

func exportPromptState(ctx context.Context, runner V16Runner) ([]byte, error) {
	state := struct {
		Version   int                            `json:"version"`
		Segments  []cache.Segment                `json:"segments"`
		Toolsets  []cache.ToolsetSnapshot        `json:"toolsets"`
		Requests  []cache.ProviderRequestRecord  `json:"requests"`
		Responses []cache.ProviderResponseRecord `json:"responses"`
	}{Version: 1}
	for _, source := range []struct {
		table string
		read  func(string) error
	}{
		{"prompt_segments", func(raw string) error {
			var value cache.Segment
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return err
			}
			state.Segments = append(state.Segments, value)
			return nil
		}},
		{"prompt_toolsets", func(raw string) error {
			var value cache.ToolsetSnapshot
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return err
			}
			state.Toolsets = append(state.Toolsets, value)
			return nil
		}},
		{"prompt_requests", func(raw string) error {
			var value cache.ProviderRequestRecord
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return err
			}
			state.Requests = append(state.Requests, value)
			return nil
		}},
		{"prompt_responses", func(raw string) error {
			var value cache.ProviderResponseRecord
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return err
			}
			state.Responses = append(state.Responses, value)
			return nil
		}},
	} {
		rows, err := runner.QueryContext(ctx, `SELECT data_json FROM `+source.table+` ORDER BY rowid`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				return nil, err
			}
			if err := source.read(raw); err != nil {
				rows.Close()
				return nil, err
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	memory, err := cache.DecodeMemoryState(encoded)
	if err != nil {
		return nil, err
	}
	return memory.EncodeMemoryState()
}
