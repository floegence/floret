package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type legacyTurnLease struct {
	ThreadID     string
	Purpose      string
	TurnID       string
	MutationID   string
	MutationKind string
	OwnerID      string
	Generation   int64
	Heartbeat    int64
	AcquiredAt   time.Time
	RenewedAt    time.Time
	ExpiresAt    time.Time
}

type legacyTurnAdmission struct {
	ThreadID            string
	TurnID              string
	RunID               string
	RequestFingerprint  string
	Lease               legacyTurnLease
	TurnStartedID       string
	UserMessageID       string
	BoundaryTerminalID  string
	BaseLeafID          string
	LegacyTerminalProof *legacyTerminalAdmissionProof `json:",omitempty"`
}

type legacyTerminalAdmissionProof struct {
	Source     string
	SourceHash string
}

type legacyTurnFinish struct {
	ThreadID           string
	TurnID             string
	RunID              string
	Generation         int64
	OutcomeFingerprint string
	FailureEntryID     string
	TerminalEntryID    string
}

type legacyV4Tombstone struct {
	ThreadID            string
	RootThreadID        string
	ParentThreadID      string
	CreateIntentID      string
	ForkOperationID     string
	ForkOperationNodeID string
	ForkedFromThreadID  string
	ForkedFromEntryID   string
	DeletedAt           time.Time
}

// ThreadTombstone was published with a mixed PascalCase/snake_case shape.
// Keep decoding both names while the v2-v4 lineage is converted into the v5
// canonical provenance fields.
func (tombstone *ThreadTombstone) UnmarshalJSON(data []byte) error {
	values, err := legacyShapeValues(data, map[string]struct{}{
		"ThreadID": {}, "thread_id": {}, "RootThreadID": {}, "root_thread_id": {},
		"ParentThreadID": {}, "parent_thread_id": {}, "OriginRequestKey": {}, "origin_request_key": {},
		"OriginFingerprint": {}, "origin_fingerprint": {}, "DeleteRequestKey": {}, "delete_request_key": {},
		"DeleteFingerprint": {}, "delete_fingerprint": {}, "ForkedFromThreadID": {}, "forked_from_thread_id": {},
		"ForkedFromEntryID": {}, "forked_from_entry_id": {}, "LegacyCreateIntent": {}, "CreateIntentID": {}, "create_intent_id": {},
		"LegacyForkRequestID": {}, "ForkOperationID": {}, "fork_operation_id": {}, "LegacyForkNodeID": {}, "ForkOperationNodeID": {}, "fork_operation_node_id": {},
		"DeletedAt": {}, "deleted_at": {},
	})
	if err != nil {
		return err
	}
	if tombstone.ThreadID, err = legacyShapeString(values, "thread_id", "ThreadID"); err != nil {
		return err
	}
	if tombstone.RootThreadID, err = legacyShapeString(values, "root_thread_id", "RootThreadID"); err != nil {
		return err
	}
	if tombstone.ParentThreadID, err = legacyShapeString(values, "parent_thread_id", "ParentThreadID"); err != nil {
		return err
	}
	if tombstone.OriginRequestKey, err = legacyShapeString(values, "origin_request_key", "OriginRequestKey"); err != nil {
		return err
	}
	if tombstone.OriginFingerprint, err = legacyShapeString(values, "origin_fingerprint", "OriginFingerprint"); err != nil {
		return err
	}
	if tombstone.DeleteRequestKey, err = legacyShapeString(values, "delete_request_key", "DeleteRequestKey"); err != nil {
		return err
	}
	if tombstone.DeleteFingerprint, err = legacyShapeString(values, "delete_fingerprint", "DeleteFingerprint"); err != nil {
		return err
	}
	if tombstone.ForkedFromThreadID, err = legacyShapeString(values, "forked_from_thread_id", "ForkedFromThreadID"); err != nil {
		return err
	}
	if tombstone.ForkedFromEntryID, err = legacyShapeString(values, "forked_from_entry_id", "ForkedFromEntryID"); err != nil {
		return err
	}
	if tombstone.LegacyCreateIntent, err = legacyShapeString(values, "create_intent_id", "CreateIntentID"); err != nil {
		if tombstone.LegacyCreateIntent, err = legacyShapeString(values, "create_intent_id", "LegacyCreateIntent"); err != nil {
			return err
		}
	}
	if tombstone.LegacyForkRequestID, err = legacyShapeString(values, "fork_operation_id", "ForkOperationID"); err != nil {
		if tombstone.LegacyForkRequestID, err = legacyShapeString(values, "fork_operation_id", "LegacyForkRequestID"); err != nil {
			return err
		}
	}
	if tombstone.LegacyForkNodeID, err = legacyShapeString(values, "fork_operation_node_id", "ForkOperationNodeID"); err != nil {
		if tombstone.LegacyForkNodeID, err = legacyShapeString(values, "fork_operation_node_id", "LegacyForkNodeID"); err != nil {
			return err
		}
	}
	if raw, found := legacyShapeValue(values, "deleted_at", "DeletedAt"); found {
		if err := json.Unmarshal(raw, &tombstone.DeletedAt); err != nil {
			return err
		}
	}
	return nil
}

func decodeLegacyMemoryStateShapes(data []byte, state *memoryState) error {
	if state == nil {
		return errors.New("legacy session-tree state is required")
	}
	var source struct {
		RootCreateIntents map[string]legacyRootCreateRecord `json:"root_create_intents"`
		Tombstones        map[string]legacyV4Tombstone      `json:"tombstones"`
	}
	if err := json.Unmarshal(data, &source); err != nil {
		return err
	}
	state.RootCreateIntents = source.RootCreateIntents
	for threadID, legacy := range source.Tombstones {
		current := state.Tombstones[threadID]
		current.ThreadID = legacy.ThreadID
		current.RootThreadID = legacy.RootThreadID
		current.ParentThreadID = legacy.ParentThreadID
		current.ForkedFromThreadID = legacy.ForkedFromThreadID
		current.ForkedFromEntryID = legacy.ForkedFromEntryID
		current.LegacyCreateIntent = legacy.CreateIntentID
		current.LegacyForkRequestID = legacy.ForkOperationID
		current.LegacyForkNodeID = legacy.ForkOperationNodeID
		current.DeletedAt = legacy.DeletedAt
		state.Tombstones[threadID] = current
	}
	return nil
}

// Version 3 repairs the exact v3.2.3 SubAgent admission gap. It validates the
// published source authority before reconstructing the missing admission.
func migrateMemoryStateV2ToV3(state *memoryState) error {
	if state == nil || state.Version != 2 {
		return errors.New("session-tree v2 migration requires exact version 2 state")
	}
	legacy, err := decodeLegacyAdmissionState(*state)
	if err != nil {
		return err
	}
	for childThreadID, inputs := range legacy.inputs {
		for _, input := range inputs {
			if input.State != SubAgentInputAdmitted {
				continue
			}
			key := legacyTurnKey(childThreadID, input.AdmittedTurnID)
			if _, exists := legacy.admissions[key]; exists {
				continue
			}
			admission, live, err := reconstructLegacySubAgentAdmission(*state, legacy, childThreadID, input)
			if err != nil {
				return err
			}
			if live {
				legacy.admissions[key] = admission
			}
		}
	}
	state.TurnAdmissions, err = json.Marshal(legacy.admissions)
	if err != nil {
		return err
	}
	state.Version = 3
	return nil
}

// Version 4 adds the transactionally derived root-thread inventory. The
// inventory is written by BackendRepo when this edge is committed.
func migrateMemoryStateV3ToV4(state *memoryState) error {
	if state == nil || state.Version != 3 {
		return errors.New("session-tree v3 migration requires exact version 3 state")
	}
	legacy, err := decodeLegacyAdmissionState(*state)
	if err != nil {
		return err
	}
	if err := validateLegacySubAgentAdmissions(*state, legacy); err != nil {
		return err
	}
	state.Version = 4
	return nil
}

type decodedLegacyAdmissionState struct {
	inputs       map[string][]SubAgentInputRecord
	leases       map[string]legacyTurnLease
	generations  map[string]int64
	admissions   map[string]legacyTurnAdmission
	finishes     map[string]legacyTurnFinish
	legacyFields map[string]json.RawMessage
}

func decodeLegacyAdmissionState(state memoryState) (decodedLegacyAdmissionState, error) {
	if err := validateRequiredMemoryStateMaps(state); err != nil {
		return decodedLegacyAdmissionState{}, err
	}
	if err := validateLegacyMemoryStateMaps(state); err != nil {
		return decodedLegacyAdmissionState{}, err
	}
	decoded := decodedLegacyAdmissionState{}
	for raw, target := range map[*json.RawMessage]any{
		&state.SubAgentInputs: &decoded.inputs, &state.Leases: &decoded.leases,
		&state.LeaseGeneration: &decoded.generations, &state.TurnAdmissions: &decoded.admissions,
		&state.TurnFinishes: &decoded.finishes,
	} {
		if err := json.Unmarshal(*raw, target); err != nil {
			return decodedLegacyAdmissionState{}, err
		}
	}
	return decoded, nil
}

func validateLegacyMemoryStateMaps(state memoryState) error {
	fields := map[string]json.RawMessage{
		"leases": state.Leases, "lease_generation": state.LeaseGeneration, "lease_policy": state.LeasePolicy,
		"authority_claims": state.AuthorityClaims, "subagent_inputs": state.SubAgentInputs,
		"subagent_input_sequence": state.SubAgentInputSequence, "subagent_publications": state.SubAgentPublications,
		"subagent_input_requests": state.SubAgentInputRequests, "turn_admissions": state.TurnAdmissions,
		"turn_finishes": state.TurnFinishes, "approval_queues": state.ApprovalQueues, "approvals": state.Approvals,
		"approval_by_effect_attempt": state.ApprovalByEffectAttempt, "approval_decisions": state.ApprovalDecisions,
		"subagent_close_operations": state.SubAgentCloseOperations, "pending_tool_completions": state.PendingToolCompletions,
		"subagent_pending_tool_completions": state.SubAgentPendingToolCompletions,
		"compaction_operations":             state.CompactionOperations, "thread_revisions": state.ThreadRevisions,
		"thread_revision_history": state.ThreadRevisionHistory,
	}
	for name, raw := range fields {
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" || trimmed == "null" {
			return fmt.Errorf("session-tree v2-v4 field %q must not be null", name)
		}
	}
	if state.RootCreateIntents == nil {
		return errors.New("session-tree v2-v4 field \"root_create_intents\" must not be null")
	}
	return nil
}

func reconstructLegacySubAgentAdmission(state memoryState, legacy decodedLegacyAdmissionState, childThreadID string, input SubAgentInputRecord) (legacyTurnAdmission, bool, error) {
	started, user, err := legacySubAgentAdmissionJournal(state, childThreadID, input)
	if err != nil {
		return legacyTurnAdmission{}, false, err
	}
	meta, exists := state.Threads[childThreadID]
	if !exists || meta.ParentThreadID != input.ParentThreadID {
		return legacyTurnAdmission{}, false, errors.New("SubAgent input does not match parent-child authority")
	}
	key := legacyTurnKey(childThreadID, input.AdmittedTurnID)
	lease, activeExists := legacy.leases[childThreadID]
	active := activeExists && legacyLeasePurpose(lease) == "turn" && lease.TurnID == input.AdmittedTurnID
	finish, finished := legacy.finishes[key]
	if !active && !finished {
		return legacyTurnAdmission{}, false, nil
	}
	if active == finished {
		return legacyTurnAdmission{}, false, errors.New("SubAgent admission must have exactly one active or terminal authority")
	}
	admission := legacyTurnAdmission{ThreadID: childThreadID, TurnID: input.AdmittedTurnID, RunID: input.AdmittedRunID, TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: started.ParentID}
	if active {
		if err := validateLegacyTurnLease(lease); err != nil || lease.ThreadID != childThreadID || legacy.generations[childThreadID] != lease.Generation {
			return legacyTurnAdmission{}, false, errors.New("SubAgent input does not match its active lease")
		}
		admission.Lease = lease
		admission.RequestFingerprint = legacySubAgentAdmissionFingerprint(input, lease.OwnerID)
		return admission, true, nil
	}
	if err := validateLegacyTerminalSource(state, legacy, childThreadID, input, finish); err != nil {
		return legacyTurnAdmission{}, false, err
	}
	hash, err := legacyTerminalAdmissionSourceHash(input, started, user, finish)
	if err != nil {
		return legacyTurnAdmission{}, false, err
	}
	admission.RequestFingerprint = "legacy-subagent-terminal:" + hash
	admission.LegacyTerminalProof = &legacyTerminalAdmissionProof{Source: "v3.2.3_subagent_input", SourceHash: hash}
	return admission, true, nil
}

func validateLegacySubAgentAdmissions(state memoryState, legacy decodedLegacyAdmissionState) error {
	seen := make(map[string]struct{})
	for childThreadID, inputs := range legacy.inputs {
		for _, input := range inputs {
			if input.State != SubAgentInputAdmitted {
				continue
			}
			key := legacyTurnKey(childThreadID, input.AdmittedTurnID)
			if _, duplicate := seen[key]; duplicate {
				return errors.New("SubAgent turn has multiple admitted inputs")
			}
			seen[key] = struct{}{}
			lease := legacy.leases[childThreadID]
			active := legacyLeasePurpose(lease) == "turn" && lease.TurnID == input.AdmittedTurnID
			_, finished := legacy.finishes[key]
			admission, exists := legacy.admissions[key]
			if !active && !finished {
				if exists && admission.LegacyTerminalProof != nil {
					return errors.New("historical SubAgent admission retains a terminal migration proof")
				}
				continue
			}
			if !exists {
				return errors.New("live SubAgent input has no turn admission")
			}
			started, user, err := legacySubAgentAdmissionJournal(state, childThreadID, input)
			if err != nil {
				return err
			}
			if admission.ThreadID != childThreadID || admission.TurnID != input.AdmittedTurnID || admission.RunID != input.AdmittedRunID || admission.TurnStartedID != started.ID || admission.UserMessageID != user.ID || admission.BaseLeafID != started.ParentID {
				return errors.New("SubAgent turn admission does not match its input journal")
			}
			if active {
				if admission.LegacyTerminalProof != nil || admission.Lease != lease || validateLegacyTurnLease(lease) != nil || legacy.generations[childThreadID] != lease.Generation || admission.RequestFingerprint != legacySubAgentAdmissionFingerprint(input, lease.OwnerID) {
					return errors.New("active SubAgent admission authority is invalid")
				}
				continue
			}
			if admission.LegacyTerminalProof == nil {
				if validateLegacyTurnLease(admission.Lease) != nil || admission.Lease.ThreadID != childThreadID || admission.Lease.TurnID != input.AdmittedTurnID || admission.Lease.Generation > legacy.generations[childThreadID] || admission.RequestFingerprint != legacySubAgentAdmissionFingerprint(input, admission.Lease.OwnerID) {
					return errors.New("terminal SubAgent admission authority is invalid")
				}
				if err := validateLegacyTerminalSource(state, legacy, childThreadID, input, legacy.finishes[key]); err != nil {
					return err
				}
				continue
			}
			if admission.Lease != (legacyTurnLease{}) || admission.LegacyTerminalProof.Source != "v3.2.3_subagent_input" || strings.TrimSpace(admission.LegacyTerminalProof.SourceHash) == "" || admission.RequestFingerprint != "legacy-subagent-terminal:"+admission.LegacyTerminalProof.SourceHash {
				return errors.New("legacy SubAgent terminal proof is invalid")
			}
			if err := validateLegacyTerminalSource(state, legacy, childThreadID, input, legacy.finishes[key]); err != nil {
				return err
			}
			hash, err := legacyTerminalAdmissionSourceHash(input, started, user, legacy.finishes[key])
			if err != nil || hash != admission.LegacyTerminalProof.SourceHash {
				return errors.New("legacy SubAgent terminal proof does not match its source")
			}
		}
	}
	for key, admission := range legacy.admissions {
		if admission.LegacyTerminalProof != nil {
			if _, exists := seen[key]; !exists {
				return errors.New("legacy SubAgent terminal proof has no admitted input")
			}
		}
	}
	for threadID, lease := range legacy.leases {
		meta, exists := state.Threads[threadID]
		if !exists || strings.TrimSpace(meta.ParentThreadID) == "" || legacyLeasePurpose(lease) != "turn" {
			continue
		}
		matches := 0
		for _, input := range legacy.inputs[threadID] {
			if input.State == SubAgentInputAdmitted && input.AdmittedTurnID == lease.TurnID {
				matches++
			}
		}
		if matches != 1 {
			return errors.New("active SubAgent lease has no unique admitted input")
		}
	}
	return nil
}

func legacySubAgentAdmissionJournal(state memoryState, childThreadID string, input SubAgentInputRecord) (Entry, Entry, error) {
	if input.ChildThreadID != childThreadID || strings.TrimSpace(input.ParentThreadID) == "" || strings.TrimSpace(input.SubAgentInputID) == "" || strings.TrimSpace(input.AdmittedTurnID) == "" || strings.TrimSpace(input.AdmittedRunID) == "" || input.AdmittedAt.IsZero() {
		return Entry{}, Entry{}, errors.New("admitted SubAgent input authority is incomplete")
	}
	origin, err := SubAgentUserMessageOrigin(input.RequestKind)
	if err != nil {
		return Entry{}, Entry{}, err
	}
	var starts, users []Entry
	for _, entry := range state.Entries[childThreadID] {
		if entry.TurnID != input.AdmittedTurnID {
			continue
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.Metadata["run_id"] == input.AdmittedRunID {
			starts = append(starts, entry)
		}
		if entry.Type == EntryUserMessage && entry.Metadata[SubAgentInputIDMetadataKey] == input.SubAgentInputID {
			users = append(users, entry)
		}
	}
	if len(starts) != 1 || len(users) != 1 {
		return Entry{}, Entry{}, errors.New("SubAgent admission journal is not unique")
	}
	started, user := starts[0], users[0]
	if ValidateEntryIntegrity(started) != nil || ValidateEntryIntegrity(user) != nil || user.ParentID != started.ID || user.Metadata[SubAgentUserMessageOriginMetadataKey] != origin || !reflect.DeepEqual(user.Message, input.Message) {
		return Entry{}, Entry{}, errors.New("SubAgent admission journal does not match its input")
	}
	return started, user, nil
}

func validateLegacyTerminalSource(state memoryState, legacy decodedLegacyAdmissionState, childThreadID string, input SubAgentInputRecord, finish legacyTurnFinish) error {
	if finish.ThreadID != childThreadID || finish.TurnID != input.AdmittedTurnID || finish.RunID != input.AdmittedRunID || finish.Generation <= 0 || finish.Generation > legacy.generations[childThreadID] || strings.TrimSpace(finish.OutcomeFingerprint) == "" || strings.TrimSpace(finish.TerminalEntryID) == "" {
		return errors.New("SubAgent terminal finish authority is invalid")
	}
	terminal, found := findEntry(state.Entries[childThreadID], finish.TerminalEntryID)
	if !found || ValidateEntryIntegrity(terminal) != nil || terminal.ThreadID != childThreadID || terminal.TurnID != input.AdmittedTurnID || terminal.Type != EntryTurnMarker || !terminalTurnMarker(terminal.TurnStatus) {
		return errors.New("SubAgent terminal journal is invalid")
	}
	if finish.FailureEntryID != "" {
		failure, found := findEntry(state.Entries[childThreadID], finish.FailureEntryID)
		if !found || ValidateEntryIntegrity(failure) != nil || failure.ThreadID != childThreadID || failure.TurnID != input.AdmittedTurnID || failure.Type != EntryRunFailure || terminal.ParentID != failure.ID {
			return errors.New("SubAgent terminal failure journal is invalid")
		}
	}
	for _, attempt := range state.EffectAttempts {
		if attempt.Invocation.ThreadID != childThreadID || attempt.Invocation.TurnID != input.AdmittedTurnID {
			continue
		}
		if attempt.Invocation.RunID != input.AdmittedRunID || attempt.Generation > finish.Generation || !effectAttemptTerminalSafe(EffectAttemptState(attempt.State)) {
			return errors.New("SubAgent terminal effect authority is invalid")
		}
	}
	return nil
}

func legacyTerminalAdmissionSourceHash(input SubAgentInputRecord, started, user Entry, finish legacyTurnFinish) (string, error) {
	source, err := json.Marshal(struct {
		Input  SubAgentInputRecord
		Start  string
		User   string
		Finish legacyTurnFinish
	}{input, started.RawHash, user.RawHash, finish})
	if err != nil {
		return "", err
	}
	return StableHash(string(source)), nil
}

func validateLegacyTurnLease(lease legacyTurnLease) error {
	if strings.TrimSpace(lease.ThreadID) == "" || legacyLeasePurpose(lease) != "turn" || strings.TrimSpace(lease.TurnID) == "" || strings.TrimSpace(lease.OwnerID) == "" || lease.Generation <= 0 || lease.Heartbeat <= 0 || lease.AcquiredAt.IsZero() || lease.RenewedAt.IsZero() || lease.ExpiresAt.IsZero() || !lease.ExpiresAt.After(lease.RenewedAt) {
		return errors.New("legacy turn lease is invalid")
	}
	return nil
}

func legacyLeasePurpose(lease legacyTurnLease) string {
	if strings.TrimSpace(lease.Purpose) == "" {
		return "turn"
	}
	return strings.TrimSpace(lease.Purpose)
}

func legacySubAgentAdmissionFingerprint(input SubAgentInputRecord, ownerID string) string {
	return StableHash(strings.Join([]string{"subagent-input-admission", strings.TrimSpace(input.ParentThreadID), strings.TrimSpace(input.ChildThreadID), strings.TrimSpace(input.AdmittedTurnID), strings.TrimSpace(input.AdmittedRunID), strings.TrimSpace(ownerID), strings.TrimSpace(input.SubAgentInputID)}, "\x00"))
}

func legacyTurnKey(threadID, turnID string) string {
	return strings.TrimSpace(threadID) + "\x00" + strings.TrimSpace(turnID)
}
