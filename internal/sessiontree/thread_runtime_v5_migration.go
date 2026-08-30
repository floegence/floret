package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type legacyRootCreateRecord struct {
	ThreadID        string `json:"thread_id"`
	CreateIntentID  string `json:"create_intent_id"`
	Fingerprint     string `json:"fingerprint"`
	ContractVersion string `json:"contract_version"`
}

func (record *legacyRootCreateRecord) UnmarshalJSON(data []byte) error {
	values, err := legacyShapeValues(data, map[string]struct{}{
		"thread_id": {}, "ThreadID": {}, "create_intent_id": {}, "CreateIntentID": {},
		"fingerprint": {}, "Fingerprint": {}, "contract_version": {}, "ContractVersion": {},
	})
	if err != nil {
		return err
	}
	if record.ThreadID, err = legacyShapeString(values, "thread_id", "ThreadID"); err != nil {
		return err
	}
	if record.CreateIntentID, err = legacyShapeString(values, "create_intent_id", "CreateIntentID"); err != nil {
		return err
	}
	if record.Fingerprint, err = legacyShapeString(values, "fingerprint", "Fingerprint"); err != nil {
		return err
	}
	if record.ContractVersion, err = legacyShapeString(values, "contract_version", "ContractVersion"); err != nil {
		return err
	}
	return nil
}

func (record *legacyV4Tombstone) UnmarshalJSON(data []byte) error {
	values, err := legacyShapeValues(data, map[string]struct{}{
		"thread_id": {}, "ThreadID": {}, "root_thread_id": {}, "RootThreadID": {},
		"parent_thread_id": {}, "ParentThreadID": {}, "create_intent_id": {}, "CreateIntentID": {},
		"fork_operation_id": {}, "ForkOperationID": {}, "fork_operation_node_id": {}, "ForkOperationNodeID": {},
		"forked_from_thread_id": {}, "ForkedFromThreadID": {}, "forked_from_entry_id": {}, "ForkedFromEntryID": {},
		"origin_request_key": {}, "OriginRequestKey": {}, "origin_fingerprint": {}, "OriginFingerprint": {},
		"delete_request_key": {}, "DeleteRequestKey": {}, "delete_fingerprint": {}, "DeleteFingerprint": {},
		"LegacyCreateIntent": {}, "LegacyForkRequestID": {}, "LegacyForkNodeID": {},
		"deleted_at": {}, "DeletedAt": {},
	})
	if err != nil {
		return err
	}
	if record.ThreadID, err = legacyShapeString(values, "thread_id", "ThreadID"); err != nil {
		return err
	}
	if record.RootThreadID, err = legacyShapeString(values, "root_thread_id", "RootThreadID"); err != nil {
		return err
	}
	if record.ParentThreadID, err = legacyShapeString(values, "parent_thread_id", "ParentThreadID"); err != nil {
		return err
	}
	if record.CreateIntentID, err = legacyShapeString(values, "create_intent_id", "CreateIntentID"); err != nil {
		return err
	}
	if record.ForkOperationID, err = legacyShapeString(values, "fork_operation_id", "ForkOperationID"); err != nil {
		return err
	}
	if record.ForkOperationNodeID, err = legacyShapeString(values, "fork_operation_node_id", "ForkOperationNodeID"); err != nil {
		return err
	}
	if record.ForkedFromThreadID, err = legacyShapeString(values, "forked_from_thread_id", "ForkedFromThreadID"); err != nil {
		return err
	}
	if record.ForkedFromEntryID, err = legacyShapeString(values, "forked_from_entry_id", "ForkedFromEntryID"); err != nil {
		return err
	}
	if raw, found := legacyShapeValue(values, "deleted_at", "DeletedAt"); found {
		if err := json.Unmarshal(raw, &record.DeletedAt); err != nil {
			return err
		}
	}
	return nil
}

func legacyShapeValues(data []byte, known map[string]struct{}) (map[string]json.RawMessage, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	for key := range values {
		if _, ok := known[key]; !ok {
			return nil, fmt.Errorf("unknown legacy session-tree field %q", key)
		}
	}
	for _, aliases := range [][2]string{
		{"thread_id", "ThreadID"}, {"root_thread_id", "RootThreadID"},
		{"parent_thread_id", "ParentThreadID"}, {"create_intent_id", "CreateIntentID"},
		{"fork_operation_id", "ForkOperationID"}, {"fork_operation_node_id", "ForkOperationNodeID"},
		{"forked_from_thread_id", "ForkedFromThreadID"}, {"forked_from_entry_id", "ForkedFromEntryID"},
		{"deleted_at", "DeletedAt"}, {"fingerprint", "Fingerprint"}, {"contract_version", "ContractVersion"},
	} {
		if _, snake := values[aliases[0]]; snake {
			if _, legacy := values[aliases[1]]; legacy {
				return nil, fmt.Errorf("ambiguous legacy session-tree fields %q and %q", aliases[0], aliases[1])
			}
		}
	}
	return values, nil
}

func legacyShapeValue(values map[string]json.RawMessage, snake, pascal string) (json.RawMessage, bool) {
	if value, found := values[snake]; found {
		return value, true
	}
	value, found := values[pascal]
	return value, found
}

func legacyShapeString(values map[string]json.RawMessage, snake, pascal string) (string, error) {
	raw, found := legacyShapeValue(values, snake, pascal)
	if !found {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

// Version 5 moves create and fork replay identity onto canonical thread
// metadata. Runtime lifecycle is rebuilt from the journal; the migration does
// not manufacture provider or tool outcomes.
func migrateMemoryStateV4ToV5(state *memoryState) error {
	if state == nil || state.Version != 4 {
		return errors.New("session-tree v4 migration requires exact version 4 state")
	}
	if err := validateRequiredMemoryStateMaps(*state); err != nil {
		return err
	}
	if err := validateLegacyMemoryStateMaps(*state); err != nil {
		return err
	}
	legacy, err := decodeLegacyAdmissionState(*state)
	if err != nil {
		return err
	}
	if err := validateLegacySubAgentAdmissions(*state, legacy); err != nil {
		return err
	}
	for requestKey, intent := range state.RootCreateIntents {
		requestKey = strings.TrimSpace(requestKey)
		if requestKey == "" || strings.TrimSpace(intent.ThreadID) == "" || strings.TrimSpace(intent.CreateIntentID) == "" || strings.TrimSpace(intent.Fingerprint) == "" || strings.TrimSpace(intent.ContractVersion) == "" || intent.CreateIntentID != requestKey {
			return errors.New("session-tree v4 root origin authority is incomplete")
		}
		expectedFingerprint := StableHash(strings.Join([]string{intent.ThreadID, intent.CreateIntentID, intent.ContractVersion}, "\x00"))
		if intent.Fingerprint != expectedFingerprint {
			return errors.New("session-tree v4 root origin fingerprint does not match its source")
		}
		meta, ok := state.Threads[intent.ThreadID]
		if !ok {
			tombstone, deleted := state.Tombstones[intent.ThreadID]
			if !deleted || tombstone.LegacyCreateIntent != intent.CreateIntentID {
				return errors.New("session-tree v4 root origin has no canonical thread or tombstone")
			}
			continue
		}
		if meta.OriginRequestKey != "" && (meta.OriginRequestKey != requestKey || meta.OriginFingerprint != intent.Fingerprint) {
			return errors.New("session-tree v4 root origin identity conflicts with canonical metadata")
		}
		meta.OriginRequestKey = strings.TrimSpace(requestKey)
		meta.OriginFingerprint = strings.TrimSpace(intent.Fingerprint)
		state.Threads[intent.ThreadID] = meta
	}
	for threadID, meta := range state.Threads {
		if meta.OriginRequestKey != "" || strings.TrimSpace(meta.LegacyForkRequestID) == "" {
			continue
		}
		meta.OriginRequestKey = strings.TrimSpace(meta.LegacyForkRequestID)
		meta.OriginFingerprint = StableHash(strings.Join([]string{
			meta.ForkedFromThreadID, meta.ForkedFromEntryID, meta.LegacyForkRequestID, meta.LegacyForkNodeID,
		}, "\x00"))
		meta.LegacyForkRequestID = ""
		meta.LegacyForkNodeID = ""
		state.Threads[threadID] = meta
	}
	for threadID, tombstone := range state.Tombstones {
		if tombstone.OriginRequestKey == "" {
			if intent, ok := state.RootCreateIntents[tombstone.LegacyCreateIntent]; ok {
				tombstone.OriginRequestKey = strings.TrimSpace(intent.CreateIntentID)
				tombstone.OriginFingerprint = strings.TrimSpace(intent.Fingerprint)
			} else if tombstone.LegacyForkRequestID != "" {
				tombstone.OriginRequestKey = strings.TrimSpace(tombstone.LegacyForkRequestID)
				tombstone.OriginFingerprint = StableHash(strings.Join([]string{
					tombstone.ForkedFromThreadID, tombstone.ForkedFromEntryID,
					tombstone.LegacyForkRequestID, tombstone.LegacyForkNodeID,
				}, "\x00"))
			}
		}
		tombstone.LegacyCreateIntent = ""
		tombstone.LegacyForkRequestID = ""
		tombstone.LegacyForkNodeID = ""
		state.Tombstones[threadID] = tombstone
	}
	// v4 persisted several materialized lifecycle authorities. Their durable
	// facts already exist in the canonical journal; v5 deliberately drops the
	// mirrors instead of migrating them into another ledger.
	state.Leases = nil
	state.LeaseGeneration = nil
	state.AuthorityClaims = nil
	state.TurnAdmissions = nil
	state.TurnFinishes = nil
	state.ApprovalQueues = nil
	state.Approvals = nil
	state.ApprovalByEffectAttempt = nil
	state.ApprovalDecisions = nil
	state.ThreadRevisions = nil
	state.ThreadRevisionHistory = nil
	state.RootCreateIntents = nil
	state.SubAgentPublications = nil
	state.SubAgentInputRequests = nil
	state.SubAgentCloseOperations = nil
	state.PendingToolCompletions = nil
	state.SubAgentPendingToolCompletions = nil
	state.CompactionOperations = nil
	if state.EffectAttempts == nil {
		state.EffectAttempts = make(map[string]legacyEffectAttemptV6)
	}
	if state.EffectAttemptByInvocation == nil {
		state.EffectAttemptByInvocation = make(map[string]string)
	}
	for effectID, attempt := range state.EffectAttempts {
		attempt.OwnerID = ""
		attempt.Generation = 0
		state.EffectAttempts[effectID] = attempt
	}
	state.Version = 5
	return nil
}
