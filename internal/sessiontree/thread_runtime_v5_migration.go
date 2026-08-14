package sessiontree

import (
	"errors"
	"strings"
)

type legacyRootCreateRecord struct {
	ThreadID        string `json:"thread_id"`
	CreateIntentID  string `json:"create_intent_id"`
	Fingerprint     string `json:"fingerprint"`
	ContractVersion string `json:"contract_version"`
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
	for requestKey, intent := range state.RootCreateIntents {
		meta, ok := state.Threads[intent.ThreadID]
		if !ok {
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
		state.EffectAttempts = make(map[string]EffectAttempt)
	}
	if state.EffectAttemptByInvocation == nil {
		state.EffectAttemptByInvocation = make(map[string]string)
	}
	state.Version = 5
	return nil
}
