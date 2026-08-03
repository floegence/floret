package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/floegence/floret/v3/internal/session/artifact"
)

const memoryStateVersion = 4

type memoryState struct {
	Version                        int                                               `json:"version"`
	Threads                        map[string]ThreadMeta                             `json:"threads"`
	Entries                        map[string][]Entry                                `json:"entries"`
	EntryOrdinals                  map[string]map[string]int                         `json:"entry_ordinals"`
	EntryDepths                    map[string]map[string]int64                       `json:"entry_depths"`
	TurnEntryOrdinals              map[string]map[string][]int                       `json:"turn_entry_ordinals"`
	TurnEntryCounts                map[string]map[string]int                         `json:"turn_entry_counts"`
	Leases                         map[string]TurnLease                              `json:"leases"`
	LeaseGeneration                map[string]int64                                  `json:"lease_generation"`
	LeasePolicy                    LeasePolicy                                       `json:"lease_policy"`
	AuthorityClaims                map[string]string                                 `json:"authority_claims"`
	Todos                          map[string]AgentTodoState                         `json:"todos"`
	SubAgentInputs                 map[string][]SubAgentInputRecord                  `json:"subagent_inputs"`
	SubAgentInputSequence          map[string]int64                                  `json:"subagent_input_sequence"`
	SubAgentPublications           map[string]subAgentRequestLedger                  `json:"subagent_publications"`
	SubAgentInputRequests          map[string]subAgentRequestLedger                  `json:"subagent_input_requests"`
	RootCreateIntents              map[string]rootCreateLedger                       `json:"root_create_intents"`
	Tombstones                     map[string]ThreadTombstone                        `json:"tombstones"`
	TurnAdmissions                 map[string]turnAdmissionLedger                    `json:"turn_admissions"`
	TurnFinishes                   map[string]turnFinishLedger                       `json:"turn_finishes"`
	EffectAttempts                 map[string]EffectAttempt                          `json:"effect_attempts"`
	EffectAttemptByInvocation      map[string]string                                 `json:"effect_attempt_by_invocation"`
	EffectAttemptSequence          int64                                             `json:"effect_attempt_sequence"`
	ApprovalQueues                 map[string]approvalQueueLedger                    `json:"approval_queues"`
	Approvals                      map[string]ApprovalRecord                         `json:"approvals"`
	ApprovalByEffectAttempt        map[string]string                                 `json:"approval_by_effect_attempt"`
	ApprovalDecisions              map[string]approvalDecisionLedger                 `json:"approval_decisions"`
	SubAgentCloseOperations        map[string]SubAgentCloseOperation                 `json:"subagent_close_operations"`
	PendingToolCompletions         map[string]pendingToolCompletionLedger            `json:"pending_tool_completions"`
	SubAgentPendingToolCompletions map[string]subAgentPendingToolCompletionLedger    `json:"subagent_pending_tool_completions"`
	CompactionOperations           map[string]CompactionOperation                    `json:"compaction_operations"`
	ProviderStates                 map[string]ProviderStateRecord                    `json:"provider_states"`
	Artifacts                      map[string]artifact.Record                        `json:"artifacts"`
	ThreadRevisions                map[string]ThreadRevision                         `json:"thread_revisions"`
	ThreadRevisionHistory          map[string]map[ThreadRevision]threadRevisionDelta `json:"thread_revision_history"`
	Sequence                       int64                                             `json:"sequence"`
}

// EncodeMemoryState returns a detached strict representation of the complete
// session-tree domain state. The caller owns the returned bytes.
func (repo *MemoryRepo) EncodeMemoryState() ([]byte, error) {
	if repo == nil {
		return nil, errors.New("memory repo is required")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return json.Marshal(repo.memoryStateLocked())
}

func (repo *MemoryRepo) memoryStateLocked() memoryState {
	return memoryState{
		Version: memoryStateVersion, Threads: repo.threads, Entries: repo.entries,
		EntryOrdinals: repo.entryOrdinals, EntryDepths: repo.entryDepths,
		TurnEntryOrdinals: repo.turnEntryOrdinals, TurnEntryCounts: repo.turnEntryCounts,
		Leases: repo.leases, LeaseGeneration: repo.leaseGeneration, LeasePolicy: repo.leasePolicy,
		AuthorityClaims: repo.authorityClaims, Todos: repo.todos,
		SubAgentInputs: repo.subAgentInputs, SubAgentInputSequence: repo.subAgentInputSequence,
		SubAgentPublications: repo.subAgentPublications, SubAgentInputRequests: repo.subAgentInputRequests,
		RootCreateIntents: repo.rootCreateIntents, Tombstones: repo.tombstones,
		TurnAdmissions: repo.turnAdmissions, TurnFinishes: repo.turnFinishes,
		EffectAttempts: repo.effectAttempts, EffectAttemptByInvocation: repo.effectAttemptByInvocation,
		EffectAttemptSequence: repo.effectAttemptSequence, ApprovalQueues: repo.approvalQueues,
		Approvals: repo.approvals, ApprovalByEffectAttempt: repo.approvalByEffectAttempt,
		ApprovalDecisions: repo.approvalDecisions, SubAgentCloseOperations: repo.subAgentCloseOperations,
		PendingToolCompletions:         repo.pendingToolCompletions,
		SubAgentPendingToolCompletions: repo.subAgentPendingToolCompletions,
		CompactionOperations:           repo.compactionOperations, ProviderStates: repo.providerStates,
		Artifacts: repo.artifacts, ThreadRevisions: repo.threadRevisions,
		ThreadRevisionHistory: repo.threadRevisionHistory, Sequence: repo.seq,
	}
}

// DecodeMemoryState constructs a repo from one exact encoded state.
func DecodeMemoryState(data []byte, now func() time.Time) (*MemoryRepo, error) {
	repo, _, err := decodeMemoryState(data, now)
	return repo, err
}

func decodeMemoryState(data []byte, now func() time.Time) (*MemoryRepo, bool, error) {
	if now == nil {
		return nil, false, errors.New("lease authority clock is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state memoryState
	if err := decoder.Decode(&state); err != nil {
		return nil, false, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, false, errors.New("session-tree state contains trailing data")
		}
		return nil, false, err
	}
	migrated := false
	switch state.Version {
	case 2:
		if err := migrateMemoryStateV2ToV3(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := migrateMemoryStateV3ToV4(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		migrated = true
	case 3:
		if err := migrateMemoryStateV3ToV4(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		migrated = true
	case memoryStateVersion:
	default:
		return nil, false, errors.New("unsupported session-tree state version")
	}
	if err := validateRequiredMemoryStateMaps(state); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := state.LeasePolicy.Validate(); err != nil {
		return nil, false, err
	}
	repo := &MemoryRepo{
		threads: state.Threads, entries: state.Entries, entryOrdinals: state.EntryOrdinals,
		entryDepths: state.EntryDepths, turnEntryOrdinals: state.TurnEntryOrdinals,
		turnEntryCounts: state.TurnEntryCounts, leases: state.Leases,
		leaseGeneration: state.LeaseGeneration, leasePolicy: state.LeasePolicy, now: now,
		authorityClaims: state.AuthorityClaims, todos: state.Todos,
		subAgentInputs: state.SubAgentInputs, subAgentInputSequence: state.SubAgentInputSequence,
		subAgentPublications: state.SubAgentPublications, subAgentInputRequests: state.SubAgentInputRequests,
		rootCreateIntents: state.RootCreateIntents, tombstones: state.Tombstones,
		turnAdmissions: state.TurnAdmissions, turnFinishes: state.TurnFinishes,
		effectAttempts: state.EffectAttempts, effectAttemptByInvocation: state.EffectAttemptByInvocation,
		effectAttemptSequence: state.EffectAttemptSequence, approvalQueues: state.ApprovalQueues,
		approvals: state.Approvals, approvalByEffectAttempt: state.ApprovalByEffectAttempt,
		approvalDecisions: state.ApprovalDecisions, approvalSignals: map[string]chan struct{}{},
		subAgentCloseOperations:        state.SubAgentCloseOperations,
		pendingToolCompletions:         state.PendingToolCompletions,
		subAgentPendingToolCompletions: state.SubAgentPendingToolCompletions,
		compactionOperations:           state.CompactionOperations, providerStates: state.ProviderStates,
		artifacts: state.Artifacts, threadRevisions: state.ThreadRevisions,
		threadRevisionHistory: state.ThreadRevisionHistory, seq: state.Sequence,
	}
	repo.ensurePersistentMaps()
	if err := ValidateThreadAuthorityGraph(values(repo.threads)); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := validateSubAgentAdmissionState(repo); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return repo, migrated, nil
}

func validateRequiredMemoryStateMaps(state memoryState) error {
	if state.Threads == nil || state.Entries == nil || state.EntryOrdinals == nil || state.EntryDepths == nil ||
		state.TurnEntryOrdinals == nil || state.TurnEntryCounts == nil || state.Leases == nil || state.LeaseGeneration == nil ||
		state.AuthorityClaims == nil || state.Todos == nil || state.SubAgentInputs == nil || state.SubAgentInputSequence == nil ||
		state.SubAgentPublications == nil || state.SubAgentInputRequests == nil || state.RootCreateIntents == nil ||
		state.Tombstones == nil || state.TurnAdmissions == nil || state.TurnFinishes == nil || state.EffectAttempts == nil ||
		state.EffectAttemptByInvocation == nil || state.ApprovalQueues == nil || state.Approvals == nil ||
		state.ApprovalByEffectAttempt == nil || state.ApprovalDecisions == nil || state.SubAgentCloseOperations == nil ||
		state.PendingToolCompletions == nil || state.SubAgentPendingToolCompletions == nil || state.CompactionOperations == nil ||
		state.ProviderStates == nil || state.Artifacts == nil || state.ThreadRevisions == nil || state.ThreadRevisionHistory == nil {
		return errors.New("session-tree persistent maps must not be null")
	}
	return nil
}

func (repo *MemoryRepo) ensurePersistentMaps() {
	emptyMap(&repo.threads)
	emptyMap(&repo.entries)
	emptyMap(&repo.entryOrdinals)
	emptyMap(&repo.entryDepths)
	emptyMap(&repo.turnEntryOrdinals)
	emptyMap(&repo.turnEntryCounts)
	emptyMap(&repo.leases)
	emptyMap(&repo.leaseGeneration)
	emptyMap(&repo.authorityClaims)
	emptyMap(&repo.todos)
	emptyMap(&repo.subAgentInputs)
	emptyMap(&repo.subAgentInputSequence)
	emptyMap(&repo.subAgentPublications)
	emptyMap(&repo.subAgentInputRequests)
	emptyMap(&repo.rootCreateIntents)
	emptyMap(&repo.tombstones)
	emptyMap(&repo.turnAdmissions)
	emptyMap(&repo.turnFinishes)
	emptyMap(&repo.effectAttempts)
	emptyMap(&repo.effectAttemptByInvocation)
	emptyMap(&repo.approvalQueues)
	emptyMap(&repo.approvals)
	emptyMap(&repo.approvalByEffectAttempt)
	emptyMap(&repo.approvalDecisions)
	emptyMap(&repo.approvalSignals)
	emptyMap(&repo.subAgentCloseOperations)
	emptyMap(&repo.pendingToolCompletions)
	emptyMap(&repo.subAgentPendingToolCompletions)
	emptyMap(&repo.compactionOperations)
	emptyMap(&repo.providerStates)
	emptyMap(&repo.artifacts)
	emptyMap(&repo.threadRevisions)
	emptyMap(&repo.threadRevisionHistory)
}

func emptyMap[K comparable, V any](target *map[K]V) {
	if *target == nil {
		*target = make(map[K]V)
	}
}

func values[K comparable, V any](source map[K]V) []V {
	result := make([]V, 0, len(source))
	for _, value := range source {
		result = append(result, value)
	}
	return result
}
