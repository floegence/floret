package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
)

// memoryStateVersion identifies the last monolithic checkpoint shape. Schema
// v6 reads this shape only while migrating legacy stores.
const memoryStateVersion = 5

type memoryState struct {
	Version           int                         `json:"version"`
	Threads           map[string]ThreadMeta       `json:"threads"`
	Entries           map[string][]Entry          `json:"entries"`
	EntryOrdinals     map[string]map[string]int   `json:"entry_ordinals"`
	EntryDepths       map[string]map[string]int64 `json:"entry_depths"`
	TurnEntryOrdinals map[string]map[string][]int `json:"turn_entry_ordinals"`
	TurnEntryCounts   map[string]map[string]int   `json:"turn_entry_counts"`
	// The fields below this marker are decoded only while migrating a v4
	// snapshot. Version 5 never encodes them; canonical entries replace their
	// lifecycle, receipt, handoff, and projection authority.
	Leases                         json.RawMessage                   `json:"leases,omitempty"`
	LeaseGeneration                json.RawMessage                   `json:"lease_generation,omitempty"`
	LeasePolicy                    json.RawMessage                   `json:"lease_policy,omitempty"`
	AuthorityClaims                json.RawMessage                   `json:"authority_claims,omitempty"`
	Todos                          map[string]AgentTodoState         `json:"todos"`
	SubAgentInputs                 json.RawMessage                   `json:"subagent_inputs,omitempty"`
	SubAgentInputSequence          json.RawMessage                   `json:"subagent_input_sequence,omitempty"`
	SubAgentPublications           json.RawMessage                   `json:"subagent_publications,omitempty"`
	SubAgentInputRequests          json.RawMessage                   `json:"subagent_input_requests,omitempty"`
	RootCreateIntents              map[string]legacyRootCreateRecord `json:"root_create_intents,omitempty"`
	Tombstones                     map[string]ThreadTombstone        `json:"tombstones"`
	TurnAdmissions                 json.RawMessage                   `json:"turn_admissions,omitempty"`
	TurnFinishes                   json.RawMessage                   `json:"turn_finishes,omitempty"`
	EffectAttempts                 map[string]legacyEffectAttemptV6  `json:"effect_attempts"`
	EffectAttemptByInvocation      map[string]string                 `json:"effect_attempt_by_invocation"`
	EffectAttemptSequence          int64                             `json:"effect_attempt_sequence,omitempty"`
	ApprovalQueues                 json.RawMessage                   `json:"approval_queues,omitempty"`
	Approvals                      json.RawMessage                   `json:"approvals,omitempty"`
	ApprovalByEffectAttempt        json.RawMessage                   `json:"approval_by_effect_attempt,omitempty"`
	ApprovalDecisions              json.RawMessage                   `json:"approval_decisions,omitempty"`
	SubAgentCloseOperations        json.RawMessage                   `json:"subagent_close_operations,omitempty"`
	PendingToolCompletions         json.RawMessage                   `json:"pending_tool_completions,omitempty"`
	SubAgentPendingToolCompletions json.RawMessage                   `json:"subagent_pending_tool_completions,omitempty"`
	CompactionOperations           json.RawMessage                   `json:"compaction_operations,omitempty"`
	ProviderStates                 map[string]ProviderStateRecord    `json:"provider_states"`
	Artifacts                      map[string]artifact.Record        `json:"artifacts"`
	ThreadRevisions                json.RawMessage                   `json:"thread_revisions,omitempty"`
	ThreadRevisionHistory          json.RawMessage                   `json:"thread_revision_history,omitempty"`
	Sequence                       int64                             `json:"sequence"`
}

// EncodeMemoryState returns the detached strict v5 checkpoint representation
// used only by the contiguous legacy migration tests and reader. Production
// schema-v7 mutations persist segmented records instead.
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
		Todos: repo.todos, Tombstones: repo.tombstones,
		EffectAttempts: map[string]legacyEffectAttemptV6{}, EffectAttemptByInvocation: map[string]string{},
		ProviderStates: repo.providerStates,
		Artifacts:      repo.artifacts, Sequence: repo.seq,
	}
}

// DecodeMemoryState constructs a repo from one exact legacy checkpoint.
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
	if state.Version >= 2 && state.Version <= 4 {
		if err := decodeLegacyMemoryStateShapes(data, &state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
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
		if err := migrateMemoryStateV4ToV5(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		migrated = true
	case 3:
		if err := migrateMemoryStateV3ToV4(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := migrateMemoryStateV4ToV5(&state); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		migrated = true
	case 4:
		if err := migrateMemoryStateV4ToV5(&state); err != nil {
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
	repo := &MemoryRepo{
		threads: state.Threads, entries: state.Entries, entryOrdinals: state.EntryOrdinals,
		entryDepths: state.EntryDepths, turnEntryOrdinals: state.TurnEntryOrdinals,
		turnEntryCounts: state.TurnEntryCounts, now: now, todos: state.Todos,
		tombstones: state.Tombstones, providerStates: state.ProviderStates,
		artifacts: state.Artifacts, seq: state.Sequence,
	}
	repo.ensurePersistentMaps()
	repairedTitles, err := repairLegacyFallbackThreadTitles(repo)
	if err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	migrated = migrated || repairedTitles
	if err := ValidateThreadAuthorityGraph(values(repo.threads)); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	return repo, migrated, nil
}

func validateRequiredMemoryStateMaps(state memoryState) error {
	if state.Threads == nil || state.Entries == nil || state.EntryOrdinals == nil || state.EntryDepths == nil ||
		state.TurnEntryOrdinals == nil || state.TurnEntryCounts == nil || state.Todos == nil ||
		state.Tombstones == nil || state.EffectAttempts == nil || state.EffectAttemptByInvocation == nil ||
		state.ProviderStates == nil || state.Artifacts == nil {
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
	emptyMap(&repo.todos)
	emptyMap(&repo.tombstones)
	emptyMap(&repo.providerStates)
	emptyMap(&repo.artifacts)
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
