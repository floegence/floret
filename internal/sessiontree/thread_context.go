package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/observation"
)

const (
	ThreadContextPolicyEntryKind     = "thread_context_policy"
	ThreadContextStatusEntryKind     = "thread_context_status"
	ThreadContextCompactionEntryKind = "thread_context_compaction"

	legacyThreadContextPolicyEntryKind     = "subagent_context_policy"
	legacyThreadContextStatusEntryKind     = "subagent_context_status"
	legacyThreadContextCompactionEntryKind = "subagent_context_compaction"

	threadContextKindKey       = "kind"
	threadContextTypeKey       = "type"
	threadContextProviderKey   = "provider"
	threadContextModelKey      = "model"
	threadContextPolicyKey     = "context_policy_json"
	threadContextStatusKey     = "context_status_json"
	threadContextCompactionKey = "context_compaction_json"
)

// ThreadContextPolicy is the durable, product-neutral context budget attached
// to one canonical context entry.
type ThreadContextPolicy struct {
	ContextWindowTokens  int64 `json:"context_window_tokens,omitempty"`
	MaxOutputTokens      int64 `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens int64 `json:"reserved_output_tokens,omitempty"`
}

// ThreadContextPolicyRecord joins a decoded policy with its provider model.
// Execution identity remains authoritative on the containing Entry.
type ThreadContextPolicyRecord struct {
	Provider string
	Model    string
	Policy   ThreadContextPolicy
}

// ThreadContextCompaction is the canonical public projection of one context
// compaction lifecycle update.
type ThreadContextCompaction struct {
	RunID               string    `json:"run_id,omitempty"`
	ThreadID            string    `json:"thread_id,omitempty"`
	TurnID              string    `json:"turn_id,omitempty"`
	Step                int       `json:"step,omitempty"`
	OperationID         string    `json:"operation_id,omitempty"`
	RequestID           string    `json:"request_id,omitempty"`
	Phase               string    `json:"phase,omitempty"`
	Status              string    `json:"status,omitempty"`
	Trigger             string    `json:"trigger,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	Source              string    `json:"source,omitempty"`
	TokensBefore        int64     `json:"tokens_before,omitempty"`
	TokensAfterEstimate int64     `json:"tokens_after_estimate,omitempty"`
	Error               string    `json:"error,omitempty"`
	ObservedAt          time.Time `json:"observed_at,omitempty"`
}

func NewThreadContextPolicyEntry(threadID, turnID, runID, providerName, modelName string, policy contextpolicy.Policy) (Entry, error) {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	providerName = strings.TrimSpace(providerName)
	modelName = strings.TrimSpace(modelName)
	if threadID == "" || turnID == "" || runID == "" || providerName == "" || modelName == "" {
		return Entry{}, errors.New("thread context policy requires thread, turn, run, provider, and model")
	}
	normalized := contextpolicy.Normalize(policy)
	record := ThreadContextPolicyRecord{
		Provider: providerName,
		Model:    modelName,
		Policy: ThreadContextPolicy{
			ContextWindowTokens:  normalized.ContextWindowTokens,
			MaxOutputTokens:      normalized.MaxOutputTokens,
			ReservedOutputTokens: normalized.ReservedOutputTokens,
		},
	}
	metadata, err := encodeThreadContextPolicyMetadata(record)
	if err != nil {
		return Entry{}, err
	}
	return Entry{ThreadID: threadID, TurnID: turnID, RunID: runID, Type: EntryCustom, Metadata: metadata}, nil
}

func NewThreadContextStatusEntry(status observation.ContextStatus) (Entry, error) {
	if err := validateThreadContextStatusIdentity(status); err != nil {
		return Entry{}, err
	}
	metadata, err := encodeThreadContextStatusMetadata(status)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		ThreadID: status.ThreadID.String(),
		TurnID:   status.TurnID.String(),
		RunID:    status.RunID.String(),
		Type:     EntryCustom,
		Metadata: metadata,
	}, nil
}

func NewThreadContextCompactionEntry(compaction ThreadContextCompaction) (Entry, error) {
	if err := validateThreadContextCompaction(compaction); err != nil {
		return Entry{}, err
	}
	metadata, err := encodeThreadContextCompactionMetadata(compaction)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		ThreadID: strings.TrimSpace(compaction.ThreadID),
		TurnID:   strings.TrimSpace(compaction.TurnID),
		RunID:    strings.TrimSpace(compaction.RunID),
		Type:     EntryCustom,
		Metadata: metadata,
	}, nil
}

func DecodeThreadContextPolicyEntry(entry Entry) (ThreadContextPolicyRecord, error) {
	if err := validateThreadContextEntryShape(entry, ThreadContextPolicyEntryKind); err != nil {
		return ThreadContextPolicyRecord{}, err
	}
	return decodeThreadContextPolicyMetadata(entry.Metadata)
}

func DecodeThreadContextStatusEntry(entry Entry) (observation.ContextStatus, error) {
	if err := validateThreadContextEntryShape(entry, ThreadContextStatusEntryKind); err != nil {
		return observation.ContextStatus{}, err
	}
	status, err := decodeThreadContextStatusPayload(entry.Metadata[threadContextStatusKey], false)
	if err != nil {
		return observation.ContextStatus{}, err
	}
	status.ThreadID = identity.ThreadID(strings.TrimSpace(entry.ThreadID))
	status.TurnID = identity.TurnID(strings.TrimSpace(entry.TurnID))
	status.RunID = identity.RunID(strings.TrimSpace(entry.RunID))
	if err := validateThreadContextStatusIdentity(status); err != nil {
		return observation.ContextStatus{}, err
	}
	return status, nil
}

func DecodeThreadContextCompactionEntry(entry Entry) (ThreadContextCompaction, error) {
	if err := validateThreadContextEntryShape(entry, ThreadContextCompactionEntryKind); err != nil {
		return ThreadContextCompaction{}, err
	}
	compaction, err := decodeThreadContextCompactionPayload(entry.Metadata[threadContextCompactionKey], false)
	if err != nil {
		return ThreadContextCompaction{}, err
	}
	compaction.ThreadID = strings.TrimSpace(entry.ThreadID)
	compaction.TurnID = strings.TrimSpace(entry.TurnID)
	compaction.RunID = strings.TrimSpace(entry.RunID)
	if err := validateThreadContextCompaction(compaction); err != nil {
		return ThreadContextCompaction{}, err
	}
	return compaction, nil
}

func ThreadContextEntryKind(entry Entry) string {
	if entry.Type != EntryCustom {
		return ""
	}
	return strings.TrimSpace(entry.Metadata[threadContextKindKey])
}

func validateThreadContextEntryV9(entry Entry) error {
	switch kind := ThreadContextEntryKind(entry); kind {
	case ThreadContextPolicyEntryKind:
		_, err := DecodeThreadContextPolicyEntry(entry)
		return err
	case ThreadContextStatusEntryKind:
		_, err := DecodeThreadContextStatusEntry(entry)
		return err
	case ThreadContextCompactionEntryKind:
		_, err := DecodeThreadContextCompactionEntry(entry)
		return err
	case legacyThreadContextPolicyEntryKind, legacyThreadContextStatusEntryKind, legacyThreadContextCompactionEntryKind:
		return errors.New("current thread context entry uses a legacy kind")
	default:
		if strings.HasPrefix(kind, "thread_context_") || strings.HasPrefix(kind, "subagent_context_") {
			return fmt.Errorf("unsupported thread context entry kind %q", kind)
		}
		return nil
	}
}

func validateThreadContextEntryShape(entry Entry, kind string) error {
	if entry.Type != EntryCustom || ThreadContextEntryKind(entry) != kind || strings.TrimSpace(entry.Metadata[threadContextTypeKey]) != kind {
		return fmt.Errorf("invalid %s entry shape", kind)
	}
	if strings.TrimSpace(entry.ThreadID) == "" || strings.TrimSpace(entry.TurnID) == "" || strings.TrimSpace(entry.RunID) == "" {
		return fmt.Errorf("%s entry requires thread, turn, and run identity", kind)
	}
	return nil
}

func encodeThreadContextPolicyMetadata(record ThreadContextPolicyRecord) (map[string]string, error) {
	if strings.TrimSpace(record.Provider) == "" || strings.TrimSpace(record.Model) == "" || record.Policy.ContextWindowTokens <= 0 {
		return nil, errors.New("thread context policy requires provider, model, and context window tokens")
	}
	raw, err := json.Marshal(record.Policy)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		threadContextKindKey:     ThreadContextPolicyEntryKind,
		threadContextTypeKey:     ThreadContextPolicyEntryKind,
		threadContextProviderKey: strings.TrimSpace(record.Provider),
		threadContextModelKey:    strings.TrimSpace(record.Model),
		threadContextPolicyKey:   string(raw),
	}, nil
}

func encodeThreadContextStatusMetadata(status observation.ContextStatus) (map[string]string, error) {
	if err := validateThreadContextStatusIdentity(status); err != nil {
		return nil, err
	}
	payload := status
	payload.RunID = ""
	payload.ThreadID = ""
	payload.TurnID = ""
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		threadContextKindKey:   ThreadContextStatusEntryKind,
		threadContextTypeKey:   ThreadContextStatusEntryKind,
		threadContextStatusKey: string(raw),
	}, nil
}

func encodeThreadContextCompactionMetadata(compaction ThreadContextCompaction) (map[string]string, error) {
	if err := validateThreadContextCompaction(compaction); err != nil {
		return nil, err
	}
	payload := compaction
	payload.RunID = ""
	payload.ThreadID = ""
	payload.TurnID = ""
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		threadContextKindKey:       ThreadContextCompactionEntryKind,
		threadContextTypeKey:       ThreadContextCompactionEntryKind,
		threadContextCompactionKey: string(raw),
	}, nil
}

func decodeThreadContextPolicyMetadata(metadata map[string]string) (ThreadContextPolicyRecord, error) {
	if err := requireExactThreadContextMetadata(metadata,
		threadContextKindKey, threadContextTypeKey, threadContextProviderKey, threadContextModelKey, threadContextPolicyKey,
	); err != nil {
		return ThreadContextPolicyRecord{}, err
	}
	record := ThreadContextPolicyRecord{
		Provider: strings.TrimSpace(metadata[threadContextProviderKey]),
		Model:    strings.TrimSpace(metadata[threadContextModelKey]),
	}
	if record.Provider == "" || record.Model == "" {
		return ThreadContextPolicyRecord{}, errors.New("thread context policy requires provider and model")
	}
	if err := decodeStrictThreadContextJSON(metadata[threadContextPolicyKey], &record.Policy, nil); err != nil {
		return ThreadContextPolicyRecord{}, fmt.Errorf("decode thread context policy: %w", err)
	}
	if record.Policy.ContextWindowTokens <= 0 {
		return ThreadContextPolicyRecord{}, errors.New("thread context policy requires context window tokens")
	}
	return record, nil
}

func decodeThreadContextStatusPayload(raw string, legacy bool) (observation.ContextStatus, error) {
	var status observation.ContextStatus
	forbidden := map[string]struct{}{"run_id": {}, "thread_id": {}, "turn_id": {}}
	if legacy {
		forbidden = nil
	}
	if err := decodeStrictThreadContextJSON(raw, &status, forbidden); err != nil {
		return observation.ContextStatus{}, fmt.Errorf("decode thread context status: %w", err)
	}
	if err := status.Validate(); err != nil {
		return observation.ContextStatus{}, err
	}
	return status, nil
}

func decodeThreadContextCompactionPayload(raw string, legacy bool) (ThreadContextCompaction, error) {
	var compaction ThreadContextCompaction
	forbidden := map[string]struct{}{"run_id": {}, "thread_id": {}, "turn_id": {}}
	if legacy {
		forbidden = nil
	}
	if err := decodeStrictThreadContextJSON(raw, &compaction, forbidden); err != nil {
		return ThreadContextCompaction{}, fmt.Errorf("decode thread context compaction: %w", err)
	}
	return compaction, nil
}

func decodeStrictThreadContextJSON(raw string, target any, forbidden map[string]struct{}) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("thread context payload is required")
	}
	if len(forbidden) != 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			return err
		}
		for field := range forbidden {
			if _, found := fields[field]; found {
				return fmt.Errorf("thread context payload duplicates %s", field)
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("thread context payload contains trailing data")
		}
		return err
	}
	return nil
}

func validateThreadContextStatusIdentity(status observation.ContextStatus) error {
	if strings.TrimSpace(status.RunID.String()) == "" || strings.TrimSpace(status.ThreadID.String()) == "" || strings.TrimSpace(status.TurnID.String()) == "" {
		return errors.New("thread context status requires run, thread, and turn identities")
	}
	return status.Validate()
}

func validateThreadContextCompaction(compaction ThreadContextCompaction) error {
	if strings.TrimSpace(compaction.OperationID) == "" {
		return errors.New("thread context compaction requires operation id")
	}
	if strings.TrimSpace(compaction.RunID) == "" || strings.TrimSpace(compaction.ThreadID) == "" || strings.TrimSpace(compaction.TurnID) == "" || strings.TrimSpace(compaction.RequestID) == "" {
		return errors.New("thread context compaction requires run, thread, turn, and request identities")
	}
	return (observation.CompactionEvent{
		Phase:  observation.CompactionPhase(compaction.Phase),
		Status: observation.CompactionStatus(compaction.Status),
	}).Validate()
}

func requireExactThreadContextMetadata(metadata map[string]string, keys ...string) error {
	if len(metadata) != len(keys) {
		return errors.New("thread context metadata shape is invalid")
	}
	for _, key := range keys {
		if _, found := metadata[key]; !found {
			return fmt.Errorf("thread context metadata is missing %s", key)
		}
	}
	return nil
}
