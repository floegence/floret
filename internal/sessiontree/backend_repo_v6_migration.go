package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v6/internal/session/artifact"
	"github.com/floegence/floret/v6/internal/storagecodec"
	"github.com/floegence/floret/v6/storage/spi"
)

const (
	backendDomainV6Namespace = "floret.domain.sessiontree.v6"
	backendDomainV6Version   = 6
	backendDomainV6ScanLimit = 256
)

type backendDomainV6Manifest struct {
	Version               int   `json:"version"`
	Sequence              int64 `json:"sequence"`
	EffectAttemptSequence int64 `json:"effect_attempt_sequence"`
}

type backendDomainV6RootIndex struct {
	ThreadIDs []string `json:"thread_ids"`
}

type backendDomainV6Record struct {
	Version  int             `json:"version"`
	Kind     string          `json:"kind"`
	ID       string          `json:"id,omitempty"`
	ThreadID string          `json:"thread_id,omitempty"`
	Ordinal  int             `json:"ordinal,omitempty"`
	Value    json.RawMessage `json:"value"`
}

type legacyEffectInvocationV6 struct {
	ThreadID              string `json:"thread_id"`
	TurnID                string `json:"turn_id"`
	RunID                 string `json:"run_id"`
	ToolCallID            string `json:"tool_call_id"`
	ToolName              string `json:"tool_name"`
	ArgumentHash          string `json:"argument_hash"`
	RetryKey              string `json:"retry_key,omitempty"`
	SourceEffectAttemptID string `json:"source_effect_attempt_id,omitempty"`
}

type legacyEffectAttemptV6 struct {
	EffectAttemptID         string                   `json:"effect_attempt_id"`
	Invocation              legacyEffectInvocationV6 `json:"invocation"`
	RequestFingerprint      string                   `json:"request_fingerprint"`
	State                   string                   `json:"state"`
	RejectionCode           string                   `json:"rejection_code,omitempty"`
	TerminalFingerprint     string                   `json:"terminal_fingerprint,omitempty"`
	ResultEntryID           string                   `json:"result_entry_id,omitempty"`
	CreatedAt               time.Time                `json:"created_at"`
	UpdatedAt               time.Time                `json:"updated_at"`
	RetryRequestKey         string                   `json:"retry_request_key,omitempty"`
	RetryRequestFingerprint string                   `json:"retry_request_fingerprint,omitempty"`
	OwnerID                 string                   `json:"owner_id,omitempty"`
	Generation              int64                    `json:"generation,omitempty"`
}

func backendDomainV6Key(kind string, components ...string) []byte {
	parts := make([][]byte, 0, len(components)+1)
	parts = append(parts, storagecodec.TupleString(kind))
	for _, component := range components {
		parts = append(parts, storagecodec.TupleString(component))
	}
	return storagecodec.Tuple(parts...)
}

func backendDomainV6EntryKey(threadID string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(backendDomainRecordEntry), storagecodec.TupleString(threadID), storagecodec.TupleOrdinal(uint64(ordinal)))
}

func decodeBackendDomainV6Record(encoded []byte) (backendDomainV6Record, error) {
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree-v6-record")
	if err != nil {
		return backendDomainV6Record{}, err
	}
	var record backendDomainV6Record
	if err := decodeBackendDomainV6ValueRaw(payload, &record); err != nil {
		return backendDomainV6Record{}, err
	}
	if record.Version != backendDomainV6Version || strings.TrimSpace(record.Kind) == "" || len(record.Value) == 0 {
		return backendDomainV6Record{}, errors.New("unsupported session-tree v6 record")
	}
	return record, nil
}

func decodeBackendDomainV6Value(record backendDomainV6Record, target any) error {
	return decodeBackendDomainV6ValueRaw(record.Value, target)
}

func decodeBackendDomainV6ValueRaw(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("session-tree v6 value contains trailing data")
		}
		return err
	}
	return nil
}

func scanBackendDomainV6(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	var records []spi.Record
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV6Namespace, After: after, Limit: backendDomainV6ScanLimit})
		if err != nil {
			return nil, err
		}
		records = append(records, page.Records...)
		if !page.HasMore {
			return records, nil
		}
		after = page.Next
	}
}

func loadBackendDomainV6(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	records, err := scanBackendDomainV6(ctx, tx)
	if err != nil || len(records) == 0 {
		return nil, false, err
	}
	memory := newMemoryRepo(now)
	entries := make(map[string]map[int]Entry)
	var manifest *backendDomainV6Manifest
	var rootIndex *backendDomainV6RootIndex
	for _, stored := range records {
		record, decodeErr := decodeBackendDomainV6Record(stored.Value)
		if decodeErr != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, decodeErr)
		}
		var wantKey []byte
		switch record.Kind {
		case backendDomainRecordManifest:
			value := backendDomainV6Manifest{}
			if manifest != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 || decodeBackendDomainV6Value(record, &value) != nil || value.Version != backendDomainV6Version {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 manifest"))
			}
			manifest, wantKey = &value, backendDomainV6Key(record.Kind)
		case backendDomainRecordRootIndex:
			value := backendDomainV6RootIndex{}
			if rootIndex != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 || decodeBackendDomainV6Value(record, &value) != nil || value.ThreadIDs == nil {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 root index"))
			}
			rootIndex, wantKey = &value, backendDomainV6Key(record.Kind)
		case backendDomainRecordThread:
			value := ThreadMeta{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || value.ID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 thread"))
			}
			if _, duplicate := memory.threads[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 thread"))
			}
			memory.threads[record.ID], wantKey = value, backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEntry:
			value := Entry{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ThreadID == "" || record.ID == "" || record.Ordinal < 0 || value.ThreadID != record.ThreadID || value.ID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 entry"))
			}
			if entries[record.ThreadID] == nil {
				entries[record.ThreadID] = make(map[int]Entry)
			}
			if _, duplicate := entries[record.ThreadID][record.Ordinal]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 entry ordinal"))
			}
			entries[record.ThreadID][record.Ordinal], wantKey = value, backendDomainV6EntryKey(record.ThreadID, record.Ordinal)
		case backendDomainRecordTodo:
			value := AgentTodoState{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 todo"))
			}
			memory.todos[record.ID], wantKey = value, backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordTombstone:
			value := ThreadTombstone{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 tombstone"))
			}
			memory.tombstones[record.ID], wantKey = value, backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEffectAttempt:
			value := legacyEffectAttemptV6{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || value.EffectAttemptID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 effect attempt"))
			}
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEffectInvocation:
			value := ""
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || strings.TrimSpace(value) == "" {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 effect invocation"))
			}
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordProviderState:
			value := ProviderStateRecord{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 provider state"))
			}
			memory.providerStates[record.ID], wantKey = value, backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordArtifact:
			value := artifact.Record{}
			if decodeBackendDomainV6Value(record, &value) != nil || record.ID == "" || artifactRecordKey(value.ThreadID, value.Ref.ID) != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("invalid session-tree v6 artifact"))
			}
			memory.artifacts[record.ID], wantKey = value, backendDomainV6Key(record.Kind, record.ID)
		default:
			return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("unknown session-tree v6 record kind %q", record.Kind))
		}
		if !bytes.Equal(stored.Key, wantKey) {
			return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree v6 %s key does not match value", record.Kind))
		}
	}
	if manifest == nil || rootIndex == nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("session-tree v6 manifest or root index is missing"))
	}
	memory.seq = manifest.Sequence
	for threadID, byOrdinal := range entries {
		ordered := make([]Entry, len(byOrdinal))
		for ordinal, entry := range byOrdinal {
			if ordinal < 0 || ordinal >= len(ordered) {
				return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree v6 entry ordinal for %q is not contiguous", threadID))
			}
			ordered[ordinal] = entry
		}
		if err := memory.replaceIndexedEntriesLocked(threadID, ordered); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if err := validateBackendDomainV6Memory(memory); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if !slices.Equal(rootIndex.ThreadIDs, backendDomainV7RootThreadIDs(memory)) {
		return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("session-tree v6 root index does not match canonical threads"))
	}
	return memory, true, nil
}

func validateBackendDomainV6Memory(memory *MemoryRepo) error {
	if memory == nil {
		return errors.New("session-tree v6 memory is required")
	}
	if err := ValidateThreadAuthorityGraph(values(memory.threads)); err != nil {
		return err
	}
	for threadID, meta := range memory.threads {
		threadEntries := memory.entries[threadID]
		if (meta.LeafID == "") != (len(threadEntries) == 0) {
			return fmt.Errorf("thread %q leaf does not match its journal", threadID)
		}
		if len(threadEntries) > 0 {
			if _, found := findEntry(threadEntries, meta.LeafID); !found {
				return fmt.Errorf("thread %q leaf is missing from its canonical journal", threadID)
			}
			for _, entry := range threadEntries {
				if err := ValidateEntryIntegrity(entry); err != nil {
					return fmt.Errorf("thread %q contains invalid entry %q: %w", threadID, entry.ID, err)
				}
			}
		}
	}
	for threadID := range memory.entries {
		if _, ok := memory.threads[threadID]; !ok {
			return fmt.Errorf("journal for missing thread %q", threadID)
		}
	}
	for threadID, state := range memory.providerStates {
		if _, ok := memory.threads[threadID]; !ok || state.ThreadID != threadID {
			return fmt.Errorf("provider state for invalid thread %q", threadID)
		}
	}
	for key, record := range memory.artifacts {
		if key != artifactRecordKey(record.ThreadID, record.Ref.ID) {
			return fmt.Errorf("artifact %q has invalid identity", key)
		}
		if err := memory.validateArtifactRecordLocked(record); err != nil {
			return err
		}
	}
	return nil
}

func deleteAllBackendDomainV6(ctx context.Context, tx spi.WriteTx) error {
	records, err := scanBackendDomainV6(ctx, tx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := tx.Delete(backendDomainV6Namespace, record.Key); err != nil {
			return err
		}
	}
	return nil
}
