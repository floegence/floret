package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v5/internal/session/artifact"
	"github.com/floegence/floret/v5/internal/storagecodec"
	"github.com/floegence/floret/v5/storage/spi"
)

const (
	backendDomainV6Namespace = "floret.domain.sessiontree.v6"
	backendDomainV6Version   = 6
	backendDomainV6ScanLimit = 256
)

const (
	backendDomainRecordManifest         = "manifest"
	backendDomainRecordRootIndex        = "root_index"
	backendDomainRecordThread           = "thread"
	backendDomainRecordEntry            = "entry"
	backendDomainRecordTodo             = "todo"
	backendDomainRecordTombstone        = "tombstone"
	backendDomainRecordEffectAttempt    = "effect_attempt"
	backendDomainRecordEffectInvocation = "effect_invocation"
	backendDomainRecordProviderState    = "provider_state"
	backendDomainRecordArtifact         = "artifact"
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

func backendDomainV6Key(kind string, components ...string) []byte {
	parts := make([][]byte, 0, len(components)+1)
	parts = append(parts, storagecodec.TupleString(kind))
	for _, component := range components {
		parts = append(parts, storagecodec.TupleString(component))
	}
	return storagecodec.Tuple(parts...)
}

func backendDomainV6EntryKey(threadID string, ordinal int) []byte {
	return storagecodec.Tuple(
		storagecodec.TupleString(backendDomainRecordEntry),
		storagecodec.TupleString(threadID),
		storagecodec.TupleOrdinal(uint64(ordinal)),
	)
}

func encodeBackendDomainV6Record(kind, id, threadID string, ordinal int, value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	record, err := json.Marshal(backendDomainV6Record{
		Version: backendDomainV6Version, Kind: kind, ID: id, ThreadID: threadID, Ordinal: ordinal, Value: payload,
	})
	if err != nil {
		return nil, err
	}
	return storagecodec.EncodeEnvelope("sessiontree-v6-record", record)
}

func decodeBackendDomainV6Record(encoded []byte) (backendDomainV6Record, error) {
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree-v6-record")
	if err != nil {
		return backendDomainV6Record{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record backendDomainV6Record
	if err := decoder.Decode(&record); err != nil {
		return backendDomainV6Record{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("session-tree v6 record contains trailing data")
		}
		return backendDomainV6Record{}, err
	}
	if record.Version != backendDomainV6Version || strings.TrimSpace(record.Kind) == "" || len(record.Value) == 0 {
		return backendDomainV6Record{}, errors.New("unsupported session-tree v6 record")
	}
	return record, nil
}

func decodeBackendDomainV6Value(record backendDomainV6Record, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
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
	if err != nil {
		return nil, false, err
	}
	if len(records) == 0 {
		return nil, false, nil
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
			if manifest != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate or malformed session-tree v6 manifest"))
			}
			value := backendDomainV6Manifest{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || value.Version != backendDomainV6Version {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 manifest")))
			}
			manifest = &value
			wantKey = backendDomainV6Key(record.Kind)
		case backendDomainRecordRootIndex:
			if rootIndex != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate or malformed session-tree v6 root index"))
			}
			value := backendDomainV6RootIndex{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || value.ThreadIDs == nil {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 root index")))
			}
			rootIndex = &value
			wantKey = backendDomainV6Key(record.Kind)
		case backendDomainRecordThread:
			value := ThreadMeta{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || value.ID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 thread")))
			}
			if _, duplicate := memory.threads[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 thread"))
			}
			memory.threads[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEntry:
			value := Entry{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ThreadID == "" || record.ID == "" || record.Ordinal < 0 || value.ThreadID != record.ThreadID || value.ID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 entry")))
			}
			if entries[record.ThreadID] == nil {
				entries[record.ThreadID] = make(map[int]Entry)
			}
			if _, duplicate := entries[record.ThreadID][record.Ordinal]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 entry ordinal"))
			}
			entries[record.ThreadID][record.Ordinal] = value
			wantKey = backendDomainV6EntryKey(record.ThreadID, record.Ordinal)
		case backendDomainRecordTodo:
			value := AgentTodoState{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 todo")))
			}
			if _, duplicate := memory.todos[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 todo"))
			}
			memory.todos[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordTombstone:
			value := ThreadTombstone{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 tombstone")))
			}
			if _, duplicate := memory.tombstones[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 tombstone"))
			}
			memory.tombstones[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEffectAttempt:
			value := EffectAttempt{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || value.EffectAttemptID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 effect attempt")))
			}
			if _, duplicate := memory.effectAttempts[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 effect attempt"))
			}
			memory.effectAttempts[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordEffectInvocation:
			value := ""
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || strings.TrimSpace(value) == "" {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 effect invocation")))
			}
			if _, duplicate := memory.effectAttemptByInvocation[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 effect invocation"))
			}
			memory.effectAttemptByInvocation[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordProviderState:
			value := ProviderStateRecord{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 provider state")))
			}
			if _, duplicate := memory.providerStates[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 provider state"))
			}
			memory.providerStates[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
		case backendDomainRecordArtifact:
			value := artifact.Record{}
			if err := decodeBackendDomainV6Value(record, &value); err != nil || record.ID == "" || artifactRecordKey(value.ThreadID, value.Ref.ID) != record.ID {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.Join(err, errors.New("invalid session-tree v6 artifact")))
			}
			if _, duplicate := memory.artifacts[record.ID]; duplicate {
				return nil, false, errors.Join(ErrAuthorityCorrupt, errors.New("duplicate session-tree v6 artifact"))
			}
			memory.artifacts[record.ID] = value
			wantKey = backendDomainV6Key(record.Kind, record.ID)
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
	memory.effectAttemptSequence = manifest.EffectAttemptSequence
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
	if !slices.Equal(rootIndex.ThreadIDs, backendDomainV6RootThreadIDs(memory)) {
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
	for threadID, todo := range memory.todos {
		if _, ok := memory.threads[threadID]; !ok || todo.ThreadID != threadID || ValidateAgentTodoItems(todo.Items) != nil {
			return fmt.Errorf("todo for invalid thread %q", threadID)
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

func backendDomainV6RootThreadIDs(memory *MemoryRepo) []string {
	metas := make([]ThreadMeta, 0, len(memory.threads))
	for _, meta := range memory.threads {
		if strings.TrimSpace(meta.ParentThreadID) == "" {
			metas = append(metas, meta)
		}
	}
	metas = ApplyThreadListOptions(metas, ListThreadsOptions{IncludeArchived: true, RootOnly: true})
	ids := make([]string, len(metas))
	for index, meta := range metas {
		ids[index] = meta.ID
	}
	return ids
}

func saveCompleteBackendDomainV6(tx spi.WriteTx, memory *MemoryRepo) error {
	if err := validateBackendDomainV6Memory(memory); err != nil {
		return err
	}
	manifest := backendDomainV6Manifest{Version: backendDomainV6Version, Sequence: memory.seq, EffectAttemptSequence: memory.effectAttemptSequence}
	if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
		return err
	}
	if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainV6RootIndex{ThreadIDs: backendDomainV6RootThreadIDs(memory)}); err != nil {
		return err
	}
	for id, value := range memory.threads {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, value); err != nil {
			return err
		}
	}
	for threadID, threadEntries := range memory.entries {
		for ordinal, value := range threadEntries {
			if err := putBackendDomainV6Record(tx, backendDomainV6EntryKey(threadID, ordinal), backendDomainRecordEntry, value.ID, threadID, ordinal, value); err != nil {
				return err
			}
		}
	}
	for id, value := range memory.todos {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordTodo, id), backendDomainRecordTodo, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.tombstones {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.effectAttempts {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordEffectAttempt, id), backendDomainRecordEffectAttempt, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.effectAttemptByInvocation {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordEffectInvocation, id), backendDomainRecordEffectInvocation, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.providerStates {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordProviderState, id), backendDomainRecordProviderState, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.artifacts {
		if err := putBackendDomainV6Record(tx, backendDomainV6Key(backendDomainRecordArtifact, id), backendDomainRecordArtifact, id, "", 0, value); err != nil {
			return err
		}
	}
	return nil
}

func putBackendDomainV6Record(tx spi.WriteTx, key []byte, kind, id, threadID string, ordinal int, value any) error {
	encoded, err := encodeBackendDomainV6Record(kind, id, threadID, ordinal, value)
	if err != nil {
		return err
	}
	return tx.Put(backendDomainV6Namespace, key, encoded)
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

func cloneMemoryRepoForBackendUpdate(source *MemoryRepo) *MemoryRepo {
	source.mu.Lock()
	defer source.mu.Unlock()
	clone := &MemoryRepo{
		threads: maps.Clone(source.threads), entries: make(map[string][]Entry, len(source.entries)),
		entryOrdinals:     make(map[string]map[string]int, len(source.entryOrdinals)),
		entryDepths:       make(map[string]map[string]int64, len(source.entryDepths)),
		turnEntryOrdinals: make(map[string]map[string][]int, len(source.turnEntryOrdinals)),
		turnEntryCounts:   make(map[string]map[string]int, len(source.turnEntryCounts)),
		now:               source.now, todos: maps.Clone(source.todos), tombstones: maps.Clone(source.tombstones),
		effectAttempts: maps.Clone(source.effectAttempts), effectAttemptByInvocation: maps.Clone(source.effectAttemptByInvocation),
		effectAttemptSequence: source.effectAttemptSequence, providerStates: maps.Clone(source.providerStates),
		artifacts: maps.Clone(source.artifacts), seq: source.seq,
	}
	for threadID, threadEntries := range source.entries {
		clone.entries[threadID] = threadEntries[:len(threadEntries):len(threadEntries)]
	}
	for threadID, values := range source.entryOrdinals {
		clone.entryOrdinals[threadID] = maps.Clone(values)
	}
	for threadID, values := range source.entryDepths {
		clone.entryDepths[threadID] = maps.Clone(values)
	}
	for threadID, values := range source.turnEntryOrdinals {
		clone.turnEntryOrdinals[threadID] = cloneOrdinalLists(values)
	}
	for threadID, values := range source.turnEntryCounts {
		clone.turnEntryCounts[threadID] = maps.Clone(values)
	}
	return clone
}

func persistBackendDomainV6Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	changed := false
	put := func(key []byte, kind, id, threadID string, ordinal int, value any) error {
		changed = true
		return putBackendDomainV6Record(tx, key, kind, id, threadID, ordinal, value)
	}
	remove := func(key []byte) error {
		changed = true
		return tx.Delete(backendDomainV6Namespace, key)
	}
	if before.seq != after.seq || before.effectAttemptSequence != after.effectAttemptSequence {
		manifest := backendDomainV6Manifest{Version: backendDomainV6Version, Sequence: after.seq, EffectAttemptSequence: after.effectAttemptSequence}
		if err := put(backendDomainV6Key(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return false, err
		}
	}
	beforeRoots, afterRoots := backendDomainV6RootThreadIDs(before), backendDomainV6RootThreadIDs(after)
	if !slices.Equal(beforeRoots, afterRoots) {
		if err := put(backendDomainV6Key(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainV6RootIndex{ThreadIDs: afterRoots}); err != nil {
			return false, err
		}
	}
	if err := persistBackendDomainV6MapChanges(before.threads, after.threads,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordThread, id) },
		func(id string, value ThreadMeta) error {
			return put(backendDomainV6Key(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	threadIDs := make(map[string]struct{}, len(before.entries)+len(after.entries))
	for id := range before.entries {
		threadIDs[id] = struct{}{}
	}
	for id := range after.entries {
		threadIDs[id] = struct{}{}
	}
	for threadID := range threadIDs {
		previous, next := before.entries[threadID], after.entries[threadID]
		common := min(len(previous), len(next))
		for ordinal := 0; ordinal < common; ordinal++ {
			if previous[ordinal].ID != next[ordinal].ID {
				return false, errors.New("canonical session-tree entries are immutable")
			}
		}
		for ordinal := common; ordinal < len(next); ordinal++ {
			entry := next[ordinal]
			if err := put(backendDomainV6EntryKey(threadID, ordinal), backendDomainRecordEntry, entry.ID, threadID, ordinal, entry); err != nil {
				return false, err
			}
		}
		for ordinal := len(next); ordinal < len(previous); ordinal++ {
			if err := remove(backendDomainV6EntryKey(threadID, ordinal)); err != nil {
				return false, err
			}
		}
	}
	if err := persistBackendDomainV6MapChanges(before.todos, after.todos,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordTodo, id) },
		func(id string, value AgentTodoState) error {
			return put(backendDomainV6Key(backendDomainRecordTodo, id), backendDomainRecordTodo, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainV6MapChanges(before.tombstones, after.tombstones,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordTombstone, id) },
		func(id string, value ThreadTombstone) error {
			return put(backendDomainV6Key(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainV6MapChanges(before.effectAttempts, after.effectAttempts,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordEffectAttempt, id) },
		func(id string, value EffectAttempt) error {
			return put(backendDomainV6Key(backendDomainRecordEffectAttempt, id), backendDomainRecordEffectAttempt, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainV6MapChanges(before.effectAttemptByInvocation, after.effectAttemptByInvocation,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordEffectInvocation, id) },
		func(id, value string) error {
			return put(backendDomainV6Key(backendDomainRecordEffectInvocation, id), backendDomainRecordEffectInvocation, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainV6MapChanges(before.providerStates, after.providerStates,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordProviderState, id) },
		func(id string, value ProviderStateRecord) error {
			return put(backendDomainV6Key(backendDomainRecordProviderState, id), backendDomainRecordProviderState, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainV6MapChanges(before.artifacts, after.artifacts,
		func(id string) []byte { return backendDomainV6Key(backendDomainRecordArtifact, id) },
		func(id string, value artifact.Record) error {
			return put(backendDomainV6Key(backendDomainRecordArtifact, id), backendDomainRecordArtifact, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	return changed, nil
}

func persistBackendDomainV6MapChanges[V any](before, after map[string]V, key func(string) []byte, put func(string, V) error, remove func([]byte) error) error {
	ids := make([]string, 0, len(before)+len(after))
	seen := make(map[string]struct{}, len(before)+len(after))
	for id := range before {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range after {
		if _, ok := seen[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		previous, hadPrevious := before[id]
		next, hasNext := after[id]
		switch {
		case !hasNext:
			if err := remove(key(id)); err != nil {
				return err
			}
		case !hadPrevious || !reflect.DeepEqual(previous, next):
			if err := put(id, next); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteLegacyBackendDomain(ctx context.Context, tx spi.WriteTx) error {
	for _, key := range [][]byte{backendStateKey, backendRootThreadInventoryKey} {
		if err := tx.Delete(backendDomainNamespace, key); err != nil {
			return err
		}
	}
	return clearBackendDomainJournal(ctx, tx)
}
