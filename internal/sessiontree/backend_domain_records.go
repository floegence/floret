package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

type backendDomainFormat struct {
	label     string
	namespace string
	envelope  string
	version   int
	scanLimit int
	validate  func(*MemoryRepo) error
}

type backendDomainManifest struct {
	MigrationTime   int64  `json:"migration_time,omitempty"`
	Version         int    `json:"version"`
	Sequence        int64  `json:"sequence"`
	SourceVersion   int    `json:"source_version,omitempty"`
	MigrationPhase  string `json:"migration_phase,omitempty"`
	MigrationCursor []byte `json:"migration_cursor,omitempty"`
	MigrationRoots  int64  `json:"migration_roots,omitempty"`
}

type backendDomainRootIndex struct {
	ThreadIDs []string `json:"thread_ids"`
}

type backendDomainRootThread struct {
	ThreadID string `json:"thread_id"`
}

type backendDomainRecord struct {
	Version  int             `json:"version"`
	Kind     string          `json:"kind"`
	ID       string          `json:"id,omitempty"`
	ThreadID string          `json:"thread_id,omitempty"`
	Ordinal  int             `json:"ordinal,omitempty"`
	Value    json.RawMessage `json:"value"`
}

func backendDomainKey(kind string, components ...string) []byte {
	parts := make([][]byte, 0, len(components)+1)
	parts = append(parts, storagecodec.TupleString(kind))
	for _, component := range components {
		parts = append(parts, storagecodec.TupleString(component))
	}
	return storagecodec.Tuple(parts...)
}

func backendDomainEntryKey(threadID string, ordinal int) []byte {
	return storagecodec.Tuple(
		storagecodec.TupleString(backendDomainRecordEntry),
		storagecodec.TupleString(threadID),
		storagecodec.TupleOrdinal(uint64(ordinal)),
	)
}

func encodeBackendDomainRecord(format backendDomainFormat, kind, id, threadID string, ordinal int, value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	record, err := json.Marshal(backendDomainRecord{
		Version: format.version, Kind: kind, ID: id, ThreadID: threadID, Ordinal: ordinal, Value: payload,
	})
	if err != nil {
		return nil, err
	}
	return storagecodec.EncodeEnvelope(format.envelope, record)
}

func decodeBackendDomainRecord(format backendDomainFormat, encoded []byte) (backendDomainRecord, error) {
	payload, err := storagecodec.DecodeEnvelope(encoded, format.envelope)
	if err != nil {
		return backendDomainRecord{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record backendDomainRecord
	if err := decoder.Decode(&record); err != nil {
		return backendDomainRecord{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("session-tree %s record contains trailing data", format.label)
		}
		return backendDomainRecord{}, err
	}
	if record.Version != format.version || strings.TrimSpace(record.Kind) == "" || len(record.Value) == 0 {
		return backendDomainRecord{}, fmt.Errorf("unsupported session-tree %s record", format.label)
	}
	return record, nil
}

func decodeBackendDomainValue(format backendDomainFormat, record backendDomainRecord, target any) error {
	if manifest, ok := target.(*backendDomainManifest); ok && format.version < backendDomainV32Version {
		// Historical manifests never carried resumable migration state. Decode
		// their exact shape so even zero-valued newer fields are rejected.
		var historical struct {
			Version  int   `json:"version"`
			Sequence int64 `json:"sequence"`
		}
		if err := decodeBackendDomainValue(format, record, &historical); err != nil {
			return err
		}
		*manifest = backendDomainManifest{Version: historical.Version, Sequence: historical.Sequence}
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("session-tree %s value contains trailing data", format.label)
		}
		return err
	}
	return nil
}

// visitBackendDomain releases each encoded page before reading the next one.
// Startup must validate every record, but need not retain a second encoded copy
// of the complete canonical journal while constructing its validated state.
func visitBackendDomain(ctx context.Context, tx spi.ReadTx, format backendDomainFormat, visit func(spi.Record) error) (bool, error) {
	var after []byte
	found := false
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: format.namespace, After: after, Limit: format.scanLimit})
		if err != nil {
			return false, err
		}
		for _, record := range page.Records {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if err := visit(record); err != nil {
				return false, err
			}
			found = true
		}
		if !page.HasMore {
			return found, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return false, errors.Join(ErrAuthorityCorrupt, errors.New("session-tree scan did not advance"))
		}
		after = bytes.Clone(page.Next)
	}
}

func scanBackendDomain(ctx context.Context, tx spi.ReadTx, format backendDomainFormat) ([]spi.Record, error) {
	var records []spi.Record
	_, err := visitBackendDomain(ctx, tx, format, func(record spi.Record) error {
		records = append(records, spi.Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)})
		return nil
	})
	return records, err
}

func loadBackendDomain(ctx context.Context, tx spi.ReadTx, now func() time.Time, format backendDomainFormat) (*MemoryRepo, bool, error) {
	memory := newMemoryRepo(now)
	// Current records validate one complete immutable entry at a time, then
	// retain only its canonical field locators. Older migration writers keep
	// their exact complete-source contract until that edge has finished.
	compact := format.version >= backendDomainV31Version
	if compact {
		memory.bodyTx, memory.bodyContext = tx, ctx
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
	}
	entries := make(map[string]map[int]Entry)
	bodyReferences := map[string]canonicalBodyRecord{}
	var manifest *backendDomainManifest
	var rootIndex *backendDomainRootIndex
	rootThreads := map[string]struct{}{}
	found, err := visitBackendDomain(ctx, tx, format, func(stored spi.Record) error {
		record, decodeErr := decodeBackendDomainRecord(format, stored.Value)
		if decodeErr != nil {
			return errors.Join(ErrAuthorityCorrupt, decodeErr)
		}
		if record.Kind != backendDomainRecordEntry && (record.ThreadID != "" || record.Ordinal != 0) {
			return ErrAuthorityCorrupt
		}
		if record.Kind == backendDomainRecordTodo || record.Kind == backendDomainRecordProviderState || record.Kind == backendDomainRecordArtifact {
			if !bytes.Equal(stored.Key, backendDomainKey(record.Kind, record.ID)) {
				return ErrAuthorityCorrupt
			}
		}
		var wantKey []byte
		switch record.Kind {
		case backendDomainRecordManifest:
			if manifest != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate or malformed session-tree %s manifest", format.label))
			}
			value := backendDomainManifest{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.Version != format.version {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s manifest", format.label)))
			}
			manifest = &value
			wantKey = backendDomainKey(record.Kind)
		case backendDomainRecordRootIndex:
			if rootIndex != nil || record.ID != "" || record.ThreadID != "" || record.Ordinal != 0 {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate or malformed session-tree %s root index", format.label))
			}
			value := backendDomainRootIndex{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.ThreadIDs == nil {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s root index", format.label)))
			}
			rootIndex = &value
			wantKey = backendDomainKey(record.Kind)
		case backendDomainRecordRootThread:
			value := backendDomainRootThread{}
			if format.version < backendDomainV32Version || record.ID == "" || record.ThreadID != "" || record.Ordinal != 0 {
				return errors.Join(ErrAuthorityCorrupt, errors.New("invalid per-thread root index record"))
			}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || value.ThreadID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if _, duplicate := rootThreads[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, errors.New("duplicate per-thread root index record"))
			}
			rootThreads[record.ID] = struct{}{}
			wantKey = backendDomainKey(record.Kind, record.ID)
		case backendDomainRecordThread:
			value := ThreadMeta{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || record.ID == "" || value.ID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s thread", format.label)))
			}
			if _, duplicate := memory.threads[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s thread", format.label))
			}
			memory.threads[record.ID] = value
			wantKey = backendDomainKey(record.Kind, record.ID)
		case backendDomainRecordEntry:
			value := Entry{}
			var entryErr error
			if format.version >= backendDomainV30Version {
				var canonical canonicalStoredEntry
				canonical, entryErr = decodeCanonicalStoredEntry(format, record)
				if entryErr == nil {
					for _, ref := range canonical.Fields {
						body := bodyReferences[ref.SHA256]
						if body.References > 0 && body.canonicalBodyRef != ref {
							return ErrAuthorityCorrupt
						}
						body.canonicalBodyRef, body.References = ref, body.References+1
						bodyReferences[ref.SHA256] = body
					}
					staged, err := stagedMigrationEntry(tx, format, record)
					if err != nil {
						return err
					}
					if staged {
						if !bytes.Equal(stored.Key, backendDomainEntryKey(record.ThreadID, record.Ordinal)) {
							return ErrAuthorityCorrupt
						}
						return nil
					}
					value, entryErr = materializeCanonicalStoredEntry(ctx, tx, canonical)
					if entryErr == nil && compact && len(canonical.Fields) > 0 {
						value = canonical.Entry
						value.bodyFields = canonical.Fields
					}
				}
			} else {
				entryErr = decodeBackendDomainValue(format, record, &value)
			}
			if err := entryErr; err != nil || record.ThreadID == "" || record.ID == "" || record.Ordinal < 0 || value.ThreadID != record.ThreadID || value.ID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s entry", format.label)))
			}
			if entries[record.ThreadID] == nil {
				entries[record.ThreadID] = make(map[int]Entry)
			}
			if _, duplicate := entries[record.ThreadID][record.Ordinal]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s entry ordinal", format.label))
			}
			entries[record.ThreadID][record.Ordinal] = value
			wantKey = backendDomainEntryKey(record.ThreadID, record.Ordinal)
		case backendDomainRecordTodo:
			value := AgentTodoState{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s todo", format.label)))
			}
			if staged, err := stagedMigrationSideRecord(tx, format, value.ThreadID); staged || err != nil {
				return err
			}
			if _, duplicate := memory.todos[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s todo", format.label))
			}
			memory.todos[record.ID] = value
			wantKey = backendDomainKey(record.Kind, record.ID)
		case backendDomainRecordTombstone:
			value := ThreadTombstone{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s tombstone", format.label)))
			}
			if _, duplicate := memory.tombstones[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s tombstone", format.label))
			}
			memory.tombstones[record.ID] = value
			wantKey = backendDomainKey(record.Kind, record.ID)
		case backendDomainRecordProviderState:
			value := ProviderStateRecord{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || record.ID == "" || value.ThreadID != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s provider state", format.label)))
			}
			if staged, err := stagedMigrationSideRecord(tx, format, value.ThreadID); staged || err != nil {
				return err
			}
			if _, duplicate := memory.providerStates[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s provider state", format.label))
			}
			memory.providerStates[record.ID] = value
			wantKey = backendDomainKey(record.Kind, record.ID)
		case backendDomainRecordArtifact:
			value := artifact.Record{}
			if err := decodeBackendDomainValue(format, record, &value); err != nil || record.ID == "" || artifactRecordKey(value.ThreadID, value.Ref.ID) != record.ID {
				return errors.Join(ErrAuthorityCorrupt, errors.Join(err, fmt.Errorf("invalid session-tree %s artifact", format.label)))
			}
			if staged, err := stagedMigrationSideRecord(tx, format, value.ThreadID); staged || err != nil {
				return err
			}
			if _, duplicate := memory.artifacts[record.ID]; duplicate {
				return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("duplicate session-tree %s artifact", format.label))
			}
			memory.artifacts[record.ID] = value
			wantKey = backendDomainKey(record.Kind, record.ID)
		default:
			return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("unknown session-tree %s record kind %q", format.label, record.Kind))
		}
		if !bytes.Equal(stored.Key, wantKey) {
			return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s %s key does not match value", format.label, record.Kind))
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	if manifest == nil || format.version < backendDomainV32Version && rootIndex == nil || format.version >= backendDomainV32Version && rootIndex != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s manifest or root index is missing", format.label))
	}
	if format.version >= backendDomainV30Version {
		if err := includeMigrationBodyReferences(ctx, tx, format, bodyReferences); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := verifyCanonicalBodyRecords(ctx, tx, bodyReferences); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	memory.seq = manifest.Sequence
	for threadID, byOrdinal := range entries {
		ordered := make([]Entry, len(byOrdinal))
		for ordinal, entry := range byOrdinal {
			if ordinal < 0 || ordinal >= len(ordered) {
				return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s entry ordinal for %q is not contiguous", format.label, threadID))
			}
			ordered[ordinal] = entry
		}
		replace := memory.replaceIndexedEntriesLocked
		if format.version < backendDomainV14Version {
			replace = memory.replaceLegacyIndexedEntriesLocked
		}
		if err := replace(threadID, ordered); err != nil {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if err := format.validate(memory); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	wantRoots := backendDomainRootThreadIDs(memory)
	if format.version < backendDomainV32Version {
		if !slices.Equal(rootIndex.ThreadIDs, wantRoots) {
			return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s root index does not match canonical threads", format.label))
		}
	} else {
		if len(rootThreads) != len(wantRoots) {
			return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s root index does not match canonical threads", format.label))
		}
		for _, id := range wantRoots {
			if _, found := rootThreads[id]; !found {
				return nil, false, errors.Join(ErrAuthorityCorrupt, fmt.Errorf("session-tree %s root index does not match canonical threads", format.label))
			}
		}
	}
	return memory, true, nil
}

func validateBackendDomainMemory(memory *MemoryRepo, label string, validateContext bool) error {
	if memory == nil {
		return fmt.Errorf("session-tree %s memory is required", label)
	}
	if err := ValidateThreadAuthorityGraph(values(memory.threads)); err != nil {
		return err
	}
	for threadID, meta := range memory.threads {
		threadEntries := memory.entries[threadID]
		if (meta.LeafID == "") != (len(threadEntries) == 0) {
			lastEntryID := ""
			if len(threadEntries) > 0 {
				lastEntryID = threadEntries[len(threadEntries)-1].ID
			}
			return fmt.Errorf("thread %q leaf %q does not match its %d journal entries (last %q)", threadID, meta.LeafID, len(threadEntries), lastEntryID)
		}
		if len(threadEntries) > 0 {
			if _, found := findEntry(threadEntries, meta.LeafID); !found {
				return fmt.Errorf("thread %q leaf is missing from its canonical journal", threadID)
			}
			for _, entry := range threadEntries {
				if formatVersionBefore(label, 13) && entry.Metadata[TurnKindMetadataKey] != "" {
					return fmt.Errorf("session-tree %s contains a v13 turn kind", label)
				}
				if formatVersionBefore(label, 21) && entry.Message.ToolResult != nil && entry.Message.ToolResult.EffectOutcome != "" {
					return fmt.Errorf("%s cannot contain returned effect uncertainty", label)
				}
				if formatVersionBefore(label, 12) && entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
					return fmt.Errorf("session-tree %s contains a v12 tool input request", label)
				}
				complete, err := memory.completeEntryLocked(entry)
				if err != nil {
					return err
				}
				if err := ValidateEntryIntegrity(complete); err != nil {
					return fmt.Errorf("thread %q contains invalid entry %q: %w", threadID, entry.ID, err)
				}
				if presentationEntryRequiresRunIdentity(entry.Type) && strings.TrimSpace(entry.TurnID) != "" && strings.TrimSpace(entry.RunID) == "" {
					return fmt.Errorf("thread %q entry %q has incomplete run identity", threadID, entry.ID)
				}
				if entry.Type == EntryEffectAttempt {
					if _, err := decodeEffectAttempt(entry); err != nil {
						return fmt.Errorf("thread %q contains invalid effect attempt %q: %w", threadID, entry.ID, err)
					}
				}
				if validateContext {
					if err := validateThreadContextEntryV9(entry); err != nil {
						return fmt.Errorf("thread %q contains invalid context entry %q: %w", threadID, entry.ID, err)
					}
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

func backendDomainRootThreadIDs(memory *MemoryRepo) []string {
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

func saveCompleteBackendDomain(tx spi.WriteTx, memory *MemoryRepo, format backendDomainFormat) error {
	return saveCompleteBackendDomainContext(context.Background(), tx, memory, format)
}

func saveCompleteBackendDomainContext(ctx context.Context, tx spi.WriteTx, memory *MemoryRepo, format backendDomainFormat) error {
	if err := format.validate(memory); err != nil {
		return err
	}
	manifest := backendDomainManifest{Version: format.version, Sequence: memory.seq}
	if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
		return err
	}
	if format.version < backendDomainV32Version {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainRootIndex{ThreadIDs: backendDomainRootThreadIDs(memory)}); err != nil {
			return err
		}
	} else {
		for _, id := range backendDomainRootThreadIDs(memory) {
			if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordRootThread, id), backendDomainRecordRootThread, id, "", 0, backendDomainRootThread{ThreadID: id}); err != nil {
				return err
			}
		}
	}
	for id, value := range memory.threads {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, value); err != nil {
			return err
		}
		if format.version >= backendDomainV32Version {
			if err := putJournalCount(tx, id, 0); err != nil {
				return err
			}
		}
	}
	for threadID, threadEntries := range memory.entries {
		for ordinal, value := range threadEntries {
			if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainEntryKey(threadID, ordinal), backendDomainRecordEntry, value.ID, threadID, ordinal, value); err != nil {
				return err
			}
		}
	}
	for id, value := range memory.todos {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordTodo, id), backendDomainRecordTodo, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.tombstones {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.providerStates {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordProviderState, id), backendDomainRecordProviderState, id, "", 0, value); err != nil {
			return err
		}
	}
	for id, value := range memory.artifacts {
		if err := putBackendDomainRecordContext(ctx, tx, format, backendDomainKey(backendDomainRecordArtifact, id), backendDomainRecordArtifact, id, "", 0, value); err != nil {
			return err
		}
	}
	return nil
}

func putBackendDomainRecord(tx spi.WriteTx, format backendDomainFormat, key []byte, kind, id, threadID string, ordinal int, value any) error {
	return putBackendDomainRecordContext(context.Background(), tx, format, key, kind, id, threadID, ordinal, value)
}

func putBackendDomainRecordContext(ctx context.Context, tx spi.WriteTx, format backendDomainFormat, key []byte, kind, id, threadID string, ordinal int, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if format.version >= backendDomainV30Version && kind == backendDomainRecordEntry {
		entry, ok := value.(Entry)
		if !ok {
			return fmt.Errorf("%w: entry record %q has unexpected value type %T", ErrAuthorityCorrupt, id, value)
		}
		return putCanonicalStoredEntry(ctx, tx, format, key, id, threadID, ordinal, entry)
	}
	encoded, err := encodeBackendDomainRecord(format, kind, id, threadID, ordinal, value)
	if err != nil {
		return err
	}
	return tx.Put(format.namespace, key, encoded)
}

func deleteAllBackendDomain(ctx context.Context, tx spi.WriteTx, format backendDomainFormat) error {
	_, err := visitBackendDomain(ctx, tx, format, func(record spi.Record) error {
		return tx.Delete(format.namespace, record.Key)
	})
	return err
}

func persistBackendDomainChanges(tx spi.WriteTx, before, after *MemoryRepo, format backendDomainFormat) (bool, error) {
	return persistBackendDomainChangesContext(context.Background(), tx, before, after, format)
}

func persistBackendDomainChangesContext(ctx context.Context, tx spi.WriteTx, before, after *MemoryRepo, format backendDomainFormat) (bool, error) {
	changed := false
	put := func(key []byte, kind, id, threadID string, ordinal int, value any) error {
		changed = true
		return putBackendDomainRecordContext(ctx, tx, format, key, kind, id, threadID, ordinal, value)
	}
	remove := func(key []byte) error {
		changed = true
		return tx.Delete(format.namespace, key)
	}
	if before.seq != after.seq {
		manifest := backendDomainManifest{Version: format.version, Sequence: after.seq}
		if format.version >= backendDomainV32Version {
			previous, err := readV32Manifest(tx)
			if err != nil {
				return false, err
			}
			manifest = previous
			manifest.Sequence = after.seq
		}
		if err := put(backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return false, err
		}
	}
	beforeRoots, afterRoots := backendDomainRootThreadIDs(before), backendDomainRootThreadIDs(after)
	if format.version < backendDomainV32Version {
		if !slices.Equal(beforeRoots, afterRoots) {
			if err := put(backendDomainKey(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainRootIndex{ThreadIDs: afterRoots}); err != nil {
				return false, err
			}
		}
	} else {
		beforeSet := make(map[string]struct{}, len(beforeRoots))
		afterSet := make(map[string]struct{}, len(afterRoots))
		for _, id := range beforeRoots {
			beforeSet[id] = struct{}{}
		}
		for _, id := range afterRoots {
			afterSet[id] = struct{}{}
		}
		for id := range beforeSet {
			if _, remains := afterSet[id]; !remains {
				if err := remove(backendDomainKey(backendDomainRecordRootThread, id)); err != nil {
					return false, err
				}
			}
		}
		for id := range afterSet {
			if _, existed := beforeSet[id]; !existed {
				if err := put(backendDomainKey(backendDomainRecordRootThread, id), backendDomainRecordRootThread, id, "", 0, backendDomainRootThread{ThreadID: id}); err != nil {
					return false, err
				}
			}
		}
	}
	if err := persistBackendDomainMapChanges(before.threads, after.threads,
		func(id string) []byte { return backendDomainKey(backendDomainRecordThread, id) },
		func(id string, value ThreadMeta) error {
			return put(backendDomainKey(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if format.version >= backendDomainV32Version {
		for id := range after.threads {
			if _, existed := before.threads[id]; !existed {
				if err := putJournalCount(tx, id, 0); err != nil {
					return false, err
				}
				changed = true
			}
		}
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
			if err := put(backendDomainEntryKey(threadID, ordinal), backendDomainRecordEntry, entry.ID, threadID, ordinal, entry); err != nil {
				return false, err
			}
		}
		for ordinal := len(previous) - 1; ordinal >= len(next); ordinal-- {
			var deleteErr error
			if format.version >= backendDomainV30Version {
				changed = true
				deleteErr = deleteCanonicalStoredEntry(ctx, tx, format, threadID, ordinal)
			} else {
				deleteErr = remove(backendDomainEntryKey(threadID, ordinal))
			}
			if err := deleteErr; err != nil {
				return false, err
			}
		}
	}
	if format.version >= backendDomainV32Version {
		for id := range before.threads {
			if _, remains := after.threads[id]; !remains {
				if err := tx.Delete(backendEntryIndexNamespace, backendDomainKey("count", id)); err != nil {
					return false, err
				}
				changed = true
			}
		}
	}
	if err := persistBackendDomainMapChanges(before.todos, after.todos,
		func(id string) []byte { return backendDomainKey(backendDomainRecordTodo, id) },
		func(id string, value AgentTodoState) error {
			return put(backendDomainKey(backendDomainRecordTodo, id), backendDomainRecordTodo, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainMapChanges(before.tombstones, after.tombstones,
		func(id string) []byte { return backendDomainKey(backendDomainRecordTombstone, id) },
		func(id string, value ThreadTombstone) error {
			return put(backendDomainKey(backendDomainRecordTombstone, id), backendDomainRecordTombstone, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainMapChanges(before.providerStates, after.providerStates,
		func(id string) []byte { return backendDomainKey(backendDomainRecordProviderState, id) },
		func(id string, value ProviderStateRecord) error {
			return put(backendDomainKey(backendDomainRecordProviderState, id), backendDomainRecordProviderState, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	if err := persistBackendDomainMapChanges(before.artifacts, after.artifacts,
		func(id string) []byte { return backendDomainKey(backendDomainRecordArtifact, id) },
		func(id string, value artifact.Record) error {
			return put(backendDomainKey(backendDomainRecordArtifact, id), backendDomainRecordArtifact, id, "", 0, value)
		}, remove); err != nil {
		return false, err
	}
	return changed, nil
}

func persistBackendDomainMapChanges[V any](before, after map[string]V, key func(string) []byte, put func(string, V) error, remove func([]byte) error) error {
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

func formatVersionBefore(label string, version int) bool {
	value, err := strconv.Atoi(strings.TrimPrefix(label, "v"))
	return err != nil || value < version
}
