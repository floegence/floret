package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

// The source index contains locators, never a second transcript. Decode only
// the requested thread and the explicit authority dependencies of its journal.
// The caller must finish recovery-journal indexing before exposing this view.
func loadSnapshotThreadClosure(ctx context.Context, tx spi.ReadTx, threadID string, now func() time.Time) (*MemoryRepo, error) {
	if ctx == nil || tx == nil || threadID == "" || now == nil {
		return nil, spi.ErrInvalidArgument
	}
	if found, err := backendNamespaceHasRecords(tx, backendDomainJournalNamespace); err != nil || found {
		return nil, errors.Join(ErrAuthorityCorrupt, err, errors.New("snapshot closure requires its recovery journal to be applied"))
	}
	checkpoint, found, err := readSnapshotImportCheckpoint(tx)
	if err != nil || !found || !checkpoint.Complete {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if size, err := snapshotRecordSize(tx); err != nil || size != checkpoint.SourceBytes {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	selected := map[string]bool{}
	queue := []string{threadID}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := queue[0]
		queue = queue[1:]
		if selected[id] {
			continue
		}
		var meta ThreadMeta
		err := decodeSnapshotSourceValue(ctx, tx, &meta, "payload", "threads", id)
		if errors.Is(err, spi.ErrNotFound) {
			var tombstone legacySnapshotTombstone
			if err := decodeSnapshotSourceValue(ctx, tx, &tombstone, "payload", "tombstones", id); err != nil {
				if id == threadID && errors.Is(err, spi.ErrNotFound) {
					return nil, ErrThreadNotFound
				}
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
			selected[id] = true
			continue
		}
		if err != nil || meta.ID != id {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		selected[id] = true
		if meta.ParentThreadID != "" {
			queue = append(queue, meta.ParentThreadID)
		}
		if meta.ForkedFromThreadID != "" {
			queue = append(queue, meta.ForkedFromThreadID)
		}
		var entries []Entry
		if err := decodeSnapshotSourceValue(ctx, tx, &entries, "payload", "entries", id); err != nil && !errors.Is(err, spi.ErrNotFound) {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Type == EntryParentEvent {
				event, err := decodeParentEvent(entry)
				if err != nil {
					return nil, err
				}
				queue = append(queue, event.SourceThreadID)
			}
		}
	}
	state, err := loadSnapshotScopedState(ctx, tx, checkpoint.Version, selected)
	if err != nil {
		return nil, err
	}
	memory, _, err := migrateDecodedMemoryState(state, now)
	if err != nil {
		return nil, err
	}
	if checkpoint.Version == 5 {
		if _, err := repairLegacyUTF8EntryProjections(memory); err != nil {
			return nil, err
		}
	}
	if err := validateBackendDomainV6Memory(memory); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	for id, tombstone := range memory.tombstones {
		if _, live := memory.threads[id]; live || validateMemoryArtifactTombstone(tombstone, id) != nil {
			return nil, ErrAuthorityCorrupt
		}
	}
	if _, deleted := memory.tombstones[threadID]; deleted {
		return nil, ErrThreadDeleted
	}
	return memory, nil
}

func loadSnapshotScopedState(ctx context.Context, tx spi.ReadTx, version int, selected map[string]bool) (memoryState, error) {
	var state memoryState
	value := reflect.ValueOf(&state).Elem()
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		name, _, _ := strings.Cut(value.Type().Field(index).Tag.Get("json"), ",")
		span, err := readSnapshotSourceSpan(tx, "payload", name)
		if errors.Is(err, spi.ErrNotFound) && strings.Contains(value.Type().Field(index).Tag.Get("json"), ",omitempty") {
			continue
		}
		if err != nil {
			return state, errors.Join(ErrAuthorityCorrupt, err)
		}
		if field.Kind() != reflect.Map && field.Type() != reflect.TypeFor[json.RawMessage]() || name == "lease_policy" || span.Kind != '{' {
			if err := decodeSnapshotSourceValue(ctx, tx, field.Addr().Interface(), "payload", name); err != nil {
				return state, err
			}
			continue
		}
		if field.Kind() == reflect.Map {
			field.Set(reflect.MakeMap(field.Type()))
		}
		members := map[string]json.RawMessage{}
		visit := func(key string, span snapshotSourceSpan) error {
			keep, err := snapshotMemberInClosure(ctx, tx, name, key, selected)
			if err != nil || !keep {
				return err
			}
			if field.Kind() != reflect.Map {
				raw, err := readSnapshotSourceBytes(ctx, tx, span.JSONSpan)
				if err != nil {
					return err
				}
				digest := sha256.New()
				_, _ = digest.Write(raw)
				if err := verifySnapshotSpanDigest(span, digest); err != nil {
					return err
				}
				members[key] = raw
				return nil
			}
			if name == "tombstones" {
				var source legacySnapshotTombstone
				if err := decodeSnapshotSourceValue(ctx, tx, &source, "payload", name, key); err != nil {
					return err
				}
				state.Tombstones[key] = snapshotTombstoneAtVersion(source, version)
				return nil
			}
			member := reflect.New(field.Type().Elem()).Elem()
			if err := decodeSnapshotSourceValue(ctx, tx, member.Addr().Interface(), "payload", name, key); err != nil {
				return err
			}
			field.SetMapIndex(reflect.ValueOf(key), member)
			return nil
		}
		if snapshotFieldKeyedByThread(name) {
			for id := range selected {
				member, readErr := readSnapshotSourceSpan(tx, "payload", name, id)
				if errors.Is(readErr, spi.ErrNotFound) {
					continue
				}
				if readErr != nil {
					return state, readErr
				}
				if err := visit(id, member); err != nil {
					return state, err
				}
			}
		} else if err := visitSnapshotCollection(ctx, tx, name, visit); err != nil {
			return state, err
		}
		if field.Kind() != reflect.Map {
			raw, err := json.Marshal(members)
			if err != nil {
				return state, err
			}
			field.SetBytes(raw)
		}
	}
	return state, nil
}

func snapshotTombstoneAtVersion(source legacySnapshotTombstone, version int) ThreadTombstone {
	current := source.current
	if version >= 2 && version <= 4 {
		legacy := source.legacy
		current.ThreadID, current.RootThreadID = legacy.ThreadID, legacy.RootThreadID
		current.ParentThreadID = legacy.ParentThreadID
		current.ForkedFromThreadID, current.ForkedFromEntryID = legacy.ForkedFromThreadID, legacy.ForkedFromEntryID
		current.LegacyCreateIntent = legacy.CreateIntentID
		current.LegacyForkRequestID, current.LegacyForkNodeID = legacy.ForkOperationID, legacy.ForkOperationNodeID
		current.DeletedAt = legacy.DeletedAt
	}
	return current
}

// These exact source collections use ThreadID as their complete map key.
// Compound-key and owner-in-value collections still require their own scan.
func snapshotFieldKeyedByThread(field string) bool {
	switch field {
	case "threads", "entries", "entry_ordinals", "entry_depths", "turn_entry_ordinals", "turn_entry_counts", "todos", "tombstones", "provider_states", "leases", "lease_generation", "subagent_inputs":
		return true
	default:
		return false
	}
}

func snapshotMemberInClosure(ctx context.Context, tx spi.ReadTx, field, key string, selected map[string]bool) (bool, error) {
	switch field {
	case "root_create_intents":
		var intent legacyRootCreateRecord
		if err := decodeSnapshotSourceValue(ctx, tx, &intent, "payload", field, key); err != nil {
			return false, err
		}
		return selected[intent.ThreadID], nil
	case "effect_attempts":
		var attempt legacyEffectAttemptV6
		if err := decodeSnapshotSourceValue(ctx, tx, &attempt, "payload", field, key); err != nil {
			return false, err
		}
		return selected[attempt.Invocation.ThreadID], nil
	case "artifacts", "turn_admissions", "turn_finishes", "effect_attempt_by_invocation":
		owner, _, ok := strings.Cut(key, "\x00")
		return ok && selected[owner], nil
	default:
		return selected[key], nil
	}
}

// Collection pages contain only spans, so skipped histories do not cause body
// reads even when a source field has many unrelated members.
func visitSnapshotCollection(ctx context.Context, tx spi.ReadTx, field string, visit func(string, snapshotSourceSpan) error) error {
	prefix := snapshotSourceKey("payload", field)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: snapshotImportNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if bytes.Equal(row.Key, prefix) {
				continue
			}
			if !bytes.HasPrefix(row.Key, prefix) {
				return ErrAuthorityCorrupt
			}
			tail := row.Key[len(prefix):]
			if len(tail) < 5 || tail[0] != 1 {
				return ErrAuthorityCorrupt
			}
			length := int(binary.BigEndian.Uint32(tail[1:5]))
			if length > len(tail)-5 {
				return ErrAuthorityCorrupt
			}
			if length != len(tail)-5 {
				continue
			} // individual entries belong to their journal member
			key := string(tail[5:])
			var span snapshotSourceSpan
			if err := decodeCanonicalBodyJSON(row.Value, &span); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := visit(key, span); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}
