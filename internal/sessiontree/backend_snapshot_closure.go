package sessiontree

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

// The source index contains locators, never a second transcript. Decode only
// the requested thread and the explicit authority dependencies of its journal.
// Indexed recovery frames overlay their immutable checkpoint in released order.
func loadSnapshotThreadClosure(ctx context.Context, tx spi.ReadTx, threadID string, now func() time.Time) (*MemoryRepo, error) {
	if ctx == nil || tx == nil || threadID == "" || now == nil {
		return nil, spi.ErrInvalidArgument
	}
	checkpoint, found, err := readSnapshotImportCheckpoint(tx)
	if err != nil || !found || !checkpoint.Complete {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if size, err := snapshotRecordSize(tx); err != nil || size != checkpoint.SourceBytes {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	source, err := newSnapshotEffectiveSource(ctx, tx, checkpoint.Version)
	if err != nil {
		return nil, err
	}
	return loadSnapshotThreadClosureFrom(ctx, source, checkpoint.Version, threadID, now)
}

func loadSnapshotThreadClosureFrom(ctx context.Context, source *snapshotEffectiveSource, version int, threadID string, now func() time.Time) (*MemoryRepo, error) {
	memory, _, err := loadSnapshotThreadClosureAt(ctx, source, version, threadID, now)
	return memory, err
}

func loadSnapshotThreadClosureAt(ctx context.Context, source *snapshotEffectiveSource, version int, threadID string, now func() time.Time) (*MemoryRepo, bool, error) {
	selected := map[string]bool{}
	queue := []string{threadID}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		id := queue[0]
		queue = queue[1:]
		if selected[id] {
			continue
		}
		var meta ThreadMeta
		err := source.decodeMember(ctx, &meta, "threads", id)
		if errors.Is(err, spi.ErrNotFound) {
			var tombstone legacySnapshotTombstone
			if err := source.decodeMember(ctx, &tombstone, "tombstones", id); err != nil {
				if id == threadID && errors.Is(err, spi.ErrNotFound) {
					return nil, false, ErrThreadNotFound
				}
				return nil, false, errors.Join(ErrAuthorityCorrupt, err)
			}
			selected[id] = true
			continue
		}
		if err != nil || meta.ID != id {
			return nil, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		selected[id] = true
		if meta.ParentThreadID != "" {
			queue = append(queue, meta.ParentThreadID)
		}
		if meta.ForkedFromThreadID != "" {
			queue = append(queue, meta.ForkedFromThreadID)
		}
		value, found, err := source.member(ctx, "entries", id)
		if err != nil {
			return nil, false, err
		}
		if found {
			if err := visitSnapshotEntries(ctx, source.tx, value, func(entry Entry) error {
				if entry.Type == EntryParentEvent {
					event, err := decodeParentEvent(entry)
					if err != nil {
						return err
					}
					queue = append(queue, event.SourceThreadID)
				}
				return nil
			}); err != nil {
				return nil, false, err
			}
		}
	}
	state, err := loadEffectiveSnapshotScopedState(ctx, source, version, selected)
	if err != nil {
		return nil, false, err
	}
	memory, migrated, err := migrateDecodedMemoryState(state, now)
	if err != nil {
		return nil, false, err
	}
	if version == 5 {
		if _, err := repairLegacyUTF8EntryProjections(memory); err != nil {
			return nil, false, err
		}
	}
	if err := validateBackendDomainV6Memory(memory); err != nil {
		return nil, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	for id, tombstone := range memory.tombstones {
		if _, live := memory.threads[id]; live || validateMemoryArtifactTombstone(tombstone, id) != nil {
			return nil, false, ErrAuthorityCorrupt
		}
	}
	if _, deleted := memory.tombstones[threadID]; deleted {
		return nil, false, ErrThreadDeleted
	}
	return memory, migrated, nil
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
