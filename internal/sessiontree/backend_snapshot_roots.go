package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

var snapshotRootsCheckpointKey = storagecodec.Tuple(storagecodec.TupleString("root-inventory-checkpoint"))

func snapshotRootsSpanKey(path ...string) []byte {
	parts := [][]byte{storagecodec.TupleString("root-inventory-span")}
	for _, part := range path {
		parts = append(parts, storagecodec.TupleString(part))
	}
	return storagecodec.Tuple(parts...)
}

func indexSnapshotRootsBatch(ctx context.Context, tx spi.WriteTx) (snapshotImportCheckpoint, error) {
	checkpoint, found, err := readJSONImportCheckpoint(tx, snapshotRootsCheckpointKey, 1, 1)
	if err != nil {
		return checkpoint, err
	}
	return indexJSONSourceBatch(ctx, tx, checkpoint, found, jsonMigrationSource{
		namespace: backendDomainNamespace, key: backendRootThreadInventoryKey, checkpointKey: snapshotRootsCheckpointKey, spanKey: snapshotRootsSpanKey,
		indexPath: func(path []string) (bool, error) {
			if len(path) == 0 {
				return true, nil
			}
			if path[0] != "payload" {
				if len(path) != 1 || path[0] != "version" && path[0] != "kind" {
					return false, ErrAuthorityCorrupt
				}
				return true, nil
			}
			if len(path) > 1 && path[1] != "version" && path[1] != "items" {
				return false, ErrAuthorityCorrupt
			}
			if len(path) > 2 && path[1] != "items" {
				return false, ErrAuthorityCorrupt
			}
			return len(path) <= 3, nil
		},
		validate: func(ctx context.Context, tx spi.ReadTx) (int, error) {
			var version, inventoryVersion int
			var kind string
			for _, field := range []struct {
				path   []string
				target any
			}{{[]string{"version"}, &version}, {[]string{"kind"}, &kind}, {[]string{"payload", "version"}, &inventoryVersion}} {
				span, err := readJSONSourceSpan(tx, snapshotRootsSpanKey(field.path...))
				if err != nil {
					return 0, err
				}
				if err := decodeJSONSourceValue(ctx, tx, backendDomainNamespace, backendRootThreadInventoryKey, span, field.target); err != nil {
					return 0, err
				}
			}
			if version != 1 || inventoryVersion != rootThreadInventoryVersion || kind != "sessiontree-root-thread-inventory" {
				return 0, ErrAuthorityCorrupt
			}
			for _, shape := range []struct {
				path []string
				kind byte
			}{{nil, '{'}, {[]string{"payload"}, '{'}, {[]string{"payload", "items"}, '['}} {
				span, err := readJSONSourceSpan(tx, snapshotRootsSpanKey(shape.path...))
				if err != nil || span.Kind != shape.kind {
					return 0, errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			return 1, nil
		},
	})
}

// Root inventory is a released independent authority. Its complete root set,
// order and each exact path must agree before the monolithic source can retire.
// Each item is decoded alone; unrelated transcript bodies never coexist here.
func verifySnapshotRootInventory(ctx context.Context, source *snapshotEffectiveSource, clock func() time.Time) error {
	checkpoint, found, err := readJSONImportCheckpoint(source.tx, snapshotRootsCheckpointKey, 1, 1)
	if err != nil || !found || !checkpoint.Complete {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := verifyIndexedJSONRecord(ctx, source.tx, backendDomainNamespace, backendRootThreadInventoryKey, checkpoint); err != nil {
		return err
	}
	ids, err := source.members(ctx, "threads")
	if err != nil {
		return err
	}
	roots := make(map[string]ThreadMeta)
	for _, id := range ids {
		var meta ThreadMeta
		if err := source.decodeMember(ctx, &meta, "threads", id); err != nil {
			return err
		}
		if meta.ID != id {
			return ErrAuthorityCorrupt
		}
		if meta.ParentThreadID == "" {
			roots[id] = meta
		}
	}
	members, err := indexedJSONMembers(ctx, source.tx, snapshotRootsSpanKey("payload", "items"))
	if err != nil {
		return err
	}
	if len(members) != len(roots) {
		return ErrAuthorityCorrupt
	}
	sort.Slice(members, func(i, j int) bool {
		a, _ := strconv.Atoi(members[i].name)
		b, _ := strconv.Atoi(members[j].name)
		return a < b
	})
	var previous *ThreadMeta
	for index, member := range members {
		if member.name != strconv.Itoa(index) {
			return ErrAuthorityCorrupt
		}
		var item RootThreadInventoryItem
		if err := decodeJSONSourceValue(ctx, source.tx, backendDomainNamespace, backendRootThreadInventoryKey, member.span, &item); err != nil {
			return err
		}
		if _, ok := roots[item.Meta.ID]; !ok {
			return ErrAuthorityCorrupt
		}
		delete(roots, item.Meta.ID)
		if previous != nil && (item.Meta.CreatedAt.After(previous.CreatedAt) || item.Meta.CreatedAt.Equal(previous.CreatedAt) && item.Meta.ID < previous.ID) {
			return ErrAuthorityCorrupt
		}
		previous = &item.Meta
		memory, err := loadSnapshotThreadClosureFrom(ctx, source, 5, item.Meta.ID, clock)
		if err != nil {
			return err
		}
		path, err := memory.canonicalPathLocked(ctx, item.Meta.ID, memory.threads[item.Meta.ID].LeafID)
		if err != nil {
			return err
		}
		// Preserve the exact released UTF-8 projection repair in both authorities.
		for i := range item.Path {
			if _, err := repairLegacyUTF8EntryProjection(&item.Path[i]); err != nil {
				return err
			}
		}
		if err := validateRootThreadInventory([]RootThreadInventoryItem{item}); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		want := RootThreadInventoryItem{Meta: memory.threads[item.Meta.ID], Path: path}
		gotRaw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		wantRaw, err := json.Marshal(want)
		if err != nil {
			return err
		}
		if !bytes.Equal(gotRaw, wantRaw) {
			return fmt.Errorf("%w: snapshot root inventory differs for %q", ErrAuthorityCorrupt, item.Meta.ID)
		}
	}
	if len(roots) != 0 {
		return ErrAuthorityCorrupt
	}
	return nil
}
