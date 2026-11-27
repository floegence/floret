//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type OfflineThreadInventory struct {
	Threads, Roots, Entries, Empty int64
	Digest                         [32]byte
}

func WalkOfflineTombstones(ctx context.Context, tx spi.ReadTx, visit func(id string, raw []byte) error) error {
	return visitCanonicalTombstones(ctx, tx, func(tombstone ThreadTombstone) error {
		id := tombstone.ThreadID
		if _, err := readPublishedThreadMeta(tx, backendDomainV32Format, id); !errors.Is(err, ErrThreadDeleted) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		key := backendDomainKey(backendDomainRecordTombstone, id)
		raw, err := tx.Get(backendDomainV32Namespace, key)
		if err != nil {
			return err
		}
		return visit(id, raw)
	})
}

func PutOfflineTombstone(tx spi.WriteTx, id string, raw []byte) error {
	key := backendDomainKey(backendDomainRecordTombstone, id)
	return storagebridge.PutCatalogRecord(tx, storagebridge.CatalogRecord{Namespace: backendDomainV32Namespace, Key: key}, raw)
}

func CheckOfflineThreadInventory(ctx context.Context, tx spi.ReadTx) (OfflineThreadInventory, error) {
	return WalkOfflineThreads(ctx, tx, nil)
}

func WalkOfflineThreads(ctx context.Context, tx spi.ReadTx, visit func(string) error) (OfflineThreadInventory, error) {
	return WalkOfflineSourceThreads(ctx, tx, backendDomainV43Version, visit)
}

func WalkOfflineSourceThreads(ctx context.Context, tx spi.ReadTx, version int, visit func(string) error) (OfflineThreadInventory, error) {
	var inventory OfflineThreadInventory
	manifest, err := readV32Manifest(tx)
	if err != nil || manifest.SourceVersion != 0 || manifest.Version != version || (version != backendDomainV42Version && version != backendDomainV43Version) {
		return inventory, errors.Join(spi.ErrMigrationRequired, err)
	}
	parents := map[string]string{}
	var order []string
	prefix := backendDomainKey(backendDomainRecordThread)
	request := spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: backendThreadEntryPageSize}
	digest := sha256.New()
	digest.Write([]byte("floret.thread-head-inventory.v1\x00"))
	for {
		page, err := tx.Scan(request)
		if err != nil {
			return OfflineThreadInventory{}, err
		}
		for _, row := range page.Records {
			if err := ctx.Err(); err != nil {
				return OfflineThreadInventory{}, err
			}
			record, err := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
			if err != nil || record.Kind != backendDomainRecordThread || record.ID == "" || !bytes.Equal(row.Key, backendDomainKey(record.Kind, record.ID)) {
				return OfflineThreadInventory{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, record.ID)
			if err != nil {
				return OfflineThreadInventory{}, err
			}
			count, err := readJournalCount(tx, meta.ID)
			if err != nil || count < 0 || (count == 0) != (meta.LeafID == "") {
				return OfflineThreadInventory{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			if meta.LeafID == "" {
				inventory.Empty++
			} else {
				leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
				if err != nil || leaf.Ordinal >= count {
					return OfflineThreadInventory{}, errors.Join(ErrAuthorityCorrupt, err)
				}
				if _, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, leaf); err != nil {
					return OfflineThreadInventory{}, err
				}
			}
			if _, duplicate := parents[meta.ID]; duplicate {
				return OfflineThreadInventory{}, ErrAuthorityCorrupt
			}
			parents[meta.ID] = meta.ParentThreadID
			order = append(order, meta.ID)
			if meta.ParentThreadID == "" {
				inventory.Roots++
			}
			inventory.Threads++
			inventory.Entries += int64(count)
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(row.Value))))
			digest.Write(row.Value)
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(count)))
		}
		if !page.HasMore {
			break
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, page.Records[len(page.Records)-1].Key) || bytes.Compare(request.After, page.Next) >= 0 {
			return OfflineThreadInventory{}, ErrAuthorityCorrupt
		}
		request.After = page.Next
	}
	verified := map[string]bool{}
	var ordered []string
	for _, thread := range order {
		path := map[string]bool{}
		var chain []string
		for next := thread; next != "" && !verified[next]; {
			parent, found := parents[next]
			if !found || path[next] {
				return OfflineThreadInventory{}, ErrAuthorityCorrupt
			}
			chain = append(chain, next)
			path[next], next = true, parent
		}
		for pos := len(chain) - 1; pos >= 0; pos-- {
			member := chain[pos]
			verified[member] = true
			ordered = append(ordered, member)
		}
	}
	if visit != nil {
		for _, thread := range ordered {
			if err := visit(thread); err != nil {
				return OfflineThreadInventory{}, err
			}
		}
	}
	copy(inventory.Digest[:], digest.Sum(nil))
	return inventory, nil
}
