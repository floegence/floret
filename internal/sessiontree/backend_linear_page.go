package sessiontree

import (
	"bytes"
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// A node whose depth equals its ordinal plus one has no skipped journal rows.
// Read that bounded interval once, validate every source link, then visit newest
// first just like the general locator walker. Branched paths keep that walker.
func visitLinearCanonicalPage(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, node entryLocator, limit int, visit func(Entry) error) (PathPage, error) {
	first := max(0, node.Ordinal-limit+1)
	page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: backendDomainEntryKey(meta.ID, first), End: backendDomainEntryKey(meta.ID, node.Ordinal+1), Limit: node.Ordinal - first + 1})
	if err != nil {
		return PathPage{}, err
	}
	if page.HasMore || len(page.Records) != node.Ordinal-first+1 {
		return PathPage{}, ErrAuthorityCorrupt
	}
	entries := make([]Entry, 0, len(page.Records))
	previous := ""
	for index, row := range page.Records {
		if err := ctx.Err(); err != nil {
			return PathPage{}, err
		}
		ordinal := first + index
		record, err := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
		if err != nil || record.ThreadID != meta.ID || record.Ordinal != ordinal || !bytes.Equal(row.Key, backendDomainEntryKey(meta.ID, ordinal)) {
			return PathPage{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
		if err != nil {
			return PathPage{}, err
		}
		entry := stored.Entry
		entry.bodyFields = stored.Fields
		if entry.PathDepth != int64(ordinal)+1 || index > 0 && entry.ParentID != previous || ordinal == 0 && entry.ParentID != "" {
			return PathPage{}, ErrAuthorityCorrupt
		}
		if index == 0 && ordinal > 0 {
			parent, err := readEntryLocator(tx, meta.ID, entry.ParentID)
			if err != nil {
				return PathPage{}, err
			}
			if parent.Ordinal != ordinal-1 || parent.Depth != entry.PathDepth-1 {
				return PathPage{}, ErrAuthorityCorrupt
			}
		}
		previous = entry.ID
		entries = append(entries, entry)
	}
	last := entries[len(entries)-1]
	if last.ID != node.ID || last.ParentID != node.Parent {
		return PathPage{}, ErrAuthorityCorrupt
	}
	result := PathPage{Entries: []Entry{}, NewestOrdinal: node.Depth, HasMore: first > 0}
	if result.HasMore {
		result.NextEntryID = entries[0].ID
	}
	for index := len(entries) - 1; index >= 0; index-- {
		if err := visit(entries[index]); err != nil {
			return PathPage{}, err
		}
	}
	return result, nil
}
