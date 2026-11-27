package sessiontree

import (
	"bytes"
	"context"

	"github.com/floegence/floret/v7/storage/spi"
)

// Backfill the published side only. Retained source rows acquire membership
// when their validated dependency closure is published, never during staging.
// The owning transaction must advance the checkpoint together with its edges.
func (repo *BackendRepo) backfillChildMembershipPage(ctx context.Context, tx spi.WriteTx, afterID string) (lastID string, more bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	format := backendDomainV32Format
	prefix := backendDomainKey(backendDomainRecordThread)
	req := spi.ScanRequest{Namespace: format.namespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: backendThreadEntryPageSize}
	if afterID != "" {
		req.After = backendDomainKey(backendDomainRecordThread, afterID)
	}
	page, err := tx.Scan(req)
	if err != nil {
		return "", false, err
	}
	previous := req.After
	for _, row := range page.Records {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if bytes.Compare(row.Key, previous) <= 0 {
			return "", false, ErrAuthorityCorrupt
		}
		previous = row.Key
		record, err := listedThreadRecord(format, row)
		if err != nil {
			return "", false, err
		}
		meta, err := listedThreadMeta(format, record)
		if err != nil {
			return "", false, err
		}
		if meta.ParentThreadID == "" {
			if err := validateListedRoot(tx, format, meta.ID); err != nil {
				return "", false, err
			}
		} else {
			if err := repo.validateListedParents(ctx, tx, format, meta); err != nil {
				return "", false, err
			}
			if err := changeChildMembership(tx, meta.ParentThreadID, meta.ID, true); err != nil {
				return "", false, err
			}
		}
		lastID = meta.ID
	}
	if page.HasMore && (lastID == "" || !bytes.Equal(page.Next, previous)) {
		return "", false, ErrAuthorityCorrupt
	}
	return lastID, page.HasMore, ctx.Err()
}
