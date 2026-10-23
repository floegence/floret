package sessiontree

import (
	"context"
	"errors"
	"sort"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// Retention streams integrity checks in bounded transactions. A final authority
// check rejects concurrent changes before callers can delete any media. It
// retains only media facts, never a complete in-memory journal or text body.
func (repo *BackendRepo) canonicalMediaEntries(ctx context.Context, threadID string) (entries []Entry, handled bool, resultErr error) {
	if repo == nil || ctx == nil {
		return nil, true, spi.ErrInvalidArgument
	}
	defer func() {
		if err := ctx.Err(); err != nil {
			entries, handled, resultErr = nil, true, err
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return nil, true, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalMediaEntries(ctx, threadID)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		return nil, false, nil
	}
	if threadID != "" {
		if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
			return nil, true, err
		}
	}
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: ctx, backend: repo.backend}, format: repo.domainFormat(), metadata: map[string]migrationMetadataRead{}}
	// Inventory enumeration uses the repository owner only for short metadata
	// transactions; journal scans release both locks between bounded pages.
	inventoryView := func(read func(spi.ReadTx) error) error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.View(ctx, read)
	}
	var ids []string
	if threadID != "" {
		ids = []string{threadID}
	} else {
		err := inventoryView(func(tx spi.ReadTx) error {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			// Unpublished source histories remain retention authorities.
			if manifest.SourceVersion != 0 {
				return spi.ErrConflict
			}
			return repo.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error { ids = append(ids, meta.ID); return nil })
		})
		if err != nil {
			return nil, true, err
		}
	}
	sort.Strings(ids)
	counts := make(map[string]int, len(ids))
	var result []Entry
	for _, id := range ids {
		if _, err := readPublishedThreadMeta(reader, repo.domainFormat(), id); err != nil {
			return nil, true, err
		}
		count, err := readJournalCount(reader, id)
		if err != nil {
			return nil, true, err
		}
		counts[id] = count
		seen, err := visitCanonicalJournal(ctx, reader, id, func(entry Entry) error {
			// The checksum covers both retention metadata and body fields. Stream
			// all fields so removing an attachment cannot authorize its deletion.
			validated, _, err := validateCanonicalStoredIntegrity(ctx, reader, entry)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry = validated
			if result := entry.Message.ToolResult; result != nil {
				if err := session.ValidateStoredMessageAttachments(result.Attachments); err != nil {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			if len(entry.Message.Attachments) > 0 || entry.Message.ToolResult != nil && len(entry.Message.ToolResult.Attachments) > 0 || entry.Type == EntryQueueAdded || entry.Type == EntryTurnMarker {
				entry.bodyFields = nil
				result = append(result, entry)
			}
			return nil
		})
		if err != nil {
			return nil, true, reader.validationError(err)
		}
		if seen != count {
			return nil, true, reader.validationError(ErrAuthorityCorrupt)
		}
	}
	err := inventoryView(func(tx spi.ReadTx) error {
		if err := reader.verify(tx); err != nil {
			return err
		}
		for id, count := range counts {
			current, err := readJournalCount(tx, id)
			if err != nil {
				return err
			}
			if current != count {
				return spi.ErrConflict
			}
		}
		if threadID == "" {
			manifest, err := readV32Manifest(tx)
			if err != nil {
				return err
			}
			if manifest.SourceVersion != 0 {
				return spi.ErrConflict
			}
			current := 0
			if err := repo.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error {
				if _, found := counts[meta.ID]; !found {
					return spi.ErrConflict
				}
				current++
				return nil
			}); err != nil {
				return err
			}
			if current != len(ids) {
				return spi.ErrConflict
			}
		}
		return ctx.Err()
	})
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}
