package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) SourceBodyPage(ctx context.Context, options SourceBodyPageOptions) (SourceBodyPage, error) {
	if ctx == nil || options.SourceID == "" || options.Limit < 1 || options.Limit > 150 || options.Field != "content" && options.Field != "reasoning" || options.AfterEntryID != "" && options.LeafID == "" {
		return SourceBodyPage{}, spi.ErrInvalidArgument
	}
	result := SourceBodyPage{Bodies: []CanonicalBodyMetadata{}}
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		result.LeafID = options.LeafID
		if result.LeafID == "" {
			result.LeafID = meta.LeafID
		}
		if result.LeafID == "" {
			if options.AfterEntryID != "" {
				return ErrEntryNotFound
			}
			return nil
		}
		if _, err := activeLocatedEntry(ctx, tx, meta, result.LeafID); err != nil {
			return err
		}
		leaf, err := readEntryLocator(tx, meta.ID, result.LeafID)
		if err != nil {
			return err
		}
		var after []byte
		if options.AfterEntryID != "" {
			previous, err := readEntryLocator(tx, meta.ID, options.AfterEntryID)
			if err != nil {
				return err
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, previous)
			if err != nil {
				return err
			}
			if !slices.Contains(entrySourceIDs(entry), options.SourceID) {
				return ErrEntryNotFound
			}
			after = sourceLocatorKey(meta.ID, options.SourceID, previous.Ordinal)
		}
		prefix := backendDomainKey("source", meta.ID, options.SourceID)
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Start: prefix, End: sourceLocatorKey(meta.ID, options.SourceID, leaf.Ordinal+1), After: after, Limit: 256})
		if err != nil {
			return err
		}
		for position, row := range page.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			node, err := readEntryLocator(tx, meta.ID, string(row.Value))
			if err != nil || node.Ordinal > leaf.Ordinal || !bytes.Equal(row.Key, sourceLocatorKey(meta.ID, options.SourceID, node.Ordinal)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, meta.ID, node)
			if err != nil || !slices.Contains(entrySourceIDs(entry), options.SourceID) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			result.NextEntryID = entry.ID
			result.HasMore = position+1 < len(page.Records) || page.HasMore
			if node.Depth > leaf.Depth {
				continue
			}
			ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, node.Depth)
			if err != nil {
				return err
			}
			if ancestor.ID != entry.ID {
				continue
			}
			sourceID := entry.Message.ContentSourceID
			if options.Field == "reasoning" {
				sourceID = entry.Message.ReasoningSourceID
			}
			if sourceID != options.SourceID {
				continue
			}
			field := options.Field
			if field == "content" && entry.Message.ControlSignal != nil {
				field = "control_output"
			}
			ref, stored := entry.bodyFields[field]
			if !stored {
				value := canonicalEntryBodyField(&entry, field)
				if value == nil {
					return ErrAuthorityCorrupt
				}
				ref = canonicalBodyRef{SHA256: stableHash(*value), Bytes: len(*value)}
			}
			if ref.Bytes == 0 && field != "control_output" {
				return ErrAuthorityCorrupt
			}
			result.Bodies = append(result.Bodies, CanonicalBodyMetadata{EntryID: entry.ID, Field: field, SHA256: ref.SHA256, Bytes: ref.Bytes})
			if options.Field == "reasoning" {
				result.HasMore = false
				break
			}
			if len(result.Bodies) == options.Limit {
				break
			}
		}
		if !result.HasMore {
			result.NextEntryID = ""
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.SourceBodyPage(ctx, options)
		return err
	})
	return result, err
}
