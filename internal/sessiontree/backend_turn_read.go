package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func (repo *BackendRepo) Entries(ctx context.Context, threadID string) ([]Entry, error) {
	var entries []Entry
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		count, err := readJournalCount(tx, meta.ID)
		if err != nil {
			return err
		}
		for ordinal := 0; ordinal < count; ordinal++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			raw, err := tx.Get(backendDomainV32Namespace, backendDomainEntryKey(meta.ID, ordinal))
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
			if err != nil || record.Kind != backendDomainRecordEntry || record.ThreadID != meta.ID || record.Ordinal != ordinal {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
			if err != nil {
				return err
			}
			entry, err := materializeCanonicalStoredEntry(ctx, tx, stored)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		entries, err = memory.Entries(ctx, threadID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (repo *BackendRepo) CanonicalTurnEntries(ctx context.Context, threadID, turnID, runID string) ([]Entry, bool, error) {
	var entries []Entry
	found := false
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		err := visitCanonicalTurnEntries(ctx, tx, meta.ID, turnID, func(entry Entry) error {
			found = true
			complete, err := completeLocatedEntry(ctx, tx, entry)
			if err != nil {
				return err
			}
			entries = append(entries, complete)
			return nil
		})
		if err != nil || !found {
			return err
		}
		entries = CanonicalTurnEntriesForRead(entries)
		if err := validateCanonicalTurnLinks(ctx, tx, meta.ID, entries); err != nil {
			return err
		}
		return ValidateCanonicalTurnEntries(entries, threadID, turnID, runID)
	}, func(memory *MemoryRepo) error {
		var err error
		entries, found, err = memory.CanonicalTurnEntries(ctx, threadID, turnID, runID)
		return err
	})
	if err != nil {
		return nil, found, err
	}
	return entries, found, nil
}

func validateCanonicalTurnLinks(ctx context.Context, tx spi.ReadTx, threadID string, entries []Entry) error {
	for index := 1; index < len(entries); index++ {
		entry, previous := entries[index], entries[index-1]
		if isCanonicalPostTerminalTurnEntry(entry) {
			continue
		}
		parentID, depth := entry.ParentID, entry.PathDepth
		for parentID != previous.ID {
			if err := ctx.Err(); err != nil {
				return err
			}
			parent, err := readCanonicalEntryMetadata(tx, threadID, parentID)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if parent.TurnID != "" || parent.PathDepth <= previous.PathDepth || parent.PathDepth >= depth {
				return ErrAuthorityCorrupt
			}
			parentID, depth = parent.ParentID, parent.PathDepth
		}
	}
	return nil
}
