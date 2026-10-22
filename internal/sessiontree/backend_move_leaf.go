package sessiontree

import (
	"context"
	"time"
)

func (repo *BackendRepo) MoveLeaf(ctx context.Context, threadID, entryID string) error {
	return repo.canonicalJournalUpdate(ctx, threadID, func(mutation *canonicalJournalMutation) error {
		if err := lifecycleRejectsWrite(mutation.meta); err != nil {
			return err
		}
		if entryID != "" {
			if _, err := readCanonicalEntryMetadata(mutation.tx, threadID, entryID); err != nil {
				return err
			}
		} else {
			count, err := readJournalCount(mutation.tx, threadID)
			if err != nil {
				return err
			}
			if count != 0 {
				return ErrAuthorityCorrupt
			}
		}
		mutation.meta.LeafID, mutation.meta.UpdatedAt = entryID, time.Now()
		if err := persistThreadMetadata(ctx, mutation.tx, mutation.meta); err != nil {
			return err
		}
		var err error
		mutation.query, err = rebuildCanonicalQuery(ctx, mutation.tx, mutation.meta)
		return err
	}, func(memory *MemoryRepo) error { return memory.MoveLeaf(ctx, threadID, entryID) })
}
