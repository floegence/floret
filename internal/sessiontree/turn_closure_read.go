package sessiontree

import (
	"context"

	"github.com/floegence/floret/v7/storage/spi"
)

func ReadTurnClosureEntries(ctx context.Context, repo JournalRepo, thread, turn string) ([]Entry, error) {
	if reader, ok := repo.(interface {
		readTurnClosureEntries(context.Context, string, string) ([]Entry, error)
	}); ok {
		return reader.readTurnClosureEntries(ctx, thread, turn)
	}
	return repo.Path(ctx, thread, "")
}

func (repo *BackendRepo) readTurnClosureEntries(ctx context.Context, thread, turn string) ([]Entry, error) {
	var entries []Entry
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		if meta.LeafID == "" {
			return nil
		}
		leaf, err := readEntryLocator(tx, thread, meta.LeafID)
		if err != nil {
			return err
		}
		return visitCanonicalTurnEntries(ctx, tx, thread, turn, func(entry Entry) error {
			if entry.PathDepth > leaf.Depth {
				return nil
			}
			ancestor, err := entryAncestor(ctx, tx, thread, leaf, entry.PathDepth)
			if err != nil || ancestor.ID != entry.ID {
				return err
			}
			if entry.Type == EntryToolCall {
				entry, err = completeLocatedEntry(ctx, tx, entry)
				if err != nil {
					return err
				}
			}
			entries = append(entries, entry)
			return nil
		})
	}, func(memory *MemoryRepo) error {
		var err error
		entries, err = memory.Path(ctx, thread, "")
		return err
	})
	return entries, err
}
