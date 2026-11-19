package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// LatestQueueFact reads the existing journal locator, including removal after
// an edit. Queue membership is journal-wide, independent of the active leaf.
func (repo *BackendRepo) LatestQueueFact(ctx context.Context, thread, id string) (Entry, bool, error) {
	var fact Entry
	var found bool
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		raw, err := tx.Get(backendEntryIndexNamespace, backendDomainKey("queue", thread, id))
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		fact, err = readCanonicalEntryMetadata(tx, thread, string(raw))
		if err != nil {
			return err
		}
		if actual, ok := canonicalQueueSource(fact); !ok || actual != id {
			return ErrAuthorityCorrupt
		}
		found = true
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		fact, found, err = memory.LatestQueueFact(ctx, thread, id)
		return err
	})
	return fact, found, err
}

func (repo *MemoryRepo) LatestQueueFact(ctx context.Context, thread, id string) (Entry, bool, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if _, ok := repo.threads[thread]; !ok {
		return Entry{}, false, ErrThreadNotFound
	}
	for i := len(repo.entries[thread]) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return Entry{}, false, err
		}
		entry := repo.entries[thread][i]
		if actual, ok := canonicalQueueSource(entry); ok && actual == id {
			return cloneEntry(entry), true, nil
		}
	}
	return Entry{}, false, nil
}
