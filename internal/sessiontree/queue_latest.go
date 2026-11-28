package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func memberQueueFact(journal *member.Journal, thread, queueID, before string) (Entry, bool, error) {
	if journal.Thread() != thread {
		return Entry{}, false, ErrInvalidThreadAuthority
	}
	var limit int64 = -1
	if before != "" {
		boundary, err := journal.Entry(before)
		if err != nil {
			return Entry{}, false, errors.Join(ErrAuthorityCorrupt, err)
		}
		limit = boundary.Number
	}
	var fact Entry
	err := journal.VisitRecoveryEntries("queue", func(event member.EntryEvent) error {
		if limit >= 0 && event.Number >= limit {
			return nil
		}
		entry, err := materializeMemberEntry(journal, event)
		if err != nil {
			return err
		}
		if id, ok := canonicalQueueSource(entry); ok && id == queueID {
			fact = entry
		}
		return nil
	})
	return fact, fact.ID != "", err
}

// LatestQueueFact reads the existing journal locator, including removal after
// an edit. Queue membership is journal-wide, independent of the active leaf.
func (repo *BackendRepo) LatestQueueFact(ctx context.Context, thread, id string) (Entry, bool, error) {
	var fact Entry
	var found bool
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		if journal := member.From(tx); journal != nil {
			var err error
			fact, found, err = memberQueueFact(journal, thread, id, "")
			return err
		}
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
