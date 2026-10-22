package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// A mutation owns only new facts and the current thread metadata. Canonical
// records remain the authority; derived cache changes publish after commit.
type canonicalJournalMutation struct {
	ctx      context.Context
	repo     *BackendRepo
	tx       spi.WriteTx
	meta     ThreadMeta
	query    *queryThread
	entries  []Entry
	parentID string
}

func (repo *BackendRepo) canonicalJournalUpdate(ctx context.Context, threadID string, mutate func(*canonicalJournalMutation) error, legacy func(*MemoryRepo) error) error {
	return repo.canonicalJournalUpdateWithRecords(ctx, threadID, mutate, legacy, nil)
}

func (repo *BackendRepo) canonicalJournalUpdateWithRecords(ctx context.Context, threadID string, mutate func(*canonicalJournalMutation) error, legacy func(*MemoryRepo) error, settle func(spi.WriteTx) error) error {
	if repo == nil || ctx == nil || strings.TrimSpace(threadID) == "" {
		return spi.ErrInvalidArgument
	}
	if repo.waitForReady != nil {
		if err := repo.waitForReady(ctx); err != nil {
			return err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.canonicalJournalUpdateWithRecords(ctx, threadID, mutate, legacy, settle)
	}
	if repo.domainFormat().version < backendDomainV32Version {
		if settle == nil {
			return repo.updateThread(ctx, threadID, legacy)
		}
		return repo.UpdateThreadDomain(ctx, threadID, func(memory *MemoryRepo, tx spi.WriteTx) error {
			if err := legacy(memory); err != nil {
				return err
			}
			return settle(tx)
		})
	}
	if err := repo.prepareThreadIfNeeded(ctx, threadID); err != nil {
		return err
	}
	for {
		continuation, err := repo.loadQueryContinuation(ctx, threadID)
		if errors.Is(err, spi.ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		retry, err := func() (bool, error) {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			mutation := &canonicalJournalMutation{ctx: ctx, repo: repo, query: continuation.state}
			stale := false
			err := repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if err := continuation.verify(tx); err != nil {
					stale = errors.Is(err, spi.ErrConflict)
					return err
				}
				meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, threadID)
				if err != nil {
					return err
				}
				mutation.tx, mutation.meta = tx, meta
				if err := mutate(mutation); err != nil {
					return err
				}
				if err := mutation.publishParentEvents(); err != nil {
					return err
				}
				if settle != nil {
					if err := settle(tx); err != nil {
						return err
					}
				}
				return ctx.Err()
			})
			if err != nil {
				return stale, err
			}
			repo.installJournalMutation(mutation)
			return false, nil
		}()
		if !retry {
			return err
		}
	}
}

func (repo *BackendRepo) installJournalMutation(mutation *canonicalJournalMutation) {
	repo.threadMemoryID, repo.threadMemory = "", nil
	repo.rememberVerifiedThread(mutation.meta.ID, mutation.meta.LeafID)
	if mutation.query != nil {
		repo.installQueryThreads(map[string]*queryThread{mutation.meta.ID: mutation.query})
	} else {
		delete(repo.queryThreads, mutation.meta.ID)
	}
	var observed []string
	if mutation.parentID != "" {
		delete(repo.queryThreads, mutation.parentID)
		delete(repo.verifiedThreadLeaves, mutation.parentID)
		observed = append(observed, mutation.parentID)
	}
	for _, entry := range mutation.entries {
		if entry.Type == EntryParentEvent || entry.Type == EntryParentEventsAdmitted {
			observed = append(observed, mutation.meta.ID)
			break
		}
	}
	if len(observed) > 0 && repo.parentEventObserver != nil {
		go repo.parentEventObserver(observed)
	}
}

func (mutation *canonicalJournalMutation) append(entry Entry, options AppendOptions) (Entry, error) {
	return mutation.appendFact(entry, options, false)
}

func (mutation *canonicalJournalMutation) appendFact(entry Entry, options AppendOptions, settlement bool) (Entry, error) {
	previous := mutation.meta
	if options.Now.IsZero() && entry.CreatedAt.IsZero() {
		options.Now = mutation.repo.now()
	}
	saved, meta, err := appendCanonicalEntry(mutation.ctx, mutation.tx, previous, entry, options, settlement)
	if err != nil {
		return Entry{}, err
	}
	projected, err := appendCanonicalQuery(mutation.ctx, mutation.tx, previous, meta, saved, mutation.query)
	if err != nil {
		return Entry{}, err
	}
	mutation.meta, mutation.query = meta, projected
	mutation.entries = append(mutation.entries, saved)
	return saved, nil
}

func (mutation *canonicalJournalMutation) publishParentEvents() error {
	if !mutation.repo.parentEventsEnabled() {
		return nil
	}
	publish := func(source Entry) error {
		event, found, err := canonicalAutomaticParentEvent(mutation.ctx, mutation.tx, mutation.meta, source)
		if err != nil || !found {
			return err
		}
		appended, err := appendCanonicalParentEvent(mutation.ctx, mutation.tx, event)
		if err == nil && appended {
			mutation.parentID = event.ParentThreadID
		}
		return err
	}
	for _, entry := range mutation.entries {
		if err := publish(entry); err != nil {
			return err
		}
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting && mutation.meta.ParentThreadID != "" {
			if err := visitCanonicalTurnEntries(mutation.ctx, mutation.tx, mutation.meta.ID, entry.TurnID, func(source Entry) error {
				if source.Type == EntryToolResult && source.RunID == entry.RunID {
					return publish(source)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if entry.Type == EntryInteractionDone {
			mutation.parentID = mutation.meta.ParentThreadID
		}
	}
	return nil
}
