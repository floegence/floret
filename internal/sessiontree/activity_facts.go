package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
)

func effectiveActivityFacts(ctx context.Context, tx spi.ReadTx, entries []Entry, defaults map[string]string) (ActivityFacts, error) {
	fields, err := effectiveActivityFields(ctx, tx, entries, defaults)
	if err != nil {
		return ActivityFacts{}, err
	}
	return ActivityFacts{Renderer: fields["/renderer"].fact.Value, Action: fields["/payload/action"].fact.Value,
		HasError: fields["/payload/error"].fact.NonZero, UncertainEffect: fields["/chips"].fact.Value == "uncertain"}, nil
}

func (repo *BackendRepo) ActivityFacts(ctx context.Context, input ActivityStructureInput) (ActivityFacts, error) {
	if ctx == nil || input.ThreadID == "" || len(input.EntryIDs) == 0 || len(input.EntryIDs) > 64 {
		return ActivityFacts{}, spi.ErrInvalidArgument
	}
	for _, id := range input.EntryIDs {
		// Merge facts already belong to the admitted summary. A safety query
		// must not prepare hidden detail locators merely to read these facts.
		ready := false
		err := repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			entry, err := activeLocatedEntry(ctx, tx, meta, id)
			if err != nil {
				return err
			}
			_, err = readActivityMergeMetadata(tx, entry.bodyFields["activity_json"])
			ready = err == nil
			if errors.Is(err, spi.ErrNotFound) {
				return nil
			}
			return err
		}, func(*MemoryRepo) error { ready = true; return nil })
		if err == nil && !ready {
			err = repo.prepareActivityFields(ctx, input.ThreadID, id)
		}
		if err != nil && !(errors.Is(err, ErrEntryNotFound) && input.Defaults[id] != "") {
			return ActivityFacts{}, err
		}
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ActivityFacts(ctx, input)
	}
	var result ActivityFacts
	err := repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		entries := make([]Entry, len(input.EntryIDs))
		for i, id := range input.EntryIDs {
			entry, err := activeLocatedEntry(ctx, tx, meta, id)
			if err != nil {
				return err
			}
			entries[i] = entry
		}
		var err error
		result, err = effectiveActivityFacts(ctx, tx, entries, input.Defaults)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ActivityFacts(ctx, input)
		return err
	})
	return result, err
}

func (repo *MemoryRepo) ActivityFacts(ctx context.Context, input ActivityStructureInput) (ActivityFacts, error) {
	if ctx == nil || input.ThreadID == "" || len(input.EntryIDs) == 0 || len(input.EntryIDs) > 64 {
		return ActivityFacts{}, spi.ErrInvalidArgument
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ActivityFacts{}, err
	}
	entries := make([]Entry, len(input.EntryIDs))
	stored := false
	for i, id := range input.EntryIDs {
		entry, err := repo.activeBodyEntryLocked(input.ThreadID, id)
		if err != nil {
			return ActivityFacts{}, err
		}
		entries[i] = entry
		stored = stored || len(entry.bodyFields) > 0
	}
	if !stored {
		return effectiveActivityFacts(ctx, nil, entries, input.Defaults)
	}
	var result ActivityFacts
	read := func(tx spi.ReadTx) error {
		var err error
		result, err = effectiveActivityFacts(ctx, tx, entries, input.Defaults)
		return err
	}
	var err error
	if repo.bodyTx != nil {
		err = read(repo.bodyTx)
	} else if repo.bodyBackend != nil {
		err = repo.bodyBackend.View(ctx, read)
	} else {
		err = ErrAuthorityCorrupt
	}
	return result, err
}
