package sessiontree

import (
	"context"

	"github.com/floegence/floret/v7/storage/spi"
)

// JournalMetadataOptions pages physical immutable records, including inactive
// branches. Through fixes the exclusive ordinal boundary without retaining a
// database transaction. This inventory is never a provider-context projection.
type JournalMetadataOptions struct {
	ThreadID              string
	After, Through, Limit int
}

type JournalMetadataPage struct {
	Entries        []PathMetadata
	After, Through int
	HasMore        bool
}

type JournalMetadataReader interface {
	JournalMetadata(context.Context, JournalMetadataOptions) (JournalMetadataPage, error)
}

func (r *MemoryRepo) JournalMetadata(ctx context.Context, options JournalMetadataOptions) (JournalMetadataPage, error) {
	if ctx == nil || options.After < 0 || options.Through < 0 || options.Limit < 1 || options.Limit > 256 {
		return JournalMetadataPage{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return JournalMetadataPage{}, err
	}
	if _, found := r.threads[options.ThreadID]; !found {
		return JournalMetadataPage{}, ErrThreadNotFound
	}
	entries := r.entries[options.ThreadID]
	through := options.Through
	if through == 0 {
		through = len(entries)
	}
	if through > len(entries) || options.After > through {
		return JournalMetadataPage{}, ErrEntryNotFound
	}
	end := min(through, options.After+options.Limit)
	page := JournalMetadataPage{Entries: make([]PathMetadata, 0, end-options.After), After: end, Through: through, HasMore: end < through}
	for ordinal := options.After; ordinal < end; ordinal++ {
		if err := ctx.Err(); err != nil {
			return JournalMetadataPage{}, err
		}
		entry := entries[ordinal]
		if entry.ThreadID != options.ThreadID || r.entryOrdinals[options.ThreadID][entry.ID] != ordinal {
			return JournalMetadataPage{}, ErrAuthorityCorrupt
		}
		metadata, err := canonicalPathMetadata(entry)
		if err != nil {
			return JournalMetadataPage{}, err
		}
		page.Entries = append(page.Entries, metadata)
	}
	return page, nil
}
