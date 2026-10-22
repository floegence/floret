package sessiontree

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// One projector serves both stores. Its source supplies exact entries and
// per-turn records, so paging never requires a second in-memory repository.
type canonicalTurnReader struct {
	entry    func(string, string) (Entry, int64, error)
	turn     func(string, string) ([]Entry, error)
	ancestor func(string, string, string) (bool, error)
	links    func(string, []Entry) error
}

func (r *MemoryRepo) canonicalTurnReaderLocked() canonicalTurnReader {
	return canonicalTurnReader{
		entry: r.canonicalTurnEntryLocked, ancestor: r.retrySourceIsAncestorLocked, links: r.validateCanonicalTurnLinksLocked,
		turn: func(threadID, turnID string) ([]Entry, error) {
			ordinals := r.turnEntryOrdinals[threadID][turnID]
			if r.turnEntryCounts[threadID][turnID] != len(ordinals) {
				return nil, ErrAuthorityCorrupt
			}
			entries := make([]Entry, 0, len(ordinals))
			for _, ordinal := range ordinals {
				if ordinal < 0 || ordinal >= len(r.entries[threadID]) {
					return nil, ErrAuthorityCorrupt
				}
				entry, _, err := r.canonicalTurnEntryLocked(threadID, r.entries[threadID][ordinal].ID)
				if err != nil || strings.TrimSpace(entry.TurnID) != turnID {
					return nil, errors.Join(ErrAuthorityCorrupt, err)
				}
				entries = append(entries, entry)
			}
			return entries, nil
		},
	}
}

func newCanonicalTurnReader(ctx context.Context, tx spi.ReadTx) canonicalTurnReader {
	reader := canonicalTurnReader{}
	reader.entry = func(threadID, entryID string) (Entry, int64, error) {
		if err := ctx.Err(); err != nil {
			return Entry{}, 0, err
		}
		entry, err := readCanonicalEntryMetadata(tx, threadID, entryID)
		if err != nil {
			return Entry{}, 0, err
		}
		entry, err = completeLocatedEntry(ctx, tx, entry)
		if err != nil {
			return Entry{}, 0, err
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return Entry{}, 0, err
		}
		return entry, entry.PathDepth, nil
	}
	reader.turn = func(threadID, turnID string) ([]Entry, error) {
		var entries []Entry
		err := visitCanonicalTurnEntries(ctx, tx, threadID, turnID, func(metadata Entry) error {
			entry, _, err := reader.entry(threadID, metadata.ID)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
			return nil
		})
		return entries, err
	}
	reader.ancestor = func(threadID, descendantID, sourceID string) (bool, error) {
		descendant, err := readEntryLocator(tx, threadID, descendantID)
		if err != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		source, err := readEntryLocator(tx, threadID, sourceID)
		if err != nil {
			return false, errors.Join(ErrAuthorityCorrupt, err)
		}
		if source.Depth > descendant.Depth {
			return false, nil
		}
		ancestor, err := entryAncestor(ctx, tx, threadID, descendant, source.Depth)
		return ancestor.ID == source.ID, err
	}
	reader.links = func(threadID string, entries []Entry) error {
		return validateCanonicalTurnLinks(ctx, tx, threadID, entries)
	}
	return reader
}

func (repo *BackendRepo) ReadCanonicalTurn(ctx context.Context, threadID, turnID string) (CanonicalTurnRead, error) {
	threadID, turnID = strings.TrimSpace(threadID), strings.TrimSpace(turnID)
	if threadID == "" || turnID == "" {
		return CanonicalTurnRead{}, ErrCanonicalTurnNotFound
	}
	var result CanonicalTurnRead
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = newCanonicalTurnReader(ctx, tx).read(threadID, turnID, meta.LeafID)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadCanonicalTurn(ctx, threadID, turnID)
		return err
	})
	return result, err
}

func (repo *BackendRepo) ListCanonicalTurns(ctx context.Context, opts ListCanonicalTurnsOptions) (CanonicalTurnsPage, error) {
	if err := ValidateListCanonicalTurnsOptions(opts); err != nil {
		return CanonicalTurnsPage{}, err
	}
	opts.ThreadID = strings.TrimSpace(opts.ThreadID)
	var result CanonicalTurnsPage
	err := repo.canonicalThreadView(ctx, opts.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = newCanonicalTurnReader(ctx, tx).list(meta, opts)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ListCanonicalTurns(ctx, opts)
		return err
	})
	return result, err
}
