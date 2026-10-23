package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// TurnCancellationReader reads the latest canonical cancellation request for a
// turn without materializing conversation bodies. No request returns a zero entry.
type TurnCancellationReader interface {
	ReadTurnCancellation(context.Context, string, string) (Entry, error)
}

func (r *MemoryRepo) ReadTurnCancellation(ctx context.Context, threadID, turnID string) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[threadID]
	if !ok {
		return Entry{}, ErrThreadNotFound
	}
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return Entry{}, err
	}
	r.ensureTurnMarkersLocked(threadID)
	position := r.turnMarkers[threadID][turnID].cancellation
	if position == 0 {
		return Entry{}, nil
	}
	if position < 0 || position > len(r.entries[threadID]) {
		return Entry{}, ErrAuthorityCorrupt
	}
	entry, _, err := r.canonicalTurnEntryLocked(threadID, r.entries[threadID][position-1].ID)
	if err != nil || entry.Type != EntryCancelRequested || entry.TurnID != turnID {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return entry, nil
}

func (repo *BackendRepo) ReadTurnCancellation(ctx context.Context, threadID, turnID string) (Entry, error) {
	var result Entry
	err := repo.canonicalThreadView(ctx, threadID, func(tx spi.ReadTx, _ ThreadMeta) error {
		turn, err := readTurnEntryLocator(tx, threadID, turnID)
		if err != nil || turn.Cancellation == "" {
			return err
		}
		entry, err := readCanonicalEntryMetadata(tx, threadID, turn.Cancellation)
		if err != nil || entry.Type != EntryCancelRequested || entry.TurnID != turnID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		result, err = completeLocatedEntry(ctx, tx, entry)
		if err != nil {
			return err
		}
		return ValidateEntryIntegrity(result)
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadTurnCancellation(ctx, threadID, turnID)
		return err
	})
	return result, err
}
