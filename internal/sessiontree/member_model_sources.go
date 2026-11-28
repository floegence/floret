package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func refreshMemberModelSources(journal *member.Journal, leaf string, entry *Entry) error {
	if entry != nil && entry.Type != EntryCompaction && entry.Type != EntryMessageRevision {
		reselect := false
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			source, err := CanonicalTurnRetrySourceForStartedEntry(*entry)
			if err != nil {
				return err
			}
			reselect = source != nil
		}
		if !reselect && entry.Type != EntrySteeringApplied && entry.Message.ModelContextSnapshot == nil {
			return nil
		}
	}
	var path []Entry
	if leaf != "" {
		err := journal.VisitModelPath(leaf, func(event member.EntryEvent) error {
			value, err := materializeMemberEntry(journal, event)
			if err == nil {
				path = append(path, value)
			}
			return err
		})
		if err != nil {
			return err
		}
	}
	refs, err := modelContextSourceRefs(path)
	if err != nil {
		return err
	}
	var ids []string
	for _, ref := range refs {
		ids = append(ids, ref.ID)
	}
	return journal.SetModelSources(ids)
}

func readMemberModelSources(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) (ModelContextSources, error) {
	result := ModelContextSources{LeafID: meta.LeafID}
	ids, err := member.From(tx).ModelSources()
	if err != nil {
		return result, err
	}
	read := func(id string) (Entry, error) { return activeLocatedEntry(ctx, tx, meta, id) }
	for _, id := range ids {
		entry, err := read(id)
		if err != nil {
			return result, err
		}
		snapshot, err := modelContextSourceSnapshot(entry, read)
		if err != nil || snapshot == nil {
			return result, errors.Join(ErrAuthorityCorrupt, err)
		}
		result.Entries = append(result.Entries, Entry{ID: id, TurnID: entry.TurnID, RunID: entry.RunID, ThreadID: meta.ID, CreatedAt: entry.CreatedAt, PathDepth: entry.PathDepth})
		result.Messages = append(result.Messages, session.Message{EntryID: id, ModelContextSnapshot: snapshot})
	}
	return result, nil
}
