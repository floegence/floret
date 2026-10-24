package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// WindowProjectionReader reads membership, dependencies and current context at
// one canonical leaf. A concurrent append cannot mix different read snapshots.
// The result contains canonical sources, never a durable presentation snapshot.
type WindowProjectionReader interface {
	ReadWindowProjection(context.Context, ItemMembershipWindowOptions, func(PathMetadata, bool) []string) (WindowProjection, error)
}

type WindowProjection struct {
	Membership   ItemMembershipWindow
	Facts        WindowSources
	Entries      []PathProjectionEntry
	Context      ThreadContextSnapshot
	ModelContext ModelContextSources
	Queue        []Entry
	ParentEvents *ParentEventPage
}

func (repo *BackendRepo) ReadWindowProjection(ctx context.Context, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string) (WindowProjection, error) {
	if ctx == nil || strings.TrimSpace(options.ThreadID) == "" || options.Limit < 1 || options.Limit > 256 || fields == nil {
		return WindowProjection{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareWindowProjection(ctx, options.ThreadID); err != nil {
		return WindowProjection{}, err
	}
	// Preparation may publish the deferred repository. Execute the complete
	// read on that owner so its options and transaction share one authority.
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ReadWindowProjection(ctx, options, fields)
	}
	if err := repo.prepareSparseSources(ctx, options.ThreadID); err != nil {
		return WindowProjection{}, err
	}
	var result WindowProjection
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		tx = newProjectionSourceTx(tx)
		var err error
		result.Membership, err = readItemMembershipWindow(ctx, tx, meta, options)
		if err != nil {
			return err
		}
		var turns, seeds []string
		for _, member := range result.Membership.Members {
			turns = append(turns, member.TurnID)
			seeds = append(seeds, member.SourceID)
		}
		result.Facts, err = readWindowSources(ctx, tx, meta, meta.LeafID, turns)
		if err != nil {
			return err
		}
		seeds = append(seeds, result.Facts.Sources...)
		if len(seeds) > 0 {
			node, err := readEntryLocator(tx, meta.ID, meta.LeafID)
			if err != nil {
				return err
			}
			sources, err := projectionSourceClosure(ctx, seeds, func(id string) (Entry, error) { return activeLocatedEntry(ctx, tx, meta, id) }, func(group projectionSourceGroup, visit func(Entry) error) error {
				return visitProjectionSources(ctx, tx, meta, node, group, visit)
			})
			if err != nil {
				return err
			}
			selectFields := projectionBodyFields(sources, fields)
			bodies := newCanonicalBodyPageReader(ctx, tx)
			page, err := materializeProjectionPage(PathMetadataWindow{LeafID: meta.LeafID, Entries: sources}, selectFields, bodies.prefetch, func(_ string, field CanonicalField) (string, error) {
				return bodies.read(canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes})
			})
			if err != nil {
				return err
			}
			result.Entries = page.Entries
		}
		projection, err := readStoredContextProjection(ctx, tx, meta)
		if err != nil {
			return err
		}
		result.Context, err = projection.Snapshot(time.Time{})
		if err != nil {
			return err
		}
		result.ModelContext, err = readModelContextSources(ctx, tx, meta)
		if err != nil {
			return err
		}
		if err := visitSparseSources(ctx, tx, meta.ID, "queue", func(entry Entry) error {
			metadata, err := canonicalPathMetadata(entry)
			if err == nil {
				result.Queue = append(result.Queue, metadata.Entry)
			}
			return err
		}); err != nil {
			return err
		}
		if repo.parentEventsEnabled() {
			records, err := canonicalParentEventRecords(ctx, tx, meta)
			if err != nil {
				return err
			}
			page, err := pageParentEvents(records, ParentEventQuery{ThreadID: meta.ID, Limit: 100})
			if err != nil {
				return err
			}
			result.ParentEvents = &page
		}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadWindowProjection(ctx, options, fields)
		return err
	})
	if err != nil {
		return WindowProjection{}, err
	}
	return result, nil
}

// Memory repositories freeze complete canonical entries under their existing
// lock. Persistent repositories alone need source indexes to bound disk reads.
func (repo *MemoryRepo) ReadWindowProjection(ctx context.Context, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string) (WindowProjection, error) {
	if ctx == nil || fields == nil || options.Limit < 1 || options.Limit > 256 {
		return WindowProjection{}, spi.ErrInvalidArgument
	}
	repo.mu.Lock()
	meta, found := repo.threads[options.ThreadID]
	if !found {
		repo.mu.Unlock()
		return WindowProjection{}, ErrThreadNotFound
	}
	if options.LeafID != "" && options.LeafID != meta.LeafID {
		repo.mu.Unlock()
		return WindowProjection{}, spi.ErrConflict
	}
	entries, err := repo.canonicalPathLocked(ctx, options.ThreadID, meta.LeafID)
	var queue []Entry
	var parents *ParentEventPage
	if err == nil {
		for _, entry := range repo.entries[options.ThreadID] {
			if sparseSourceCategory(entry) != "queue" {
				continue
			}
			metadata, readErr := canonicalPathMetadata(entry)
			if readErr != nil {
				err = readErr
				break
			}
			queue = append(queue, metadata.Entry)
		}
	}
	if err == nil && repo.parentEvents {
		records, readErr := repo.parentEventRecordsLocked(options.ThreadID)
		if readErr == nil {
			page, pageErr := pageParentEvents(records, ParentEventQuery{ThreadID: options.ThreadID, Limit: 100})
			parents, readErr = &page, pageErr
		}
		err = readErr
	}
	repo.mu.Unlock()
	if err != nil {
		return WindowProjection{}, err
	}
	var membership ItemMembership
	var projection ThreadContextProjection
	facts := WindowSources{LeafID: meta.LeafID, Anchors: map[string]string{}, CompactionOrdinals: map[string]uint64{}}
	sources := make([]PathProjectionEntry, 0, len(entries))
	byID := map[string]Entry{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return WindowProjection{}, err
		}
		if _, err := membership.Observe(entry, uint64(entry.PathDepth)); err != nil {
			return WindowProjection{}, err
		}
		if err := projection.Observe(entry); err != nil {
			return WindowProjection{}, err
		}
		if ThreadContextEntryKind(entry) == ThreadContextCompactionEntryKind {
			compact, err := DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				return WindowProjection{}, err
			}
			if _, exists := facts.Anchors[compact.OperationID]; !exists {
				anchor := ""
				for i := len(membership.Members) - 1; i >= 0; i-- {
					if !membership.Members[i].Hidden {
						anchor = membership.Members[i].ID
						break
					}
				}
				facts.Anchors[compact.OperationID], facts.CompactionOrdinals[compact.OperationID] = anchor, uint64(entry.PathDepth)
			}
		}
		facts.Sources = append(facts.Sources, entry.ID)
		sources = append(sources, PathProjectionEntry{Entry: entry})
		byID[entry.ID] = entry
	}
	members := []ItemMember{}
	anchor := -1
	for _, member := range membership.Members {
		if member.Hidden {
			continue
		}
		if member.ID == options.AnchorID {
			anchor = len(members)
		}
		members = append(members, member)
	}
	if options.AnchorID == "" {
		anchor = len(members)
	} else if anchor < 0 {
		return WindowProjection{}, ErrEntryNotFound
	}
	first, last, err := membershipWindowBounds(len(members), anchor, options)
	if err != nil {
		return WindowProjection{}, err
	}
	page := ItemMembershipWindow{LeafID: meta.LeafID, Members: members[first:last], Ordinals: []uint64{}, HasBefore: first > 0, HasAfter: last < len(members)}
	for i := first; i < last; i++ {
		page.Ordinals = append(page.Ordinals, uint64(i+1))
	}
	contextSnapshot, err := projection.Snapshot(time.Time{})
	if err != nil {
		return WindowProjection{}, err
	}
	refs, err := modelContextSourceRefs(entries)
	if err != nil {
		return WindowProjection{}, err
	}
	model := ModelContextSources{LeafID: meta.LeafID}
	for _, ref := range refs {
		entry := byID[ref.ID]
		snapshot, err := modelContextSourceSnapshot(entry, func(id string) (Entry, error) {
			source, found := byID[id]
			if !found {
				return Entry{}, ErrAuthorityCorrupt
			}
			return source, nil
		})
		if err != nil || snapshot == nil {
			return WindowProjection{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		model.Entries = append(model.Entries, Entry{ID: entry.ID, ThreadID: entry.ThreadID, TurnID: entry.TurnID, RunID: entry.RunID, CreatedAt: entry.CreatedAt, PathDepth: entry.PathDepth})
		model.Messages = append(model.Messages, session.Message{EntryID: entry.ID, ModelContextSnapshot: snapshot})
	}
	return WindowProjection{Membership: page, Facts: facts, Entries: sources, Context: contextSnapshot, ModelContext: model, Queue: queue, ParentEvents: parents}, nil
}
