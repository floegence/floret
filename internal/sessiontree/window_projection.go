package sessiontree

import (
	"context"
	"errors"
	"slices"
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
	Meta              ThreadMeta
	Summary           *WindowSummary
	Companions        []WindowProjection
	ActivitySummaries map[string]ItemActivitySummary
	Membership        ItemMembershipWindow
	Facts             WindowSources
	Entries           []PathProjectionEntry
	Context           ThreadContextSnapshot
	ModelContext      ModelContextSources
	Queue             []Entry
	QueueSources      *QueueSourceSnapshot
	ParentEvents      *ParentEventPage
}

func (repo *BackendRepo) ReadWindowProjection(ctx context.Context, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string) (WindowProjection, error) {
	for {
		result, err := repo.readWindowProjection(ctx, options, fields)
		var summaries *activitySummaryWindowPreparationRequired
		if errors.As(err, &summaries) {
			for _, source := range summaries.sources {
				if err := repo.prepareItemActivitySummary(ctx, options.ThreadID, source); err != nil {
					return WindowProjection{}, err
				}
			}
			continue
		}
		var summary *activitySummaryPreparationRequired
		if errors.As(err, &summary) {
			if err := repo.prepareItemActivitySummary(ctx, options.ThreadID, summary.source); err != nil {
				return WindowProjection{}, err
			}
			continue
		}
		var missing missingWindowTextFacts
		if !errors.As(err, &missing) {
			return result, err
		}
		if err := repo.prepareWindowTextFacts(ctx, options.ThreadID, missing); err != nil {
			return WindowProjection{}, err
		}
	}
}

func (repo *BackendRepo) readWindowProjection(ctx context.Context, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string) (WindowProjection, error) {
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
	if err := repo.prepareCurrentQueueSources(ctx, options.ThreadID); err != nil {
		return WindowProjection{}, err
	}
	var result WindowProjection
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = readWindowProjection(ctx, tx, meta, options, fields, repo.parentEventsEnabled())
		if err != nil {
			return err
		}
		for _, companion := range options.Companions {
			if companion.ThreadID != options.ThreadID || len(companion.Companions) != 0 || companion.Limit < 1 || companion.Limit > 256 {
				return spi.ErrInvalidArgument
			}
			value, err := readWindowProjection(ctx, tx, meta, companion, fields, repo.parentEventsEnabled())
			if err != nil {
				return err
			}
			result.Companions = append(result.Companions, value)
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

func readWindowProjection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, options ItemMembershipWindowOptions, fields func(PathMetadata, bool) []string, parentEvents bool) (WindowProjection, error) {
	result := WindowProjection{Meta: meta}
	tx = newProjectionSourceTx(tx)
	var err error
	result.Membership, err = readItemMembershipWindow(ctx, tx, meta, options)
	if errors.Is(err, ErrEntryNotFound) && options.MissingAnchorTail && options.AnchorID != "" {
		fallback := options
		fallback.AnchorID, fallback.AnchorOffset, fallback.Direction = "", 0, PathWindowBefore
		result.Membership, err = readItemMembershipWindow(ctx, tx, meta, fallback)
		result.Membership.AnchorUnavailable = true
	}
	if err != nil {
		return WindowProjection{}, err
	}
	if options.ActivityFields {
		// Read no summary bodies until the complete requested membership is
		// known. Resolve its misses together, rather than rereading all prior
		// summaries once for every cold tool in this window.
		var pending []string
		for _, member := range result.Membership.Members {
			if member.ActivityPending {
				pending = append(pending, member.SourceID)
			}
		}
		if len(pending) > 0 {
			return WindowProjection{}, &activitySummaryWindowPreparationRequired{sources: pending}
		}
		result.ActivitySummaries = map[string]ItemActivitySummary{}
		for _, member := range result.Membership.Members {
			if member.Kind != "tool" {
				continue
			}
			source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
			if err != nil {
				return WindowProjection{}, err
			}
			if source.Type != EntryToolCall && source.Type != EntryToolResult || source.Message.ToolName == "goal" {
				continue
			}
			summary, err := readItemActivitySummary(ctx, tx, meta, member)
			if err != nil {
				return WindowProjection{}, err
			}
			result.ActivitySummaries[member.ID] = summary
		}
	}
	var turns, seeds []string
	for _, member := range result.Membership.Members {
		turns = append(turns, member.TurnID)
		seeds = append(seeds, member.SourceID)
	}
	result.Facts, err = readWindowSources(ctx, tx, meta, meta.LeafID, turns)
	if err != nil {
		return WindowProjection{}, err
	}
	if options.Summary {
		result.Summary, err = readWindowSummary(ctx, tx, meta, result.Facts.SummarySource)
		if err != nil {
			return WindowProjection{}, err
		}
	}
	seeds = append(seeds, result.Facts.Sources...)
	if len(seeds) > 0 {
		node, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return WindowProjection{}, err
		}
		sources, err := projectionSourceClosureMode(ctx, seeds, func(id string) (Entry, error) { return activeLocatedEntry(ctx, tx, meta, id) }, func(group projectionSourceGroup, visit func(Entry) error) error {
			if options.ActivityFields {
				handled, err := visitWindowToolSources(ctx, tx, meta, node, group, visit)
				if handled || err != nil {
					return err
				}
			}
			return visitProjectionSources(ctx, tx, meta, node, group, visit)
		}, false, func(entry Entry, visit func(Entry) error) error {
			return goalProjectionDependencies(ctx, tx, meta, node, entry, visit)
		})
		if err != nil {
			return WindowProjection{}, err
		}
		selectedFields := projectionBodyFields(sources, fields)
		textFacts := map[string]map[string]CanonicalTextFacts{}
		var missing missingWindowTextFacts
		if options.ActivityFields {
			for _, source := range sources {
				for _, field := range selectedFields(source) {
					if !windowTextField(source.Entry, field) {
						continue
					}
					for _, body := range source.Bodies {
						if body.Field != field {
							continue
						}
						ref := canonicalBodyRef{SHA256: body.SHA256, Bytes: body.Bytes}
						facts, err := readBodyTextFacts(tx, ref)
						if errors.Is(err, spi.ErrNotFound) {
							missing = append(missing, missingBodyTextFacts{source.Entry.ID, field, ref})
							continue
						}
						if err != nil {
							return WindowProjection{}, err
						}
						facts, err = readBodyTextPreview(ctx, tx, ref, facts)
						if err != nil {
							return WindowProjection{}, err
						}
						if textFacts[source.Entry.ID] == nil {
							textFacts[source.Entry.ID] = map[string]CanonicalTextFacts{}
						}
						textFacts[source.Entry.ID][field] = facts
					}
				}
			}
		}
		if len(missing) > 0 {
			return WindowProjection{}, missing
		}
		selectFields := func(source PathMetadata) []string {
			selected := selectedFields(source)
			if len(textFacts[source.Entry.ID]) == 0 {
				return selected
			}
			filtered := make([]string, 0, len(selected))
			for _, field := range selected {
				if _, omitted := textFacts[source.Entry.ID][field]; !omitted {
					filtered = append(filtered, field)
				}
			}
			return filtered
		}
		bodies := newCanonicalBodyPageReader(ctx, tx)
		page, err := materializeProjectionPage(PathMetadataWindow{LeafID: meta.LeafID, Entries: sources}, selectFields, bodies.prefetch, func(_ string, field CanonicalField) (string, error) {
			return bodies.read(canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes})
		})
		if err != nil {
			return WindowProjection{}, err
		}
		for i := range page.Entries {
			page.Entries[i].Entry.ProjectedText = textFacts[page.Entries[i].Entry.ID]
		}
		result.Entries = page.Entries
		if err := attachGoalProjectionBefore(ctx, tx, meta.ID, result.Entries); err != nil {
			return WindowProjection{}, err
		}
	}
	projection, err := readStoredContextProjection(ctx, tx, meta)
	if err != nil {
		return WindowProjection{}, err
	}
	result.Context, err = projection.Snapshot(time.Time{})
	if err != nil {
		return WindowProjection{}, err
	}
	result.ModelContext, err = readModelContextSources(ctx, tx, meta)
	if err != nil {
		return WindowProjection{}, err
	}
	queue, err := readCurrentQueueSources(ctx, tx, meta.ID)
	if err != nil {
		return WindowProjection{}, err
	}
	result.QueueSources = &queue

	if parentEvents {
		records, err := canonicalParentEventRecords(ctx, tx, meta)
		if err != nil {
			return WindowProjection{}, err
		}
		page, err := pageParentEvents(records, ParentEventQuery{ThreadID: meta.ID, Limit: 100})
		if err != nil {
			return WindowProjection{}, err
		}
		result.ParentEvents = &page
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
	if options.Direction == PathWindowAround {
		page.ResolvedAnchorID = members[anchor+options.AnchorOffset].ID
	}
	for i := first; i < last; i++ {
		page.Ordinals = append(page.Ordinals, uint64(i+1))
	}
	if options.ItemIDs != nil {
		page, err = selectMemoryMembershipItems(meta.LeafID, members, options.ItemIDs, options.OmitMissing)
		if err != nil {
			return WindowProjection{}, err
		}
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
	result := WindowProjection{Meta: meta, Membership: page, Facts: facts, Entries: sources, Context: contextSnapshot, ModelContext: model, Queue: queue, ParentEvents: parents}
	if options.Summary || slices.ContainsFunc(options.Companions, func(value ItemMembershipWindowOptions) bool { return value.Summary }) {
		result.Summary = &WindowSummary{}
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if summarySourceCandidate(entry) && strings.TrimSpace(entry.Message.Content) != "" {
				text := []rune(strings.TrimSpace(entry.Message.Content))
				result.Summary = &WindowSummary{Text: string(text[:min(160, len(text))]), CreatedAt: entry.CreatedAt}
				break
			}
		}
	}
	for _, companion := range options.Companions {
		if companion.ThreadID != options.ThreadID || len(companion.Companions) != 0 || companion.Limit < 1 || companion.Limit > 256 {
			return WindowProjection{}, spi.ErrInvalidArgument
		}
		value := result
		if !companion.Summary {
			value.Summary = nil
		}
		value.Companions = nil
		value.Membership, err = memoryProjectionMembership(meta.LeafID, members, companion)
		if err != nil {
			return WindowProjection{}, err
		}
		result.Companions = append(result.Companions, value)
	}
	if !options.Summary {
		result.Summary = nil
	}
	return result, nil
}

func memoryProjectionMembership(leaf string, members []ItemMember, options ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	if err := validateMembershipSeek(options); err != nil {
		return ItemMembershipWindow{}, err
	}
	if options.ItemIDs != nil {
		return selectMemoryMembershipItems(leaf, members, options.ItemIDs, options.OmitMissing)
	}
	anchor, missing := len(members), false
	if options.AnchorID != "" {
		anchor = -1
		for i, member := range members {
			if member.ID == options.AnchorID {
				anchor = i
				break
			}
		}
		if anchor < 0 {
			if !options.MissingAnchorTail {
				return ItemMembershipWindow{}, ErrEntryNotFound
			}
			anchor, missing = len(members), true
			options.AnchorID, options.AnchorOffset, options.Direction = "", 0, PathWindowBefore
		}
	}
	first, last, err := membershipWindowBounds(len(members), anchor, options)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	page := ItemMembershipWindow{LeafID: leaf, Members: members[first:last], HasBefore: first > 0, HasAfter: last < len(members), AnchorUnavailable: missing}
	if options.Direction == PathWindowAround {
		page.ResolvedAnchorID = members[anchor+options.AnchorOffset].ID
	}
	for i := first; i < last; i++ {
		page.Ordinals = append(page.Ordinals, uint64(i+1))
	}
	return page, nil
}
