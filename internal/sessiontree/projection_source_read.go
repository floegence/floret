package sessiontree

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// A result's kind alone cannot identify a control. Resolve that fact from its
// exact call in the validated closure before selecting body fields. Ordinary
// tool results need no body read just to prove they are not control results.
func projectionBodyFields(sources []PathMetadata, fields func(PathMetadata, bool) []string) func(PathMetadata) []string {
	controls := map[controlCallIdentity]bool{}
	for _, source := range sources {
		entry := source.Entry
		if signal := entry.Message.ControlSignal; signal != nil {
			controls[controlCallIdentity{entry.ThreadID, entry.TurnID, entry.RunID, signal.CallID}] = true
		}
	}
	return func(source PathMetadata) []string {
		entry := source.Entry
		return fields(source, controls[controlCallIdentity{entry.ThreadID, entry.TurnID, entry.RunID, entry.Message.ToolCallID}])
	}
}

// ProjectionSources returns an atomic, body-free dependency closure. It does
// not truncate dependencies to an arbitrary raw-entry budget. Presentation
// limits apply later to selected items, never to their required source facts.
func (repo *BackendRepo) ProjectionSources(ctx context.Context, thread, leaf string, seeds []string) (ProjectionSourceSet, error) {
	var result ProjectionSourceSet
	err := repo.withProjectionSources(ctx, thread, leaf, seeds, func(_ spi.ReadTx, set ProjectionSourceSet) error { result = set; return nil }, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ProjectionSources(ctx, thread, leaf, seeds)
		return err
	})
	return result, err
}

func (repo *BackendRepo) withProjectionSources(ctx context.Context, thread, leaf string, seeds []string, visit func(spi.ReadTx, ProjectionSourceSet) error, memoryRead func(*MemoryRepo) error) error {
	if repo == nil || ctx == nil || strings.TrimSpace(thread) == "" || len(seeds) == 0 || len(seeds) > 256 {
		return spi.ErrInvalidArgument
	}
	if err := repo.prepareProjectionSources(ctx, thread); err != nil {
		return err
	}
	return repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		tx = newProjectionSourceTx(tx)
		selected := leaf
		if selected == "" {
			selected = meta.LeafID
		}
		if _, err := activeLocatedEntry(ctx, tx, meta, selected); err != nil {
			return err
		}
		node, err := readEntryLocator(tx, thread, selected)
		if err != nil {
			return err
		}
		frozen := meta
		frozen.LeafID = selected
		sources, err := projectionSourceClosure(ctx, seeds, func(id string) (Entry, error) { return activeLocatedEntry(ctx, tx, frozen, id) }, func(group projectionSourceGroup, visit func(Entry) error) error {
			return visitProjectionSources(ctx, tx, meta, node, group, visit)
		})
		if err != nil {
			return err
		}
		return visit(tx, ProjectionSourceSet{LeafID: selected, Sources: sources})
	}, memoryRead)
}

func (repo *BackendRepo) ReadProjectionSources(ctx context.Context, thread, leaf string, seeds []string, fields func(PathMetadata, bool) []string) (PathProjectionPage, error) {
	if fields == nil {
		return PathProjectionPage{}, spi.ErrInvalidArgument
	}
	var result PathProjectionPage
	err := repo.withProjectionSources(ctx, thread, leaf, seeds, func(tx spi.ReadTx, set ProjectionSourceSet) error {
		bodies := newCanonicalBodyPageReader(ctx, tx)
		var err error
		result, err = materializeProjectionPage(PathMetadataWindow{LeafID: set.LeafID, Entries: set.Sources}, projectionBodyFields(set.Sources, fields), bodies.prefetch, func(_ string, field CanonicalField) (string, error) {
			return bodies.read(canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes})
		})
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadProjectionSources(ctx, thread, leaf, seeds, fields)
		return err
	})
	if err != nil {
		return PathProjectionPage{}, err
	}
	return result, nil
}

func (repo *MemoryRepo) ReadProjectionSources(ctx context.Context, thread, leaf string, seeds []string, fields func(PathMetadata, bool) []string) (PathProjectionPage, error) {
	if fields == nil {
		return PathProjectionPage{}, spi.ErrInvalidArgument
	}
	set, err := repo.ProjectionSources(ctx, thread, leaf, seeds)
	if err != nil {
		return PathProjectionPage{}, err
	}
	return repo.materializeProjectionPage(ctx, thread, PathMetadataWindow{LeafID: set.LeafID, Entries: set.Sources}, projectionBodyFields(set.Sources, fields))
}

// Memory stores use the same closure semantics. Only persistent stores need
// durable membership to avoid cold-process traversal.
func (repo *MemoryRepo) ProjectionSources(ctx context.Context, thread, leaf string, seeds []string) (ProjectionSourceSet, error) {
	if ctx == nil || strings.TrimSpace(thread) == "" || len(seeds) == 0 || len(seeds) > 256 {
		return ProjectionSourceSet{}, spi.ErrInvalidArgument
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ProjectionSourceSet{}, err
	}
	meta, found := repo.threads[thread]
	if !found {
		return ProjectionSourceSet{}, ErrThreadNotFound
	}
	active, err := repo.canonicalMetadataPathLocked(thread, meta.LeafID)
	if err != nil {
		return ProjectionSourceSet{}, err
	}
	if leaf == "" {
		leaf = meta.LeafID
	}
	stop := slices.IndexFunc(active, func(e Entry) bool { return e.ID == leaf })
	if stop < 0 {
		return ProjectionSourceSet{}, ErrEntryNotFound
	}
	path := active[:stop+1]
	byID := make(map[string]Entry, len(path))
	groups := make(map[projectionSourceGroup][]Entry)
	for _, entry := range path {
		byID[entry.ID] = entry
		memberships, err := projectionSourceGroups(entry)
		if err != nil {
			return ProjectionSourceSet{}, err
		}
		for _, group := range memberships {
			groups[group] = append(groups[group], entry)
		}
	}
	sources, err := projectionSourceClosure(ctx, seeds, func(id string) (Entry, error) {
		entry, ok := byID[id]
		if !ok {
			return Entry{}, ErrEntryNotFound
		}
		return entry, nil
	}, func(group projectionSourceGroup, visit func(Entry) error) error {
		for _, entry := range groups[group] {
			if err := visit(entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return ProjectionSourceSet{}, err
	}
	return ProjectionSourceSet{LeafID: leaf, Sources: sources}, nil
}

func projectionSourceClosure(ctx context.Context, seeds []string, read func(string) (Entry, error), groupSources func(projectionSourceGroup, func(Entry) error) error) ([]PathMetadata, error) {
	return projectionSourceClosureMode(ctx, seeds, read, groupSources, false)
}

func projectionSourceClosureMode(ctx context.Context, seeds []string, read func(string) (Entry, error), groupSources func(projectionSourceGroup, func(Entry) error) error, membershipOnly bool) ([]PathMetadata, error) {
	entries := make(map[string]Entry)
	var pending []Entry
	add := func(entry Entry) error {
		if existing, found := entries[entry.ID]; found {
			if existing.RawHash != entry.RawHash || existing.PathDepth != entry.PathDepth || existing.ParentID != entry.ParentID {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		entries[entry.ID] = entry
		pending = append(pending, entry)
		return nil
	}
	readRequired := func(id string) error {
		if _, found := entries[id]; found {
			return nil
		}
		entry, err := read(id)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return add(entry)
	}
	for _, id := range seeds {
		if id == "" {
			return nil, spi.ErrInvalidArgument
		}
		entry, err := read(id)
		if err != nil {
			return nil, err
		}
		if err := add(entry); err != nil {
			return nil, err
		}
	}
	visited := make(map[projectionSourceGroup]map[string]bool)
	for next := 0; next < len(pending); next++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := pending[next]
		groups, err := projectionSourceGroups(entry)
		if err != nil {
			return nil, err
		}
		// Boundaries are dependencies, not every source in an arbitrarily long turn.
		if entry.TurnID != "" && !membershipOnly {
			groups = append(groups, projectionSourceGroup{"turn_boundary", entry.TurnID, "", entry.TurnID})
		}
		if membershipOnly {
			groups = slices.DeleteFunc(groups, func(group projectionSourceGroup) bool {
				switch group.Kind {
				case "output", "tool", "interaction", "input_batch", "hosted":
					return false
				}
				return true
			})
		}
		ownGroups, err := projectionSourceGroups(entry)
		if err != nil {
			return nil, err
		}
		for _, group := range groups {
			members, done := visited[group]
			if !done {
				members = make(map[string]bool)
				if err := groupSources(group, func(source Entry) error { members[source.ID] = true; return add(source) }); err != nil {
					return nil, err
				}
				visited[group] = members
			}
			// A complete index cannot silently lose the seed's entire group.
			if slices.Contains(ownGroups, group) && !members[entry.ID] {
				return nil, ErrAuthorityCorrupt
			}
		}
		if entry.Type == EntryTurnMarker && !membershipOnly {
			refs, _, err := FinalResponseEntryIDs(entry)
			if err != nil {
				return nil, err
			}
			for _, id := range refs {
				if err := readRequired(id); err != nil {
					return nil, err
				}
			}
			if entry.TurnStatus == TurnStarted {
				source, err := CanonicalTurnRetrySourceForStartedEntry(entry)
				if err != nil {
					return nil, err
				}
				if source != nil {
					if err := readRequired(source.EntryID); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	sources := make([]PathMetadata, 0, len(entries))
	for _, entry := range entries {
		source, err := canonicalPathMetadata(entry)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	slices.SortFunc(sources, func(a, b PathMetadata) int {
		if a.Entry.PathDepth < b.Entry.PathDepth {
			return -1
		}
		if a.Entry.PathDepth > b.Entry.PathDepth {
			return 1
		}
		return strings.Compare(a.Entry.ID, b.Entry.ID)
	})
	return sources, nil
}
