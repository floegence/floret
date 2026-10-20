package sessiontree

import (
	"context"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

type PathWindowDirection string

const (
	PathWindowBefore PathWindowDirection = "before"
	PathWindowAfter  PathWindowDirection = "after"
	PathWindowAround PathWindowDirection = "around"
)

// PathWindowOptions fixes a canonical leaf and selects a bounded interval around
// a stable entry identity. A leaf is a snapshot boundary, never an access grant.
type PathWindowOptions struct {
	ThreadID, LeafID, AnchorID string
	Direction                  PathWindowDirection
	Limit                      int
}

// PathMetadataWindow is chronological and contains no extracted body fields.
// Append-only growth preserves older boundaries. Consumers must check the
// current active path when accepting a prior leaf or source locator.
type PathMetadataWindow struct {
	LeafID                                    string
	Entries                                   []PathMetadata
	HasBefore, HasAfter                       bool
	FirstEntryID, LastEntryID                 string
	FirstOrdinal, LastOrdinal, ThroughOrdinal int64
}

type PathWindowReader interface {
	PathMetadataWindow(context.Context, PathWindowOptions) (PathMetadataWindow, error)
}

func (r *MemoryRepo) PathMetadataWindow(ctx context.Context, options PathWindowOptions) (PathMetadataWindow, error) {
	if ctx == nil || options.Limit < 1 || options.Limit > 256 {
		return PathMetadataWindow{}, spi.ErrInvalidArgument
	}
	if options.Direction != PathWindowBefore && options.Direction != PathWindowAfter && options.Direction != PathWindowAround {
		return PathMetadataWindow{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return PathMetadataWindow{}, err
	}
	meta, found := r.threads[options.ThreadID]
	if !found {
		return PathMetadataWindow{}, ErrThreadNotFound
	}
	leafID := options.LeafID
	if leafID == "" {
		leafID = meta.LeafID
	}
	result := PathMetadataWindow{LeafID: leafID, Entries: []PathMetadata{}}
	if leafID == "" {
		if options.AnchorID != "" || options.Direction != PathWindowBefore {
			return PathMetadataWindow{}, ErrEntryNotFound
		}
		return result, nil
	}
	r.ensurePathIndexLocked(options.ThreadID)
	index := r.pathIndexes[options.ThreadID]
	leaf, err := r.pathWindowOrdinalLocked(options.ThreadID, leafID)
	if err != nil {
		return PathMetadataWindow{}, err
	}
	if index[leaf].err != nil {
		return PathMetadataWindow{}, index[leaf].err
	}
	result.ThroughOrdinal = index[leaf].depth
	// A requested frozen leaf must remain on the active path. This invalidates a
	// branch change without expiring unrelated completed history on normal append.
	active, err := r.pathWindowOrdinalLocked(options.ThreadID, meta.LeafID)
	if err != nil {
		return PathMetadataWindow{}, err
	}
	if index[active].err != nil {
		return PathMetadataWindow{}, index[active].err
	}
	if pathAncestor(index, active, index[leaf].depth) != leaf {
		return PathMetadataWindow{}, ErrEntryNotFound
	}
	var anchor int64
	if options.AnchorID != "" {
		ordinal, err := r.pathWindowOrdinalLocked(options.ThreadID, options.AnchorID)
		if err != nil {
			return PathMetadataWindow{}, err
		}
		anchor = index[ordinal].depth
		if pathAncestor(index, leaf, anchor) != ordinal {
			return PathMetadataWindow{}, ErrEntryNotFound
		}
	} else if options.Direction != PathWindowBefore {
		return PathMetadataWindow{}, spi.ErrInvalidArgument
	}
	limit := int64(options.Limit)
	var first, last int64
	switch options.Direction {
	case PathWindowBefore:
		last = result.ThroughOrdinal
		if anchor > 0 {
			last = anchor - 1
		}
		first = max(int64(1), last-limit+1)
	case PathWindowAfter:
		first = anchor + 1
		last = min(result.ThroughOrdinal, first+limit-1)
	case PathWindowAround:
		first = max(int64(1), anchor-limit/2)
		last = min(result.ThroughOrdinal, first+limit-1)
		first = max(int64(1), last-limit+1)
	}
	result.HasBefore = first > 1
	result.HasAfter = last < result.ThroughOrdinal
	if first > last {
		return result, nil
	}
	ordinal := pathAncestor(index, leaf, last)
	result.Entries = make([]PathMetadata, 0, last-first+1)
	for depth := last; depth >= first; depth-- {
		if err := ctx.Err(); err != nil {
			return PathMetadataWindow{}, err
		}
		node := index[ordinal]
		entry := r.entries[options.ThreadID][ordinal]
		if node.depth != depth || entry.ID != node.id || entry.ThreadID != options.ThreadID || entry.PathDepth != depth || node.parent < 0 && entry.ParentID != "" || node.parent >= 0 && entry.ParentID != index[node.parent].id {
			return PathMetadataWindow{}, ErrAuthorityCorrupt
		}
		metadata, err := canonicalPathMetadata(entry)
		if err != nil {
			return PathMetadataWindow{}, err
		}
		result.Entries = append(result.Entries, metadata)
		ordinal = node.parent
	}
	slices.Reverse(result.Entries)
	result.FirstOrdinal, result.LastOrdinal = first, last
	result.FirstEntryID = result.Entries[0].Entry.ID
	result.LastEntryID = result.Entries[len(result.Entries)-1].Entry.ID
	return result, nil
}

func (r *MemoryRepo) pathWindowOrdinalLocked(threadID, entryID string) (int, error) {
	ordinal, found := r.entryOrdinals[threadID][entryID]
	if !found {
		return 0, ErrEntryNotFound
	}
	index := r.pathIndexes[threadID]
	if ordinal < 0 || ordinal >= len(index) || index[ordinal].id != entryID {
		return 0, ErrAuthorityCorrupt
	}
	return ordinal, nil
}
