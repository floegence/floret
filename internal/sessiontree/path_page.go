package sessiontree

import "context"

// pathIndexEntry is derived from immutable journal entries. A single skip link
// clears the lowest set depth bit, allowing ancestor lookup without walking or
// cloning the intervening history. No payload or alternate journal lives here.
type pathIndexEntry struct {
	id     string
	parent int
	skip   int
	depth  int64
	err    error
}

func appendPathIndex(index []pathIndexEntry, ordinals map[string]int, entry Entry) []pathIndexEntry {
	node := pathIndexEntry{id: entry.ID, parent: -1, skip: -1, depth: entry.PathDepth}
	node.err = ValidateEntryIntegrity(entry)
	if entry.ParentID != "" {
		parent, found := ordinals[entry.ParentID]
		if !found || parent < 0 || parent >= len(index) || index[parent].id != entry.ParentID {
			node.err = ErrInvalidParent
		} else {
			node.parent = parent
			if index[parent].err != nil {
				node.err = index[parent].err
			} else if node.depth != index[parent].depth+1 {
				node.err = ErrAuthorityCorrupt
			} else if node.err == nil {
				node.skip = pathAncestor(index, parent, node.depth&(node.depth-1))
			}
		}
	} else if node.depth != 1 {
		node.err = ErrAuthorityCorrupt
	}
	return append(index, node)
}

func pathAncestor(index []pathIndexEntry, ordinal int, depth int64) int {
	for ordinal >= 0 && index[ordinal].depth > depth {
		node := index[ordinal]
		if node.skip >= 0 && index[node.skip].depth >= depth {
			ordinal = node.skip
		} else {
			ordinal = node.parent
		}
	}
	return ordinal
}

func (r *MemoryRepo) rebuildPathIndexLocked(threadID string) {
	if r.pathIndexes == nil {
		r.pathIndexes = map[string][]pathIndexEntry{}
	}
	index := make([]pathIndexEntry, 0, len(r.entries[threadID]))
	for _, entry := range r.entries[threadID] {
		complete, err := r.completeEntryLocked(entry)
		if err == nil {
			entry = complete
		}
		index = appendPathIndex(index, r.entryOrdinals[threadID], entry)
		if err != nil {
			index[len(index)-1].err = err
		}
	}
	r.pathIndexes[threadID] = index
}

func (r *MemoryRepo) ensurePathIndexLocked(threadID string) {
	if len(r.pathIndexes[threadID]) != len(r.entries[threadID]) {
		r.rebuildPathIndexLocked(threadID)
	} else if r.pathIndexes == nil {
		r.pathIndexes = map[string][]pathIndexEntry{}
	}
}

func (r *MemoryRepo) pathPageLocked(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (PathPage, error) {
	if err := ctx.Err(); err != nil {
		return PathPage{}, err
	}
	meta, found := r.threads[threadID]
	if !found {
		return PathPage{}, ErrThreadNotFound
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if leafID == "" {
		if beforeEntryID != "" {
			return PathPage{}, ErrEntryNotFound
		}
		return PathPage{Entries: []Entry{}}, nil
	}
	r.ensurePathIndexLocked(threadID)
	index := r.pathIndexes[threadID]
	ordinal, found := r.entryOrdinals[threadID][leafID]
	if !found {
		return PathPage{}, ErrEntryNotFound
	}
	if ordinal < 0 || ordinal >= len(index) || index[ordinal].id != leafID {
		return PathPage{}, ErrAuthorityCorrupt
	}
	if err := index[ordinal].err; err != nil {
		return PathPage{}, err
	}
	if beforeEntryID != "" {
		before, found := r.entryOrdinals[threadID][beforeEntryID]
		if !found {
			return PathPage{}, ErrEntryNotFound
		}
		if before < 0 || before >= len(index) || index[before].id != beforeEntryID {
			return PathPage{}, ErrAuthorityCorrupt
		}
		if pathAncestor(index, ordinal, index[before].depth) != before {
			return PathPage{}, ErrEntryNotFound
		}
		ordinal = index[before].parent
	}
	page := PathPage{Entries: []Entry{}}
	if ordinal < 0 {
		return page, nil
	}
	page.NewestOrdinal = index[ordinal].depth
	capacity := limit
	if int64(capacity) > page.NewestOrdinal {
		capacity = int(page.NewestOrdinal)
	}
	page.Entries = make([]Entry, 0, capacity)
	for ordinal >= 0 && len(page.Entries) < limit {
		if err := ctx.Err(); err != nil {
			return PathPage{}, err
		}
		node := index[ordinal]
		entry, err := r.completeEntryLocked(r.entries[threadID][ordinal])
		if err != nil {
			return PathPage{}, err
		}
		if entry.ID != node.id || entry.ThreadID != threadID || entry.PathDepth != node.depth || (node.parent < 0 && entry.ParentID != "") || (node.parent >= 0 && entry.ParentID != index[node.parent].id) {
			return PathPage{}, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return PathPage{}, err
		}
		page.Entries = append(page.Entries, cloneEntry(entry))
		ordinal = node.parent
	}
	page.HasMore = ordinal >= 0
	if page.HasMore {
		page.NextEntryID = page.Entries[len(page.Entries)-1].ID
	}
	return page, nil
}
