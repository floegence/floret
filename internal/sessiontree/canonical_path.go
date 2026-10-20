package sessiontree

import (
	"context"
	"slices"
)

// canonicalPathLocked follows the validated ancestry index. Explicit complete
// reads still materialize their requested path, but never map inactive siblings.
func (r *MemoryRepo) canonicalPathLocked(ctx context.Context, threadID, leafID string) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, ok := r.threads[threadID]
	if !ok {
		return nil, ErrThreadNotFound
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if leafID == "" {
		return nil, nil
	}
	// Validation-only repositories are constructed from canonical records without
	// derived indexes. Build the same index once, never substitute for a corrupt
	// existing locator map.
	if r.entryOrdinals[threadID] == nil {
		if r.entryOrdinals == nil {
			r.entryOrdinals = map[string]map[string]int{}
		}
		r.entryOrdinals[threadID] = map[string]int{}
		for ordinal, entry := range r.entries[threadID] {
			if entry.ID == "" {
				return nil, ErrAuthorityCorrupt
			}
			if _, duplicate := r.entryOrdinals[threadID][entry.ID]; duplicate {
				return nil, ErrAuthorityCorrupt
			}
			r.entryOrdinals[threadID][entry.ID] = ordinal
		}
	}
	r.ensurePathIndexLocked(threadID)
	index := r.pathIndexes[threadID]
	ordinal, found := r.entryOrdinals[threadID][leafID]
	if !found {
		return nil, ErrEntryNotFound
	}
	if ordinal < 0 || ordinal >= len(index) || index[ordinal].id != leafID {
		return nil, ErrAuthorityCorrupt
	}
	if err := index[ordinal].err; err != nil {
		return nil, err
	}
	result := make([]Entry, 0, min(index[ordinal].depth, 256))
	for ordinal >= 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node := index[ordinal]
		entry, err := r.completeEntryLocked(r.entries[threadID][ordinal])
		if err != nil {
			return nil, err
		}
		if entry.ID != node.id || entry.ThreadID != threadID || entry.PathDepth != node.depth || (node.parent < 0 && entry.ParentID != "") || (node.parent >= 0 && entry.ParentID != index[node.parent].id) {
			return nil, ErrAuthorityCorrupt
		}
		if err := ValidateEntryIntegrity(entry); err != nil {
			return nil, err
		}
		result = append(result, cloneEntry(entry))
		ordinal = node.parent
	}
	slices.Reverse(result)
	return result, nil
}
