package sessiontree

// Source locators retain the first occurrence on each independent branch.
// Repeated segments on the same path share that locator. Canonical entries own
// the content and identity; this rebuildable index contains ordinals only.
func (r *MemoryRepo) ensureOutputSourcesLocked(threadID string) {
	if r.outputSources == nil {
		r.outputSources = map[string]map[string][]int{}
	}
	if r.outputSources[threadID] == nil {
		r.rebuildOutputSourcesLocked(threadID)
	}
}

func (r *MemoryRepo) rebuildOutputSourcesLocked(threadID string) {
	if r.outputSources == nil {
		r.outputSources = map[string]map[string][]int{}
	}
	r.ensurePathIndexLocked(threadID)
	r.outputSources[threadID] = map[string][]int{}
	for ordinal, entry := range r.entries[threadID] {
		r.observeOutputSourcesLocked(threadID, ordinal, entry)
	}
}

func (r *MemoryRepo) outputSourceAncestorLocked(threadID, parentID string, ordinal int) bool {
	parent, found := r.entryOrdinals[threadID][parentID]
	if !found {
		return false
	}
	index := r.pathIndexes[threadID]
	return ordinal >= 0 && ordinal < len(index) && parent >= 0 && parent < len(index) && pathAncestor(index, parent, index[ordinal].depth) == ordinal
}

func (r *MemoryRepo) observeOutputSourcesLocked(threadID string, ordinal int, entry Entry) {
	for _, source := range []string{entry.Message.ContentSourceID, entry.Message.ReasoningSourceID} {
		if source == "" {
			continue
		}
		locators := r.outputSources[threadID][source]
		covered := false
		for _, previous := range locators {
			if r.outputSourceAncestorLocked(threadID, entry.ParentID, previous) {
				covered = true
				break
			}
		}
		if !covered {
			r.outputSources[threadID][source] = append(locators, ordinal)
		}
	}
}
