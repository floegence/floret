package sessiontree

import (
	"errors"
	"fmt"
)

// removeEffectAuthorityEntries removes execution authority from a copied or
// migrated journal while preserving the visible conversation path.
func removeEffectAuthorityEntries(entries []Entry) ([]Entry, map[string]string, error) {
	removedParents := make(map[string]string)
	retained := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type == EntryEffectAttempt {
			removedParents[entry.ID] = entry.ParentID
			continue
		}
		retained = append(retained, cloneEntry(entry))
	}

	depths := make(map[string]int64, len(retained))
	for index := range retained {
		parentID, err := resolveRemovedEffectEntryID(retained[index].ParentID, removedParents)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve retained entry parent: %w", err)
		}
		retained[index].ParentID = parentID
		retained[index].PathDepth = 1
		if parentID != "" {
			parentDepth, found := depths[parentID]
			if !found || parentDepth <= 0 {
				return nil, nil, fmt.Errorf("retained entry %q has missing parent %q", retained[index].ID, parentID)
			}
			retained[index].PathDepth = parentDepth + 1
		}
		depths[retained[index].ID] = retained[index].PathDepth
	}
	return retained, removedParents, nil
}

func resolveRemovedEffectEntryID(entryID string, removedParents map[string]string) (string, error) {
	seen := make(map[string]struct{})
	for {
		parentID, removed := removedParents[entryID]
		if !removed {
			return entryID, nil
		}
		if _, duplicate := seen[entryID]; duplicate {
			return "", errors.New("effect authority parent cycle")
		}
		seen[entryID] = struct{}{}
		entryID = parentID
	}
}
