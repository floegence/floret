package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/identity"
)

// Legacy started markers were indexed by the canonical run_id metadata field.
// Keep that source contract in migration loading only; current repositories
// index and read the explicit entry identity.
func (memory *MemoryRepo) replaceLegacyIndexedEntriesLocked(threadID string, entries []Entry) error {
	return memory.replaceIndexedEntriesWithStartedRunIdentityLocked(threadID, entries, legacyStartedMarkerRunIdentity)
}

func legacyStartedMarkerRunIdentity(entry Entry) (string, error) {
	annotated := entry.Metadata["run_id"]
	if _, err := identity.ParseRunID(annotated); err != nil {
		return "", errors.Join(ErrAuthorityCorrupt, fmt.Errorf("started marker %q has no exact legacy run identity", entry.ID))
	}
	if entry.RunID != "" && entry.RunID != annotated {
		return "", errors.Join(ErrAuthorityCorrupt, fmt.Errorf("started marker %q has conflicting run identities", entry.ID))
	}
	return annotated, nil
}

// Version 14 makes explicit marker RunID authoritative. Observation metadata
// remains descriptive and never supplies a canonical execution identity.
func migrateBackendDomainV13ToV14(ctx context.Context, memory *MemoryRepo) error {
	if memory == nil {
		return errors.New("v13 to v14 migration requires memory")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV13Memory(memory); err != nil {
		return err
	}
	resolvedByThread := make(map[string][]Entry, len(memory.entries))
	for threadID, entries := range memory.entries {
		resolved := append([]Entry(nil), entries...)
		ancestors := make(map[string]Entry, len(entries))
		for index, source := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry := source
			if entry.Type == EntryTurnMarker && entry.TurnID != "" {
				annotated := entry.Metadata["run_id"]
				if entry.TurnStatus == TurnStarted {
					if _, err := legacyStartedMarkerRunIdentity(entry); err != nil {
						return err
					}
				}
				ancestorRunID, hasAncestorRunID, err := migrationAncestorRunID(ancestors, entry)
				if err != nil {
					return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("marker %q has an invalid canonical parent path: %w", entry.ID, err))
				}
				if entry.RunID != "" && annotated != "" && entry.RunID != annotated {
					return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("marker %q has conflicting run identities", entry.ID))
				}
				if entry.RunID == "" {
					if annotated != "" {
						if entry.TurnStatus != TurnStarted && (!hasAncestorRunID || ancestorRunID != annotated) {
							return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("marker %q metadata identity has no matching canonical ancestor", entry.ID))
						}
						entry.RunID = annotated
					} else if hasAncestorRunID {
						entry.RunID = ancestorRunID
					} else {
						// Resolve only the actual ancestor path. Journal order can
						// include inactive branches with different executions.
						return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("marker %q has no canonical run identity source", entry.ID))
					}
				}
				if _, err := identity.ParseRunID(entry.RunID); err != nil {
					return errors.Join(ErrAuthorityCorrupt, fmt.Errorf("marker %q has no exact canonical run identity", entry.ID))
				}
			}
			resolved[index] = entry
			ancestors[entry.ID] = entry
		}
		resolvedByThread[threadID] = resolved
	}

	// Validate the complete candidate before changing the source memory. This
	// keeps the migration atomic even when a later thread contains conflicting
	// or ambiguous identity evidence.
	candidate := cloneMemoryRepoForStartup(memory)
	for threadID, resolved := range resolvedByThread {
		if err := candidate.replaceIndexedEntriesLocked(threadID, resolved); err != nil {
			return err
		}
	}
	if err := validateBackendDomainV14Memory(candidate); err != nil {
		return err
	}
	for threadID, resolved := range resolvedByThread {
		if err := memory.replaceIndexedEntriesLocked(threadID, resolved); err != nil {
			return err
		}
	}
	return nil
}

func migrationAncestorRunID(ancestors map[string]Entry, entry Entry) (string, bool, error) {
	seen := map[string]struct{}{}
	for parentID := strings.TrimSpace(entry.ParentID); parentID != ""; {
		if _, duplicate := seen[parentID]; duplicate {
			return "", false, ErrAuthorityCorrupt
		}
		seen[parentID] = struct{}{}
		parent, found := ancestors[parentID]
		if !found {
			return "", false, ErrAuthorityCorrupt
		}
		if parent.Type != EntryCustom && parent.TurnID == entry.TurnID && strings.TrimSpace(parent.RunID) != "" {
			return strings.TrimSpace(parent.RunID), true, nil
		}
		parentID = strings.TrimSpace(parent.ParentID)
	}
	return "", false, nil
}
