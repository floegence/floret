package sessiontree

import (
	"strings"

	"github.com/floegence/floret/v7/identity"
)

// CurrentRunID follows explicit canonical entries, including continuation runs
// that do not introduce another started marker.
func CurrentRunID(path []Entry, turnID string) (string, error) {
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.Type == EntryCustom || entry.TurnID != turnID || strings.TrimSpace(entry.RunID) == "" {
			continue
		}
		if _, err := identity.ParseRunID(entry.RunID); err != nil {
			return "", ErrAuthorityCorrupt
		}
		return entry.RunID, nil
	}
	return "", ErrAuthorityCorrupt
}

func validateCanonicalEntryRunIdentity(entry Entry) error {
	if entry.RunID != "" {
		if _, err := identity.ParseRunID(entry.RunID); err != nil {
			return ErrAuthorityCorrupt
		}
	}
	if entry.Type != EntryTurnMarker || entry.TurnID == "" {
		return nil
	}
	if entry.RunID == "" {
		return ErrAuthorityCorrupt
	}
	if recorded := entry.Metadata["run_id"]; recorded != "" && recorded != entry.RunID {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateCanonicalRunIdentities(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		for _, entry := range entries {
			if err := validateCanonicalEntryRunIdentity(entry); err != nil {
				return err
			}
		}
	}
	return nil
}
