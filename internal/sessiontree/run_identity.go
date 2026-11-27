package sessiontree

import (
	"errors"
	"strings"

	"github.com/floegence/floret/v7/identity"
)

var ErrFinalResponseOwnership = errors.New("final response is outside its canonical turn")

// ExecutionRun identifies facts of an executing turn. Out-of-band events
// retain their source run, which need not be the current execution.
func (entry Entry) ExecutionRun() string {
	switch entry.Type {
	case EntryTurnMarker, EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult,
		EntryInteractionAsked, EntryInteractionDone, EntryRunFailure, EntryModelContext,
		EntryCompaction, EntryHostWorkAdmitted, EntryParentEventsAdmitted, EntryGoalContext:
	case EntryCancelRequested:
		if entry.ID == HostWorkStopEntryID(entry.TurnID) {
			return ""
		}
	default:
		return ""
	}
	return strings.TrimSpace(entry.RunID)
}

// CurrentRunID follows explicit canonical entries, including continuation runs
// that do not introduce another started marker.
func CurrentRunID(path []Entry, turnID string) (string, error) {
	for index := len(path) - 1; index >= 0; index-- {
		entry := path[index]
		if entry.TurnID != turnID || entry.ExecutionRun() == "" {
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
