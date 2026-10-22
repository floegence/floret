package sessiontree

import (
	"context"
	"fmt"
)

// Version 13 admits typed context-only turns. Existing journal bytes remain
// unchanged; historical slash-command conversations stay ordinary turns.
func migrateBackendDomainV12ToV13(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV12Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV13Memory(memory)
}

func validateTurnKindRecords(memory *MemoryRepo) error {
	return validateTurnKindRecordsForSchema(memory, false)
}

func validateCurrentTurnKindRecords(memory *MemoryRepo) error {
	return validateTurnKindRecordsForSchema(memory, true)
}

func validateTurnKindRecordsForSchema(memory *MemoryRepo, goalContinuation bool) error {
	for threadID, entries := range memory.entries {
		if err := validateTurnKindEntries(threadID, entries, goalContinuation); err != nil {
			return err
		}
	}
	return nil
}

func validateTurnKindEntries(threadID string, entries []Entry, goalContinuation bool) error {
	kinds := map[string]string{}
	for _, entry := range entries {
		kind := entry.Metadata[TurnKindMetadataKey]
		intent := entry.Metadata[ContinueGoalMetadataKey]
		if intent != "" && (!goalContinuation || intent != "1" || kind == "") {
			return ErrAuthorityCorrupt
		}
		if kind == "" {
			continue
		}
		if (kind != TurnKindContextCompaction && !(goalContinuation && kind == TurnKindGoalContinuation)) || entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || entry.TurnID == "" || entry.RunID == "" || entry.RequestKey == "" {
			return fmt.Errorf("invalid typed turn %q", entry.ID)
		}
		if kind == TurnKindGoalContinuation && intent != "1" && entry.Metadata[RetrySourceEntryIDMetadataKey] == "" {
			return ErrAuthorityCorrupt
		}
		if kind == TurnKindContextCompaction && entry.Metadata[RetrySourceEntryIDMetadataKey] != "" {
			return ErrAuthorityCorrupt
		}
		kinds[entry.TurnID] = kind
	}
	for _, entry := range entries {
		if kinds[entry.TurnID] == TurnKindGoalContinuation {
			if entry.Type == EntryUserMessage {
				return ErrAuthorityCorrupt
			}
			continue
		}
		if kinds[entry.TurnID] == "" {
			continue
		}
		switch entry.Type {
		case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryInteractionAsked, EntryEffectAttempt:
			return fmt.Errorf("context-only turn in %q contains conversation or effect entry %q", threadID, entry.ID)
		}
	}
	return nil
}
