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
	for threadID, entries := range memory.entries {
		kinds := map[string]string{}
		for _, entry := range entries {
			kind := entry.Metadata[TurnKindMetadataKey]
			if kind == "" {
				continue
			}
			if kind != TurnKindContextCompaction || entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || entry.TurnID == "" || entry.RunID == "" || entry.RequestKey == "" {
				return fmt.Errorf("invalid typed turn %q", entry.ID)
			}
			if entry.Metadata[RetrySourceEntryIDMetadataKey] != "" {
				return ErrAuthorityCorrupt
			}
			kinds[entry.TurnID] = kind
		}
		for _, entry := range entries {
			if kinds[entry.TurnID] == "" {
				continue
			}
			switch entry.Type {
			case EntryUserMessage, EntryAssistantMessage, EntryToolCall, EntryToolResult, EntryInteractionAsked, EntryEffectAttempt:
				return fmt.Errorf("context-only turn in %q contains conversation or effect entry %q", threadID, entry.ID)
			}
		}
	}
	return nil
}
