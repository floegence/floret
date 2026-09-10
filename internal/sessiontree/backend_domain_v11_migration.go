package sessiontree

import (
	"context"
	"fmt"
)

// migrateBackendDomainV10ToV11 preserves historical bytes and admits explicit
// stop provenance and confirmed canceled effect results. Missing historical
// provenance is never inferred from a failure or an effect state.
func migrateBackendDomainV10ToV11(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV10Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV11Memory(memory)
}

func validateCancellationRecords(memory *MemoryRepo) error {
	for threadID, entries := range memory.entries {
		for _, entry := range entries {
			if entry.Type == EntryCancelRequested && (entry.Metadata["cancellation_source"] != "" || entry.Metadata["cancellation_mode"] != "") {
				switch entry.Metadata["cancellation_source"] {
				case "user_stop", "execution_context", "runtime_shutdown", "thread_delete", "snapshot_restore":
				default:
					return fmt.Errorf("invalid cancellation source on %q", entry.ID)
				}
				mode := entry.Metadata["cancellation_mode"]
				if (mode != "immediate" && mode != "graceful") || entry.RunID == "" || entry.TurnID == "" || entry.CreatedAt.IsZero() {
					return fmt.Errorf("incomplete cancellation on %q", entry.ID)
				}
			}
			if entry.Type != EntryEffectAttempt {
				continue
			}
			attempt, err := decodeEffectAttempt(entry)
			if err != nil {
				return err
			}
			if attempt.State != EffectAttemptCancelled || attempt.ResultEntryID == "" {
				continue
			}
			result, found := memoryEntryByID(memory.entries[threadID], attempt.ResultEntryID)
			if !found || result.Type != EntryToolResult || result.TurnID != entry.TurnID || result.RunID != entry.RunID || result.Message.ToolCallID != attempt.Invocation.ToolCallID || result.Message.ToolName != attempt.Invocation.ToolName || result.Message.ToolResult == nil || result.Message.ToolResult.Status != "canceled" || result.Metadata[PendingToolEffectAttemptIDKey] != attempt.EffectAttemptID {
				return fmt.Errorf("canceled effect %q has no matching confirmed result", attempt.EffectAttemptID)
			}
		}
	}
	return nil
}
