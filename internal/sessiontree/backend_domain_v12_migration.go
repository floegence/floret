package sessiontree

import (
	"context"
	"fmt"
)

// Version 12 adds completed-tool input requests. Historical facts remain
// byte-identical; the schema edge makes this durable wait contract explicit.
func migrateBackendDomainV11ToV12(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV11Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV12Memory(memory)
}

func validateToolInputRecords(memory *MemoryRepo) error {
	return validateCompletedToolInputRecords(memory, false)
}

func validateCurrentToolInputRecords(memory *MemoryRepo) error {
	return validateCompletedToolInputRecords(memory, true)
}

func validateCompletedToolInputRecords(memory *MemoryRepo, allowError bool) error {
	for _, entries := range memory.entries {
		if err := validateCompletedToolInputEntries(entries, allowError); err != nil {
			return err
		}
	}
	return nil
}

func validateCompletedToolInputEntries(entries []Entry, allowError bool) error {
	for _, entry := range entries {
		result := entry.Message.ToolResult
		if result == nil || result.InputRequired == nil {
			continue
		}
		if entry.Type != EntryToolResult || entry.TurnID == "" || entry.RunID == "" || entry.Message.ToolCallID == "" || (result.Status != "success" && !(allowError && result.Status == "error")) {
			return fmt.Errorf("tool input request %q has no completed tool result identity", entry.ID)
		}
		if err := result.InputRequired.Validate(); err != nil {
			return fmt.Errorf("tool input request %q is invalid: %w", entry.ID, err)
		}
	}
	return nil
}
