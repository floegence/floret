package sessiontree

import (
	"context"
	"fmt"
	"github.com/floegence/floret/v7/tools"
)

// Version 21 distinguishes returned uncertainty from an unsettled execution.
func migrateBackendDomainV20ToV21(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV20Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV21Memory(memory)
}

func validateEffectOutcomeRecords(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		for _, entry := range entries {
			result := entry.Message.ToolResult
			if result == nil || result.EffectOutcome == "" {
				continue
			}
			if result.EffectOutcome != tools.EffectOutcomeUncertain || entry.Type != EntryToolResult || result.Status != "error" || result.InputRequired != nil || entry.TurnID == "" || entry.RunID == "" || entry.Message.ToolCallID == "" {
				return fmt.Errorf("tool result %q has invalid effect outcome", entry.ID)
			}
		}
	}
	return nil
}
