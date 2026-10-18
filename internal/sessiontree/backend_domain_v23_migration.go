package sessiontree

import "context"

// Version 23 admits canonical goals and typed reflection facts. Existing entries are unchanged.
func migrateBackendDomainV22ToV23(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV22Memory(memory); err != nil {
		return err
	}
	for _, entries := range memory.entries {
		for _, entry := range entries {
			if entry.Type == EntryGoal || entry.Type == EntryGoalReview || entry.Type == EntryGoalContext {
				return ErrAuthorityCorrupt
			}
		}
	}
	return validateBackendDomainV23Memory(memory)
}
