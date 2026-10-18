package sessiontree

import "context"

// Version 20 permits completed error results to request user input without replay.
func migrateBackendDomainV19ToV20(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV19Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV20Memory(memory)
}
