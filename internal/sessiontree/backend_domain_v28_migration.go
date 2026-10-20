package sessiontree

import "context"

// Version 28 fences request-bound native measurement semantics. Historical
// messages, usage, checkpoints and absent protocol evidence remain unchanged.
func migrateBackendDomainV27ToV28(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV27Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV28Memory(memory)
}
