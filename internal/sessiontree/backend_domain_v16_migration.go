package sessiontree

import "context"

// Steering adds typed facts without rewriting existing messages or queues.
func migrateBackendDomainV15ToV16(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV15Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV16Memory(memory)
}
