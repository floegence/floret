package sessiontree

import "context"

// Managed snapshots are additive; opaque historical context is never inferred.
func migrateBackendDomainV16ToV17(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV16Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV17Memory(memory)
}
