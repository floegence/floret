package sessiontree

import "context"

// Version 29 admits explicit Goal continuation without changing historical facts.
func migrateBackendDomainV28ToV29(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV28Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV29Memory(memory)
}
