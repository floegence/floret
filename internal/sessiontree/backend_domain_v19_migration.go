package sessiontree

import "context"

func migrateBackendDomainV18ToV19(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV18Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV19Memory(memory)
}
