package sessiontree

import "context"

// Request-fitted budgets are additive. Historical observations keep their exact
// meaning; no capability or request limit is inferred from old reservations.
func migrateBackendDomainV17ToV18(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV17Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV18Memory(memory)
}
