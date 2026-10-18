package sessiontree

import "context"

// Version24 admits frozen visible-character attribution and input measurement
// scope. Canonical messages, checkpoints and historical observations are untouched.
func migrateBackendDomainV23ToV24(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV23Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV24Memory(memory)
}
