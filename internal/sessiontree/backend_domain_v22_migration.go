package sessiontree

import "context"

// Version 22 admits request-bound token attribution and usage relationships.
// Canonical entries are preserved byte-for-byte; historical estimates stay historical.
func migrateBackendDomainV21ToV22(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV21Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV22Memory(memory)
}
