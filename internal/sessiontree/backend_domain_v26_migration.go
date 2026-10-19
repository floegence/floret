package sessiontree

import "context"

// Existing records retain their exact payloads: missing decision evidence is
// unavailable. Only the domain envelope advances to reject older writers.
func migrateBackendDomainV25ToV26(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV25SourceMemory(memory); err != nil {
		return err
	}
	return validateBackendDomainV26Memory(memory)
}
