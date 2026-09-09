package sessiontree

import "context"

// migrateBackendDomainV9ToV10 admits durable model context and typed validation
// feedback. Existing canonical entries and provider request records are unchanged;
// the provider projection revision establishes its own explicit request boundary.
func migrateBackendDomainV9ToV10(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV9Memory(memory); err != nil {
		return err
	}
	return validateBackendDomainV10Memory(memory)
}
