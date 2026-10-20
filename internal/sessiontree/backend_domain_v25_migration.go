package sessiontree

import "context"

func migrateBackendDomainV24ToV25(ctx context.Context, memory *MemoryRepo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDomainV24Memory(memory); err != nil {
		return err
	}
	for id, entries := range memory.entries {
		normalized, err := normalizeLegacyOutputSources(ctx, entries)
		if err != nil {
			return err
		}
		memory.entries[id] = normalized
		delete(memory.pathIndexes, id)
		delete(memory.outputSources, id)
	}
	return validateBackendDomainV25Memory(memory)
}
