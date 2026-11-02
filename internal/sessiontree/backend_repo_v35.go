package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV35Version = 35

func validateBackendDomainV35Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainConversationMemory(memory, "v35", true); err != nil {
		return err
	}
	if err := validateOutputSources(memory); err != nil {
		return err
	}
	if err := validateCompactionBoundariesV27(memory); err != nil {
		return err
	}
	for threadID, entries := range memory.entries {
		for _, entry := range entries {
			complete, err := memory.completeEntryLocked(entry)
			if err != nil {
				return err
			}
			if err := validateHostWorkEntry(complete); err != nil {
				return err
			}
		}
		path, err := memory.canonicalPathLocked(context.Background(), threadID, memory.threads[threadID].LeafID)
		if err != nil {
			return err
		}
		if _, _, err := hostWorksFromPath(path); err != nil {
			return err
		}
	}
	return nil
}

// Schema 35 adds canonical host-work facts, notices and continuation turns.
// Fence older writers without rewriting any historical conversation records.
func migrateBackendDomainV34ToV35(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV34Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV35Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV35Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}
