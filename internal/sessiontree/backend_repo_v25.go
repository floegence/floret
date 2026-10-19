package sessiontree

import (
	"context"
	"errors"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV25Namespace = backendDomainV24Namespace
	backendDomainV25Version   = 25
	backendDomainV25ScanLimit = 256
)

var backendDomainV25Format = backendDomainFormat{
	label:     "v25",
	namespace: backendDomainV25Namespace,
	envelope:  "sessiontree-v25-record",
	version:   backendDomainV25Version,
	scanLimit: backendDomainV25ScanLimit,
	validate:  validateBackendDomainV25SourceMemory,
}

type backendDomainV25Manifest = backendDomainManifest

func backendDomainV25Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV25(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV25Format)
}

func loadBackendDomainV25(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV25Format)
}

func validateBackendDomainV25Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainV24Memory(memory); err != nil {
		return err
	}
	return validateOutputSources(memory)
}

func saveCompleteBackendDomainV25(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV25Format)
}

func persistBackendDomainV25Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV25Format)
}

func validateBackendDomainV25SourceMemory(memory *MemoryRepo) error {
	if err := validateBackendDomainV25Memory(memory); err != nil {
		return err
	}
	for _, entries := range memory.entries {
		for _, entry := range entries {
			if ThreadContextEntryKind(entry) != ThreadContextCompactionEntryKind {
				continue
			}
			compact, err := DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				return err
			}
			if compact.Decision != nil {
				return errors.New("v25 compaction cannot contain v26 decision evidence")
			}
		}
	}
	return nil
}
