package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV17Namespace = backendDomainV16Namespace
	backendDomainV17Version   = 17
	backendDomainV17ScanLimit = 256
)

var backendDomainV17Format = backendDomainFormat{
	label:     "v17",
	namespace: backendDomainV17Namespace,
	envelope:  "sessiontree-v17-record",
	version:   backendDomainV17Version,
	scanLimit: backendDomainV17ScanLimit,
	validate:  validateBackendDomainV17Memory,
}

type backendDomainV17Manifest = backendDomainManifest

func backendDomainV17Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV17(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV17Format)
}

func loadBackendDomainV17(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV17Format)
}

func validateBackendDomainV17Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v17", true); err != nil {
		return err
	}
	if err := validateCancellationRecords(memory); err != nil {
		return err
	}
	if err := validateToolInputRecords(memory); err != nil {
		return err
	}
	if err := validateTurnKindRecords(memory); err != nil {
		return err
	}
	if err := validateCanonicalRunIdentities(memory); err != nil {
		return err
	}
	return validateSteeringRecords(memory)
}

func saveCompleteBackendDomainV17(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV17Format)
}

func persistBackendDomainV17Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV17Format)
}
