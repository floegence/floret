package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV19Namespace = backendDomainV18Namespace
	backendDomainV19Version   = 19
	backendDomainV19ScanLimit = 256
)

var backendDomainV19Format = backendDomainFormat{
	label:     "v19",
	namespace: backendDomainV19Namespace,
	envelope:  "sessiontree-v19-record",
	version:   backendDomainV19Version,
	scanLimit: backendDomainV19ScanLimit,
	validate:  validateBackendDomainV19Memory,
}

type backendDomainV19Manifest = backendDomainManifest

func backendDomainV19Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV19(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV19Format)
}

func loadBackendDomainV19(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV19Format)
}

func validateBackendDomainV19Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v19", true); err != nil {
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
	if err := validateSteeringRecords(memory); err != nil {
		return err
	}
	return validateParentEventRecords(memory)
}

func saveCompleteBackendDomainV19(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV19Format)
}

func persistBackendDomainV19Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV19Format)
}
