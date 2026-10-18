package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV21Namespace = backendDomainV20Namespace
	backendDomainV21Version   = 21
	backendDomainV21ScanLimit = 256
)

var backendDomainV21Format = backendDomainFormat{
	label:     "v21",
	namespace: backendDomainV21Namespace,
	envelope:  "sessiontree-v21-record",
	version:   backendDomainV21Version,
	scanLimit: backendDomainV21ScanLimit,
	validate:  validateBackendDomainV21Memory,
}

type backendDomainV21Manifest = backendDomainManifest

func backendDomainV21Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV21(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV21Format)
}

func loadBackendDomainV21(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV21Format)
}

func validateBackendDomainV21Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v21", true); err != nil {
		return err
	}
	if err := validateCancellationRecords(memory); err != nil {
		return err
	}
	if err := validateCurrentToolInputRecords(memory); err != nil {
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
	if err := validateParentEventRecords(memory); err != nil {
		return err
	}
	return validateEffectOutcomeRecords(memory)
}

func saveCompleteBackendDomainV21(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV21Format)
}

func persistBackendDomainV21Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV21Format)
}
