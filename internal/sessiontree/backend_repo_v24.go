package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV24Namespace = backendDomainV23Namespace
	backendDomainV24Version   = 24
	backendDomainV24ScanLimit = 256
)

var backendDomainV24Format = backendDomainFormat{
	label:     "v24",
	namespace: backendDomainV24Namespace,
	envelope:  "sessiontree-v24-record",
	version:   backendDomainV24Version,
	scanLimit: backendDomainV24ScanLimit,
	validate:  validateBackendDomainV24Memory,
}

type backendDomainV24Manifest = backendDomainManifest

func backendDomainV24Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV24(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV24Format)
}

func loadBackendDomainV24(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV24Format)
}

func validateBackendDomainV24Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v24", true); err != nil {
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
	if err := validateEffectOutcomeRecords(memory); err != nil {
		return err
	}
	return validateGoalRecords(memory)
}

func saveCompleteBackendDomainV24(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV24Format)
}

func persistBackendDomainV24Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV24Format)
}
