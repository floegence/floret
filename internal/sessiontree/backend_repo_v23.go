package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV23Namespace = backendDomainV22Namespace
	backendDomainV23Version   = 23
	backendDomainV23ScanLimit = 256
)

var backendDomainV23Format = backendDomainFormat{
	label:     "v23",
	namespace: backendDomainV23Namespace,
	envelope:  "sessiontree-v23-record",
	version:   backendDomainV23Version,
	scanLimit: backendDomainV23ScanLimit,
	validate:  validateBackendDomainV23Memory,
}

type backendDomainV23Manifest = backendDomainManifest

func backendDomainV23Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV23(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV23Format)
}

func loadBackendDomainV23(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV23Format)
}

func validateBackendDomainV23Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v23", true); err != nil {
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

func saveCompleteBackendDomainV23(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV23Format)
}

func persistBackendDomainV23Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV23Format)
}
