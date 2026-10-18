package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV22Namespace = backendDomainV21Namespace
	backendDomainV22Version   = 22
	backendDomainV22ScanLimit = 256
)

var backendDomainV22Format = backendDomainFormat{
	label:     "v22",
	namespace: backendDomainV22Namespace,
	envelope:  "sessiontree-v22-record",
	version:   backendDomainV22Version,
	scanLimit: backendDomainV22ScanLimit,
	validate:  validateBackendDomainV22Memory,
}

type backendDomainV22Manifest = backendDomainManifest

func backendDomainV22Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV22(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV22Format)
}

func loadBackendDomainV22(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV22Format)
}

func validateBackendDomainV22Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v22", true); err != nil {
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

func saveCompleteBackendDomainV22(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV22Format)
}

func persistBackendDomainV22Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV22Format)
}
