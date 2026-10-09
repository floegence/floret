package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV13Namespace = "floret.domain.sessiontree.v13"
	backendDomainV13Version   = 13
	backendDomainV13ScanLimit = 256
)

var backendDomainV13Format = backendDomainFormat{
	label:     "v13",
	namespace: backendDomainV13Namespace,
	envelope:  "sessiontree-v13-record",
	version:   backendDomainV13Version,
	scanLimit: backendDomainV13ScanLimit,
	validate:  validateBackendDomainV13Memory,
}

type backendDomainV13Manifest = backendDomainManifest

func backendDomainV13Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV13(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV13Format)
}

func loadBackendDomainV13(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV13Format)
}

func validateBackendDomainV13Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v13", true); err != nil {
		return err
	}
	if err := validateCancellationRecords(memory); err != nil {
		return err
	}
	if err := validateToolInputRecords(memory); err != nil {
		return err
	}
	return validateTurnKindRecords(memory)
}

func saveCompleteBackendDomainV13(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV13Format)
}

func persistBackendDomainV13Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV13Format)
}
