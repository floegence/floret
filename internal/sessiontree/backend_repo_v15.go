package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV15Namespace = "floret.domain.sessiontree.v15"
	backendDomainV15Version   = 15
	backendDomainV15ScanLimit = 256
)

var backendDomainV15Format = backendDomainFormat{
	label:     "v15",
	namespace: backendDomainV15Namespace,
	envelope:  "sessiontree-v15-record",
	version:   backendDomainV15Version,
	scanLimit: backendDomainV15ScanLimit,
	validate:  validateBackendDomainV15Memory,
}

type backendDomainV15Manifest = backendDomainManifest

func backendDomainV15Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV15(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV15Format)
}

func loadBackendDomainV15(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV15Format)
}

func validateBackendDomainV15Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v15", true); err != nil {
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
	return validateCanonicalRunIdentities(memory)
}

func saveCompleteBackendDomainV15(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV15Format)
}

func persistBackendDomainV15Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV15Format)
}
