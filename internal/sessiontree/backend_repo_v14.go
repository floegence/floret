package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV14Namespace = "floret.domain.sessiontree.v14"
	backendDomainV14Version   = 14
	backendDomainV14ScanLimit = 256
)

var backendDomainV14Format = backendDomainFormat{
	label:     "v14",
	namespace: backendDomainV14Namespace,
	envelope:  "sessiontree-v14-record",
	version:   backendDomainV14Version,
	scanLimit: backendDomainV14ScanLimit,
	validate:  validateBackendDomainV14Memory,
}

type backendDomainV14Manifest = backendDomainManifest

func backendDomainV14Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV14(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV14Format)
}

func loadBackendDomainV14(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV14Format)
}

func validateBackendDomainV14Memory(memory *MemoryRepo) error {
	if err := validatePreThreadReferenceMemory(memory); err != nil {
		return err
	}
	if err := validateBackendDomainMemory(memory, "v14", true); err != nil {
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

func saveCompleteBackendDomainV14(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV14Format)
}

func persistBackendDomainV14Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV14Format)
}
