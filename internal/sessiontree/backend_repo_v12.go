package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV12Namespace = "floret.domain.sessiontree.v12"
	backendDomainV12Version   = 12
	backendDomainV12ScanLimit = 256
)

var backendDomainV12Format = backendDomainFormat{
	label:     "v12",
	namespace: backendDomainV12Namespace,
	envelope:  "sessiontree-v12-record",
	version:   backendDomainV12Version,
	scanLimit: backendDomainV12ScanLimit,
	validate:  validateBackendDomainV12Memory,
}

type backendDomainV12Manifest = backendDomainManifest

func backendDomainV12Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV12(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV12Format)
}

func loadBackendDomainV12(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV12Format)
}

func validateBackendDomainV12Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v12", true); err != nil {
		return err
	}
	if err := validateCancellationRecords(memory); err != nil {
		return err
	}
	return validateToolInputRecords(memory)
}

func saveCompleteBackendDomainV12(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV12Format)
}

func persistBackendDomainV12Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV12Format)
}
