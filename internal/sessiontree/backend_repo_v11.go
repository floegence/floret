package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV11Namespace = "floret.domain.sessiontree.v11"
	backendDomainV11Version   = 11
	backendDomainV11ScanLimit = 256
)

var backendDomainV11Format = backendDomainFormat{
	label:     "v11",
	namespace: backendDomainV11Namespace,
	envelope:  "sessiontree-v11-record",
	version:   backendDomainV11Version,
	scanLimit: backendDomainV11ScanLimit,
	validate:  validateBackendDomainV11Memory,
}

type backendDomainV11Manifest = backendDomainManifest

func backendDomainV11Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV11(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV11Format)
}

func loadBackendDomainV11(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV11Format)
}

func validateBackendDomainV11Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v11", true); err != nil {
		return err
	}
	return validateCancellationRecords(memory)
}

func saveCompleteBackendDomainV11(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV11Format)
}

func persistBackendDomainV11Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV11Format)
}
