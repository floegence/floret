package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV27Namespace = backendDomainV26Namespace
	backendDomainV27Version   = 27
	backendDomainV27ScanLimit = 256
)

var backendDomainV27Format = backendDomainFormat{
	label:     "v27",
	namespace: backendDomainV27Namespace,
	envelope:  "sessiontree-v27-record",
	version:   backendDomainV27Version,
	scanLimit: backendDomainV27ScanLimit,
	validate:  validateBackendDomainV27Memory,
}

type backendDomainV27Manifest = backendDomainManifest

func backendDomainV27Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV27(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV27Format)
}

func loadBackendDomainV27(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV27Format)
}

func validateBackendDomainV27Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainV26Memory(memory); err != nil {
		return err
	}
	return validateCompactionBoundariesV27(memory)
}

func saveCompleteBackendDomainV27(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV27Format)
}

func persistBackendDomainV27Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV27Format)
}
