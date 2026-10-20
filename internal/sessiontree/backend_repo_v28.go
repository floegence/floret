package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV28Namespace = backendDomainV27Namespace
	backendDomainV28Version   = 28
	backendDomainV28ScanLimit = 256
)

var backendDomainV28Format = backendDomainFormat{
	label:     "v28",
	namespace: backendDomainV28Namespace,
	envelope:  "sessiontree-v28-record",
	version:   backendDomainV28Version,
	scanLimit: backendDomainV28ScanLimit,
	validate:  validateBackendDomainV28Memory,
}

type backendDomainV28Manifest = backendDomainManifest

func backendDomainV28Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV28(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV28Format)
}

func loadBackendDomainV28(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV28Format)
}

func validateBackendDomainV28Memory(memory *MemoryRepo) error {
	return validateBackendDomainV27Memory(memory)
}

func saveCompleteBackendDomainV28(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV28Format)
}

func persistBackendDomainV28Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV28Format)
}
