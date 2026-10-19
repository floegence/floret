package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV26Namespace = backendDomainV25Namespace
	backendDomainV26Version   = 26
	backendDomainV26ScanLimit = 256
)

var backendDomainV26Format = backendDomainFormat{
	label:     "v26",
	namespace: backendDomainV26Namespace,
	envelope:  "sessiontree-v26-record",
	version:   backendDomainV26Version,
	scanLimit: backendDomainV26ScanLimit,
	validate:  validateBackendDomainV26Memory,
}

type backendDomainV26Manifest = backendDomainManifest

func backendDomainV26Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV26(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV26Format)
}

func loadBackendDomainV26(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV26Format)
}

func validateBackendDomainV26Memory(memory *MemoryRepo) error {
	return validateBackendDomainV25Memory(memory)
}

func saveCompleteBackendDomainV26(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV26Format)
}

func persistBackendDomainV26Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV26Format)
}
