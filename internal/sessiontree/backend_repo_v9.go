package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV9Namespace = "floret.domain.sessiontree.v9"
	backendDomainV9Version   = 9
	backendDomainV9ScanLimit = 256
)

var backendDomainV9Format = backendDomainFormat{
	label:     "v9",
	namespace: backendDomainV9Namespace,
	envelope:  "sessiontree-v9-record",
	version:   backendDomainV9Version,
	scanLimit: backendDomainV9ScanLimit,
	validate:  validateBackendDomainV9Memory,
}

type backendDomainV9Manifest = backendDomainManifest

func backendDomainV9Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV9(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV9Format)
}

func loadBackendDomainV9(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV9Format)
}

func validateBackendDomainV9Memory(memory *MemoryRepo) error {
	return validateBackendDomainMemory(memory, "v9", true)
}

func saveCompleteBackendDomainV9(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV9Format)
}

func persistBackendDomainV9Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV9Format)
}
