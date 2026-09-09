package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV10Namespace = "floret.domain.sessiontree.v10"
	backendDomainV10Version   = 10
	backendDomainV10ScanLimit = 256
)

var backendDomainV10Format = backendDomainFormat{
	label:     "v10",
	namespace: backendDomainV10Namespace,
	envelope:  "sessiontree-v10-record",
	version:   backendDomainV10Version,
	scanLimit: backendDomainV10ScanLimit,
	validate:  validateBackendDomainV10Memory,
}

type backendDomainV10Manifest = backendDomainManifest

func backendDomainV10Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV10(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV10Format)
}

func loadBackendDomainV10(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV10Format)
}

func validateBackendDomainV10Memory(memory *MemoryRepo) error {
	return validateBackendDomainMemory(memory, "v10", true)
}

func saveCompleteBackendDomainV10(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV10Format)
}

func persistBackendDomainV10Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV10Format)
}
