package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV29Namespace = backendDomainV28Namespace
	backendDomainV29Version   = 29
	backendDomainV29ScanLimit = 256
)

var backendDomainV29Format = backendDomainFormat{
	label:     "v29",
	namespace: backendDomainV29Namespace,
	envelope:  "sessiontree-v29-record",
	version:   backendDomainV29Version,
	scanLimit: backendDomainV29ScanLimit,
	validate:  validateBackendDomainV29Memory,
}

type backendDomainV29Manifest = backendDomainManifest

func backendDomainV29Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV29(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV29Format)
}

func loadBackendDomainV29(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV29Format)
}

func validateBackendDomainV29Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainConversationMemory(memory, "v29", true); err != nil {
		return err
	}
	if err := validateOutputSources(memory); err != nil {
		return err
	}
	return validateCompactionBoundariesV27(memory)
}

func saveCompleteBackendDomainV29(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV29Format)
}

func persistBackendDomainV29Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV29Format)
}
