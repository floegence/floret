package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV20Namespace = backendDomainV19Namespace
	backendDomainV20Version   = 20
	backendDomainV20ScanLimit = 256
)

var backendDomainV20Format = backendDomainFormat{
	label:     "v20",
	namespace: backendDomainV20Namespace,
	envelope:  "sessiontree-v20-record",
	version:   backendDomainV20Version,
	scanLimit: backendDomainV20ScanLimit,
	validate:  validateBackendDomainV20Memory,
}

type backendDomainV20Manifest = backendDomainManifest

func backendDomainV20Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV20(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV20Format)
}

func loadBackendDomainV20(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV20Format)
}

func validateBackendDomainV20Memory(memory *MemoryRepo) error {
	if err := validateBackendDomainMemory(memory, "v20", true); err != nil {
		return err
	}
	if err := validateCancellationRecords(memory); err != nil {
		return err
	}
	if err := validateCurrentToolInputRecords(memory); err != nil {
		return err
	}
	if err := validateTurnKindRecords(memory); err != nil {
		return err
	}
	if err := validateCanonicalRunIdentities(memory); err != nil {
		return err
	}
	if err := validateSteeringRecords(memory); err != nil {
		return err
	}
	return validateParentEventRecords(memory)
}

func saveCompleteBackendDomainV20(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV20Format)
}

func persistBackendDomainV20Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV20Format)
}
