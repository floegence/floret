package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	// Keep the physical namespace so an older writer sees an unsupported envelope,
	// rather than treating an upgraded store as fresh.
	backendDomainV18Namespace = backendDomainV17Namespace
	backendDomainV18Version   = 18
	backendDomainV18ScanLimit = 256
)

var backendDomainV18Format = backendDomainFormat{
	label:     "v18",
	namespace: backendDomainV18Namespace,
	envelope:  "sessiontree-v18-record",
	version:   backendDomainV18Version,
	scanLimit: backendDomainV18ScanLimit,
	validate:  validateBackendDomainV18Memory,
}

type backendDomainV18Manifest = backendDomainManifest

func backendDomainV18Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV18(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV18Format)
}

func loadBackendDomainV18(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV18Format)
}

func validateBackendDomainV18Memory(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		for _, entry := range entries {
			if entry.Type == EntryParentEvent || entry.Type == EntryParentEventsAdmitted {
				return ErrAuthorityCorrupt
			}
		}
	}
	if err := validateBackendDomainMemory(memory, "v18", true); err != nil {
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
	if err := validateCanonicalRunIdentities(memory); err != nil {
		return err
	}
	return validateSteeringRecords(memory)
}

func saveCompleteBackendDomainV18(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV18Format)
}

func persistBackendDomainV18Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV18Format)
}
