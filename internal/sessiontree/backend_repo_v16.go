package sessiontree

import (
	"context"
	"encoding/json"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV16Namespace = "floret.domain.sessiontree.v16"
	backendDomainV16Version   = 16
	backendDomainV16ScanLimit = 256
)

var backendDomainV16Format = backendDomainFormat{
	label:     "v16",
	namespace: backendDomainV16Namespace,
	envelope:  "sessiontree-v16-record",
	version:   backendDomainV16Version,
	scanLimit: backendDomainV16ScanLimit,
	validate:  validateBackendDomainV16Memory,
}

type backendDomainV16Manifest = backendDomainManifest

func backendDomainV16Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV16(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV16Format)
}

func loadBackendDomainV16(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV16Format)
}

func validateBackendDomainV16Memory(memory *MemoryRepo) error {
	for _, entries := range memory.entries {
		for _, entry := range entries {
			if entry.Type == EntryQueueAdded {
				var queued struct {
					Input struct {
						ModelContextSnapshot *session.ModelContextSnapshot `json:"model_context_snapshot"`
					} `json:"input"`
				}
				if err := json.Unmarshal(entry.Payload, &queued); err != nil || queued.Input.ModelContextSnapshot != nil {
					return ErrAuthorityCorrupt
				}
			}
			if entry.Type == EntryModelContext || entry.Message.ModelContextSnapshot != nil || entry.ModelContextBaseline != nil {
				return ErrAuthorityCorrupt
			}
		}
	}
	if err := validateBackendDomainMemory(memory, "v16", true); err != nil {
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

func saveCompleteBackendDomainV16(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV16Format)
}

func persistBackendDomainV16Changes(tx spi.WriteTx, before, after *MemoryRepo) (bool, error) {
	return persistBackendDomainChanges(tx, before, after, backendDomainV16Format)
}
