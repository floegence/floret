package sessiontree

import (
	"context"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

const (
	backendDomainV8Namespace = "floret.domain.sessiontree.v8"
	backendDomainV8Version   = 8
	backendDomainV8ScanLimit = 256
)

var backendDomainV8Format = backendDomainFormat{
	label:     "v8",
	namespace: backendDomainV8Namespace,
	envelope:  "sessiontree-v8-record",
	version:   backendDomainV8Version,
	scanLimit: backendDomainV8ScanLimit,
	validate:  validateBackendDomainV8Memory,
}

type backendDomainV8Manifest = backendDomainManifest

func backendDomainV8Key(kind string, components ...string) []byte {
	return backendDomainKey(kind, components...)
}

func scanBackendDomainV8(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	return scanBackendDomain(ctx, tx, backendDomainV8Format)
}

func loadBackendDomainV8(ctx context.Context, tx spi.ReadTx, now func() time.Time) (*MemoryRepo, bool, error) {
	return loadBackendDomain(ctx, tx, now, backendDomainV8Format)
}

func validateBackendDomainV8Memory(memory *MemoryRepo) error {
	return validateBackendDomainMemory(memory, "v8", false)
}

func saveCompleteBackendDomainV8(tx spi.WriteTx, memory *MemoryRepo) error {
	return saveCompleteBackendDomain(tx, memory, backendDomainV8Format)
}

func putBackendDomainV8Record(tx spi.WriteTx, key []byte, kind, id, threadID string, ordinal int, value any) error {
	return putBackendDomainRecord(tx, backendDomainV8Format, key, kind, id, threadID, ordinal, value)
}

func deleteAllBackendDomainV8(ctx context.Context, tx spi.WriteTx) error {
	return deleteAllBackendDomain(ctx, tx, backendDomainV8Format)
}
