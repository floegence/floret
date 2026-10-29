package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV34Version = 34

// Schema 34 adds typed image-count and image-byte compaction evidence.
// Existing records and their v32 storage envelopes are unchanged. The manifest
// envelope alone fences older writers; the contiguous edge is one atomic write.
func migrateBackendDomainV33ToV34(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV33Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV34Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV34Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}

// Upgrade each released manifest edge inside the owning storage transaction.
func migrateCurrentManifest(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if source.Version == backendDomainV32Version {
		if err := migrateBackendDomainV32ToV33(ctx, tx, source); err != nil {
			return err
		}
		var err error
		source, err = readV32Manifest(tx)
		if err != nil {
			return err
		}
	}
	if source.Version == backendDomainV33Version {
		return migrateBackendDomainV33ToV34(ctx, tx, source)
	}
	if source.Version != backendDomainV34Version {
		return ErrAuthorityCorrupt
	}
	return ctx.Err()
}
