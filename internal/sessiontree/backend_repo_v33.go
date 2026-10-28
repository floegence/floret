package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV33Version = 33

// Schema 33 adds optional capacity evidence to future compaction observations.
// Existing records and their v32 storage envelopes are unchanged. The manifest
// envelope alone fences older writers; the contiguous edge is one atomic write.
func migrateBackendDomainV32ToV33(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV32Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV33Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV33Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}

func validDomainManifestVersion(format backendDomainFormat, version int) bool {
	return version == format.version || format.version == backendDomainV32Version && version == backendDomainV33Version
}
