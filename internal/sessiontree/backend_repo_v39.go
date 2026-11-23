package sessiontree

import (
	"context"
	"errors"
	"reflect"

	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainV39Version = 39

const backendDomainV40Version = 40

const backendDomainV41Version = 41

// Schema 39 admits segmented output references and streaming host-work capture.
// Historical inline artifacts remain authoritative and are not rescanned or
// rewritten. The manifest and logical fence exclude older writers atomically.
func migrateBackendDomainV38ToV39(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	return advanceUnchangedDomainRecords(ctx, tx, source, backendDomainV38Version, backendDomainV39Version)
}

func advanceUnchangedDomainRecords(ctx context.Context, tx spi.WriteTx, source backendDomainManifest, previous, next int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != previous || next != previous+1 || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = next
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || !reflect.DeepEqual(verified, actual) {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}
