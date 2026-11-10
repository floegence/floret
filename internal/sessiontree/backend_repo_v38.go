package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV38Version = 38

// Schema 38 admits immutable same-thread message revision branches. The manifest
// fences older writers without rewriting any historical conversation records.
func migrateBackendDomainV37ToV38(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV37Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV38Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV38Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}
