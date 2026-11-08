package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV37Version = 37

// Schema 37 records built-in tool semantics independently of callable names.
// Only the manifest changes. Historical entries, effects and provider signatures
// retain their exact bytes and remain interpreted by their released contracts.
func migrateBackendDomainV36ToV37(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV36Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV37Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV37Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}
