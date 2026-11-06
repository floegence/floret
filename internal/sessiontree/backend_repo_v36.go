package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
)

const backendDomainV36Version = 36

// Schema 36 fences writers that copied tool preparation onto reasoning members.
// Existing canonical bytes remain authoritative and unchanged. Membership with
// no ActivityPendingVersion is explicitly unprepared: the existing bounded
// per-thread preparation owner validates sources and atomically publishes its
// corrected readiness anchor. Opening a store never scans unrelated histories.
func migrateBackendDomainV35ToV36(ctx context.Context, tx spi.WriteTx, source backendDomainManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := readV32Manifest(tx)
	if err != nil {
		return err
	}
	if actual.Version != backendDomainV35Version || !reflect.DeepEqual(source, actual) {
		return ErrAuthorityCorrupt
	}
	actual.Version = backendDomainV36Version
	if err := putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, actual); err != nil {
		return err
	}
	verified, err := readV32Manifest(tx)
	if err != nil || verified.Version != backendDomainV36Version {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return ctx.Err()
}
