//go:build floret_layout_cutover

package runtime

import (
	"context"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

func VerifyOfflineItemWindows(ctx context.Context, source, target spi.ReadTx, thread string) (int, error) {
	return sessiontree.VerifyOfflineItemWindows(ctx, source, target, thread, func(metadata sessiontree.PathMetadata, control bool) []string {
		return coldProjectionFields(metadata, false, control)
	})
}

func CheckOfflineLayoutLogicalSchema(records spi.ReadTx) error {
	if records == nil {
		return spi.ErrInvalidArgument
	}
	state, err := inspectLogicalSchemaTransaction(records)
	if err != nil {
		return err
	}
	if state != logicalSchemaCurrent {
		return spi.ErrMigrationRequired
	}
	return nil
}

func OfflineSourceDomainVersion(records spi.ReadTx) (int, error) {
	state, err := inspectLogicalSchemaTransaction(records)
	if err != nil {
		return 0, err
	}
	switch state {
	case logicalSchemaV19:
		return 42, nil
	case logicalSchemaCurrent:
		return 43, nil
	default:
		return 0, spi.ErrMigrationRequired
	}
}
