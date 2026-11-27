//go:build floret_layout_cutover

package storage

import (
	"context"
	"database/sql"

	"github.com/floegence/floret/v7/storage/spi"
)

func CheckOfflineLayoutPhysicalSchema(ctx context.Context, tx *sql.Tx) error {
	if ctx == nil || tx == nil {
		return spi.ErrInvalidArgument
	}
	version, err := inspectSQLitePhysicalSchema(ctx, tx)
	if err != nil {
		return err
	}
	if version != "5" {
		return spi.ErrMigrationRequired
	}
	var exact bool
	err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*')=2 AND COUNT(*)=1 AND COUNT(*) FILTER (WHERE name='physical_schema' AND value=CAST(? AS BLOB))=1 FROM floret_backend_metadata`, version).Scan(&exact)
	if err != nil {
		return err
	}
	if !exact {
		return ErrUnsupportedSQLiteFormat
	}
	return nil
}
