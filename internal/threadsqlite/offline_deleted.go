//go:build floret_layout_cutover

package threadsqlite

import (
	"context"
	"database/sql"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/storage/spi"
)

func (backend *Backend) ImportDeletedThread(ctx context.Context, thread identity.ThreadID, write func(spi.WriteTx) error) error {
	if thread == "" || write == nil {
		return spi.ErrInvalidArgument
	}
	return backend.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_routes(thread,file) VALUES(?,NULL)`, thread.String()); err != nil {
			return err
		}
		records := sqliterecords.New(ctx, tx, false)
		defer sqliterecords.End(records)
		return write(catalogWrite{records})
	})
}
