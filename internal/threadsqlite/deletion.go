package threadsqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func (backend *Backend) DeleteThreadFiles(ctx context.Context, write storagebridge.TreeDeletion) error {
	if write == nil {
		return spi.ErrInvalidArgument
	}
	var retired []Reference
	err := backend.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
		records := sqliterecords.New(ctx, tx, false)
		defer sqliterecords.End(records)
		return write(records, func(thread identity.ThreadID) error {
			var raw []byte
			if err := tx.QueryRowContext(ctx, `SELECT file FROM floret_thread_routes WHERE thread=?`, thread.String()).Scan(&raw); err != nil {
				return err
			}
			ref := Reference{Thread: thread}
			if len(raw) != len(ref.File) {
				return ErrFileIdentity
			}
			copy(ref.File[:], raw)
			if _, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_staging(file,thread) VALUES(?,?)`, raw, thread.String()); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE floret_thread_routes SET file=NULL WHERE thread=? AND file=?`, thread.String(), raw); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM floret_backend_records WHERE namespace=? AND value=?`, promptOwnerNamespace, []byte(thread)); err != nil {
				return err
			}
			retired = append(retired, ref)
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, ref := range retired {
		err = errors.Join(err, backend.discardStagedFile(ctx, ref))
	}
	return err
}
