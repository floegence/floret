package threadsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type threadWrite struct {
	*sqliterecords.Tx
	catalog *sqliterecords.Tx
	owner   identity.ThreadID
}

func (tx threadWrite) ThreadOwner() identity.ThreadID { return tx.owner }
func (tx threadWrite) CatalogReader() spi.ReadTx      { return tx.catalog }
func (tx threadWrite) CatalogWriter() spi.WriteTx     { return tx.catalog }
func (tx threadWrite) PutCatalogRecord(record storagebridge.CatalogRecord, raw []byte) error {
	return tx.catalog.Put(record.Namespace, record.Key, raw)
}

func (backend *Backend) CreateThreadRecords(ctx context.Context, request storagebridge.ThreadCreation, write func(spi.WriteTx) error) error {
	if write == nil {
		return spi.ErrInvalidArgument
	}
	return backend.buildThread(ctx, Reference{}, request.Thread, func(_ spi.ReadTx, tx spi.WriteTx, _ bool) error { return write(tx) })
}

func (backend *Backend) ForkThreadRecords(ctx context.Context, source, target identity.ThreadID, write storagebridge.ForkWriter) error {
	if source == "" || source == target || write == nil {
		return spi.ErrInvalidArgument
	}
	ref, err := backend.Catalog.Resolve(ctx, source)
	if err != nil {
		return err
	}
	return backend.buildThread(ctx, ref, target, write)
}

func (backend *Backend) buildThread(ctx context.Context, source Reference, target identity.ThreadID, write storagebridge.ForkWriter) (err error) {
	if ctx == nil || target == "" {
		return spi.ErrInvalidArgument
	}
	ref, err := Allocate(target)
	if err != nil {
		return err
	}
	err = backend.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
		var file []byte
		err := tx.QueryRowContext(ctx, `SELECT file FROM floret_thread_routes WHERE thread=?`, ref.Thread.String()).Scan(&file)
		if err == nil {
			if file == nil {
				return storagebridge.ErrRouteDeleted
			}
			return spi.ErrConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO floret_thread_staging(file,thread) VALUES(?,?)`, ref.File[:], ref.Thread.String())
		return err
	})
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = errors.Join(err, backend.discardStagedFile(cleanup, ref))
		}
	}()
	build := func(reader spi.ReadTx, db *sql.DB) error {
		return backend.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
			if source.Thread != "" {
				var current bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_thread_routes WHERE thread=? AND file=?)`, source.Thread.String(), source.File[:]).Scan(&current); err != nil || !current {
					return errors.Join(spi.ErrConflict, err)
				}
			}
			catalog := sqliterecords.New(ctx, tx, false)
			defer sqliterecords.End(catalog)
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			err = backend.Files.onConnection(ctx, conn, ref, false, func(local *sql.Tx) error {
				records := sqliterecords.New(ctx, local, false)
				defer sqliterecords.End(records)
				return write(reader, threadWrite{records, catalog, target}, source.Thread != "")
			})
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_routes(thread,file) VALUES(?,?)`, ref.Thread.String(), ref.File[:]); err != nil {
				return err
			}
			return removeStagingRecord(ctx, tx, ref)
		})
	}
	slots := 1
	if source.Thread != "" {
		slots++
	}
	err = backend.Files.pool.admit(ctx, slots, func() error {
		targetFile := func(reader spi.ReadTx) error {
			return backend.Files.pool.accessFile(ctx, ref.File, true, true, func(ctx context.Context, _ FileID) (*sql.DB, error) { return backend.Files.create(ctx, ref) }, func(db *sql.DB) error { return build(reader, db) })
		}
		if source.Thread == "" {
			return targetFile(nil)
		}
		return backend.Files.pool.accessGranted(ctx, source.File, false, backend.Files.open, func(db *sql.DB) error {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			return backend.Files.onConnection(ctx, conn, source, true, func(tx *sql.Tx) error {
				reader := sqliterecords.New(ctx, tx, true)
				defer sqliterecords.End(reader)
				return targetFile(reader)
			})
		})
	})
	complete = err == nil
	return err
}

func removeStagingRecord(ctx context.Context, tx *sql.Tx, ref Reference) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM floret_thread_staging WHERE file=? AND thread=?`, ref.File[:], ref.Thread.String())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return spi.ErrConflict
	}
	return err
}

func (backend *Backend) RecoverThreadFiles(ctx context.Context) error {
	for {
		var ref Reference
		err := backend.catalogTransaction(ctx, true, func(tx *sql.Tx) error {
			var raw []byte
			err := tx.QueryRowContext(ctx, `SELECT file,thread FROM floret_thread_staging ORDER BY file LIMIT 1`).Scan(&raw, &ref.Thread)
			if err != nil {
				return err
			}
			if len(raw) != len(ref.File) || ref.Thread == "" {
				return ErrFileIdentity
			}
			copy(ref.File[:], raw)
			if ref.File == (FileID{}) {
				return ErrFileIdentity
			}
			return nil
		})
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := backend.discardStagedFile(ctx, ref); err != nil {
			return err
		}
	}
}

func (backend *Backend) discardStagedFile(ctx context.Context, ref Reference) error {
	pool := backend.Files.pool
	return pool.admit(ctx, pool.limit, func() error {
		if err := pool.evict(ctx, ref.File); err != nil {
			return err
		}
		return backend.catalogTransaction(ctx, false, func(tx *sql.Tx) error {
			var staged []byte
			err := tx.QueryRowContext(ctx, `SELECT file FROM floret_thread_staging WHERE thread=?`, ref.Thread.String()).Scan(&staged)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil || !bytes.Equal(staged, ref.File[:]) {
				return errors.Join(ErrFileIdentity, err)
			}
			var published bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_thread_routes WHERE (thread=? AND file IS NOT NULL) OR file=?)`, ref.Thread.String(), ref.File[:]).Scan(&published); err != nil || published {
				return errors.Join(ErrFileIdentity, err)
			}
			path := backend.Files.path(ref.File)
			bucket := filepath.Dir(path)
			if err := checkFile(bucket, true); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				return removeStagingRecord(ctx, tx, ref)
			}
			original, err := os.Lstat(path)
			if err == nil {
				db, err := backend.Files.open(ctx, ref.File)
				if err != nil {
					return err
				}
				conn, err := db.Conn(ctx)
				if err == nil {
					err = backend.Files.onConnection(ctx, conn, ref, true, func(*sql.Tx) error { return nil })
					err = errors.Join(err, conn.Close())
				}
				if err := errors.Join(err, db.Close()); err != nil {
					return err
				}
				current, err := os.Lstat(path)
				if err != nil || !os.SameFile(original, current) {
					return errors.Join(ErrFileIdentity, err)
				}
				if err := checkSidecars(path, false); err != nil {
					return err
				}
				if err := os.Remove(path); err != nil {
					return err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			} else if err := checkSidecars(path, false); err != nil {
				return err
			}
			if err := syncDirectory(bucket); err != nil {
				return err
			}
			return removeStagingRecord(ctx, tx, ref)
		})
	})
}
