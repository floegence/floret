package threadsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"path/filepath"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func (backend *Backend) UpdateThreadRecords(ctx context.Context, thread identity.ThreadID, write func(spi.WriteTx) error) error {
	return backend.UpdateRelatedThread(ctx, thread, "", write)
}

type relatedWriter struct {
	threadWrite
	thread  identity.ThreadID
	related threadWrite
}

func (writer relatedWriter) RelatedWriter(thread identity.ThreadID) (spi.WriteTx, error) {
	if thread != writer.thread {
		return nil, ErrFileIdentity
	}
	return writer.related, nil
}

func (backend *Backend) UpdateRelatedThread(ctx context.Context, thread, related identity.ThreadID, write func(spi.WriteTx) error) error {
	if ctx == nil || thread == "" || related == thread || write == nil {
		return spi.ErrInvalidArgument
	}
	ref, err := backend.Catalog.Resolve(ctx, thread)
	if err != nil {
		return err
	}
	var other Reference
	slots := 1
	if related != "" {
		other, err = backend.Catalog.Resolve(ctx, related)
		if err != nil {
			return err
		}
		slots++
	}
	pool := backend.Files.pool
	return pool.admit(ctx, slots, func() error {
		return pool.accessGranted(ctx, ref.File, false, pool.open, func(db *sql.DB) (err error) {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			if err := backend.Catalog.acquire(ctx); err != nil {
				return err
			}
			defer backend.Catalog.release()
			path := backend.Catalog.path
			if err := checkFile(path, false); err != nil {
				return err
			}
			if err := checkSidecars(path, true); err != nil {
				return err
			}
			uri := databaseURI(path, url.Values{"mode": {"rw"}})
			if _, err := conn.ExecContext(ctx, `ATTACH DATABASE ? AS floret_catalog`, uri); err != nil {
				return err
			}
			defer func() {
				if _, detachErr := conn.ExecContext(context.WithoutCancel(ctx), `DETACH DATABASE floret_catalog`); detachErr != nil {
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
					err = errors.Join(err, detachErr)
				}
			}()
			if _, err := conn.ExecContext(ctx, `PRAGMA floret_catalog.synchronous=FULL`); err != nil {
				return err
			}
			schemas := []string{"main", "floret_catalog"}
			if related != "" {
				path := backend.Files.path(other.File)
				if err := checkFile(filepath.Dir(path), true); err != nil {
					return err
				}
				if err := checkFile(path, false); err != nil {
					return err
				}
				if err := checkSidecars(path, true); err != nil {
					return err
				}
				if _, err := conn.ExecContext(ctx, `ATTACH DATABASE ? AS floret_related`, databaseURI(path, url.Values{"mode": {"rw"}})); err != nil {
					return err
				}
				defer func() {
					if _, detachErr := conn.ExecContext(context.WithoutCancel(ctx), `DETACH DATABASE floret_related`); detachErr != nil {
						_ = conn.Raw(func(any) error { return driver.ErrBadConn })
						err = errors.Join(err, detachErr)
					}
				}()
				if _, err := conn.ExecContext(ctx, `PRAGMA floret_related.synchronous=FULL`); err != nil {
					return err
				}
				schemas = append(schemas, "floret_related")
			}
			return backend.Files.onConnection(ctx, conn, ref, false, func(tx *sql.Tx) error {
				for _, schema := range schemas {
					var mode string
					var syncLevel int
					if err := tx.QueryRowContext(ctx, `PRAGMA `+schema+`.journal_mode`).Scan(&mode); err != nil || mode != "delete" {
						return errors.Join(ErrFileFormat, err)
					}
					if err := tx.QueryRowContext(ctx, `PRAGMA `+schema+`.synchronous`).Scan(&syncLevel); err != nil || syncLevel < 2 {
						return errors.Join(ErrFileFormat, err)
					}
				}
				var store, file []byte
				if err := tx.QueryRowContext(ctx, `SELECT value FROM floret_catalog.floret_backend_metadata WHERE name='store'`).Scan(&store); err != nil || !bytes.Equal(store, backend.Catalog.Store[:]) {
					return errors.Join(ErrFileIdentity, err)
				}
				err := tx.QueryRowContext(ctx, `SELECT file FROM floret_catalog.floret_thread_routes WHERE thread=?`, thread.String()).Scan(&file)
				if errors.Is(err, sql.ErrNoRows) {
					return storagebridge.ErrRouteMissing
				}
				if err != nil {
					return err
				}
				if file == nil {
					return storagebridge.ErrRouteDeleted
				}
				if !bytes.Equal(file, ref.File[:]) {
					return spi.ErrConflict
				}
				local, catalog := sqliterecords.New(ctx, tx, false), sqliterecords.AttachedCatalog(ctx, tx)
				defer sqliterecords.End(local)
				defer sqliterecords.End(catalog)
				if related != "" {
					if err := checkDatabaseSchema(ctx, tx, "floret_related", []string{recordsSchema, headerSchema, sealSchema}); err != nil {
						return err
					}
					var valid bool
					if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_related.floret_thread_header AS h JOIN floret_catalog.floret_thread_routes AS r ON h.thread=r.thread AND h.file=r.file WHERE h.singleton=1 AND h.store=? AND h.file=? AND h.thread=? AND h.format=1)`, backend.Catalog.Store[:], other.File[:], related.String()).Scan(&valid); err != nil || !valid {
						return errors.Join(ErrFileIdentity, err)
					}
					attached := sqliterecords.AttachedThread(ctx, tx)
					defer sqliterecords.End(attached)
					return write(relatedWriter{threadWrite{local, catalog, thread}, related, threadWrite{attached, catalog, related}})
				}
				return write(threadWrite{local, catalog, thread})
			})
		})
	})
}
