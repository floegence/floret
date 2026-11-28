package threadsqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"path/filepath"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/storage/spi"
)

func (backend *Backend) UpdateThreadRecords(ctx context.Context, thread identity.ThreadID, write func(spi.WriteTx) error) error {
	return backend.UpdateRelatedThread(ctx, thread, "", write)
}

func detachCatalog(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(context.WithoutCancel(ctx), `DETACH DATABASE floret_catalog`)
	if err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
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
			return backend.Catalog.withAttached(ctx, conn, false, func() (err error) {
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
					if err := backend.Catalog.checkAttachedRoute(ctx, tx, ref); err != nil {
						return err
					}
					catalog := sqliterecords.AttachedCatalog(ctx, tx, false)
					defer sqliterecords.End(catalog)
					owner := threadCatalog{catalog: catalog, owner: thread, memberPeers: memberPeers{backend.Files, tx, ctx}}
					if related != "" {
						if err := checkDatabaseSchema(ctx, tx, "floret_related", backend.Files.format.schema); err != nil {
							return err
						}
						var valid bool
						if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_related.floret_thread_header AS h JOIN floret_catalog.floret_thread_routes AS r ON h.thread=r.thread AND h.file=r.file WHERE h.singleton=1 AND h.store=? AND h.file=? AND h.thread=? AND h.format=?)`, backend.Catalog.Store[:], other.File[:], related.String(), backend.Files.format.version).Scan(&valid); err != nil || !valid {
							return errors.Join(ErrFileIdentity, err)
						}
						return backend.Files.withLocal(ctx, tx, related, true, false, &threadCatalog{catalog: catalog, owner: related}, func(relatedTx spi.WriteTx) error {
							owner.other, owner.related = related, relatedTx
							return backend.Files.withLocal(ctx, tx, thread, false, false, &owner, write)
						})
					}
					return backend.Files.withLocal(ctx, tx, thread, false, false, &owner, write)
				})
			})
		})
	})
}
