package threadsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/url"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type ThreadViews struct {
	Catalog *Catalog
	Files   *Files
}

func (views ThreadViews) UsesMemberJournals() bool {
	return views.Files != nil && views.Files.format.version == member.Format
}

func (views ThreadViews) ViewThreadRecords(ctx context.Context, request storagebridge.ThreadRead, read func(spi.ReadTx) error) error {
	return views.Catalog.ViewThread(ctx, views.Files, request.Thread, read)
}

func (catalog *Catalog) ViewThread(ctx context.Context, files *Files, thread identity.ThreadID, read func(spi.ReadTx) error) error {
	if catalog == nil || files == nil || files.store != catalog.Store || read == nil {
		return spi.ErrInvalidArgument
	}
	ref, err := catalog.Resolve(ctx, thread)
	if err != nil {
		return err
	}
	if files.format.version == member.Format {
		return files.pool.withFile(ctx, ref.File, func(db *sql.DB) error {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			return catalog.withAttached(ctx, conn, true, func() error {
				return files.onConnection(ctx, conn, ref, true, func(tx *sql.Tx) error {
					if err := catalog.checkAttachedRoute(ctx, tx, ref); err != nil {
						return err
					}
					records := sqliterecords.AttachedCatalog(ctx, tx, true)
					defer sqliterecords.End(records)
					journal, err := member.OpenJournal(ctx, tx, thread, false)
					if err != nil {
						return err
					}
					journal.RestrictToReads()
					return read(memberRead{memberTx{journal}, records, memberPeers{files, tx, ctx}})
				})
			})
		})
	}
	return (LocalRecords{files, ref}).View(ctx, func(tx spi.ReadTx) error {
		current, err := catalog.Resolve(ctx, thread)
		if err != nil {
			return err
		}
		if current != ref {
			return spi.ErrConflict
		}
		return read(tx)
	})
}

func (catalog *Catalog) withAttached(ctx context.Context, conn *sql.Conn, readOnly bool, use func() error) (err error) {
	if err := checkFile(catalog.path, false); err != nil {
		return err
	}
	if err := checkSidecars(catalog.path, true); err != nil {
		return err
	}
	mode := "rw"
	if readOnly {
		mode = "ro"
	}
	if _, err := conn.ExecContext(ctx, `ATTACH DATABASE ? AS floret_catalog`, databaseURI(catalog.path, url.Values{"mode": {mode}})); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, detachCatalog(ctx, conn)) }()
	return use()
}

func (catalog *Catalog) checkAttachedRoute(ctx context.Context, tx *sql.Tx, ref Reference) error {
	var store, file []byte
	err := tx.QueryRowContext(ctx, `SELECT (SELECT value FROM floret_catalog.floret_backend_metadata WHERE name='store'),file FROM floret_catalog.floret_thread_routes WHERE thread=?`, ref.Thread.String()).Scan(&store, &file)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return storagebridge.ErrRouteMissing
	case err != nil:
		return err
	case !bytes.Equal(store, catalog.Store[:]):
		return ErrFileIdentity
	case file == nil:
		return storagebridge.ErrRouteDeleted
	case !bytes.Equal(file, ref.File[:]):
		return spi.ErrConflict
	default:
		return nil
	}
}

type LocalRecords struct {
	Files     *Files
	Reference Reference
}

func (file LocalRecords) View(ctx context.Context, read func(spi.ReadTx) error) error {
	if file.Files == nil || read == nil {
		return spi.ErrInvalidArgument
	}
	return file.Files.View(ctx, file.Reference, func(transaction *sql.Tx) error {
		return file.Files.withLocal(ctx, transaction, file.Reference.Thread, false, true, nil, func(tx spi.WriteTx) error { return read(tx) })
	})
}

func (file LocalRecords) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	if file.Files == nil || write == nil {
		return spi.ErrInvalidArgument
	}
	return file.Files.Update(ctx, file.Reference, func(transaction *sql.Tx) error {
		return file.Files.withLocal(ctx, transaction, file.Reference.Thread, false, false, nil, write)
	})
}

func (files *Files) PrepareFork(ctx context.Context, source, target Reference, fingerprint [32]byte, project func(spi.ReadTx, spi.WriteTx) error) error {
	if project == nil || fingerprint == ([32]byte{}) {
		return spi.ErrInvalidArgument
	}
	return files.Project(ctx, source, target, func(sourceTx, targetTx *sql.Tx) error {
		var saved []byte
		err := targetTx.QueryRowContext(ctx, `SELECT fingerprint FROM floret_thread_preparation WHERE singleton=1`).Scan(&saved)
		if err == nil {
			if !bytes.Equal(saved, fingerprint[:]) {
				return spi.ErrConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var occupied bool
		query := `SELECT EXISTS(SELECT 1 FROM floret_backend_records)`
		if files.format.version == member.Format {
			query = `SELECT commit_rev <> 0 FROM floret_thread_header WHERE singleton=1`
		}
		if err := targetTx.QueryRowContext(ctx, query).Scan(&occupied); err != nil {
			return err
		}
		if occupied {
			return spi.ErrConflict
		}
		if err := files.withLocal(ctx, sourceTx, source.Thread, false, true, nil, func(reader spi.WriteTx) error {
			return files.withLocal(ctx, targetTx, target.Thread, false, false, nil, func(writer spi.WriteTx) error { return project(reader, writer) })
		}); err != nil {
			return err
		}
		_, err = targetTx.ExecContext(ctx, `INSERT INTO floret_thread_preparation VALUES(1,?)`, fingerprint[:])
		return err
	})
}

func (catalog *Catalog) PublishFork(ctx context.Context, files *Files, operation string, source, target Reference, fingerprint [32]byte) error {
	if source.Thread == "" || target.Thread == "" || source.Thread == target.Thread || source.File == target.File || source.File == (FileID{}) || target.File == (FileID{}) || fingerprint == ([32]byte{}) {
		return spi.ErrInvalidArgument
	}
	return catalog.publish(ctx, files, operation, []Replacement{{Thread: target.Thread, Next: target.File}}, &source, fingerprint[:])
}
