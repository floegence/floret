package threadsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type ThreadViews struct {
	Catalog *Catalog
	Files   *Files
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
