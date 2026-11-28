package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type Source struct {
	CatalogPath string
	Directory   string
	Limit       int
	MaxIdle     int
	IdleTTL     time.Duration
}

type Backend struct {
	ThreadViews
	closeOnce sync.Once
	closeErr  error
}

var ErrUnscopedWrite = errors.New("thread file layout requires an explicit record owner")

func (source Source) Open(ctx context.Context) (spi.Backend, error) {
	catalog, err := OpenCatalog(ctx, source.CatalogPath)
	if err != nil {
		return nil, err
	}
	files, err := NewFiles(source.Directory, catalog.Store, source.Limit, source.MaxIdle, source.IdleTTL)
	if err != nil {
		return nil, errors.Join(err, catalog.Close())
	}
	return &Backend{ThreadViews: ThreadViews{Catalog: catalog, Files: files}}, nil
}

type catalogWrite struct{ *sqliterecords.Tx }

func (tx catalogWrite) EmptyThreadCatalog() (bool, error) {
	return sqliterecords.Access(tx.Tx, false, func(ctx context.Context, sqlTx *sql.Tx) (bool, error) {
		var empty bool
		err := sqlTx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM floret_backend_records) AND NOT EXISTS(SELECT 1 FROM floret_thread_routes) AND NOT EXISTS(SELECT 1 FROM floret_thread_staging) AND NOT EXISTS(SELECT 1 FROM floret_thread_decisions)`).Scan(&empty)
		return empty, err
	})
}

func (tx catalogWrite) Put(string, []byte, []byte) error { return ErrUnscopedWrite }
func (tx catalogWrite) Delete(string, []byte) error      { return ErrUnscopedWrite }
func (tx catalogWrite) WriteFloretRecords(string, []spi.Record) error {
	return ErrUnscopedWrite
}

func (tx catalogWrite) PutCatalogRecord(record storagebridge.CatalogRecord, value []byte) error {
	return tx.Tx.Put(record.Namespace, record.Key, value)
}

func (backend *Backend) transaction(ctx context.Context, readOnly bool, use func(*sqliterecords.Tx) error) error {
	return backend.catalogTransaction(ctx, readOnly, func(tx *sql.Tx) error {
		records := sqliterecords.New(ctx, tx, readOnly)
		defer sqliterecords.End(records)
		return use(records)
	})
}

func (backend *Backend) catalogTransaction(ctx context.Context, readOnly bool, use func(*sql.Tx) error) error {
	if ctx == nil {
		return spi.ErrInvalidArgument
	}
	release, err := backend.Catalog.acquireOwner(ctx, readOnly)
	if err != nil {
		return err
	}
	defer release()
	tx, err := backend.Catalog.owner.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if errors.Is(err, sql.ErrConnDone) {
		return spi.ErrClosed
	}
	if err != nil {
		return sqliterecords.ClassifyError(ctx, err)
	}
	defer tx.Rollback()
	err = use(tx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil || readOnly {
		return err
	}
	return sqliterecords.ClassifyError(ctx, tx.Commit())
}

func (backend *Backend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	if read == nil {
		return spi.ErrInvalidArgument
	}
	return backend.transaction(ctx, true, func(tx *sqliterecords.Tx) error { return read(tx) })
}

func (backend *Backend) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	if write == nil {
		return spi.ErrInvalidArgument
	}
	return backend.transaction(ctx, false, func(tx *sqliterecords.Tx) error { return write(catalogWrite{tx}) })
}

func (backend *Backend) Close() error {
	backend.closeOnce.Do(func() {
		backend.closeErr = errors.Join(backend.Files.Close(context.Background()), backend.Catalog.Close())
	})
	return backend.closeErr
}
