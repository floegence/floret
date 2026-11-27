package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

const sqliteDriverName = "sqlite"

type sqliteSource struct {
	path string
}

var sqliteOwnership = struct {
	sync.Mutex
	open        map[string]int
	maintenance map[string]bool
}{open: make(map[string]int), maintenance: make(map[string]bool)}

// SQLite returns a Source backed by the SQLite file at path. The physical
// database contains only backend metadata and opaque namespaced records.
func SQLite(path string) Source {
	return NewSource(sqliteSource{path: path})
}

func (source sqliteSource) Open(ctx context.Context) (spi.Backend, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: open context is required", spi.ErrInvalidArgument)
	}
	if strings.TrimSpace(source.path) == "" {
		return nil, fmt.Errorf("%w: SQLite path is required", spi.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(source.path), 0o700); err != nil {
			return nil, err
		}
	}
	ownedPath := ""
	if source.path != ":memory:" {
		var err error
		ownedPath, err = filepath.Abs(source.path)
		if err != nil {
			return nil, err
		}
		ownedPath = filepath.Clean(ownedPath)
		if resolved, e := filepath.EvalSymlinks(filepath.Dir(ownedPath)); e == nil {
			ownedPath = filepath.Join(resolved, filepath.Base(ownedPath))
		}
		if resolved, e := filepath.EvalSymlinks(ownedPath); e == nil {
			ownedPath = resolved
		}
		sqliteOwnership.Lock()
		if sqliteOwnership.maintenance[ownedPath] {
			sqliteOwnership.Unlock()
			return nil, spi.ErrConflict
		}
		sqliteOwnership.open[ownedPath]++
		sqliteOwnership.Unlock()
	}
	releaseOwnership := func() {
		if ownedPath == "" {
			return
		}
		sqliteOwnership.Lock()
		if sqliteOwnership.open[ownedPath] <= 1 {
			delete(sqliteOwnership.open, ownedPath)
		} else {
			sqliteOwnership.open[ownedPath]--
		}
		sqliteOwnership.Unlock()
	}
	db, err := sql.Open(sqliteDriverName, source.path)
	if err != nil {
		releaseOwnership()
		return nil, err
	}
	// Initialization PRAGMAs and the first schema transaction must share one
	// connection so a fresh database persists auto_vacuum before table creation.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	backend := &sqliteBackend{db: db, ownedPath: ownedPath}
	if err := backend.initialize(ctx); err != nil {
		_ = db.Close()
		releaseOwnership()
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	return backend, nil
}

type sqliteBackend struct {
	maintenanceSidecars []string
	maintenance         bool
	db                  *sql.DB
	ownedPath           string
	closeMu             sync.Mutex
	closed              bool
}

// beginTransaction owns a connection until rollback has actually completed.
// database/sql can return ErrTxDone while its cancellation goroutine is still
// rolling back; Conn.Close joins that work before another owner can open files.
func (backend *sqliteBackend) beginTransaction(ctx context.Context, options *sql.TxOptions) (*sql.Tx, func(), error) {
	connection, err := backend.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	transaction, err := connection.BeginTx(ctx, options)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	return transaction, func() { _ = transaction.Rollback(); _ = connection.Close() }, nil
}

func (backend *sqliteBackend) initialize(ctx context.Context) error {
	if _, err := backend.db.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		return err
	}
	var existingTableCount int
	if err := backend.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
	`).Scan(&existingTableCount); err != nil {
		return err
	}
	// auto_vacuum must be selected before the first table is created. Existing
	// databases are converted only by MaintainSQLite while no runtime is open.
	if existingTableCount == 0 {
		if _, err := backend.db.ExecContext(ctx, `PRAGMA auto_vacuum = INCREMENTAL`); err != nil {
			return err
		}
	}
	if _, err := backend.db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		return err
	}
	tx, finish, err := backend.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer finish()
	var tableCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
	`).Scan(&tableCount); err != nil {
		return err
	}
	if tableCount == 0 {
		statements := []string{
			`CREATE TABLE floret_backend_metadata (
				name TEXT PRIMARY KEY,
				value BLOB NOT NULL
			) WITHOUT ROWID`,
			`CREATE TABLE floret_backend_records (
				namespace TEXT NOT NULL,
				key BLOB NOT NULL,
				value BLOB NOT NULL,
				PRIMARY KEY (namespace, key)
			)`,
			`INSERT INTO floret_backend_metadata(name, value) VALUES ('physical_schema', CAST('5' AS BLOB))`,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	} else {
		if _, err := inspectSQLitePhysicalSchema(ctx, tx); err != nil {
			return fmt.Errorf("%w: %w", spi.ErrInvalidArgument, err)
		}
	}
	return tx.Commit()
}

func (backend *sqliteBackend) View(ctx context.Context, callback func(spi.ReadTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	if backend.isClosed() {
		return spi.ErrClosed
	}
	sqlTx, finish, err := backend.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return sqliterecords.ClassifyError(ctx, err)
	}
	defer finish()
	tx := &sqliteTx{sqliterecords.New(ctx, sqlTx, true)}
	if err := func() error {
		defer sqliterecords.End(tx.Tx)
		return callback(tx)
	}(); err != nil {
		return err
	}
	return ctx.Err()
}

func (backend *sqliteBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	if backend.isClosed() {
		return spi.ErrClosed
	}
	sqlTx, finish, err := backend.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return sqliterecords.ClassifyError(ctx, err)
	}
	defer finish()
	tx := &sqliteTx{sqliterecords.New(ctx, sqlTx, false)}
	if err := func() error {
		defer sqliterecords.End(tx.Tx)
		return callback(tx)
	}(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return sqliterecords.ClassifyError(ctx, sqlTx.Commit())
}

func (backend *sqliteBackend) Close() error {
	backend.closeMu.Lock()
	defer backend.closeMu.Unlock()
	if backend.closed {
		return nil
	}
	backend.closed = true
	err := backend.db.Close()
	if err == nil {
		for _, path := range backend.maintenanceSidecars {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, removeErr)
			}
		}
	}
	if backend.ownedPath != "" {
		sqliteOwnership.Lock()
		if backend.maintenance {
			delete(sqliteOwnership.maintenance, backend.ownedPath)
		}
		if sqliteOwnership.open[backend.ownedPath] <= 1 {
			delete(sqliteOwnership.open, backend.ownedPath)
		} else {
			sqliteOwnership.open[backend.ownedPath]--
		}
		sqliteOwnership.Unlock()
	}
	return err
}

func (backend *sqliteBackend) ResetFloretStorage(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("%w: reset context is required", spi.ErrInvalidArgument)
	}
	if backend.isClosed() {
		return false, spi.ErrClosed
	}
	tx, finish, err := backend.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, sqliterecords.ClassifyError(ctx, err)
	}
	defer finish()
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM floret_backend_records`).Scan(&count); err != nil {
		return false, sqliterecords.ClassifyError(ctx, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM floret_backend_records`); err != nil {
		return false, sqliterecords.ClassifyError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, sqliterecords.ClassifyError(ctx, err)
	}
	return count > 0, nil
}

func (backend *sqliteBackend) isClosed() bool {
	backend.closeMu.Lock()
	defer backend.closeMu.Unlock()
	return backend.closed
}

type sqliteTx struct{ *sqliterecords.Tx }
