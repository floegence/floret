package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

const sqliteDriverName = "sqlite"

type sqliteSource struct {
	path string
}

// SQLite returns a Source backed by the SQLite file at path. The physical
// database contains only backend metadata and opaque namespaced records.
func SQLite(path string) Source {
	return sqliteSource{path: path}
}

func (source sqliteSource) Open(ctx context.Context) (Backend, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: open context is required", ErrInvalidArgument)
	}
	if strings.TrimSpace(source.path) == "" {
		return nil, fmt.Errorf("%w: SQLite path is required", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(source.path), 0o700); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open(sqliteDriverName, source.path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	backend := &sqliteBackend{db: db}
	if err := backend.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return backend, nil
}

type sqliteBackend struct {
	db      *sql.DB
	closeMu sync.Mutex
	closed  bool
}

func (backend *sqliteBackend) initialize(ctx context.Context) error {
	if _, err := backend.db.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		return err
	}
	if _, err := backend.db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		return err
	}
	tx, err := backend.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
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
			) WITHOUT ROWID`,
			`INSERT INTO floret_backend_metadata(name, value) VALUES ('physical_schema', CAST('1' AS BLOB))`,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	} else {
		var exactTables int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM sqlite_master
			WHERE type = 'table'
			  AND name IN ('floret_backend_metadata', 'floret_backend_records')
		`).Scan(&exactTables); err != nil {
			return err
		}
		if tableCount != 2 || exactTables != 2 {
			return fmt.Errorf("%w: database is not a Floret backend", ErrInvalidArgument)
		}
		var physicalSchema []byte
		if err := tx.QueryRowContext(ctx, `
			SELECT value FROM floret_backend_metadata WHERE name = 'physical_schema'
		`).Scan(&physicalSchema); err != nil {
			return fmt.Errorf("%w: invalid backend metadata: %v", ErrInvalidArgument, err)
		}
		if !bytes.Equal(physicalSchema, []byte("1")) {
			return fmt.Errorf("%w: unsupported backend physical schema %q", ErrInvalidArgument, physicalSchema)
		}
	}
	return tx.Commit()
}

func (backend *sqliteBackend) View(ctx context.Context, callback func(ReadTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	if backend.isClosed() {
		return ErrClosed
	}
	sqlTx, err := backend.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return classifySQLiteError(ctx, err)
	}
	defer sqlTx.Rollback()
	tx := &sqliteTx{ctx: ctx, tx: sqlTx, active: true, readOnly: true}
	if err := func() error {
		defer tx.expire()
		return callback(tx)
	}(); err != nil {
		return err
	}
	return ctx.Err()
}

func (backend *sqliteBackend) Update(ctx context.Context, callback func(WriteTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	if backend.isClosed() {
		return ErrClosed
	}
	sqlTx, err := backend.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return classifySQLiteError(ctx, err)
	}
	defer sqlTx.Rollback()
	tx := &sqliteTx{ctx: ctx, tx: sqlTx, active: true}
	if err := func() error {
		defer tx.expire()
		return callback(tx)
	}(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return classifySQLiteError(ctx, sqlTx.Commit())
}

func (backend *sqliteBackend) Close() error {
	backend.closeMu.Lock()
	defer backend.closeMu.Unlock()
	if backend.closed {
		return nil
	}
	backend.closed = true
	return backend.db.Close()
}

func (backend *sqliteBackend) isClosed() bool {
	backend.closeMu.Lock()
	defer backend.closeMu.Unlock()
	return backend.closed
}

type sqliteTx struct {
	mu       sync.Mutex
	ctx      context.Context
	tx       *sql.Tx
	active   bool
	readOnly bool
}

func (tx *sqliteTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return nil, err
	}
	var value []byte
	err := tx.tx.QueryRowContext(tx.ctx, `
		SELECT value FROM floret_backend_records WHERE namespace = ? AND key = ?
	`, namespace, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, classifySQLiteError(tx.ctx, err)
	}
	return bytes.Clone(value), nil
}

func (tx *sqliteTx) Scan(request ScanRequest) (ScanPage, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return ScanPage{}, err
	}
	if request.Namespace == "" || request.Limit <= 0 {
		return ScanPage{}, fmt.Errorf("%w: namespace and positive limit are required", ErrInvalidArgument)
	}
	if len(request.End) > 0 && len(request.Start) > 0 && bytes.Compare(request.Start, request.End) >= 0 {
		return ScanPage{}, fmt.Errorf("%w: scan start must precede end", ErrInvalidArgument)
	}
	query := `SELECT key, value FROM floret_backend_records WHERE namespace = ?`
	arguments := []any{request.Namespace}
	if len(request.Start) > 0 {
		query += ` AND key >= ?`
		arguments = append(arguments, request.Start)
	}
	if len(request.End) > 0 {
		query += ` AND key < ?`
		arguments = append(arguments, request.End)
	}
	if len(request.After) > 0 {
		query += ` AND key > ?`
		arguments = append(arguments, request.After)
	}
	query += ` ORDER BY key LIMIT ?`
	arguments = append(arguments, request.Limit+1)
	rows, err := tx.tx.QueryContext(tx.ctx, query, arguments...)
	if err != nil {
		return ScanPage{}, classifySQLiteError(tx.ctx, err)
	}
	defer rows.Close()
	page := ScanPage{Records: make([]Record, 0, request.Limit)}
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.Key, &record.Value); err != nil {
			return ScanPage{}, classifySQLiteError(tx.ctx, err)
		}
		if len(page.Records) == request.Limit {
			page.HasMore = true
			break
		}
		record.Key = bytes.Clone(record.Key)
		record.Value = bytes.Clone(record.Value)
		page.Records = append(page.Records, record)
	}
	if err := rows.Err(); err != nil {
		return ScanPage{}, classifySQLiteError(tx.ctx, err)
	}
	if page.HasMore {
		page.Next = bytes.Clone(page.Records[len(page.Records)-1].Key)
	}
	return page, nil
}

func (tx *sqliteTx) Put(namespace string, key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	_, err := tx.tx.ExecContext(tx.ctx, `
		INSERT INTO floret_backend_records(namespace, key, value) VALUES (?, ?, ?)
		ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value
	`, namespace, bytes.Clone(key), bytes.Clone(value))
	return classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) Delete(namespace string, key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	result, err := tx.tx.ExecContext(tx.ctx, `
		DELETE FROM floret_backend_records WHERE namespace = ? AND key = ?
	`, namespace, key)
	if err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if deleted == 0 {
		return ErrNotFound
	}
	return nil
}

func (tx *sqliteTx) validate(namespace string, key []byte) error {
	if err := tx.usable(); err != nil {
		return err
	}
	if namespace == "" || len(key) == 0 {
		return fmt.Errorf("%w: namespace and key are required", ErrInvalidArgument)
	}
	return nil
}

func (tx *sqliteTx) validateWrite(namespace string, key []byte) error {
	if err := tx.validate(namespace, key); err != nil {
		return err
	}
	if tx.readOnly {
		return fmt.Errorf("%w: read transaction cannot write", ErrInvalidArgument)
	}
	return nil
}

func (tx *sqliteTx) usable() error {
	if !tx.active {
		return ErrTransactionClosed
	}
	return tx.ctx.Err()
}

func (tx *sqliteTx) expire() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.active = false
	tx.tx = nil
}

func classifySQLiteError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 0xff {
		case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
			return fmt.Errorf("%w: %v", ErrConflict, err)
		}
	}
	return err
}
