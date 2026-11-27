package sqliterecords

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/floegence/floret/v7/storage/spi"
)

type Tx struct {
	mu              sync.Mutex
	ctx             context.Context
	tx              *sql.Tx
	table           string
	active          bool
	readOnly        bool
	getStatement    *sql.Stmt
	putStatement    *sql.Stmt
	deleteStatement *sql.Stmt
}

func (tx *Tx) Get(namespace string, key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return nil, err
	}
	if tx.getStatement == nil {
		statement, err := tx.tx.PrepareContext(tx.ctx, `SELECT value FROM `+tx.table+` WHERE namespace = ? AND key = ?`)
		if err != nil {
			return nil, ClassifyError(tx.ctx, err)
		}
		tx.getStatement = statement
	}
	var value []byte
	err := tx.getStatement.QueryRowContext(tx.ctx, namespace, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, spi.ErrNotFound
	}
	if err != nil {
		return nil, ClassifyError(tx.ctx, err)
	}
	// database/sql Scan into []byte already returns caller-owned storage.
	return value, nil
}

func (tx *Tx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return spi.ScanPage{}, err
	}
	if request.Namespace == "" || request.Limit <= 0 {
		return spi.ScanPage{}, fmt.Errorf("%w: namespace and positive limit are required", spi.ErrInvalidArgument)
	}
	if len(request.End) > 0 && len(request.Start) > 0 && bytes.Compare(request.Start, request.End) >= 0 {
		return spi.ScanPage{}, fmt.Errorf("%w: scan start must precede end", spi.ErrInvalidArgument)
	}
	query := `SELECT key, value FROM `+tx.table+` WHERE namespace = ?`
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
		return spi.ScanPage{}, ClassifyError(tx.ctx, err)
	}
	defer rows.Close()
	page := spi.ScanPage{Records: make([]spi.Record, 0, request.Limit)}
	for rows.Next() {
		var record spi.Record
		if err := rows.Scan(&record.Key, &record.Value); err != nil {
			return spi.ScanPage{}, ClassifyError(tx.ctx, err)
		}
		if len(page.Records) == request.Limit {
			page.HasMore = true
			break
		}
		// Scan into []byte detaches both columns from the driver and other rows.
		page.Records = append(page.Records, record)
	}
	if err := rows.Err(); err != nil {
		return spi.ScanPage{}, ClassifyError(tx.ctx, err)
	}
	if page.HasMore {
		page.Next = bytes.Clone(page.Records[len(page.Records)-1].Key)
	}
	return page, nil
}

func (tx *Tx) Put(namespace string, key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	if tx.putStatement == nil {
		statement, err := tx.tx.PrepareContext(tx.ctx, `
		INSERT INTO `+tx.table+`(namespace, key, value) VALUES (?, ?, ?)
		ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value WHERE floret_backend_records.value IS NOT excluded.value
	`)
		if err != nil {
			return ClassifyError(tx.ctx, err)
		}
		tx.putStatement = statement
	}
	_, err := tx.putStatement.ExecContext(tx.ctx, namespace, bytes.Clone(key), bytes.Clone(value))
	return ClassifyError(tx.ctx, err)
}

func (tx *Tx) Delete(namespace string, key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	if tx.deleteStatement == nil {
		statement, err := tx.tx.PrepareContext(tx.ctx, `
		DELETE FROM `+tx.table+` WHERE namespace = ? AND key = ?
	`)
		if err != nil {
			return ClassifyError(tx.ctx, err)
		}
		tx.deleteStatement = statement
	}
	result, err := tx.deleteStatement.ExecContext(tx.ctx, namespace, key)
	if err != nil {
		return ClassifyError(tx.ctx, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if deleted == 0 {
		return spi.ErrNotFound
	}
	return nil
}

func (tx *Tx) validate(namespace string, key []byte) error {
	if err := tx.usable(); err != nil {
		return err
	}
	if namespace == "" || len(key) == 0 {
		return fmt.Errorf("%w: namespace and key are required", spi.ErrInvalidArgument)
	}
	return nil
}

func (tx *Tx) validateWrite(namespace string, key []byte) error {
	if err := tx.validate(namespace, key); err != nil {
		return err
	}
	if tx.readOnly {
		return fmt.Errorf("%w: read transaction cannot write", spi.ErrInvalidArgument)
	}
	return nil
}

func (tx *Tx) usable() error {
	if !tx.active {
		return spi.ErrTransactionClosed
	}
	return tx.ctx.Err()
}

func End(tx *Tx) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.active = false
	tx.tx = nil
}

func ClassifyError(ctx context.Context, err error) error {
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
			return fmt.Errorf("%w: %v", spi.ErrConflict, err)
		}
	}
	return err
}
