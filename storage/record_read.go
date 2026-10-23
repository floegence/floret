package storage

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/floegence/floret/v7/storage/spi"
)

var _ spi.BoundedRecordReader = (*memoryTx)(nil)
var _ spi.BoundedRecordReader = (*sqliteTx)(nil)

func (tx *memoryTx) NamespaceHasRecords(namespace string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, []byte{0}); err != nil {
		return false, err
	}
	return len(tx.records[namespace]) != 0, nil
}

func (tx *memoryTx) RecordSize(namespace string, key []byte) (int64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return 0, err
	}
	value, ok := tx.records[namespace][string(key)]
	if !ok {
		return 0, spi.ErrNotFound
	}
	return int64(len(value)), nil
}

func (tx *memoryTx) ReadRecordRange(namespace string, key []byte, offset int64, limit int) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return nil, err
	}
	if offset < 0 || limit <= 0 {
		return nil, fmt.Errorf("%w: nonnegative offset and positive limit are required", spi.ErrInvalidArgument)
	}
	value, ok := tx.records[namespace][string(key)]
	if !ok {
		return nil, spi.ErrNotFound
	}
	if offset >= int64(len(value)) {
		return []byte{}, nil
	}
	length := min(int64(limit), int64(len(value))-offset)
	return bytes.Clone(value[offset : offset+length]), nil
}

func (tx *sqliteTx) NamespaceHasRecords(namespace string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, []byte{0}); err != nil {
		return false, err
	}
	var found bool
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT EXISTS(SELECT 1 FROM floret_backend_records WHERE namespace = ? LIMIT 1)`, namespace).Scan(&found)
	return found, classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) RecordSize(namespace string, key []byte) (int64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return 0, err
	}
	var size int64
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT length(value) FROM floret_backend_records WHERE namespace = ? AND key = ?`, namespace, key).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, spi.ErrNotFound
	}
	return size, classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) ReadRecordRange(namespace string, key []byte, offset int64, limit int) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return nil, err
	}
	if offset < 0 || limit <= 0 {
		return nil, fmt.Errorf("%w: nonnegative offset and positive limit are required", spi.ErrInvalidArgument)
	}
	// SQLite uses one-based byte positions. The largest valid offset is
	// necessarily beyond a stored value, without overflowing offset + 1.
	start := offset
	if start < math.MaxInt64 {
		start++
	}
	var value []byte
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT CASE WHEN ? >= length(value) THEN X'' ELSE substr(value, ?, ?) END FROM floret_backend_records WHERE namespace = ? AND key = ?`, offset, start, limit, namespace, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, spi.ErrNotFound
	}
	if err != nil {
		return nil, classifySQLiteError(tx.ctx, err)
	}
	// database/sql owns the detachment from the driver.
	return value, nil
}
