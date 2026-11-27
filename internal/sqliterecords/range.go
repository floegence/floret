package sqliterecords

import (
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/floegence/floret/v7/storage/spi"
)

var _ spi.BoundedRecordReader = (*Tx)(nil)

func (tx *Tx) NamespaceHasRecords(namespace string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, []byte{0}); err != nil {
		return false, err
	}
	var found bool
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT EXISTS(SELECT 1 FROM `+tx.table+` WHERE namespace = ? LIMIT 1)`, namespace).Scan(&found)
	return found, ClassifyError(tx.ctx, err)
}

func (tx *Tx) RecordSize(namespace string, key []byte) (int64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return 0, err
	}
	var size int64
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT length(value) FROM `+tx.table+` WHERE namespace = ? AND key = ?`, namespace, key).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, spi.ErrNotFound
	}
	return size, ClassifyError(tx.ctx, err)
}

func (tx *Tx) ReadRecordRange(namespace string, key []byte, offset int64, limit int) ([]byte, error) {
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
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT CASE WHEN ? >= length(value) THEN X'' ELSE substr(value, ?, ?) END FROM `+tx.table+` WHERE namespace = ? AND key = ?`, offset, start, limit, namespace, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, spi.ErrNotFound
	}
	if err != nil {
		return nil, ClassifyError(tx.ctx, err)
	}
	// database/sql owns the detachment from the driver.
	return value, nil
}
