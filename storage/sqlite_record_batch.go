package storage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func (tx *sqliteTx) ReadFloretRecords(request storagebridge.RecordBatch) ([][]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return nil, err
	}
	if request.Namespace == "" || len(request.Keys) == 0 || len(request.Keys) > storagebridge.MaxRecordBatch {
		return nil, spi.ErrInvalidArgument
	}
	positions := make(map[string][]int, len(request.Keys))
	arguments := []any{request.Namespace}
	for i, key := range request.Keys {
		if len(key) == 0 {
			return nil, spi.ErrInvalidArgument
		}
		positions[string(key)] = append(positions[string(key)], i)
		arguments = append(arguments, key)
	}
	query := "SELECT key, value FROM floret_backend_records WHERE namespace = ? AND key IN (" + strings.TrimSuffix(strings.Repeat("?,", len(request.Keys)), ",") + ")"
	rows, err := tx.tx.QueryContext(tx.ctx, query, arguments...)
	if err != nil {
		return nil, classifySQLiteError(tx.ctx, err)
	}
	defer rows.Close()
	result := make([][]byte, len(request.Keys))
	for rows.Next() {
		var key, value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, classifySQLiteError(tx.ctx, err)
		}
		indices, found := positions[string(key)]
		if !found {
			return nil, fmt.Errorf("%w: unexpected batch record", spi.ErrConflict)
		}
		for i, index := range indices {
			if i == 0 {
				result[index] = value
			} else {
				result[index] = bytes.Clone(value)
			}
		}
		delete(positions, string(key))
	}
	if err := rows.Err(); err != nil {
		return nil, classifySQLiteError(tx.ctx, err)
	}
	if len(positions) > 0 {
		return nil, spi.ErrNotFound
	}
	return result, nil
}

// Coalesce a bounded write set without committing or changing the durability
// of the surrounding transaction. Duplicate keys retain ordered Put semantics.
func (tx *sqliteTx) WriteFloretRecords(namespace string, records []spi.Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return err
	}
	if namespace == "" || len(records) == 0 || len(records) > storagebridge.MaxRecordBatch {
		return spi.ErrInvalidArgument
	}
	arguments := make([]any, 0, len(records)*3)
	for _, record := range records {
		if err := tx.validateWrite(namespace, record.Key); err != nil {
			return err
		}
		arguments = append(arguments, namespace, bytes.Clone(record.Key), bytes.Clone(record.Value))
	}
	query := "INSERT INTO floret_backend_records(namespace, key, value) VALUES " + strings.TrimSuffix(strings.Repeat("(?,?,?),", len(records)), ",") + " ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value"
	_, err := tx.tx.ExecContext(tx.ctx, query, arguments...)
	return classifySQLiteError(tx.ctx, err)
}
