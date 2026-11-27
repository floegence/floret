package storage

import (
	"bytes"
	"fmt"

	"github.com/floegence/floret/v7/storage/spi"
)

var _ spi.BoundedRecordReader = (*memoryTx)(nil)

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
