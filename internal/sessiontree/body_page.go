package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/storagebridge"

	"github.com/floegence/floret/v7/storage/spi"
)

const canonicalBodyPageBytes = 1 << 20

// A single canonical page may reference the same immutable body many times.
// Share its validated bytes only within that read transaction, with a fixed
// retention budget. A later transaction always validates its own source.
type canonicalBodyPageReader struct {
	ctx    context.Context
	tx     spi.ReadTx
	bodies map[canonicalBodyRef]string
	bytes  int
}

func newCanonicalBodyPageReader(ctx context.Context, tx spi.ReadTx) *canonicalBodyPageReader {
	return &canonicalBodyPageReader{ctx: ctx, tx: tx, bodies: make(map[canonicalBodyRef]string)}
}

func (reader *canonicalBodyPageReader) read(ref canonicalBodyRef) (string, error) {
	if err := reader.ctx.Err(); err != nil {
		return "", err
	}
	if value, found := reader.bodies[ref]; found {
		return value, nil
	}
	value, err := materializeCanonicalBody(reader.ctx, reader.tx, ref)
	if err != nil {
		return "", err
	}
	if len(value) <= canonicalBodyPageBytes-reader.bytes {
		reader.bodies[ref] = value
		reader.bytes += len(value)
	}
	return value, nil
}

// Prefetch only bodies that fit this page's existing retention budget. Both
// metadata and blocks remain in the same transaction and undergo the normal
// reference, extent and digest checks before any value is retained.
func (reader *canonicalBodyPageReader) prefetch(refs []canonicalBodyRef) error {
	selected := make([]canonicalBodyRef, 0, len(refs))
	seen := make(map[canonicalBodyRef]bool, len(refs))
	budget := canonicalBodyPageBytes - reader.bytes
	var keys [][]byte
	for _, ref := range refs {
		if err := reader.ctx.Err(); err != nil {
			return err
		}
		if !validCanonicalBodyRef(ref) {
			return ErrAuthorityCorrupt
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		if _, found := reader.bodies[ref]; found || ref.Bytes > budget {
			continue
		}
		budget -= ref.Bytes
		selected = append(selected, ref)
		keys = append(keys, canonicalBodyKey(ref.SHA256))
		for ordinal := 0; ordinal < (ref.Bytes-1)/canonicalBodyBlockBytes+1; ordinal++ {
			keys = append(keys, canonicalBodyBlockKey(ref.SHA256, ordinal))
		}
	}
	records := make(map[string][]byte, len(keys))
	for start := 0; start < len(keys); start += storagebridge.MaxRecordBatch {
		if err := reader.ctx.Err(); err != nil {
			return err
		}
		batch := keys[start:min(len(keys), start+storagebridge.MaxRecordBatch)]
		values, err := storagebridge.ReadRecords(reader.tx, storagebridge.RecordBatch{Namespace: canonicalBodyNamespace, Keys: batch})
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		for i, key := range batch {
			records[string(key)] = values[i]
		}
	}
	tx := canonicalBodyBatchTx{ReadTx: reader.tx, records: records}
	for _, ref := range selected {
		value, err := materializeCanonicalBody(reader.ctx, tx, ref)
		if err != nil {
			return err
		}
		reader.bodies[ref] = value
		reader.bytes += len(value)
	}
	return nil
}

type canonicalBodyBatchTx struct {
	spi.ReadTx
	records map[string][]byte
}

func (tx canonicalBodyBatchTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace != canonicalBodyNamespace {
		return tx.ReadTx.Get(namespace, key)
	}
	value, found := tx.records[string(key)]
	if !found {
		return nil, spi.ErrNotFound
	}
	return value, nil
}
