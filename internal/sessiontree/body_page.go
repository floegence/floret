package sessiontree

import (
	"context"

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
