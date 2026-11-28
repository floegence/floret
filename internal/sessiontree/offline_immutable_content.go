//go:build floret_layout_cutover

package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type offlineBodyReader struct {
	ctx    context.Context
	source spi.ReadTx
	body   canonicalBodyRef
	offset int
}

func (reader *offlineBodyReader) Read(dest []byte) (int, error) {
	if reader.offset == reader.body.Bytes {
		return 0, io.EOF
	}
	data, err := readCanonicalBodyBytes(reader.ctx, reader.source, reader.body, reader.offset, min(len(dest), canonicalBodyReadBytes))
	if err != nil {
		return 0, err
	}
	reader.offset += len(data)
	return copy(dest, data), nil
}

func stageImmutableBody(ctx context.Context, source spi.ReadTx, destination *member.Journal, class string, at int64, body canonicalBodyRef) (member.BodyRef, error) {
	if _, err := readCanonicalBodyRecord(source, body); err != nil {
		return member.BodyRef{}, err
	}
	var digest [sha256.Size]byte
	if _, err := hex.Decode(digest[:], []byte(body.SHA256)); err != nil {
		return member.BodyRef{}, err
	}
	return destination.PutBodyStream(class, at, int64(body.Bytes), digest, &offlineBodyReader{ctx: ctx, source: source, body: body})
}
