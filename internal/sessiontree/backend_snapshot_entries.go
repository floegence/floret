package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Validate array members one at a time. Global source verification must not
// materialize every body in even one unrelated thread's journal.
func visitSnapshotEntries(ctx context.Context, tx spi.ReadTx, value snapshotValueSource, visit func(Entry) error) error {
	reader, err := storagecodec.OpenRecordRange(ctx, tx, value.namespace, value.key, value.span.Offset, value.span.Length)
	if err != nil {
		return err
	}
	digest := sha256.New()
	decoder := json.NewDecoder(io.TeeReader(reader, digest))
	decoder.DisallowUnknownFields()
	token, err := decoder.Token()
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if token != nil {
		if token != json.Delim('[') {
			return ErrAuthorityCorrupt
		}
		for decoder.More() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var entry Entry
			if err := decoder.Decode(&entry); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if visit != nil {
				if err := visit(entry); err != nil {
					return err
				}
			}
		}
		if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return verifySnapshotSpanDigest(value.span, digest)
}
