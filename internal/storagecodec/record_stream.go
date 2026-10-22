package storagecodec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/floegence/floret/v7/storage/spi"
)

const recordReadBlockBytes = 64 << 10

// OpenRecord keeps every physical read bounded when the backend supports it.
// The reader belongs to tx and must be consumed before that transaction ends.
// Custom backends without range access retain their existing Get contract.
func OpenRecord(ctx context.Context, tx spi.ReadTx, namespace string, key []byte) (io.Reader, error) {
	return OpenRecordRange(ctx, tx, namespace, key, 0, -1)
}

// OpenRecordRange opens an immutable source span. A negative length reads to
// the end; explicit spans must fit completely inside the source record.
func OpenRecordRange(ctx context.Context, tx spi.ReadTx, namespace string, key []byte, offset, length int64) (io.Reader, error) {
	if ctx == nil || tx == nil || namespace == "" || len(key) == 0 {
		return nil, spi.ErrInvalidArgument
	}
	if offset < 0 || length < -1 {
		return nil, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bounded, ok := tx.(spi.BoundedRecordReader); ok {
		size, err := bounded.RecordSize(namespace, key)
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, errors.New("storage record has negative size")
		}
		if offset > size || length >= 0 && length > size-offset {
			return nil, io.ErrUnexpectedEOF
		}
		if length >= 0 {
			size = offset + length
		}
		return &recordStream{ctx: ctx, tx: bounded, namespace: namespace, key: bytes.Clone(key), size: size, offset: offset}, nil
	}
	raw, err := tx.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	if offset > int64(len(raw)) || length >= 0 && length > int64(len(raw))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	end := int64(len(raw))
	if length >= 0 {
		end = offset + length
	}
	return &contextReader{ctx: ctx, reader: bytes.NewReader(raw[offset:end])}, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(destination)
}

type recordStream struct {
	ctx          context.Context
	tx           spi.BoundedRecordReader
	namespace    string
	key          []byte
	size, offset int64
}

func (reader *recordStream) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if len(destination) == 0 {
		return 0, nil
	}
	if reader.offset == reader.size {
		return 0, io.EOF
	}
	limit := int(min(int64(len(destination)), int64(recordReadBlockBytes), reader.size-reader.offset))
	part, err := reader.tx.ReadRecordRange(reader.namespace, reader.key, reader.offset, limit)
	if err != nil {
		return 0, err
	}
	if len(part) == 0 || len(part) > limit {
		return 0, errors.New("storage range read disagrees with record size")
	}
	count := copy(destination, part)
	reader.offset += int64(count)
	return count, nil
}

// DecodeEnvelopeStream consumes the payload directly from the source decoder.
// No envelope-sized RawMessage or second copy of the payload is allocated.
// Consumers must decode exactly one payload value. The caller must not publish
// any result until the entire envelope, including trailing input, is validated.
func DecodeEnvelopeStream(source io.Reader, kind string, consume func(*json.Decoder) error) error {
	if source == nil || kind == "" || consume == nil {
		return errors.New("storage envelope requires source, kind, and payload consumer")
	}
	decoder := json.NewDecoder(source)
	decoder.DisallowUnknownFields()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.Join(errors.New("invalid storage envelope"), err)
	}
	seen := map[string]bool{}
	var version int
	var actualKind string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok || seen[name] {
			return errors.New("ambiguous storage envelope field")
		}
		seen[name] = true
		switch name {
		case "version":
			err = decoder.Decode(&version)
		case "kind":
			err = decoder.Decode(&actualKind)
		case "payload":
			err = consume(decoder)
		default:
			return fmt.Errorf("unknown storage envelope field %q", name)
		}
		if err != nil {
			return err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.Join(errors.New("incomplete storage envelope"), err)
	}
	if version != envelopeVersion || actualKind != kind || !seen["payload"] {
		return errors.New("unsupported storage envelope")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.Join(errors.New("storage envelope contains trailing data"), err)
	}
	return nil
}
