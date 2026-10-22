package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const snapshotImportNamespace = "floret.domain.sessiontree.snapshot-import.v32"

var snapshotImportCheckpointKey = storagecodec.Tuple(storagecodec.TupleString("checkpoint"))

type snapshotImportCheckpoint struct {
	SourceBytes  int64
	Version      int
	Complete     bool
	Scan         storagecodec.JSONScanState
	Digests      []snapshotPartialDigest
	VerifyOffset int64
	VerifyDigest []byte
	ScanDigest   []byte
}

type snapshotSourceSpan struct {
	storagecodec.JSONSpan
	Kind   byte
	Digest []byte
}

func snapshotSourceKey(path ...string) []byte {
	parts := [][]byte{storagecodec.TupleString("span")}
	for _, part := range path {
		parts = append(parts, storagecodec.TupleString(part))
	}
	return storagecodec.Tuple(parts...)
}

func readSnapshotImportCheckpoint(tx spi.ReadTx) (snapshotImportCheckpoint, bool, error) {
	return readJSONImportCheckpoint(tx, snapshotImportCheckpointKey, 2, 5)
}

func readJSONImportCheckpoint(tx spi.ReadTx, key []byte, minimum, maximum int) (snapshotImportCheckpoint, bool, error) {
	raw, err := tx.Get(snapshotImportNamespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return snapshotImportCheckpoint{}, false, nil
	}
	if err != nil {
		return snapshotImportCheckpoint{}, false, err
	}
	var checkpoint snapshotImportCheckpoint
	if err := decodeCanonicalBodyJSON(raw, &checkpoint); err != nil || checkpoint.SourceBytes <= 0 || checkpoint.Scan.Offset < 0 || checkpoint.Scan.Offset > checkpoint.SourceBytes || checkpoint.Complete && (!checkpoint.Scan.Done || checkpoint.Scan.Offset != checkpoint.SourceBytes || checkpoint.Version < minimum || checkpoint.Version > maximum) {
		return checkpoint, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := checkpoint.Scan.Validate(4); err != nil || checkpoint.Version != 0 && (!checkpoint.Scan.Done || checkpoint.Scan.Offset != checkpoint.SourceBytes || checkpoint.Version < minimum || checkpoint.Version > maximum) {
		return checkpoint, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := checkpoint.validateDigests(); err != nil {
		return checkpoint, false, err
	}
	return checkpoint, true, nil
}

func snapshotRecordSize(tx spi.ReadTx) (int64, error) {
	return historicalRecordSize(tx, backendDomainNamespace, backendStateKey)
}

func historicalRecordSize(tx spi.ReadTx, namespace string, key []byte) (int64, error) {
	if bounded, ok := tx.(spi.BoundedRecordReader); ok {
		return bounded.RecordSize(namespace, key)
	}
	raw, err := tx.Get(namespace, key)
	return int64(len(raw)), err
}

// Index the immutable v2-v5 source in bounded transactions. Locators and the
// byte-level parser checkpoint commit together, including in the middle of a
// large body. No imported fact is visible until its complete source is verified.
func indexHistoricalSnapshotBatch(ctx context.Context, tx spi.WriteTx) (snapshotImportCheckpoint, error) {
	checkpoint, found, err := readSnapshotImportCheckpoint(tx)
	if err != nil {
		return checkpoint, err
	}
	fields := snapshotStateFields()
	return indexJSONSourceBatch(ctx, tx, checkpoint, found, jsonMigrationSource{
		namespace: backendDomainNamespace, key: backendStateKey,
		checkpointKey: snapshotImportCheckpointKey, spanKey: snapshotSourceKey,
		indexPath: func(path []string) (bool, error) {
			// Only collection members and individual journal entries need random
			// access. Nested message fields remain inside their original source.
			if len(path) == 4 && (path[0] != "payload" || path[1] != "entries") {
				return false, nil
			}
			if len(path) > 0 && path[0] != "payload" && len(path) != 1 {
				return false, ErrAuthorityCorrupt
			}
			if len(path) == 1 && path[0] != "payload" && path[0] != "version" && path[0] != "kind" {
				return false, ErrAuthorityCorrupt
			}
			if len(path) >= 2 {
				if _, ok := fields[path[1]]; !ok {
					return false, ErrAuthorityCorrupt
				}
			}
			return true, nil
		},
		validate: validateSnapshotSourceShape,
	})
}

type jsonMigrationSource struct {
	namespace          string
	key, checkpointKey []byte
	spanKey            func(...string) []byte
	indexPath          func([]string) (bool, error)
	validate           func(context.Context, spi.ReadTx) (int, error)
}

func indexJSONSourceBatch(ctx context.Context, tx spi.WriteTx, checkpoint snapshotImportCheckpoint, found bool, source jsonMigrationSource) (snapshotImportCheckpoint, error) {
	size, err := historicalRecordSize(tx, source.namespace, source.key)
	if err != nil || size <= 0 {
		return checkpoint, errors.Join(ErrAuthorityCorrupt, err)
	}
	if !found {
		checkpoint.SourceBytes = size
	}
	if checkpoint.SourceBytes != size {
		return checkpoint, ErrAuthorityCorrupt
	}
	if checkpoint.Complete {
		return checkpoint, nil
	}
	if checkpoint.Version != 0 {
		if err := verifyJSONSourceBatch(ctx, tx, source.namespace, source.key, &checkpoint); err != nil {
			return checkpoint, err
		}
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			return checkpoint, err
		}
		return checkpoint, tx.Put(snapshotImportNamespace, source.checkpointKey, raw)
	}
	reader, err := storagecodec.OpenRecordRange(ctx, tx, source.namespace, source.key, checkpoint.Scan.Offset, -1)
	if err != nil {
		return checkpoint, err
	}
	batch, err := io.ReadAll(io.LimitReader(reader, backendThreadMigrationBatchBytes))
	if err != nil {
		return checkpoint, err
	}
	previous := checkpoint
	err = storagecodec.ScanJSON(bytes.NewReader(batch), &checkpoint.Scan, backendThreadMigrationBatchBytes, 4,
		func(span storagecodec.JSONSpan) ([]byte, error) {
			return readJSONSourceBytes(ctx, tx, source.namespace, source.key, span)
		},
		func(path []string, span storagecodec.JSONSpan, kind byte) error {
			keep, err := source.indexPath(path)
			if err != nil || !keep {
				return err
			}
			key := source.spanKey(path...)
			if _, err := tx.Get(snapshotImportNamespace, key); !errors.Is(err, spi.ErrNotFound) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			digest, err := snapshotSpanDigest(previous, batch, span)
			if err != nil {
				return err
			}
			raw, err := json.Marshal(snapshotSourceSpan{JSONSpan: span, Kind: kind, Digest: digest.Sum(nil)})
			if err != nil {
				return err
			}
			return tx.Put(snapshotImportNamespace, key, raw)
		})
	if err != nil {
		return checkpoint, errors.Join(ErrAuthorityCorrupt, err)
	}
	scanDigest := sha256.New()
	if previous.Scan.Offset > 0 {
		scanDigest, err = restoreSnapshotDigest(previous.ScanDigest)
		if err != nil {
			return checkpoint, err
		}
	}
	_, _ = scanDigest.Write(batch[:checkpoint.Scan.Offset-previous.Scan.Offset])
	checkpoint.ScanDigest, err = scanDigest.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return checkpoint, err
	}
	checkpoint.Digests, err = snapshotNextDigests(previous, batch, checkpoint.Scan)
	if err != nil {
		return checkpoint, err
	}
	if checkpoint.Scan.Offset == size {
		if !checkpoint.Scan.Done || checkpoint.Scan.Token != 0 || len(checkpoint.Scan.Frames) != 0 {
			return checkpoint, ErrAuthorityCorrupt
		}
		checkpoint.Version, err = source.validate(ctx, tx)
		if err != nil {
			return checkpoint, err
		}
		if err := verifyJSONSourceBatch(ctx, tx, source.namespace, source.key, &checkpoint); err != nil {
			return checkpoint, err
		}
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return checkpoint, err
	}
	return checkpoint, tx.Put(snapshotImportNamespace, source.checkpointKey, raw)
}

func snapshotStateFields() map[string]reflect.StructField {
	fields := map[string]reflect.StructField{}
	shape := reflect.TypeFor[memoryState]()
	for index := 0; index < shape.NumField(); index++ {
		field := shape.Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		fields[name] = field
	}
	return fields
}

func readSnapshotSourceBytes(ctx context.Context, tx spi.ReadTx, span storagecodec.JSONSpan) ([]byte, error) {
	return readJSONSourceBytes(ctx, tx, backendDomainNamespace, backendStateKey, span)
}

func readJSONSourceBytes(ctx context.Context, tx spi.ReadTx, namespace string, key []byte, span storagecodec.JSONSpan) ([]byte, error) {
	if span.Offset < 0 || span.Length <= 0 {
		return nil, ErrAuthorityCorrupt
	}
	reader, err := storagecodec.OpenRecordRange(ctx, tx, namespace, key, span.Offset, span.Length)
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	return io.ReadAll(reader)
}

func readSnapshotSourceSpan(tx spi.ReadTx, path ...string) (snapshotSourceSpan, error) {
	return readJSONSourceSpan(tx, snapshotSourceKey(path...))
}

func readJSONSourceSpan(tx spi.ReadTx, key []byte) (snapshotSourceSpan, error) {
	raw, err := tx.Get(snapshotImportNamespace, key)
	if err != nil {
		return snapshotSourceSpan{}, err
	}
	var span snapshotSourceSpan
	if err := decodeCanonicalBodyJSON(raw, &span); err != nil || span.Offset < 0 || span.Length <= 0 || len(span.Digest) != sha256.Size || !bytes.ContainsRune([]byte("{[snl"), rune(span.Kind)) {
		return span, errors.Join(ErrAuthorityCorrupt, err)
	}
	return span, nil
}

func decodeSnapshotSourceValue(ctx context.Context, tx spi.ReadTx, target any, path ...string) error {
	span, err := readSnapshotSourceSpan(tx, path...)
	if err != nil {
		return err
	}
	return decodeJSONSourceValue(ctx, tx, backendDomainNamespace, backendStateKey, span, target)
}

func decodeJSONSourceValue(ctx context.Context, tx spi.ReadTx, namespace string, key []byte, span snapshotSourceSpan, target any) error {
	reader, err := storagecodec.OpenRecordRange(ctx, tx, namespace, key, span.Offset, span.Length)
	if err != nil {
		return err
	}
	digest := sha256.New()
	decoder := json.NewDecoder(io.TeeReader(reader, digest))
	decoder.DisallowUnknownFields()
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return spi.ErrInvalidArgument
	}
	if err := decodeSnapshotCollection(decoder, value.Elem()); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return verifySnapshotSpanDigest(span, digest)
}

func validateSnapshotSourceShape(ctx context.Context, tx spi.ReadTx) (int, error) {
	var envelopeVersion, version int
	var kind string
	for _, field := range []struct {
		path   []string
		target any
	}{
		{[]string{"version"}, &envelopeVersion}, {[]string{"kind"}, &kind}, {[]string{"payload", "version"}, &version},
	} {
		if err := decodeSnapshotSourceValue(ctx, tx, field.target, field.path...); err != nil {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	if envelopeVersion != 1 || kind != "sessiontree" || version < 2 || version > 5 {
		return 0, ErrAuthorityCorrupt
	}
	for _, path := range [][]string{nil, {"payload"}} {
		span, err := readSnapshotSourceSpan(tx, path...)
		if err != nil || span.Kind != '{' {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	for name, field := range snapshotStateFields() {
		span, err := readSnapshotSourceSpan(tx, "payload", name)
		optional := strings.Contains(field.Tag.Get("json"), ",omitempty")
		if errors.Is(err, spi.ErrNotFound) && optional {
			continue
		}
		if err != nil {
			return 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		if field.Type.Kind() == reflect.Map && span.Kind != '{' {
			return 0, ErrAuthorityCorrupt
		}
	}
	return version, nil
}
