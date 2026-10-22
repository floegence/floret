package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

func snapshotJournalCheckpointKey(sequence uint64) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("journal-checkpoint"), storagecodec.TupleOrdinal(sequence))
}
func snapshotJournalSpanKey(sequence uint64, path ...string) []byte {
	parts := [][]byte{storagecodec.TupleString("journal-span"), storagecodec.TupleOrdinal(sequence)}
	for _, part := range path {
		parts = append(parts, storagecodec.TupleString(part))
	}
	return storagecodec.Tuple(parts...)
}

// A recovery frame remains immutable source authority. Index only source spans;
// neither a parsed patch nor a second transcript becomes durable runtime state.
func indexSnapshotJournalBatch(ctx context.Context, tx spi.WriteTx, sequence uint64) (snapshotImportCheckpoint, error) {
	if sequence == 0 {
		return snapshotImportCheckpoint{}, spi.ErrInvalidArgument
	}
	checkpoint, found, err := readJSONImportCheckpoint(tx, snapshotJournalCheckpointKey(sequence), 1, 1)
	if err != nil {
		return checkpoint, err
	}
	return indexJSONSourceBatch(ctx, tx, checkpoint, found, jsonMigrationSource{
		namespace: backendDomainJournalNamespace, key: backendJournalKey(sequence), checkpointKey: snapshotJournalCheckpointKey(sequence),
		spanKey: func(path ...string) []byte { return snapshotJournalSpanKey(sequence, path...) },
		indexPath: func(path []string) (bool, error) {
			if len(path) == 0 {
				return true, nil
			}
			if path[0] != "patch" {
				if len(path) != 1 || path[0] != "version" && path[0] != "sequence" && path[0] != "checksum" {
					return false, ErrAuthorityCorrupt
				}
				return true, nil
			}
			if len(path) >= 2 && path[1] != "set" && path[1] != "delete" && path[1] != "replace" {
				return false, ErrAuthorityCorrupt
			}
			return true, nil
		},
		validate: func(ctx context.Context, tx spi.ReadTx) (int, error) {
			var version int
			var number uint64
			var checksum string
			for _, field := range []struct {
				name   string
				target any
			}{{"version", &version}, {"sequence", &number}, {"checksum", &checksum}} {
				if err := decodeSnapshotJournalValue(ctx, tx, sequence, field.target, field.name); err != nil {
					return 0, err
				}
			}
			digest, err := hex.DecodeString(checksum)
			if err != nil || len(digest) != sha256.Size || checksum != hex.EncodeToString(digest) || version != 1 || number != sequence {
				return 0, ErrAuthorityCorrupt
			}
			for _, path := range [][]string{nil, {"patch"}} {
				span, err := readJSONSourceSpan(tx, snapshotJournalSpanKey(sequence, path...))
				if err != nil || span.Kind != '{' {
					return 0, errors.Join(ErrAuthorityCorrupt, err)
				}
			}
			return 1, nil
		},
	})
}

func decodeSnapshotJournalValue(ctx context.Context, tx spi.ReadTx, sequence uint64, target any, path ...string) error {
	span, err := readJSONSourceSpan(tx, snapshotJournalSpanKey(sequence, path...))
	if err != nil {
		return err
	}
	return decodeJSONSourceValue(ctx, tx, backendDomainJournalNamespace, backendJournalKey(sequence), span, target)
}

type namedSnapshotSpan struct {
	name string
	span snapshotSourceSpan
}

func indexedJSONMembers(ctx context.Context, tx spi.ReadTx, prefix []byte) ([]namedSnapshotSpan, error) {
	var result []namedSnapshotSpan
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: snapshotImportNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return nil, err
		}
		for _, row := range page.Records {
			if bytes.Equal(row.Key, prefix) {
				continue
			}
			if !bytes.HasPrefix(row.Key, prefix) {
				return nil, ErrAuthorityCorrupt
			}
			tail := row.Key[len(prefix):]
			if len(tail) < 5 || tail[0] != 1 {
				return nil, ErrAuthorityCorrupt
			}
			length := uint64(binary.BigEndian.Uint32(tail[1:5]))
			if length > uint64(len(tail)-5) {
				return nil, ErrAuthorityCorrupt
			}
			if length != uint64(len(tail)-5) {
				continue
			}
			span, err := readJSONSourceSpan(tx, row.Key)
			if err != nil {
				return nil, err
			}
			result = append(result, namedSnapshotSpan{name: string(tail[5:]), span: span})
		}
		if !page.HasMore {
			break
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return nil, ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].name < result[j].name })
	return result, nil
}

// Match encoding/json's exact checksum encoding of the released patch shape.
// Raw replacement values stream directly; outer maps retain metadata keys only.
func verifySnapshotJournalFrame(ctx context.Context, tx spi.ReadTx, sequence uint64) error {
	checkpoint, found, err := readJSONImportCheckpoint(tx, snapshotJournalCheckpointKey(sequence), 1, 1)
	if err != nil || !found || !checkpoint.Complete {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	size, err := historicalRecordSize(tx, backendDomainJournalNamespace, backendJournalKey(sequence))
	if err != nil || size != checkpoint.SourceBytes {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	// Value locators alone do not cover keys or skipped wrapper bytes. Recheck
	// the indexed source digest before trusting its patch field names.
	if err := verifyIndexedJSONRecord(ctx, tx, backendDomainJournalNamespace, backendJournalKey(sequence), checkpoint); err != nil {
		return err
	}
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "{\"version\":1,\"sequence\":%d,\"patch\":{", sequence)
	comma := ""
	for _, operation := range []string{"set", "delete", "replace"} {
		span, err := readJSONSourceSpan(tx, snapshotJournalSpanKey(sequence, "patch", operation))
		if errors.Is(err, spi.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if span.Kind == 'l' {
			var value any
			if err := decodeSnapshotJournalValue(ctx, tx, sequence, &value, "patch", operation); err != nil || value != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			continue
		}
		if span.Kind != '{' {
			return ErrAuthorityCorrupt
		}
		fields, err := indexedJSONMembers(ctx, tx, snapshotJournalSpanKey(sequence, "patch", operation))
		if err != nil {
			return err
		}
		if len(fields) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(digest, "%s%q:{", comma, operation)
		comma = ","
		for i, field := range fields {
			if i > 0 {
				_, _ = io.WriteString(digest, ",")
			}
			key, _ := json.Marshal(field.name)
			_, _ = digest.Write(key)
			_, _ = io.WriteString(digest, ":")
			if operation == "set" && field.span.Kind == '{' {
				members, err := indexedJSONMembers(ctx, tx, snapshotJournalSpanKey(sequence, "patch", operation, field.name))
				if err != nil {
					return err
				}
				_, _ = io.WriteString(digest, "{")
				for j, member := range members {
					if j > 0 {
						_, _ = io.WriteString(digest, ",")
					}
					key, _ := json.Marshal(member.name)
					_, _ = digest.Write(key)
					_, _ = io.WriteString(digest, ":")
					if err := writeSnapshotJournalRaw(ctx, tx, sequence, member.span, digest); err != nil {
						return err
					}
				}
				_, _ = io.WriteString(digest, "}")
			} else if operation == "delete" {
				var keys []string
				if err := decodeJSONSourceValue(ctx, tx, backendDomainJournalNamespace, backendJournalKey(sequence), field.span, &keys); err != nil {
					return err
				}
				encoded, err := json.Marshal(keys)
				if err != nil {
					return err
				}
				_, _ = digest.Write(encoded)
			} else {
				if operation == "set" {
					var value any
					if field.span.Kind != 'l' {
						return ErrAuthorityCorrupt
					}
					if err := decodeJSONSourceValue(ctx, tx, backendDomainJournalNamespace, backendJournalKey(sequence), field.span, &value); err != nil || value != nil {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
				}
				if err := writeSnapshotJournalRaw(ctx, tx, sequence, field.span, digest); err != nil {
					return err
				}
			}
		}
		_, _ = io.WriteString(digest, "}")
	}
	_, _ = io.WriteString(digest, "}}")
	var expected string
	if err := decodeSnapshotJournalValue(ctx, tx, sequence, &expected, "checksum"); err != nil {
		return err
	}
	if hex.EncodeToString(digest.Sum(nil)) != expected {
		return ErrAuthorityCorrupt
	}
	return nil
}

func writeSnapshotJournalRaw(ctx context.Context, tx spi.ReadTx, sequence uint64, span snapshotSourceSpan, output io.Writer) error {
	reader, err := storagecodec.OpenRecordRange(ctx, tx, backendDomainJournalNamespace, backendJournalKey(sequence), span.Offset, span.Length)
	if err != nil {
		return err
	}
	digest := sha256.New()
	if err := storagecodec.WriteCompactJSON(output, io.TeeReader(reader, digest)); err != nil {
		return err
	}
	return verifySnapshotSpanDigest(span, digest)
}
