package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const migrationBodyNamespace = "floret.domain.sessiontree.migration.bodies.v32"

// A staging claim protects immutable blocks from foreground reclamation until
// the owning entry and its reference count commit together. It is never a
// readable body or execution authority.
func migrationBodyKey(ref canonicalBodyRef, threadID string, ordinal int, field string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(ref.SHA256), storagecodec.TupleString(threadID), storagecodec.TupleOrdinal(uint64(ordinal)), storagecodec.TupleString(field))
}

func migrationBodyClaimed(tx spi.ReadTx, digest string) (bool, error) {
	prefix := storagecodec.Tuple(storagecodec.TupleString(digest))
	page, err := tx.Scan(spi.ScanRequest{Namespace: migrationBodyNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 1})
	return len(page.Records) != 0, err
}

func migrationSourceFormat(version int) (backendDomainFormat, error) {
	switch version {
	case backendDomainV27Version:
		return backendDomainV27Format, nil
	case backendDomainV28Version:
		return backendDomainV28Format, nil
	case backendDomainV29Version:
		return backendDomainV29Format, nil
	case backendDomainV30Version:
		return backendDomainV30Format, nil
	case backendDomainV31Version:
		return backendDomainV31Format, nil
	default:
		return backendDomainFormat{}, ErrAuthorityCorrupt
	}
}

// These released edges preserve canonical facts: v27 -> v28 fences request
// measurement, v28 -> v29 admits explicit Goal continuation, v29 -> v30 segments
// text, and v30 -> v31 segments typed activity. Source-specific validation still
// rejects future contracts before the current invariant is checked.
func migrationStoredEntry(source backendDomainFormat, record backendDomainRecord) (canonicalStoredEntry, error) {
	if source.version >= backendDomainV30Version {
		return decodeCanonicalStoredEntry(source, record)
	}
	var entry Entry
	if err := decodeBackendDomainValue(source, record, &entry); err != nil {
		return canonicalStoredEntry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.Kind != backendDomainRecordEntry || entry.ID != record.ID || entry.ThreadID != record.ThreadID || record.ID == "" || record.ThreadID == "" || record.Ordinal < 0 {
		return canonicalStoredEntry{}, ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return canonicalStoredEntry{}, err
	}
	if source.version < backendDomainV29Version {
		if err := validateTurnKindEntries(entry.ThreadID, []Entry{entry}, false); err != nil {
			return canonicalStoredEntry{}, err
		}
	}
	entry.Raw = ""
	return canonicalStoredEntry{Entry: entry, Fields: map[string]canonicalBodyRef{}}, nil
}

func expectedMigrationStoredEntry(original canonicalStoredEntry) (canonicalStoredEntry, map[string]string, error) {
	generated, err := canonicalStoredFields(original.Entry, true)
	if err != nil {
		return canonicalStoredEntry{}, nil, err
	}
	candidate := original
	candidate.Fields = make(map[string]canonicalBodyRef, len(original.Fields)+len(generated))
	for field, ref := range original.Fields {
		candidate.Fields[field] = ref
	}
	for field, value := range generated {
		if _, exists := candidate.Fields[field]; exists {
			return canonicalStoredEntry{}, nil, ErrAuthorityCorrupt
		}
		candidate.Fields[field] = canonicalBodyRef{SHA256: stableHash(value), Bytes: len(value)}
		if err := clearCanonicalField(&candidate.Entry, field); err != nil {
			return canonicalStoredEntry{}, nil, err
		}
	}
	return candidate, generated, nil
}

func compareMigrationStoredEntry(source backendDomainFormat, original, candidate canonicalStoredEntry) error {
	expected, _, err := expectedMigrationStoredEntry(original)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, candidate) {
		return ErrAuthorityCorrupt
	}
	return nil
}

// Body blocks are staged without a published reference count. The owning entry
// retains all fields exactly once in its final transaction. Checkpoint progress
// only selects immutable bytes and never becomes runtime authority.
func stageMigrationBodies(ctx context.Context, tx spi.WriteTx, checkpoint *threadMigrationCheckpoint, original canonicalStoredEntry) (canonicalStoredEntry, bool, error) {
	candidate, generated, err := expectedMigrationStoredEntry(original)
	if err != nil {
		return candidate, false, err
	}
	fields := make([]string, 0, len(generated))
	for field := range generated {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	if checkpoint.BodyField < 0 || checkpoint.BodyField > len(fields) {
		return candidate, false, ErrAuthorityCorrupt
	}
	budget := backendThreadMigrationBatchBytes / (2 * canonicalBodyBlockBytes)
	for checkpoint.BodyField < len(fields) {
		field := fields[checkpoint.BodyField]
		value, ref := generated[field], candidate.Fields[field]
		if checkpoint.BodySHA256 != "" && checkpoint.BodySHA256 != ref.SHA256 {
			return candidate, false, ErrAuthorityCorrupt
		}
		claim, err := json.Marshal(ref)
		if err != nil {
			return candidate, false, err
		}
		if err := tx.Put(migrationBodyNamespace, migrationBodyKey(ref, checkpoint.ThreadID, checkpoint.NextOrdinal, field), claim); err != nil {
			return candidate, false, err
		}
		if _, err := tx.Get(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256)); err == nil {
			if _, err := readCanonicalBodyRecord(tx, ref); err != nil {
				return candidate, false, err
			}
		} else if !errors.Is(err, spi.ErrNotFound) {
			return candidate, false, err
		} else {
			blocks := (len(value)-1)/canonicalBodyBlockBytes + 1
			if checkpoint.BodyBlocks < 0 || checkpoint.BodyBlocks > blocks {
				return candidate, false, ErrAuthorityCorrupt
			}
			checkpoint.BodySHA256 = ref.SHA256
			end := min(blocks, checkpoint.BodyBlocks+budget)
			for ordinal := checkpoint.BodyBlocks; ordinal < end; ordinal++ {
				if err := ctx.Err(); err != nil {
					return candidate, false, err
				}
				start := ordinal * canonicalBodyBlockBytes
				payload, err := json.Marshal([]byte(value[start:min(len(value), start+canonicalBodyBlockBytes)]))
				if err != nil {
					return candidate, false, err
				}
				encoded, err := storagecodec.EncodeEnvelope("sessiontree-v30-body-block", payload)
				if err != nil {
					return candidate, false, err
				}
				if err := tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, ordinal), encoded); err != nil {
					return candidate, false, err
				}
			}
			budget -= end - checkpoint.BodyBlocks
			checkpoint.BodyBlocks = end
			if end != blocks {
				return candidate, false, nil
			}
		}
		checkpoint.BodyField++
		checkpoint.BodyBlocks, checkpoint.BodySHA256 = 0, ""
	}
	checkpoint.BodyField = 0
	return candidate, true, nil
}

func retainMigrationFields(tx spi.WriteTx, checkpoint threadMigrationCheckpoint, source, candidate canonicalStoredEntry) error {
	for field, ref := range candidate.Fields {
		if _, err := tx.Get(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256)); err == nil {
			body, err := readCanonicalBodyRecord(tx, ref)
			if err != nil || body.References == int64(^uint64(0)>>1) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			body.References++
			if err := writeCanonicalBodyRecord(tx, body); err != nil {
				return err
			}
		} else if !errors.Is(err, spi.ErrNotFound) {
			return err
		} else {
			if _, retained := source.Fields[field]; retained {
				return ErrAuthorityCorrupt
			}
			if err := writeCanonicalBodyRecord(tx, canonicalBodyRecord{canonicalBodyRef: ref, References: 1}); err != nil {
				return err
			}
		}
		if _, retained := source.Fields[field]; !retained {
			if err := tx.Delete(migrationBodyNamespace, migrationBodyKey(ref, checkpoint.ThreadID, checkpoint.NextOrdinal, field)); err != nil {
				return err
			}
		}
	}
	return nil
}
