package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const (
	canonicalBodyBlockBytes = 16 << 10
	canonicalBodyReadBytes  = 64 << 10
	canonicalBodyNamespace  = "floret.domain.sessiontree.bodies.v30"
)

type canonicalBodyRef struct {
	SHA256 string
	Bytes  int
}

type canonicalBodyRecord struct {
	canonicalBodyRef
	References int64
}

func canonicalBodyKey(digest string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("body"), storagecodec.TupleString(digest))
}

func canonicalBodyBlockKey(digest string, ordinal int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("block"), storagecodec.TupleString(digest), storagecodec.TupleOrdinal(uint64(ordinal)))
}

func validCanonicalBodyRef(ref canonicalBodyRef) bool {
	digest, err := hex.DecodeString(ref.SHA256)
	return err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == ref.SHA256 && ref.Bytes > 0
}

func readCanonicalBodyRecord(tx spi.ReadTx, ref canonicalBodyRef) (canonicalBodyRecord, error) {
	if !validCanonicalBodyRef(ref) {
		return canonicalBodyRecord{}, ErrAuthorityCorrupt
	}
	raw, err := tx.Get(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256))
	if err != nil {
		return canonicalBodyRecord{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var body canonicalBodyRecord
	payload, err := storagecodec.DecodeEnvelope(raw, "sessiontree-v30-body")
	if err != nil {
		return body, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := decodeCanonicalBodyJSON(payload, &body); err != nil {
		return body, errors.Join(ErrAuthorityCorrupt, err)
	}
	if body.canonicalBodyRef != ref || body.References <= 0 {
		return body, ErrAuthorityCorrupt
	}
	return body, nil
}

func writeCanonicalBodyRecord(tx spi.WriteTx, body canonicalBodyRecord) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	raw, err := storagecodec.EncodeEnvelope("sessiontree-v30-body", payload)
	if err != nil {
		return err
	}
	return tx.Put(canonicalBodyNamespace, canonicalBodyKey(body.SHA256), raw)
}

func retainCanonicalBody(ctx context.Context, tx spi.WriteTx, text string) (canonicalBodyRef, error) {
	if err := ctx.Err(); err != nil {
		return canonicalBodyRef{}, err
	}
	ref := canonicalBodyRef{SHA256: stableHash(text), Bytes: len(text)}
	if !validCanonicalBodyRef(ref) {
		return canonicalBodyRef{}, ErrAuthorityCorrupt
	}
	if _, err := tx.Get(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256)); err == nil {
		body, err := readCanonicalBodyRecord(tx, ref)
		if err != nil {
			return canonicalBodyRef{}, err
		}
		if body.References == int64(^uint64(0)>>1) {
			return canonicalBodyRef{}, ErrAuthorityCorrupt
		}
		body.References++
		return ref, writeCanonicalBodyRecord(tx, body)
	} else if !errors.Is(err, spi.ErrNotFound) {
		return canonicalBodyRef{}, err
	}
	for offset, ordinal := 0, 0; offset < len(text); ordinal++ {
		if err := ctx.Err(); err != nil {
			return canonicalBodyRef{}, err
		}
		end := min(len(text), offset+canonicalBodyBlockBytes)
		payload, err := json.Marshal([]byte(text[offset:end]))
		if err != nil {
			return canonicalBodyRef{}, err
		}
		raw, err := storagecodec.EncodeEnvelope("sessiontree-v30-body-block", payload)
		if err != nil {
			return canonicalBodyRef{}, err
		}
		if err := tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, ordinal), raw); err != nil {
			return canonicalBodyRef{}, err
		}
		offset = end
	}
	return ref, writeCanonicalBodyRecord(tx, canonicalBodyRecord{canonicalBodyRef: ref, References: 1})
}

func releaseCanonicalBody(ctx context.Context, tx spi.WriteTx, ref canonicalBodyRef) error {
	body, err := readCanonicalBodyRecord(tx, ref)
	if err != nil {
		return err
	}
	body.References--
	if body.References > 0 {
		return writeCanonicalBodyRecord(tx, body)
	}
	for ordinal := 0; ordinal < (ref.Bytes-1)/canonicalBodyBlockBytes+1; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, ordinal)); err != nil {
			return err
		}
	}
	releaseCachedBody(ref)
	return tx.Delete(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256))
}

// Physical blocks preserve byte offsets even when an encoded rune spans blocks.
// The public text boundary selects complete UTF-8 runes.
func readCanonicalBodyBytes(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, offset, limit int) ([]byte, error) {
	if !validCanonicalBodyRef(ref) || offset < 0 || offset > ref.Bytes || limit < 0 || limit > canonicalBodyReadBytes {
		return nil, spi.ErrInvalidArgument
	}
	end := offset + min(limit, ref.Bytes-offset)
	result := make([]byte, 0, end-offset)
	for position := offset; position < end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal := position / canonicalBodyBlockBytes
		raw, err := tx.Get(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, ordinal))
		if err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		payload, err := storagecodec.DecodeEnvelope(raw, "sessiontree-v30-body-block")
		if err != nil {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		var block []byte
		err = decodeCanonicalBodyJSON(payload, &block)
		want := min(canonicalBodyBlockBytes, ref.Bytes-ordinal*canonicalBodyBlockBytes)
		if err != nil || len(block) != want {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		start := position % canonicalBodyBlockBytes
		n := min(end-position, len(block)-start)
		result = append(result, block[start:start+n]...)
		position += n
	}
	return result, nil
}

func readCanonicalBodyText(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, offset, limit int) (string, error) {
	if limit < utf8.UTFMax || limit > canonicalBodyBlockBytes {
		return "", spi.ErrInvalidArgument
	}
	if _, err := readCanonicalBodyRecord(tx, ref); err != nil {
		return "", err
	}
	text, err := readCanonicalBodyBytes(ctx, tx, ref, offset, limit)
	if err != nil {
		return "", err
	}
	if len(text) > 0 && !utf8.RuneStart(text[0]) {
		return "", spi.ErrInvalidArgument
	}
	if offset+len(text) < ref.Bytes && len(text) > 0 {
		start := len(text) - 1
		for start > 0 && !utf8.RuneStart(text[start]) {
			start--
		}
		if !utf8.FullRune(text[start:]) {
			text = text[:start]
		}
	}
	if !utf8.Valid(text) {
		return "", ErrAuthorityCorrupt
	}
	return string(text), nil
}

func visitCanonicalBody(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, visit func([]byte) error) error {
	if _, err := readCanonicalBodyRecord(tx, ref); err != nil {
		return err
	}
	hash := sha256.New()
	for offset := 0; offset < ref.Bytes; {
		part, err := readCanonicalBodyBytes(ctx, tx, ref, offset, canonicalBodyReadBytes)
		if err != nil {
			return err
		}
		hash.Write(part)
		if err := visit(part); err != nil {
			return err
		}
		offset += len(part)
	}
	if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return ErrAuthorityCorrupt
	}
	return nil
}

func materializeCanonicalBody(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef) (string, error) {
	var result strings.Builder
	if !validCanonicalBodyRef(ref) {
		return "", ErrAuthorityCorrupt
	}
	result.Grow(ref.Bytes)
	err := visitCanonicalBody(ctx, tx, ref, func(part []byte) error { _, err := result.Write(part); return err })
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

func decodeCanonicalBodyJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrAuthorityCorrupt
	}
	return nil
}

type canonicalStoredEntry struct {
	Entry  Entry
	Fields map[string]canonicalBodyRef
}

func canonicalEntryBodyFields(entry *Entry) map[string]*string {
	return map[string]*string{"content": &entry.Message.Content, "reasoning": &entry.Message.Reasoning, "arguments": &entry.Message.ToolArgs, "summary": &entry.Summary, "error": &entry.Error}
}

func decodeCanonicalStoredEntry(record backendDomainRecord) (canonicalStoredEntry, error) {
	var stored canonicalStoredEntry
	if err := decodeBackendDomainValue(backendDomainV30Format, record, &stored); err != nil {
		return stored, errors.Join(ErrAuthorityCorrupt, err)
	}
	if record.Kind != backendDomainRecordEntry || record.ID == "" || record.ThreadID == "" || record.Ordinal < 0 || stored.Entry.ID != record.ID || stored.Entry.ThreadID != record.ThreadID || stored.Entry.Raw != "" {
		return stored, ErrAuthorityCorrupt
	}
	fields := canonicalEntryBodyFields(&stored.Entry)
	for _, value := range fields {
		if *value != "" {
			return stored, ErrAuthorityCorrupt
		}
	}
	for field, ref := range stored.Fields {
		if fields[field] == nil || !validCanonicalBodyRef(ref) {
			return stored, ErrAuthorityCorrupt
		}
	}
	return stored, nil
}

func materializeCanonicalStoredEntry(ctx context.Context, tx spi.ReadTx, stored canonicalStoredEntry) (Entry, error) {
	entry := stored.Entry
	for field, value := range canonicalEntryBodyFields(&entry) {
		if ref, ok := stored.Fields[field]; ok {
			text, err := materializeCanonicalBody(ctx, tx, ref)
			if err != nil {
				return Entry{}, err
			}
			*value = text
		}
	}
	entry.Raw = rawForEntry(entry)
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func putCanonicalStoredEntry(ctx context.Context, tx spi.WriteTx, key []byte, id, threadID string, ordinal int, entry Entry) error {
	if err := ValidateEntryIntegrity(entry); err != nil {
		return err
	}
	var previous *canonicalStoredEntry
	if raw, err := tx.Get(backendDomainV30Namespace, key); err == nil {
		record, decodeErr := decodeBackendDomainRecord(backendDomainV30Format, raw)
		if decodeErr == nil {
			stored, err := decodeCanonicalStoredEntry(record)
			if err != nil {
				return err
			}
			previous = &stored
		} else {
			return errors.Join(ErrAuthorityCorrupt, decodeErr)
		}
	} else if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	stored := canonicalStoredEntry{Entry: entry, Fields: map[string]canonicalBodyRef{}}
	stored.Entry.Raw = ""
	for field, value := range canonicalEntryBodyFields(&stored.Entry) {
		if *value == "" {
			continue
		}
		ref, err := retainCanonicalBody(ctx, tx, *value)
		if err != nil {
			return err
		}
		stored.Fields[field], *value = ref, ""
	}
	if previous != nil {
		for _, ref := range previous.Fields {
			if err := releaseCanonicalBody(ctx, tx, ref); err != nil {
				return err
			}
		}
	}
	encoded, err := encodeBackendDomainRecord(backendDomainV30Format, backendDomainRecordEntry, id, threadID, ordinal, stored)
	if err != nil {
		return err
	}
	return tx.Put(backendDomainV30Namespace, key, encoded)
}

func deleteCanonicalStoredEntry(ctx context.Context, tx spi.WriteTx, threadID string, ordinal int) error {
	key := backendDomainEntryKey(threadID, ordinal)
	raw, err := tx.Get(backendDomainV30Namespace, key)
	if err != nil {
		return err
	}
	record, err := decodeBackendDomainRecord(backendDomainV30Format, raw)
	if err != nil {
		return err
	}
	stored, err := decodeCanonicalStoredEntry(record)
	if err != nil {
		return err
	}
	for _, ref := range stored.Fields {
		if err := releaseCanonicalBody(ctx, tx, ref); err != nil {
			return err
		}
	}
	return tx.Delete(backendDomainV30Namespace, key)
}

type canonicalBodyKeyParts struct {
	digest  string
	block   bool
	ordinal int
}

func decodeCanonicalBodyKey(key []byte) (canonicalBodyKeyParts, error) {
	var result canonicalBodyKeyParts
	if len(key) == 0 || key[0] != 1 {
		return result, ErrAuthorityCorrupt
	}
	remaining := key[1:]
	parts := make([][]byte, 0, 3)
	for len(remaining) > 0 {
		if len(remaining) < 5 {
			return result, ErrAuthorityCorrupt
		}
		n := uint64(binary.BigEndian.Uint32(remaining[1:5]))
		if n > uint64(len(remaining)-5) {
			return result, ErrAuthorityCorrupt
		}
		parts = append(parts, remaining[5:5+int(n)])
		remaining = remaining[5+int(n):]
		if len(parts) > 3 {
			return result, ErrAuthorityCorrupt
		}
	}
	if len(parts) < 2 {
		return result, ErrAuthorityCorrupt
	}
	result.digest = string(parts[1])
	switch string(parts[0]) {
	case "body":
		if len(parts) != 2 {
			return result, ErrAuthorityCorrupt
		}
	case "block":
		if len(parts) != 3 || len(parts[2]) != 8 {
			return result, ErrAuthorityCorrupt
		}
		ordinal := binary.BigEndian.Uint64(parts[2])
		if ordinal > uint64(^uint(0)>>1) {
			return result, ErrAuthorityCorrupt
		}
		result.block, result.ordinal = true, int(ordinal)
	default:
		return result, ErrAuthorityCorrupt
	}
	return result, nil
}
