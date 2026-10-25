package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
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
	claimed, err := migrationBodyClaimed(ctx, tx, ref.SHA256)
	if err != nil {
		return err
	}
	if claimed {
		return tx.Delete(canonicalBodyNamespace, canonicalBodyKey(ref.SHA256))
	}
	for ordinal := 0; ordinal < (ref.Bytes-1)/canonicalBodyBlockBytes+1; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, ordinal)); err != nil {
			return err
		}
	}
	if err := deleteActivityFieldIndex(ctx, tx, ref.SHA256); err != nil {
		return err
	}
	if err := deleteCanonicalOptionalRecord(tx, bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256)); err != nil {
		return err
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
		var block []byte
		err = storagecodec.DecodeEnvelopeInto(raw, "sessiontree-v30-body-block", &block)
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
	return storagecodec.DecodeJSON(raw, target)
}

type canonicalStoredEntry struct {
	Entry  Entry
	Fields map[string]canonicalBodyRef
}

// canonicalEntryBodyField detaches only a requested nested control. Ordinary
// field access neither builds a map nor forces a complete Entry copy to escape.
func canonicalEntryBodyField(entry *Entry, field string) *string {
	switch field {
	case "content":
		return &entry.Message.Content
	case "reasoning":
		return &entry.Message.Reasoning
	case "arguments":
		return &entry.Message.ToolArgs
	case "summary":
		return &entry.Summary
	case "error":
		return &entry.Error
	case "control_output":
		if entry.Message.ControlSignal != nil {
			signal := *entry.Message.ControlSignal
			entry.Message.ControlSignal = &signal
			return &signal.OutputText
		}
	}
	return nil
}

// canonicalStoredFields uses one block store for plain text and typed activity.
// Activity is encoded once as canonical JSON; readers must decode the complete
// value before interpreting it. A partial JSON chunk is never a presentation.
func canonicalStoredFields(entry Entry, includeActivity bool) (map[string]string, error) {
	fields := map[string]string{}
	for _, field := range []string{"content", "reasoning", "arguments", "summary", "error", "control_output"} {
		value := canonicalEntryBodyField(&entry, field)
		if value != nil && *value != "" {
			fields[field] = *value
		}
	}
	if includeActivity && entry.Message.Activity != nil {
		raw, err := json.Marshal(entry.Message.Activity)
		if err != nil {
			return nil, err
		}
		fields["activity_json"] = string(raw)
	}
	return fields, nil
}

// canonicalFieldValue inspects only the requested field. Ordinary content
// paging must not serialize unrelated structured payloads on every chunk.
func canonicalFieldValue(entry Entry, field string) (string, error) {
	if field == "activity_json" {
		if entry.Message.Activity == nil {
			return "", nil
		}
		raw, err := json.Marshal(entry.Message.Activity)
		return string(raw), err
	}
	value := canonicalEntryBodyField(&entry, field)
	if value == nil {
		return "", spi.ErrInvalidArgument
	}
	return *value, nil
}

func clearCanonicalField(entry *Entry, field string) error {
	if field == "activity_json" {
		entry.Message.Activity = nil
		return nil
	}
	value := canonicalEntryBodyField(entry, field)
	if value == nil {
		return ErrAuthorityCorrupt
	}
	*value = ""
	return nil
}

func restoreCanonicalField(entry *Entry, field, text string) error {
	if field == "activity_json" {
		if entry.Message.Activity != nil {
			return ErrAuthorityCorrupt
		}
		var activity tools.ActivityPresentation
		if err := decodeCanonicalBodyJSON([]byte(text), &activity); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if err := activity.Validate(); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		entry.Message.Activity = &activity
		return nil
	}
	value := canonicalEntryBodyField(entry, field)
	if value == nil || *value != "" {
		return ErrAuthorityCorrupt
	}
	*value = text
	return nil
}

func decodeCanonicalStoredEntry(format backendDomainFormat, record backendDomainRecord) (canonicalStoredEntry, error) {
	var stored canonicalStoredEntry
	if err := decodeBackendDomainValue(format, record, &stored); err != nil {
		return stored, errors.Join(ErrAuthorityCorrupt, err)
	}
	return validateCanonicalStoredEntry(format, record, stored)
}

func validateCanonicalStoredEntry(format backendDomainFormat, record backendDomainRecord, stored canonicalStoredEntry) (canonicalStoredEntry, error) {
	if record.Kind != backendDomainRecordEntry || record.ID == "" || record.ThreadID == "" || record.Ordinal < 0 || stored.Entry.ID != record.ID || stored.Entry.ThreadID != record.ThreadID || stored.Entry.Raw != "" {
		return stored, ErrAuthorityCorrupt
	}
	message := &stored.Entry.Message
	if message.Content != "" || message.Reasoning != "" || message.ToolArgs != "" || stored.Entry.Summary != "" || stored.Entry.Error != "" || message.ControlSignal != nil && message.ControlSignal.OutputText != "" {
		return stored, ErrAuthorityCorrupt
	}
	for field, ref := range stored.Fields {
		valid := false
		switch field {
		case "content", "reasoning", "arguments", "summary", "error":
			valid = true
		case "control_output":
			valid = message.ControlSignal != nil
		case "activity_json":
			valid = format.version >= backendDomainV31Version
		}
		if !valid || !validCanonicalBodyRef(ref) {
			return stored, ErrAuthorityCorrupt
		}
	}
	if format.version >= backendDomainV31Version && stored.Entry.Message.Activity != nil {
		return stored, ErrAuthorityCorrupt
	}
	return stored, nil
}

func materializeCanonicalStoredEntry(ctx context.Context, tx spi.ReadTx, stored canonicalStoredEntry) (Entry, error) {
	return materializeCanonicalEntry(stored, func(ref canonicalBodyRef) (string, error) {
		return materializeCanonicalBody(ctx, tx, ref)
	})
}

func materializeCanonicalEntry(stored canonicalStoredEntry, read func(canonicalBodyRef) (string, error)) (Entry, error) {
	entry := stored.Entry
	for field, ref := range stored.Fields {
		text, err := read(ref)
		if err != nil {
			return Entry{}, err
		}
		if err := restoreCanonicalField(&entry, field, text); err != nil {
			return Entry{}, err
		}
	}
	entry.Raw = rawForEntry(entry)
	if err := ValidateEntryIntegrity(entry); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func putCanonicalStoredEntry(ctx context.Context, tx spi.WriteTx, format backendDomainFormat, key []byte, id, threadID string, ordinal int, entry Entry) error {
	if id != entry.ID || threadID != entry.ThreadID || ordinal < 0 || !bytes.Equal(key, backendDomainEntryKey(threadID, ordinal)) {
		return ErrAuthorityCorrupt
	}
	if err := ValidateEntryIntegrity(entry); err != nil {
		return err
	}
	var previous *canonicalStoredEntry
	if raw, err := tx.Get(format.namespace, key); err == nil {
		if format.version >= backendDomainV32Version {
			return errors.Join(ErrAuthorityCorrupt, errors.New("canonical session-tree entries are immutable"))
		}
		record, decodeErr := decodeBackendDomainRecord(format, raw)
		if decodeErr == nil {
			stored, err := decodeCanonicalStoredEntry(format, record)
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
	values, err := canonicalStoredFields(entry, format.version >= backendDomainV31Version)
	if err != nil {
		return err
	}
	for field, value := range values {
		ref, err := retainCanonicalBody(ctx, tx, value)
		if err != nil {
			return err
		}
		if field == "content" || field == "reasoning" || field == "error" {
			if err := writeBodyTextFacts(tx, ref, value); err != nil {
				return err
			}
		}
		if field == "activity_json" {
			if err := writeActivityFieldIndex(ctx, tx, ref, value); err != nil {
				return err
			}
		}
		stored.Fields[field] = ref
		if err := clearCanonicalField(&stored.Entry, field); err != nil {
			return err
		}
	}
	if previous != nil {
		for _, ref := range previous.Fields {
			if err := releaseCanonicalBody(ctx, tx, ref); err != nil {
				return err
			}
		}
	}
	encoded, err := encodeBackendDomainRecord(format, backendDomainRecordEntry, id, threadID, ordinal, stored)
	if err != nil {
		return err
	}
	if err := tx.Put(format.namespace, key, encoded); err != nil {
		return err
	}
	if format.version >= backendDomainV32Version {
		return putEntryLocator(ctx, tx, threadID, ordinal, entry)
	}
	return nil
}

func deleteCanonicalStoredEntry(ctx context.Context, tx spi.WriteTx, format backendDomainFormat, threadID string, ordinal int) error {
	key := backendDomainEntryKey(threadID, ordinal)
	raw, err := tx.Get(format.namespace, key)
	if err != nil {
		return err
	}
	record, err := decodeBackendDomainRecord(format, raw)
	if err != nil {
		return err
	}
	stored, err := decodeCanonicalStoredEntry(format, record)
	if err != nil {
		return err
	}
	for _, ref := range stored.Fields {
		if err := releaseCanonicalBody(ctx, tx, ref); err != nil {
			return err
		}
	}
	if format.version >= backendDomainV32Version {
		if err := removeTailEntryLocators(ctx, tx, threadID, ordinal, stored.Entry); err != nil {
			return err
		}
	}
	return tx.Delete(format.namespace, key)
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

// The migration rewrites validated entries in the same transaction. Release the
// old block inventory first so sharing counts are rebuilt exactly once.
func deleteCanonicalBodyRecords(ctx context.Context, tx spi.WriteTx) error {
	for _, namespace := range []string{canonicalBodyNamespace, bodyTextFactsNamespace, activityFieldsNamespace} {
		if err := deleteCanonicalBodyNamespace(ctx, tx, namespace); err != nil {
			return err
		}
	}
	return nil
}

func deleteCanonicalBodyNamespace(ctx context.Context, tx spi.WriteTx, namespace string) error {
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: namespace, After: after, Limit: 256})
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := tx.Delete(namespace, record.Key); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

// Read-only typed decoding preserves the canonical envelope and source checks
// without copying the encoded entry through two nested RawMessage fields.
func decodeCanonicalEntryRecord(format backendDomainFormat, encoded []byte) (backendDomainRecord, canonicalStoredEntry, error) {
	var value struct {
		Version  int
		Kind     string
		ID       string
		ThreadID string `json:"thread_id,omitempty"`
		Ordinal  int
		Value    storagecodec.JSONValue[*canonicalStoredEntry]
	}
	if err := storagecodec.DecodeEnvelopeInto(encoded, format.envelope, &value); err != nil {
		return backendDomainRecord{}, canonicalStoredEntry{}, err
	}
	record := backendDomainRecord{Version: value.Version, Kind: value.Kind, ID: value.ID, ThreadID: value.ThreadID, Ordinal: value.Ordinal}
	if value.Version != format.version || value.Value.Value == nil {
		return record, canonicalStoredEntry{}, ErrAuthorityCorrupt
	}
	stored, err := validateCanonicalStoredEntry(format, record, *value.Value.Value)
	return record, stored, err
}
