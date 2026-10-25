package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const activityFieldsNamespace = "floret.sessiontree.activity-fields.v6"

func activityFieldKey(hash, path string) []byte { return backendDomainKey(hash, "field", path) }
func activityFieldPartKey(hash, path string, part int) []byte {
	return backendDomainKey(hash, "part", path, strconv.Itoa(part))
}
func activityFieldReadyKey(hash string) []byte { return backendDomainKey(hash, "ready") }
func writeActivityFieldIndex(ctx context.Context, tx spi.WriteTx, ref canonicalBodyRef, text string) error {
	if ref.SHA256 != stableHash(text) || ref.Bytes != len(text) {
		return ErrAuthorityCorrupt
	}
	if raw, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(ref.SHA256)); err == nil {
		var ready canonicalBodyRef
		if json.Unmarshal(raw, &ready) != nil || ready != ref {
			return ErrAuthorityCorrupt
		}
		_, err = readActivityMergeMetadata(tx, ref)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	} else if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	if err := readActivitySummaryReady(tx, ref); err != nil && (!errors.Is(err, spi.ErrNotFound) || errors.Is(err, ErrAuthorityCorrupt)) {
		return err
	}
	nodes, err := indexActivityJSON(ctx, text)
	if err != nil {
		return err
	}
	records, err := activityFieldRecords(ref, nodes)
	if err != nil {
		return err
	}
	for first := 0; first < len(records); first += storagebridge.MaxRecordBatch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := storagebridge.WriteRecords(tx, activityFieldsNamespace, records[first:min(len(records), first+storagebridge.MaxRecordBatch)]); err != nil {
			return err
		}
	}
	if err := writeActivityMergeMetadata(tx, ref, text, nodes); err != nil {
		return err
	}
	raw, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	return tx.Put(activityFieldsNamespace, activityFieldReadyKey(ref.SHA256), raw)
}
func readActivityFieldNode(tx spi.ReadTx, ref canonicalBodyRef, path string) (activityFieldNode, error) {
	return readActivityFieldNodeMode(tx, ref, path, false)
}
func readActivityFieldNodeMode(tx spi.ReadTx, ref canonicalBodyRef, path string, shape bool) (activityFieldNode, error) {
	raw, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(ref.SHA256))
	if errors.Is(err, spi.ErrNotFound) && (activityfields.SummaryText(path) || shape && activitySummaryPath(path)) {
		if err = readActivitySummaryReady(tx, ref); err != nil {
			return activityFieldNode{}, err
		}
	} else {
		if err != nil {
			return activityFieldNode{}, err
		}
		var ready canonicalBodyRef
		if json.Unmarshal(raw, &ready) != nil || ready != ref {
			return activityFieldNode{}, ErrAuthorityCorrupt
		}
	}
	raw, err = tx.Get(activityFieldsNamespace, activityFieldKey(ref.SHA256, path))
	if errors.Is(err, spi.ErrNotFound) {
		return activityFieldNode{}, ErrEntryNotFound
	}
	if err != nil {
		return activityFieldNode{}, err
	}
	var node activityFieldNode
	if decodeActivityMetadata(raw, "activity-field-v1", &node) != nil || !validActivityFieldNode(ref, node) {
		return node, ErrAuthorityCorrupt
	}
	if node.ShapeOnly && !shape {
		return activityFieldNode{}, spi.ErrNotFound
	}
	return node, nil
}
func readRequestedActivityTextRange(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, node activityFieldNode, input ActivityFieldInput) (string, int, error) {
	start := input.Offset
	var alignment []*int
	if input.AlignStart {
		alignment = []*int{&start}
	}
	text, err := readActivityTextRange(ctx, tx, ref, node, input.Path, input.Offset, input.Limit, alignment...)
	return text, start, err
}

func readActivityTextRange(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, node activityFieldNode, path string, offset, limit int, alignment ...*int) (string, error) {
	if node.Kind != "string" || offset < 0 || offset > node.Bytes || limit < utf8.UTFMax || limit > canonicalBodyBlockBytes {
		return "", spi.ErrInvalidArgument
	}
	if _, err := readCanonicalBodyRecord(tx, ref); err != nil {
		return "", err
	}
	end := min(node.Bytes, offset+limit)
	result := strings.Builder{}
	// Part ordinals are binary searched by decoded position. Reading a suffix
	// never fetches the metadata for preceding text or sibling array members.
	lo, hi := 0, node.PartCount
	cache := map[int]activityTextPart{}
	readPart := func(index int) (activityTextPart, error) {
		if part, ok := cache[index]; ok {
			return part, nil
		}
		part, err := readActivityTextPart(tx, ref, node, path, index)
		if err == nil {
			cache[index] = part
		}
		return part, err
	}
	for lo < hi {
		mid := lo + (hi-lo)/2
		part, err := readPart(mid)
		if err != nil {
			return "", err
		}
		if part.DecodedOffset+part.Bytes <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	expected := -1
	for i := lo; i < node.PartCount; i++ {
		part, err := readPart(i)
		if err != nil {
			return "", err
		}
		position := part.DecodedOffset
		next := position + part.Bytes
		if expected >= 0 && position != expected || i == lo && (offset < position || offset >= next) {
			return "", ErrAuthorityCorrupt
		}
		expected = next
		if position >= end {
			break
		}
		if next > offset {
			if part.Offset <= node.Offset || part.EncodedBytes < 0 || part.Bytes < 1 || part.Bytes > canonicalBodyBlockBytes || part.Offset+part.EncodedBytes >= node.Offset+node.EncodedBytes || part.EncodedBytes > 6*canonicalBodyBlockBytes {
				return "", ErrAuthorityCorrupt
			}
			encoded := make([]byte, 1, part.EncodedBytes+2)
			encoded[0] = '"'
			for read := 0; read < part.EncodedBytes; {
				size := min(canonicalBodyReadBytes, part.EncodedBytes-read)
				value, err := readCanonicalBodyBytes(ctx, tx, ref, part.Offset+read, size)
				if err != nil {
					return "", err
				}
				encoded = append(encoded, value...)
				read += len(value)
			}
			encoded = append(encoded, '"')
			var text string
			if json.Unmarshal(encoded, &text) != nil || len(text) != part.Bytes || stableHash(text) != part.SHA256 {
				return "", ErrAuthorityCorrupt
			}
			first, last := max(0, offset-position), min(len(text), end-position)
			if i == lo && len(alignment) > 0 {
				for first > 0 && first < len(text) && !utf8.RuneStart(text[first]) {
					first--
				}
				offset = position + first
				*alignment[0] = offset
				end = min(node.Bytes, offset+limit)
				last = min(len(text), end-position)
			}
			if first < len(text) && !utf8.RuneStart(text[first]) {
				return "", spi.ErrInvalidArgument
			}
			for last < len(text) && last > first && !utf8.RuneStart(text[last]) {
				last--
			}
			result.WriteString(text[first:last])
		}
	}
	if offset < node.Bytes && result.Len() == 0 {
		return "", ErrAuthorityCorrupt
	}
	return result.String(), nil
}

func deleteActivityFieldIndex(ctx context.Context, tx spi.WriteTx, hash string) error {
	prefix := backendDomainKey(hash)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: activityFieldsNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128})
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := tx.Delete(activityFieldsNamespace, record.Key); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
	}
}

func activityFieldRecords(ref canonicalBodyRef, nodes map[string]activityFieldNode) ([]spi.Record, error) {
	records := make([]spi.Record, 0, len(nodes))
	for path, node := range nodes {
		raw, err := encodeActivityFieldNode(node)
		if err != nil {
			return nil, err
		}
		records = append(records, spi.Record{Key: activityFieldKey(ref.SHA256, path), Value: raw})
		for i, part := range node.Parts {
			encoded, err := encodeActivityMetadata("activity-part-v1", part)
			if err != nil {
				return nil, err
			}
			records = append(records, spi.Record{Key: activityFieldPartKey(ref.SHA256, path, i), Value: encoded})
		}
	}
	return records, nil
}
func encodeActivityFieldNode(node activityFieldNode) ([]byte, error) {
	node.Parts = nil
	if node.Kind == "array" {
		node.Children = nil
	}
	return encodeActivityMetadata("activity-field-v1", node)
}
func readActivityTextPart(tx spi.ReadTx, ref canonicalBodyRef, node activityFieldNode, path string, ordinal int) (activityTextPart, error) {
	raw, err := tx.Get(activityFieldsNamespace, activityFieldPartKey(ref.SHA256, path, ordinal))
	if err != nil {
		return activityTextPart{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	var part activityTextPart
	if decodeActivityMetadata(raw, "activity-part-v1", &part) != nil || part.Offset <= node.Offset || part.DecodedOffset < 0 || part.DecodedOffset > node.Bytes || part.Bytes < 1 || part.Bytes > canonicalBodyBlockBytes || part.Bytes > node.Bytes-part.DecodedOffset || part.EncodedBytes < 1 || part.EncodedBytes > 6*canonicalBodyBlockBytes || part.Offset >= node.Offset+node.EncodedBytes-1 || part.EncodedBytes > node.Offset+node.EncodedBytes-1-part.Offset || len(part.SHA256) != 64 {
		return part, ErrAuthorityCorrupt
	}
	if ordinal == 0 && (part.DecodedOffset != 0 || part.Offset != node.Offset+1) || ordinal == node.PartCount-1 && (part.DecodedOffset+part.Bytes != node.Bytes || part.Offset+part.EncodedBytes != node.Offset+node.EncodedBytes-1) {
		return part, ErrAuthorityCorrupt
	}
	return part, nil
}
func validActivityFieldNode(ref canonicalBodyRef, node activityFieldNode) bool {
	if node.TextExtentVersion < 0 || node.TextExtentVersion > 1 || node.TextExtentVersion != 0 && node.Kind != "string" {
		return false
	}
	if node.Offset < 0 || node.EncodedBytes < 0 || node.Offset > ref.Bytes || node.EncodedBytes > ref.Bytes-node.Offset || node.Bytes < 0 || node.Count < 0 || node.PartCount < 0 || node.TrimOffset < 0 || node.TrimOffset > node.Bytes || node.TrimBytes < 0 || node.TrimBytes > node.Bytes-node.TrimOffset || len(node.Parts) != 0 {
		return false
	}
	switch node.Kind {
	case "string":
		if node.ShapeOnly {
			return node.EncodedBytes >= 2 && node.SHA256 == "" && node.PartCount == 0 && len(node.Children) == 0 && node.Count == 0
		}
		return node.EncodedBytes >= 2 && len(node.SHA256) == 64 && len(node.Children) == 0 && node.Count == 0 && (node.Bytes == 0) == (node.PartCount == 0) && node.PartCount <= node.Bytes
	case "array", "object":
		if node.EncodedBytes < 2 || node.Bytes != 0 || node.PartCount != 0 || node.SHA256 != "" {
			return false
		}
		if node.Kind == "array" {
			return len(node.Children) == 0
		}
		if node.ShapeOnly {
			return len(node.Children) == 0
		}
		if len(node.Children) != node.Count {
			return false
		}
		seen := make(map[string]bool, len(node.Children))
		for _, key := range node.Children {
			if seen[key] {
				return false
			}
			seen[key] = true
		}
		return true
	case "scalar":
		return node.EncodedBytes > 0 && len(node.Scalar) <= 64 && json.Valid(node.Scalar) && node.Bytes == 0 && len(node.Children) == 0 && node.PartCount == 0 && node.Count == 0 && node.SHA256 == ""
	default:
		return false
	}
}

// Metadata remains rebuildable, but a malformed or bit-damaged published node
// must fail before returning an incorrect extent, count or source identity.
func encodeActivityMetadata(kind string, value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(struct {
		Value  json.RawMessage
		SHA256 string
	}{raw, stableHash(string(raw))})
	if err != nil {
		return nil, err
	}
	return storagecodec.EncodeEnvelope(kind, payload)
}
func decodeActivityMetadata[T any](raw []byte, kind string, value *T) error {
	var payload struct {
		Value  json.RawMessage
		SHA256 string
	}
	if storagecodec.DecodeEnvelopeInto(raw, kind, &payload) != nil || stableHash(string(payload.Value)) != payload.SHA256 {
		return ErrAuthorityCorrupt
	}
	if err := storagecodec.DecodeJSON(payload.Value, value); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}
