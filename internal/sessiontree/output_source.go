package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const (
	outputSourceNamespace        = "floret.domain.sessiontree.outputs.v39"
	OutputSourceMaxBytes   int64 = 64 << 20
	outputStoreMaxBytes    int64 = 1 << 30
	OutputSourceChunkBytes       = 64 << 10
)

type outputRetentionError struct {
	reason string
	cause  error
}

func (e *outputRetentionError) Error() string {
	return fmt.Sprintf("output retention unavailable: %s: %v", e.reason, e.cause)
}
func (e *outputRetentionError) Unwrap() error { return e.cause }
func classifyOutputRetentionError(err error) error {
	var typed *outputRetentionError
	if errors.As(err, &typed) || errors.Is(err, ErrAuthorityCorrupt) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &outputRetentionError{reason: "storage_error", cause: err}
}

// OutputSource holds storage facts only. Canonical entries own membership and
// execution; this record must never become a second work lifecycle.
type OutputSource struct {
	ID            string
	Bytes         int64
	ObservedBytes int64
	Chunks        int64
	Sealed        bool
	Reason        string
	SHA256        string
	HashState     []byte
	References    int64
}

type OutputSourceChunk struct {
	Seq    int64
	Offset int64
	Text   string
	SHA256 string
}

type OutputSourcePage struct {
	Source     OutputSource
	Offset     int64
	NextOffset int64
	FirstSeq   int64
	LastSeq    int64
	Text       string
	HasMore    bool
}

func outputSourceKey(id string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("source"), storagecodec.TupleString(id))
}
func outputChunkKey(id string, seq int64) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("chunk"), storagecodec.TupleString(id), storagecodec.TupleOrdinal(uint64(seq)))
}
func outputChunkOffsetKey(id string, seq int64) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("offset"), storagecodec.TupleString(id), storagecodec.TupleOrdinal(uint64(seq)))
}
func outputUsageKey() []byte                     { return storagecodec.Tuple(storagecodec.TupleString("bytes")) }
func outputSourceID(thread, entry string) string { return "output-" + StableHash(thread+"\x00"+entry) }

func readOutputRecord(tx spi.ReadTx, key []byte, target any) error {
	raw, err := tx.Get(outputSourceNamespace, key)
	if err != nil {
		return err
	}
	payload, err := storagecodec.DecodeEnvelope(raw, "sessiontree-v39-output")
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := storagecodec.DecodeJSON(payload, target); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}
func putOutputRecord(tx spi.WriteTx, key []byte, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	raw, err := storagecodec.EncodeEnvelope("sessiontree-v39-output", payload)
	if err != nil {
		return err
	}
	return tx.Put(outputSourceNamespace, key, raw)
}
func readOutputSource(tx spi.ReadTx, id string) (OutputSource, error) {
	var source OutputSource
	if err := readOutputRecord(tx, outputSourceKey(id), &source); err != nil {
		return source, err
	}
	if source.ID != id || source.Bytes < 0 || source.Bytes > OutputSourceMaxBytes || source.ObservedBytes < source.Bytes || source.Chunks < 0 || source.Chunks > source.Bytes || source.References < 1 || !validOutputReason(source.Reason) || source.Bytes == 0 && source.Chunks != 0 || source.Bytes > 0 && source.Chunks == 0 {
		return source, ErrAuthorityCorrupt
	}
	if source.Reason == "" && source.ObservedBytes != source.Bytes {
		return source, ErrAuthorityCorrupt
	}
	hash := sha256.New()
	if err := hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(source.HashState); err != nil || hex.EncodeToString(hash.Sum(nil)) != source.SHA256 {
		return source, ErrAuthorityCorrupt
	}
	return source, nil
}
func newOutputSource(tx spi.WriteTx, id string) (OutputSource, error) {
	if _, err := tx.Get(outputSourceNamespace, outputSourceKey(id)); err == nil {
		return OutputSource{}, ErrRequestConflict
	} else if !errors.Is(err, spi.ErrNotFound) {
		return OutputSource{}, err
	}
	hash := sha256.New()
	state, _ := hash.(encoding.BinaryMarshaler).MarshalBinary()
	source := OutputSource{ID: id, SHA256: hex.EncodeToString(hash.Sum(nil)), HashState: state, References: 1}
	return source, putOutputRecord(tx, outputSourceKey(id), source)
}
func outputStoreUsage(tx spi.ReadTx) (int64, error) {
	var used int64
	err := readOutputRecord(tx, outputUsageKey(), &used)
	if errors.Is(err, spi.ErrNotFound) {
		return 0, nil
	}
	if err != nil || used < 0 {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	return used, nil
}

// appendOutputSource commits one complete UTF-8 chunk and its watermark in the
// same transaction. Quota exhaustion retains the prefix and reports the gap;
// it never deletes another source or authorizes replay of the producing tool.
func appendOutputSource(ctx context.Context, tx spi.WriteTx, source OutputSource, text string) (OutputSource, error) {
	if err := ctx.Err(); err != nil {
		return source, err
	}
	if source.Sealed || !utf8.ValidString(text) || len(text) > OutputSourceChunkBytes || source.ObservedBytes > math.MaxInt64-int64(len(text)) {
		return source, spi.ErrInvalidArgument
	}
	source.ObservedBytes += int64(len(text))
	if source.Reason != "" || text == "" {
		return source, putOutputRecord(tx, outputSourceKey(source.ID), source)
	}
	used, err := outputStoreUsage(tx)
	if err != nil {
		return source, err
	}
	if int64(len(text)) > OutputSourceMaxBytes-source.Bytes {
		source.Reason = "output_limit"
	} else if int64(len(text)) > outputStoreMaxBytes-used {
		source.Reason = "storage_limit"
	}
	if source.Reason != "" {
		return source, putOutputRecord(tx, outputSourceKey(source.ID), source)
	}
	digest := sha256.Sum256([]byte(text))
	chunk := OutputSourceChunk{Seq: source.Chunks + 1, Offset: source.Bytes, Text: text, SHA256: hex.EncodeToString(digest[:])}
	if err := putOutputRecord(tx, outputChunkKey(source.ID, chunk.Seq), chunk); err != nil {
		return source, err
	}
	if err := putOutputRecord(tx, outputChunkOffsetKey(source.ID, chunk.Seq), chunk.Offset); err != nil {
		return source, err
	}
	hash := sha256.New()
	if err := hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(source.HashState); err != nil {
		return source, ErrAuthorityCorrupt
	}
	_, _ = hash.Write([]byte(text))
	source.HashState, _ = hash.(encoding.BinaryMarshaler).MarshalBinary()
	source.SHA256 = hex.EncodeToString(hash.Sum(nil))
	source.Bytes += int64(len(text))
	source.Chunks++
	if err := putOutputRecord(tx, outputUsageKey(), used+int64(len(text))); err != nil {
		return source, err
	}
	return source, putOutputRecord(tx, outputSourceKey(source.ID), source)
}
func sealOutputSource(tx spi.WriteTx, source OutputSource, reason string) (OutputSource, error) {
	if !validOutputReason(reason) {
		return source, spi.ErrInvalidArgument
	}
	if source.Sealed {
		return source, nil
	}
	source.Sealed = true
	if source.Reason == "" {
		source.Reason = reason
	}
	return source, putOutputRecord(tx, outputSourceKey(source.ID), source)
}
func writeOutputSource(ctx context.Context, tx spi.WriteTx, id, text string) (OutputSource, error) {
	if int64(len(text)) > OutputSourceMaxBytes {
		return OutputSource{}, &outputRetentionError{reason: "output_limit"}
	}
	if !utf8.ValidString(text) {
		return OutputSource{}, spi.ErrInvalidArgument
	}
	source, err := newOutputSource(tx, id)
	if err != nil {
		return source, err
	}
	for len(text) > 0 {
		end := min(len(text), OutputSourceChunkBytes)
		for end < len(text) && !utf8.RuneStart(text[end]) {
			end--
		}
		source, err = appendOutputSource(ctx, tx, source, text[:end])
		if err != nil {
			return source, err
		}
		if source.Reason != "" {
			return source, &outputRetentionError{reason: source.Reason}
		}
		text = text[end:]
	}
	return sealOutputSource(tx, source, "")
}
func readOutputChunk(tx spi.ReadTx, source OutputSource, seq int64) (OutputSourceChunk, error) {
	var chunk OutputSourceChunk
	if seq < 1 || seq > source.Chunks {
		return chunk, spi.ErrInvalidArgument
	}
	if err := readOutputRecord(tx, outputChunkKey(source.ID, seq), &chunk); err != nil {
		return chunk, errors.Join(ErrAuthorityCorrupt, err)
	}
	digest := sha256.Sum256([]byte(chunk.Text))
	if chunk.Seq != seq || chunk.Offset < 0 || chunk.Offset+int64(len(chunk.Text)) > source.Bytes || len(chunk.Text) == 0 || len(chunk.Text) > OutputSourceChunkBytes || !utf8.ValidString(chunk.Text) || chunk.SHA256 != hex.EncodeToString(digest[:]) {
		return chunk, ErrAuthorityCorrupt
	}
	return chunk, nil
}

func readOutputSourcePage(ctx context.Context, tx spi.ReadTx, source OutputSource, offset int64, limit int) (OutputSourcePage, error) {
	page := OutputSourcePage{Source: source, Offset: offset, NextOffset: offset}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if offset < 0 || offset > source.Bytes || limit < utf8.UTFMax || limit > OutputSourceChunkBytes {
		return page, spi.ErrInvalidArgument
	}
	if offset == source.Bytes {
		return page, nil
	}
	// Sparse chunk offsets locate a requested range without reading its prefix.
	lo, hi := int64(1), source.Chunks
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		mid := (lo + hi + 1) / 2
		var chunkOffset int64
		if err := readOutputRecord(tx, outputChunkOffsetKey(source.ID, mid), &chunkOffset); err != nil {
			return page, errors.Join(ErrAuthorityCorrupt, err)
		}
		if chunkOffset < 0 || chunkOffset >= source.Bytes {
			return page, ErrAuthorityCorrupt
		}
		if chunkOffset <= offset {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	var out strings.Builder
	for seq := lo; seq <= source.Chunks && out.Len() < limit; seq++ {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		chunk, err := readOutputChunk(tx, source, seq)
		if err != nil {
			return page, err
		}
		start := page.NextOffset - chunk.Offset
		if start < 0 || start >= int64(len(chunk.Text)) {
			return page, ErrAuthorityCorrupt
		}
		if !utf8.RuneStart(chunk.Text[start]) {
			return page, spi.ErrInvalidArgument
		}
		end := min(len(chunk.Text), int(start)+limit-out.Len())
		for end < len(chunk.Text) && !utf8.RuneStart(chunk.Text[end]) {
			end--
		}
		if end == int(start) {
			break
		}
		out.WriteString(chunk.Text[start:end])
		page.NextOffset += int64(end) - start
		if page.FirstSeq == 0 {
			page.FirstSeq = seq
		}
		page.LastSeq = seq
		if end < len(chunk.Text) {
			break
		}
	}
	page.Text = out.String()
	page.HasMore = page.NextOffset < source.Bytes
	return page, nil
}
func verifyOutputSource(ctx context.Context, tx spi.ReadTx, source OutputSource) error {
	hash := sha256.New()
	offset := int64(0)
	for seq := int64(1); seq <= source.Chunks; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := readOutputChunk(tx, source, seq)
		if err != nil {
			return err
		}
		var indexed int64
		if err := readOutputRecord(tx, outputChunkOffsetKey(source.ID, seq), &indexed); err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if chunk.Offset != offset || indexed != offset {
			return ErrAuthorityCorrupt
		}
		hash.Write([]byte(chunk.Text))
		offset += int64(len(chunk.Text))
	}
	if offset != source.Bytes || hex.EncodeToString(hash.Sum(nil)) != source.SHA256 {
		return ErrAuthorityCorrupt
	}
	return nil
}

func materializeOutputSource(ctx context.Context, tx spi.ReadTx, source OutputSource) (string, error) {
	var out strings.Builder
	out.Grow(int(source.Bytes))
	for offset := int64(0); offset < source.Bytes; {
		page, err := readOutputSourcePage(ctx, tx, source, offset, OutputSourceChunkBytes)
		if err != nil {
			return "", err
		}
		if page.NextOffset <= offset {
			return "", ErrAuthorityCorrupt
		}
		out.WriteString(page.Text)
		offset = page.NextOffset
	}
	digest := sha256.Sum256([]byte(out.String()))
	if hex.EncodeToString(digest[:]) != source.SHA256 {
		return "", ErrAuthorityCorrupt
	}
	return out.String(), nil
}
func retainOutputSource(tx spi.WriteTx, id string) error {
	source, err := readOutputSource(tx, id)
	if err != nil {
		return err
	}
	source.References++
	if source.References < 1 {
		return ErrAuthorityCorrupt
	}
	return putOutputRecord(tx, outputSourceKey(id), source)
}
func releaseOutputSource(ctx context.Context, tx spi.WriteTx, id string) error {
	source, err := readOutputSource(tx, id)
	if err != nil {
		return err
	}
	if source.References > 1 {
		source.References--
		return putOutputRecord(tx, outputSourceKey(id), source)
	}
	for seq := int64(1); seq <= source.Chunks; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Delete(outputSourceNamespace, outputChunkKey(id, seq)); err != nil {
			return err
		}
		if err := tx.Delete(outputSourceNamespace, outputChunkOffsetKey(id, seq)); err != nil {
			return err
		}
	}
	used, err := outputStoreUsage(tx)
	if err != nil {
		return err
	}
	if used < source.Bytes {
		return ErrAuthorityCorrupt
	}
	if err := putOutputRecord(tx, outputUsageKey(), used-source.Bytes); err != nil {
		return err
	}
	return tx.Delete(outputSourceNamespace, outputSourceKey(id))
}

func storeCanonicalArtifact(ctx context.Context, tx spi.WriteTx, record artifact.Record) error {
	if record.OutputSourceID == "" && len(record.Text) > OutputSourceChunkBytes {
		source, err := writeOutputSource(ctx, tx, outputSourceID(record.ThreadID, record.Ref.ID), record.Text)
		if err != nil {
			return err
		}
		if source.Bytes != record.Ref.SizeBytes || source.SHA256 != record.Ref.SHA256 {
			return ErrAuthorityCorrupt
		}
		record.OutputSourceID = source.ID
	}
	if record.OutputSourceID != "" {
		source, err := readOutputSource(tx, record.OutputSourceID)
		if err != nil {
			return err
		}
		if !source.Sealed || source.Bytes != record.Ref.SizeBytes || source.SHA256 != record.Ref.SHA256 {
			return ErrAuthorityCorrupt
		}
		record.Text = ""
	}
	key := artifactRecordKey(record.ThreadID, record.Ref.ID)
	return putBackendDomainRecordContext(ctx, tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0, record)
}

func hydrateArtifactOutput(ctx context.Context, tx spi.ReadTx, record *artifact.Record) error {
	if record.OutputSourceID == "" {
		return nil
	}
	if record.Text != "" {
		return ErrAuthorityCorrupt
	}
	source, err := readOutputSource(tx, record.OutputSourceID)
	if err != nil {
		return err
	}
	if !source.Sealed || source.Bytes != record.Ref.SizeBytes || source.SHA256 != record.Ref.SHA256 {
		return ErrAuthorityCorrupt
	}
	record.Text, err = materializeOutputSource(ctx, tx, source)
	return err
}
