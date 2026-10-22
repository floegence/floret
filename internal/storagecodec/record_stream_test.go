package storagecodec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

type boundedStreamTx struct {
	spi.ReadTx
	data      []byte
	maxRead   int
	readError error
	short     bool
}

func (tx *boundedStreamTx) NamespaceHasRecords(string) (bool, error) { return true, nil }
func (tx *boundedStreamTx) RecordSize(string, []byte) (int64, error) { return int64(len(tx.data)), nil }
func (tx *boundedStreamTx) ReadRecordRange(_ string, _ []byte, offset int64, limit int) ([]byte, error) {
	tx.maxRead = max(tx.maxRead, limit)
	if tx.readError != nil {
		return nil, tx.readError
	}
	if tx.short {
		return nil, nil
	}
	return bytes.Clone(tx.data[offset:min(int64(len(tx.data)), offset+int64(limit))]), nil
}

func TestRecordStreamBoundsReadsAndStopsOnFailure(t *testing.T) {
	tx := &boundedStreamTx{data: bytes.Repeat([]byte("retained ✓"), 100000)}
	ctx, cancel := context.WithCancel(t.Context())
	reader, err := OpenRecord(ctx, tx, "history", []byte("snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(raw, tx.data) || tx.maxRead > recordReadBlockBytes {
		t.Fatalf("read mismatch: bytes=%d maximum=%d err=%v", len(raw), tx.maxRead, err)
	}
	cancel()
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	injected := errors.New("range access failed")
	tx.readError = injected
	reader, err = OpenRecord(t.Context(), tx, "history", []byte("snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	tx.readError, tx.short = nil, true
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("truncated source accepted")
	}
}

func TestStreamingEnvelopeValidatesCompleteEnvelope(t *testing.T) {
	for _, input := range []string{
		"{\"version\":1,\"kind\":\"history\",\"payload\":[1,2]}",
		"{\"payload\":[1,2],\"kind\":\"history\",\"version\":1}",
	} {
		var values []int
		if err := DecodeEnvelopeStream(strings.NewReader(input), "history", func(decoder *json.Decoder) error { return decoder.Decode(&values) }); err != nil || len(values) != 2 {
			t.Fatalf("decode = %v, %v", values, err)
		}
	}
	for _, input := range []string{
		"null", "{}", "{\"version\":1,\"kind\":\"history\"}",
		"{\"version\":2,\"kind\":\"history\",\"payload\":[]}",
		"{\"version\":1,\"kind\":\"wrong\",\"payload\":[]}",
		"{\"version\":1,\"kind\":\"history\",\"payload\":[],\"unknown\":true}",
		"{\"version\":1,\"kind\":\"history\",\"payload\":[],\"payload\":[]}",
		"{\"version\":1,\"kind\":\"history\",\"payload\":[]}{}",
		"{\"version\":1,\"kind\":\"history\",\"payload\":[]",
	} {
		if err := DecodeEnvelopeStream(strings.NewReader(input), "history", func(decoder *json.Decoder) error { var values []int; return decoder.Decode(&values) }); err == nil {
			t.Fatalf("invalid envelope accepted: %s", input)
		}
	}
}
