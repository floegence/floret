package storagecodec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"runtime"
	"testing"
)

func TestDecodeJSONPreservesStrictStorageSemantics(t *testing.T) {
	type value struct {
		Label  string
		Count  int
		Nested struct{ Flag bool }
		Raw    json.RawMessage
	}
	for _, raw := range []string{
		`{"Label":"🌸","Count":2,"Nested":{"Flag":true},"Raw":{"a":1}}`,
		`{"label":"lowercase","Label":"last"}`,
		`{"Count":2,"Count":3}`,
		`{"Unknown":true}`,
		`{"Nested":{"Unknown":true}}`,
		`{"Count":1.2}`,
		`{"Raw": {"Label":"retained"}}`,
		`{} {}`, `{} trailing`, `null`, `[]`, "", "  ",
	} {
		t.Run(raw, func(t *testing.T) {
			var got, want value
			decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
			decoder.DisallowUnknownFields()
			expected := decoder.Decode(&want)
			if expected == nil {
				if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
					expected = errors.New("trailing")
				}
			}
			actual := DecodeJSON([]byte(raw), &got)
			if (actual == nil) != (expected == nil) || actual == nil && !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%+v err=%v want=%+v err=%v", got, actual, want, expected)
			}
		})
	}
}

func TestDecodeJSONDoesNotCopyCompleteInput(t *testing.T) {
	raw := append([]byte(`{"Flag":true}`), bytes.Repeat([]byte(" "), 1<<20)...)
	var warm struct{ Flag bool }
	if err := DecodeJSON(raw, &warm); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 8 {
		var value struct{ Flag bool }
		if err := DecodeJSON(raw, &value); err != nil || !value.Flag {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if bytes := after.TotalAlloc - before.TotalAlloc; bytes > 1<<20 {
		t.Fatalf("decoding already resident input allocated %d bytes", bytes)
	}
}

func TestDecodeEnvelopeIntoMatchesDetachedDecoder(t *testing.T) {
	type record struct {
		Name   string
		Values []int
	}
	source := record{Name: "source", Values: []int{1, 2, 3}}
	payload, _ := json.Marshal(source)
	raw, _ := EncodeEnvelope("typed", payload)
	var got record
	if err := DecodeEnvelopeInto(raw, "typed", &got); err != nil || !reflect.DeepEqual(got, source) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	raw[0] = '!'
	if got.Name != "source" {
		t.Fatal("result aliases caller bytes")
	}
	for _, raw := range []string{
		`{"version":1,"kind":"wrong","payload":{}}`,
		`{"version":2,"kind":"typed","payload":{}}`,
		`{"version":1,"kind":"typed","payload":{"unknown":1}}`,
		`{"version":1,"kind":"typed","payload":{},"unknown":1}`,
		`{"version":1,"kind":"typed"}`,
	} {
		var got record
		if err := DecodeEnvelopeInto([]byte(raw), "typed", &got); err == nil {
			t.Fatalf("accepted invalid envelope %s", raw)
		}
	}
}

func TestTypedEnvelopeReplacesDuplicatePayload(t *testing.T) {
	type record struct {
		Name  string
		Count int
	}
	var got record
	raw := []byte("{\"version\":1,\"kind\":\"typed\",\"payload\":{\"Name\":\"discard\",\"Count\":3},\"payload\":{\"Count\":2}}")
	if err := DecodeEnvelopeInto(raw, "typed", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "" || got.Count != 2 {
		t.Fatalf("duplicate payload merged: %+v", got)
	}
}
