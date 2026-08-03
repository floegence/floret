package storagecodec

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

func TestTupleEncodingIsUnambiguousAndOrdinalOrdered(t *testing.T) {
	if got, want := TupleString("abc"), []byte{1, 0, 0, 0, 3, 'a', 'b', 'c'}; !bytes.Equal(got, want) {
		t.Fatalf("tuple string encoding = %v, want %v", got, want)
	}
	left := Tuple(TupleString("a"), TupleString("bc"))
	right := Tuple(TupleString("ab"), TupleString("c"))
	if bytes.Equal(left, right) {
		t.Fatal("length-delimited tuples collided")
	}
	for lower := uint64(0); lower < 255; lower++ {
		if bytes.Compare(Tuple(TupleOrdinal(lower)), Tuple(TupleOrdinal(lower+1))) >= 0 {
			t.Fatalf("ordinal encoding does not preserve order at %d", lower)
		}
	}
}

func TestTupleComponentRejectsUnrepresentableLength(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("tuple component accepted a length larger than uint32")
		}
	}()
	tupleComponentHeader(1, uint64(math.MaxUint32)+1)
}

func TestEnvelopeRejectsUnknownAndTrailingShapes(t *testing.T) {
	encoded, err := EncodeEnvelope("test", []byte(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DecodeEnvelope(encoded, "test")
	if err != nil || string(payload) != `{"value":1}` {
		t.Fatalf("decoded payload = %s, err = %v", payload, err)
	}

	var shape map[string]any
	if err := json.Unmarshal(encoded, &shape); err != nil {
		t.Fatal(err)
	}
	shape["unknown"] = true
	unknown, _ := json.Marshal(shape)
	for name, invalid := range map[string][]byte{
		"unknown":  unknown,
		"trailing": append(append([]byte(nil), encoded...), []byte(` {}`)...),
	} {
		if _, err := DecodeEnvelope(invalid, "test"); err == nil {
			t.Fatalf("%s envelope passed validation", name)
		}
	}
}
