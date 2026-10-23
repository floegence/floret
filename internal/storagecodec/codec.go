// Package storagecodec owns Floret's internal backend key and value encoding.
package storagecodec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const (
	tupleVersion    = 1
	envelopeVersion = 1
)

// TupleString encodes a string component with an explicit length.
func TupleString(value string) []byte {
	return tupleComponent(1, []byte(value))
}

// TupleOrdinal encodes an ordinal so byte order preserves numeric order.
func TupleOrdinal(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return tupleComponent(2, encoded)
}

// Tuple joins already encoded components into one versioned binary key.
func Tuple(components ...[]byte) []byte {
	var encoded bytes.Buffer
	encoded.WriteByte(tupleVersion)
	for _, component := range components {
		encoded.Write(component)
	}
	return encoded.Bytes()
}

func tupleComponent(tag byte, value []byte) []byte {
	header := tupleComponentHeader(tag, uint64(len(value)))
	return append(header[:], value...)
}

func tupleComponentHeader(tag byte, length uint64) [5]byte {
	if length > math.MaxUint32 {
		panic("storagecodec: tuple component exceeds uint32 length")
	}
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:], uint32(length))
	return header
}

type envelope struct {
	Version int             `json:"version"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// EncodeEnvelope wraps one domain payload in the strict logical value format.
func EncodeEnvelope(kind string, payload []byte) ([]byte, error) {
	if kind == "" || len(payload) == 0 || !json.Valid(payload) {
		return nil, errors.New("storage envelope requires kind and JSON payload")
	}
	return json.Marshal(envelope{Version: envelopeVersion, Kind: kind, Payload: payload})
}

// DecodeEnvelope validates and returns one exact domain payload.
func DecodeEnvelope(encoded []byte, kind string) ([]byte, error) {
	var value envelope
	if err := DecodeJSON(encoded, &value); err != nil {
		return nil, err
	}
	if value.Version != envelopeVersion || value.Kind != kind || len(value.Payload) == 0 {
		return nil, errors.New("unsupported storage envelope")
	}
	return value.Payload, nil
}

// JSONValue preserves RawMessage's replace-on-duplicate semantics while decoding
// a nested value directly into detached typed storage.
type JSONValue[T any] struct {
	Value   T
	Present bool
}

func (value *JSONValue[T]) UnmarshalJSON(raw []byte) error {
	var next T
	if err := DecodeJSON(raw, &next); err != nil {
		return err
	}
	value.Value, value.Present = next, true
	return nil
}

// DecodeEnvelopeInto validates the same envelope while decoding a typed payload.
func DecodeEnvelopeInto[T any](encoded []byte, kind string, target *T) error {
	var value struct {
		Version int          `json:"version"`
		Kind    string       `json:"kind"`
		Payload JSONValue[T] `json:"payload"`
	}
	if err := DecodeJSON(encoded, &value); err != nil {
		return err
	}
	if value.Version != envelopeVersion || value.Kind != kind || !value.Payload.Present {
		return errors.New("unsupported storage envelope")
	}
	*target = value.Payload.Value
	return nil
}
