// Package storagecodec owns Floret's internal backend key and value encoding.
package storagecodec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
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
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var value envelope
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("storage envelope contains trailing data")
		}
		return nil, err
	}
	if value.Version != envelopeVersion || value.Kind != kind || len(value.Payload) == 0 {
		return nil, errors.New("unsupported storage envelope")
	}
	return bytes.Clone(value.Payload), nil
}
