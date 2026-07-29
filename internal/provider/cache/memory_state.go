package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const memoryStateVersion = 1

type memoryState struct {
	Version   int                      `json:"version"`
	Segments  []Segment                `json:"segments"`
	Toolsets  []ToolsetSnapshot        `json:"toolsets"`
	Requests  []ProviderRequestRecord  `json:"requests"`
	Responses []ProviderResponseRecord `json:"responses"`
}

// EncodeMemoryState returns a detached strict representation of prompt state.
func (store *MemoryStore) EncodeMemoryState() ([]byte, error) {
	if store == nil {
		return nil, errors.New("prompt memory store is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return json.Marshal(memoryState{
		Version: memoryStateVersion, Segments: store.segments, Toolsets: store.toolsets,
		Requests: store.requests, Responses: store.responses,
	})
}

// DecodeMemoryState constructs prompt state from one exact encoded value.
func DecodeMemoryState(encoded []byte) (*MemoryStore, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var state memoryState
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("prompt state contains trailing data")
		}
		return nil, err
	}
	if state.Version != memoryStateVersion {
		return nil, errors.New("unsupported prompt state version")
	}
	return &MemoryStore{
		segments:  append([]Segment(nil), state.Segments...),
		toolsets:  append([]ToolsetSnapshot(nil), state.Toolsets...),
		requests:  append([]ProviderRequestRecord(nil), state.Requests...),
		responses: append([]ProviderResponseRecord(nil), state.Responses...),
	}, nil
}
