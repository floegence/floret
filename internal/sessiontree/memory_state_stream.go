package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Decode map members and journal entries separately. json.Decoder otherwise
// buffers the entire snapshot before constructing its first typed value.
// This reader is confined to the historical migration path.
func decodeMemoryStateValue(decoder *json.Decoder) (memoryState, error) {
	var state memoryState
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return state, errors.Join(ErrAuthorityCorrupt, err)
	}
	value := reflect.ValueOf(&state).Elem()
	fields := map[string]reflect.Value{}
	for index := 0; index < value.NumField(); index++ {
		name, _, _ := strings.Cut(value.Type().Field(index).Tag.Get("json"), ",")
		fields[name] = value.Field(index)
	}
	seen := map[string]bool{}
	var tombstones map[string]legacySnapshotTombstone
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return state, err
		}
		name, ok := token.(string)
		field, known := fields[name]
		if !ok || !known || seen[name] {
			return state, fmt.Errorf("%w: unknown or repeated snapshot field %q", ErrAuthorityCorrupt, name)
		}
		seen[name] = true
		if name == "tombstones" {
			field = reflect.ValueOf(&tombstones).Elem()
		}
		if err := decodeSnapshotCollection(decoder, field); err != nil {
			return state, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return state, errors.Join(ErrAuthorityCorrupt, err)
	}
	if tombstones != nil {
		state.Tombstones = make(map[string]ThreadTombstone, len(tombstones))
		for id, source := range tombstones {
			state.Tombstones[id] = snapshotTombstoneAtVersion(source, state.Version)
		}
	}
	return state, nil
}

type legacySnapshotTombstone struct {
	current ThreadTombstone
	legacy  legacyV4Tombstone
}

func (value *legacySnapshotTombstone) UnmarshalJSON(raw []byte) error {
	if err := json.Unmarshal(raw, &value.current); err != nil {
		return err
	}
	return json.Unmarshal(raw, &value.legacy)
}

func decodeSnapshotCollection(decoder *json.Decoder, value reflect.Value) error {
	// RawMessage is a historical opaque authority record, not a JSON array.
	if value.Type() == reflect.TypeFor[json.RawMessage]() || value.Kind() != reflect.Map && value.Kind() != reflect.Slice {
		return decoder.Decode(value.Addr().Interface())
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return nil
	}
	if value.Kind() == reflect.Map {
		if token != json.Delim('{') || value.Type().Key().Kind() != reflect.String {
			return ErrAuthorityCorrupt
		}
		value.Set(reflect.MakeMap(value.Type()))
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return ErrAuthorityCorrupt
			}
			mapKey := reflect.ValueOf(key).Convert(value.Type().Key())
			if value.MapIndex(mapKey).IsValid() {
				return fmt.Errorf("%w: repeated snapshot map key %q", ErrAuthorityCorrupt, key)
			}
			member := reflect.New(value.Type().Elem()).Elem()
			if err := decodeSnapshotCollection(decoder, member); err != nil {
				return err
			}
			value.SetMapIndex(mapKey, member)
		}
		if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	}
	if token != json.Delim('[') {
		return ErrAuthorityCorrupt
	}
	value.Set(reflect.MakeSlice(value.Type(), 0, 0))
	for decoder.More() {
		member := reflect.New(value.Type().Elem()).Elem()
		if err := decodeSnapshotCollection(decoder, member); err != nil {
			return err
		}
		value.Set(reflect.Append(value, member))
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}
