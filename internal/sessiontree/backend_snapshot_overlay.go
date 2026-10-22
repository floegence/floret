package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

type snapshotValueSource struct {
	namespace string
	key       []byte
	span      snapshotSourceSpan
	constant  json.RawMessage
}

type snapshotEffectiveSource struct {
	tx          spi.ReadTx
	sequences   []uint64
	journalBase bool
}

// Released frames use contiguous fixed-width decimal keys. Probe their sizes
// and the empty gaps without loading valid frame values merely to discover keys.
func snapshotJournalSequences(ctx context.Context, tx spi.ReadTx) ([]uint64, error) {
	var sequences []uint64
	for sequence := uint64(1); ; sequence++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := backendJournalKey(sequence)
		start := []byte(nil)
		if sequence > 1 {
			start = append(backendJournalKey(sequence-1), 0)
		}
		gap, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainJournalNamespace, Start: start, End: key, Limit: 1})
		if err != nil {
			return nil, err
		}
		if len(gap.Records) > 0 || gap.HasMore {
			return nil, ErrAuthorityCorrupt
		}
		size, err := historicalRecordSize(tx, backendDomainJournalNamespace, key)
		if errors.Is(err, spi.ErrNotFound) {
			tail, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainJournalNamespace, Start: key, Limit: 1})
			if err != nil {
				return nil, err
			}
			if len(tail.Records) > 0 || tail.HasMore {
				return nil, ErrAuthorityCorrupt
			}
			return sequences, nil
		}
		if err != nil {
			return nil, err
		}
		if size <= 0 || sequence == math.MaxUint64 {
			return nil, ErrAuthorityCorrupt
		}
		sequences = append(sequences, sequence)
	}
}

func newSnapshotEffectiveSource(ctx context.Context, tx spi.ReadTx, version int) (*snapshotEffectiveSource, error) {
	sequences, err := snapshotJournalSequences(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(sequences) > 0 && version != 5 {
		return nil, ErrAuthorityCorrupt
	}
	for _, sequence := range sequences {
		if err := verifySnapshotJournalFrame(ctx, tx, sequence); err != nil {
			return nil, err
		}
	}
	source := &snapshotEffectiveSource{tx: tx, sequences: sequences}
	if err := source.validatePatches(ctx); err != nil {
		return nil, err
	}
	if len(sequences) > 0 {
		if err := source.verifyJournalReplay(ctx); err != nil {
			return nil, err
		}
		source.journalBase = true
	}
	return source, nil
}

func (source *snapshotEffectiveSource) field(ctx context.Context, name string) (snapshotValueSource, bool, error) {
	span, err := readSnapshotSourceSpan(source.tx, "payload", name)
	found := err == nil
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return snapshotValueSource{}, false, err
	}
	value := snapshotValueSource{namespace: backendDomainNamespace, key: backendStateKey, span: span}
	if source.journalBase {
		if !snapshotJournalBaseField(name) {
			found = false
		} else if name == "effect_attempts" || name == "effect_attempt_by_invocation" {
			value.constant = json.RawMessage("{}")
			value.span.Kind = '{'
			found = true
		}
	}
	for _, sequence := range source.sequences {
		replacement, err := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", "replace", name))
		if errors.Is(err, spi.ErrNotFound) {
			continue
		}
		if err != nil {
			return value, false, err
		}
		value = snapshotValueSource{namespace: backendDomainJournalNamespace, key: backendJournalKey(sequence), span: replacement}
		found = true
		if replacement.Kind == 'l' {
			var literal any
			if err := value.decode(ctx, source.tx, &literal); err != nil {
				return value, false, err
			}
			if literal == nil {
				found = false
			}
		}
	}
	return value, found, nil
}

// Members are resolved in the released Replace -> Set -> Delete order for
// every frame. Return a source locator, never a separately persisted value.
func (source *snapshotEffectiveSource) member(ctx context.Context, field, key string) (snapshotValueSource, bool, error) {
	span, err := readSnapshotSourceSpan(source.tx, "payload", field, key)
	found := err == nil
	if err != nil && !errors.Is(err, spi.ErrNotFound) {
		return snapshotValueSource{}, false, err
	}
	value := snapshotValueSource{namespace: backendDomainNamespace, key: backendStateKey, span: span}
	if source.journalBase && (!snapshotJournalBaseField(field) || field == "effect_attempts" || field == "effect_attempt_by_invocation") {
		found = false
	}
	for _, sequence := range source.sequences {
		replacement, err := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", "replace", field))
		if err == nil {
			found = false
			if replacement.Kind == '{' {
				span, readErr := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", "replace", field, key))
				if readErr != nil && !errors.Is(readErr, spi.ErrNotFound) {
					return value, false, readErr
				}
				if readErr == nil {
					value = snapshotValueSource{namespace: backendDomainJournalNamespace, key: backendJournalKey(sequence), span: span}
					found = true
				}
			}
		} else if !errors.Is(err, spi.ErrNotFound) {
			return value, false, err
		}
		set, err := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", "set", field, key))
		if err == nil {
			value = snapshotValueSource{namespace: backendDomainJournalNamespace, key: backendJournalKey(sequence), span: set}
			found = true
		} else if !errors.Is(err, spi.ErrNotFound) {
			return value, false, err
		}
		deleted, err := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", "delete", field))
		if err == nil {
			var keys []string
			if err := decodeJSONSourceValue(ctx, source.tx, backendDomainJournalNamespace, backendJournalKey(sequence), deleted, &keys); err != nil {
				return value, false, err
			}
			for _, candidate := range keys {
				if candidate == key {
					found = false
					break
				}
			}
		} else if !errors.Is(err, spi.ErrNotFound) {
			return value, false, err
		}
	}
	return value, found, nil
}

func (value snapshotValueSource) decode(ctx context.Context, tx spi.ReadTx, target any) error {
	if value.constant != nil {
		return json.Unmarshal(value.constant, target)
	}
	return decodeJSONSourceValue(ctx, tx, value.namespace, value.key, value.span, target)
}

func (value snapshotValueSource) raw(ctx context.Context, tx spi.ReadTx) ([]byte, error) {
	var raw json.RawMessage
	if err := value.decode(ctx, tx, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (source *snapshotEffectiveSource) decodeMember(ctx context.Context, target any, field, key string) error {
	value, found, err := source.member(ctx, field, key)
	if err != nil {
		return err
	}
	if !found {
		return spi.ErrNotFound
	}
	return value.decode(ctx, source.tx, target)
}

func (source *snapshotEffectiveSource) members(ctx context.Context, field string) ([]string, error) {
	keys := map[string]bool{}
	members, err := indexedJSONMembers(ctx, source.tx, snapshotSourceKey("payload", field))
	if err != nil {
		return nil, err
	}
	for _, member := range members {
		keys[member.name] = true
	}
	if source.journalBase && (!snapshotJournalBaseField(field) || field == "effect_attempts" || field == "effect_attempt_by_invocation") {
		clear(keys)
	}
	for _, sequence := range source.sequences {
		for _, operation := range []string{"replace", "set"} {
			if operation == "replace" {
				if _, err := readJSONSourceSpan(source.tx, snapshotJournalSpanKey(sequence, "patch", operation, field)); err == nil {
					clear(keys)
				} else if !errors.Is(err, spi.ErrNotFound) {
					return nil, err
				}
			}
			members, err := indexedJSONMembers(ctx, source.tx, snapshotJournalSpanKey(sequence, "patch", operation, field))
			if err != nil {
				return nil, err
			}
			for _, member := range members {
				keys[member.name] = true
			}
		}
		var removed []string
		if err := decodeSnapshotJournalValue(ctx, source.tx, sequence, &removed, "patch", "delete", field); err == nil {
			for _, key := range removed {
				delete(keys, key)
			}
		} else if !errors.Is(err, spi.ErrNotFound) {
			return nil, err
		}
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	return result, nil
}

func loadEffectiveSnapshotScopedState(ctx context.Context, source *snapshotEffectiveSource, version int, selected map[string]bool) (memoryState, error) {
	var state memoryState
	shape := reflect.ValueOf(&state).Elem()
	for index := 0; index < shape.NumField(); index++ {
		field := shape.Field(index)
		definition := shape.Type().Field(index)
		name, _, _ := strings.Cut(definition.Tag.Get("json"), ",")
		value, found, err := source.field(ctx, name)
		if err != nil {
			return state, err
		}
		if !found && strings.Contains(definition.Tag.Get("json"), ",omitempty") {
			continue
		}
		if !found {
			return state, ErrAuthorityCorrupt
		}
		if field.Kind() != reflect.Map && field.Type() != reflect.TypeFor[json.RawMessage]() || name == "lease_policy" || value.span.Kind != '{' {
			if err := value.decode(ctx, source.tx, field.Addr().Interface()); err != nil {
				return state, err
			}
			continue
		}
		if field.Kind() == reflect.Map {
			field.Set(reflect.MakeMap(field.Type()))
		}
		var keys []string
		if snapshotFieldKeyedByThread(name) {
			for id := range selected {
				keys = append(keys, id)
			}
		} else {
			keys, err = source.members(ctx, name)
			if err != nil {
				return state, err
			}
		}
		rawMembers := map[string]json.RawMessage{}
		for _, key := range keys {
			member, found, err := source.member(ctx, name, key)
			if err != nil {
				return state, err
			}
			if !found {
				continue
			}
			keep := selected[key]
			switch name {
			case "root_create_intents":
				var record legacyRootCreateRecord
				if err := member.decode(ctx, source.tx, &record); err != nil {
					return state, err
				}
				keep = selected[record.ThreadID]
			case "effect_attempts":
				var record legacyEffectAttemptV6
				if err := member.decode(ctx, source.tx, &record); err != nil {
					return state, err
				}
				keep = selected[record.Invocation.ThreadID]
			case "artifacts", "turn_admissions", "turn_finishes", "effect_attempt_by_invocation":
				owner, _, ok := strings.Cut(key, "\x00")
				keep = ok && selected[owner]
			}
			if !keep {
				continue
			}
			if field.Kind() != reflect.Map {
				raw, err := member.raw(ctx, source.tx)
				if err != nil {
					return state, err
				}
				rawMembers[key] = raw
				continue
			}
			if name == "tombstones" {
				var tombstone legacySnapshotTombstone
				if err := member.decode(ctx, source.tx, &tombstone); err != nil {
					return state, err
				}
				state.Tombstones[key] = snapshotTombstoneAtVersion(tombstone, version)
				continue
			}
			decoded := reflect.New(field.Type().Elem()).Elem()
			if err := member.decode(ctx, source.tx, decoded.Addr().Interface()); err != nil {
				return state, err
			}
			field.SetMapIndex(reflect.ValueOf(key), decoded)
		}
		if field.Kind() != reflect.Map {
			raw, err := json.Marshal(rawMembers)
			if err != nil {
				return state, err
			}
			field.SetBytes(raw)
		}
	}
	return state, nil
}

// Validate patch field types even when the requested closure has no member in
// that field. An invalid operation must not be hidden by selecting a small view.
func (source *snapshotEffectiveSource) validatePatches(ctx context.Context) error {
	kinds := map[string]byte{}
	fields := snapshotStateFields()
	for name := range fields {
		span, err := readSnapshotSourceSpan(source.tx, "payload", name)
		if err == nil && (len(source.sequences) == 0 || snapshotJournalBaseField(name)) {
			kinds[name] = span.Kind
		} else if err != nil && !errors.Is(err, spi.ErrNotFound) {
			return err
		}
	}
	for _, sequence := range source.sequences {
		for _, operation := range []string{"replace", "set", "delete"} {
			members, err := indexedJSONMembers(ctx, source.tx, snapshotJournalSpanKey(sequence, "patch", operation))
			if err != nil {
				return err
			}
			for _, member := range members {
				if _, known := fields[member.name]; !known {
					return ErrAuthorityCorrupt
				}
				if operation == "replace" {
					if member.name == "version" {
						var version int
						if err := decodeJSONSourceValue(ctx, source.tx, backendDomainJournalNamespace, backendJournalKey(sequence), member.span, &version); err != nil || version != 5 {
							return errors.Join(ErrAuthorityCorrupt, err)
						}
					}
					if fields[member.name].Type.Kind() == reflect.Map && member.span.Kind != '{' && !strings.Contains(fields[member.name].Tag.Get("json"), ",omitempty") {
						return ErrAuthorityCorrupt
					}
					kinds[member.name] = member.span.Kind
					continue
				}
				if kinds[member.name] != '{' {
					return ErrAuthorityCorrupt
				}
			}
		}
	}
	return nil
}

// Released replay serializes the decoded checkpoint before applying frames.
// Obsolete fields disappear and effect receipt maps become empty at that point.
func snapshotJournalBaseField(name string) bool {
	switch name {
	case "version", "threads", "entries", "entry_ordinals", "entry_depths", "turn_entry_ordinals", "turn_entry_counts", "todos", "tombstones", "effect_attempts", "effect_attempt_by_invocation", "provider_states", "artifacts", "sequence":
		return true
	default:
		return false
	}
}
