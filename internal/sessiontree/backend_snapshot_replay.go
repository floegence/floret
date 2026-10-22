package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// Match the released decoder at each recovery boundary without retaining the
// whole repository. Typed members are decoded independently; authority needs
// only thread metadata and the first user message used for legacy titles.
func (source *snapshotEffectiveSource) verifyJournalReplay(ctx context.Context) error {
	base := *source
	base.sequences = nil
	for name, definition := range snapshotStateFields() {
		value, found, err := base.field(ctx, name)
		if err != nil {
			return err
		}
		if !found {
			if strings.Contains(definition.Tag.Get("json"), ",omitempty") {
				continue
			}
			return ErrAuthorityCorrupt
		}
		if err := validateSnapshotTypedValue(ctx, source.tx, definition.Type, value, snapshotSourceKey("payload", name)); err != nil {
			return err
		}
	}
	if err := base.validateReplayAuthority(ctx, true); err != nil {
		return err
	}
	for count := range source.sequences {
		if err := ctx.Err(); err != nil {
			return err
		}
		prefix := *source
		prefix.sequences = source.sequences[:count+1]
		prefix.journalBase = true
		if err := prefix.validateReplayTypes(ctx, source.sequences[count]); err != nil {
			return err
		}
		if err := prefix.validateReplayAuthority(ctx, false); err != nil {
			return err
		}
	}
	return nil
}

func validateSnapshotTypedValue(ctx context.Context, tx spi.ReadTx, shape reflect.Type, value snapshotValueSource, prefix []byte) error {
	if shape == reflect.TypeFor[json.RawMessage]() {
		// The bounded scanner has already checked the entire JSON syntax.
		return nil
	}
	if shape.Kind() != reflect.Map || value.span.Kind != '{' {
		return value.decode(ctx, tx, reflect.New(shape).Interface())
	}
	members, err := indexedJSONMembers(ctx, tx, prefix)
	if err != nil {
		return err
	}
	for _, member := range members {
		if err := ctx.Err(); err != nil {
			return err
		}
		value.span = member.span
		target := reflect.New(shape.Elem()).Interface()
		if shape.Elem() == reflect.TypeFor[ThreadTombstone]() {
			target = new(legacySnapshotTombstone)
		}
		if err := decodeSnapshotTypedMember(ctx, tx, value, target); err != nil {
			return err
		}
	}
	return nil
}

func (source *snapshotEffectiveSource) validateReplayAuthority(ctx context.Context, rejectRepair bool) error {
	ids, err := source.members(ctx, "threads")
	if err != nil {
		return err
	}
	metas := make([]ThreadMeta, 0, len(ids))
	for _, id := range ids {
		var meta ThreadMeta
		if err := source.decodeMember(ctx, &meta, "threads", id); err != nil || meta.ID != id {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if meta.Title == "" {
			value, found, err := source.member(ctx, "entries", id)
			if err != nil {
				return err
			}
			first := true
			if found {
				err = visitSnapshotEntries(ctx, source.tx, value, func(entry Entry) error {
					if !first || entry.Type != EntryUserMessage || entry.Message.Role != session.User {
						return nil
					}
					first = false
					updated, repaired, err := installFallbackThreadTitle(meta, entry.Message, entry.CreatedAt)
					if err != nil || rejectRepair && repaired {
						return errors.Join(ErrAuthorityCorrupt, err)
					}
					meta = updated
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		metas = append(metas, meta)
	}
	if err := ValidateThreadAuthorityGraph(metas); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

// Only replacements and set members can introduce newly typed values. Deletes
// still participate in effective resolution before validation, matching the
// released Replace -> Set -> Delete order within a single atomic frame.
func (source *snapshotEffectiveSource) validateReplayTypes(ctx context.Context, sequence uint64) error {
	fields := snapshotStateFields()
	changed := map[string]bool{}
	for _, operation := range []string{"replace", "set"} {
		members, err := indexedJSONMembers(ctx, source.tx, snapshotJournalSpanKey(sequence, "patch", operation))
		if err != nil {
			return err
		}
		for _, member := range members {
			changed[member.name] = true
		}
	}
	for name := range changed {
		definition := fields[name]
		value, found, err := source.field(ctx, name)
		if err != nil {
			return err
		}
		if !found {
			if strings.Contains(definition.Tag.Get("json"), ",omitempty") {
				continue
			}
			return ErrAuthorityCorrupt
		}
		shape := definition.Type
		if shape == reflect.TypeFor[json.RawMessage]() {
			continue
		}
		if shape.Kind() != reflect.Map || value.span.Kind != '{' {
			if err := value.decode(ctx, source.tx, reflect.New(shape).Interface()); err != nil {
				return err
			}
			continue
		}
		var keys []string
		_, replaced, err := snapshotJournalOptionalSpan(source.tx, sequence, "patch", "replace", name)
		if err != nil {
			return err
		}
		if replaced {
			keys, err = source.members(ctx, name)
			if err != nil {
				return err
			}
		} else {
			members, err := indexedJSONMembers(ctx, source.tx, snapshotJournalSpanKey(sequence, "patch", "set", name))
			if err != nil {
				return err
			}
			for _, member := range members {
				keys = append(keys, member.name)
			}
		}
		for _, key := range keys {
			target := reflect.New(shape.Elem()).Interface()
			if shape.Elem() == reflect.TypeFor[ThreadTombstone]() {
				target = new(legacySnapshotTombstone)
			}
			value, found, err := source.member(ctx, name, key)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if err := decodeSnapshotTypedMember(ctx, source.tx, value, target); err != nil {
				return err
			}
		}
	}
	return nil
}

func snapshotJournalOptionalSpan(tx spi.ReadTx, sequence uint64, path ...string) (snapshotSourceSpan, bool, error) {
	span, err := readJSONSourceSpan(tx, snapshotJournalSpanKey(sequence, path...))
	if errors.Is(err, spi.ErrNotFound) {
		return snapshotSourceSpan{}, false, nil
	}
	return span, err == nil, err
}

func decodeSnapshotTypedMember(ctx context.Context, tx spi.ReadTx, value snapshotValueSource, target any) error {
	if _, entries := target.(*[]Entry); entries {
		return visitSnapshotEntries(ctx, tx, value, nil)
	}
	return value.decode(ctx, tx, target)
}
