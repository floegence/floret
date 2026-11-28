//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func StageMemberJournal(ctx context.Context, source spi.ReadTx, destination *member.Journal) (int, error) {
	thread := destination.Thread()
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, thread)
	if err != nil {
		return 0, err
	}
	expected, err := readJournalCount(source, thread)
	if err != nil {
		return 0, err
	}
	number := int64(0)
	count, err := visitStoredJournal(ctx, source, thread, func(_ spi.Record, stored canonicalStoredEntry) error {
		if err := stageStoredMemberEntry(ctx, source, destination, number, stored); err != nil {
			return fmt.Errorf("stage entry %d (%s): %w", number, stored.Entry.Type, err)
		}
		number++
		return nil
	})
	if err != nil || count != expected || (count == 0) != (meta.LeafID == "") {
		return 0, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := destination.SetLeaf(meta.LeafID); err != nil {
		return 0, err
	}
	if err := transferMemberCheckpoints(source, destination, false); err != nil {
		return 0, err
	}
	return count, nil
}

func StageMemberReadItems(ctx context.Context, source spi.ReadTx, destination spi.WriteTx) error {
	journal := member.From(destination)
	if journal == nil {
		return ErrInvalidThreadAuthority
	}
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, journal.Thread())
	if err != nil {
		return err
	}
	if err := persistThreadMetadata(ctx, destination, meta); err != nil {
		return err
	}
	_, err = rebuildCanonicalQuery(ctx, destination, meta)
	if err != nil {
		return err
	}
	valid, err := journal.WindowValid()
	if err != nil || !valid {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

func transferMemberCheckpoints(source spi.ReadTx, target *member.Journal, verify bool) error {
	thread := target.Thread()
	todo, hasTodo, err := readThreadTodo(source, thread)
	if err != nil {
		return err
	}
	state, hasState, err := currentProviderState(source, thread)
	if err != nil {
		return err
	}
	check := func(value any, found bool, at time.Time, read func() ([]byte, bool, error), write func(int64, []byte) error) error {
		var raw []byte
		if found {
			if !time.Unix(0, at.UnixNano()).Equal(at) {
				return ErrAuthorityCorrupt
			}
			var err error
			raw, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}
		current, exists, err := read()
		if err != nil {
			return err
		}
		if verify {
			if exists != found || !bytes.Equal(current, raw) {
				return ErrAuthorityCorrupt
			}
			return nil
		}
		if exists {
			return spi.ErrConflict
		}
		if !found {
			return nil
		}
		return write(at.UnixNano(), raw)
	}
	if err := check(todo, hasTodo, todo.UpdatedAt, target.Todo, target.StoreTodo); err != nil {
		return err
	}
	return check(state, hasState, state.UpdatedAt, target.Continuation, target.StoreContinuation)
}

func stageStoredMemberEntry(ctx context.Context, source spi.ReadTx, destination *member.Journal, number int64, stored canonicalStoredEntry) error {
	entry := stored.Entry
	if entry.ThreadID != destination.Thread() {
		return ErrAuthorityCorrupt
	}
	event, err := entryMemberEvent(entry, number)
	if err != nil {
		return err
	}
	data := newMemberEntryData(entry)
	for _, field := range slices.Sorted(maps.Keys(stored.Fields)) {
		body := stored.Fields[field]
		if body.Bytes <= memberInlineFieldBytes {
			value, err := materializeCanonicalBody(ctx, source, body)
			if err != nil {
				return err
			}
			if field == "activity_json" {
				data.ActivityJSON = value
			} else if err := restoreCanonicalField(&data.Facts, field, value); err != nil {
				return err
			}
			continue
		}
		ref, err := stageImmutableBody(ctx, source, destination, memberBodyClass(entry.Type, field), event.At, body)
		if err != nil {
			return err
		}
		if field == "activity_json" && body.Bytes > member.BlockBytes {
			value, err := materializeCanonicalBody(ctx, source, body)
			if err != nil {
				return err
			}
			if err := indexMemberActivity(destination, ref, value); err != nil {
				return err
			}
		}
		if data.Bodies == nil {
			data.Bodies = map[string]member.BodyRef{}
		}
		if windowTextField(entry, field) {
			facts, err := readBodyTextFacts(source, body)
			if errors.Is(err, spi.ErrNotFound) {
				var value string
				value, err = materializeCanonicalBody(ctx, source, body)
				if err == nil {
					facts = canonicalTextFacts(value)
				}
			}
			if err != nil {
				return err
			}
			if data.Text == nil {
				data.Text = map[string]CanonicalTextFacts{}
			}
			data.Text[field] = facts
		}
		data.Bodies[field] = ref
	}
	_, err = storeMemberEntry(destination, event, data)
	return err
}

func VerifyMemberJournal(ctx context.Context, source spi.ReadTx, destination *member.Journal) (OfflineJournalProof, error) {
	var proof OfflineJournalProof
	thread := destination.Thread()
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, thread)
	if err != nil {
		return proof, err
	}
	count, leaf, err := destination.Head()
	if err != nil || leaf != meta.LeafID {
		return proof, errors.Join(ErrAuthorityCorrupt, err)
	}
	expected, err := readJournalCount(source, thread)
	if err != nil || count != int64(expected) || (expected == 0) != (leaf == "") {
		return proof, errors.Join(ErrAuthorityCorrupt, err)
	}
	hash := sha256.New()
	hash.Write([]byte("floret.member.journal.v2\x00"))
	frame := func(raw []byte) { hash.Write(binary.BigEndian.AppendUint64(nil, uint64(len(raw)))); hash.Write(raw) }
	frame([]byte(thread))
	frame([]byte(leaf))
	actualCount, err := visitStoredJournal(ctx, source, thread, func(_ spi.Record, stored canonicalStoredEntry) error {
		entry, err := materializeCanonicalStoredEntry(ctx, source, stored)
		if err != nil {
			return err
		}
		event, err := destination.EntryNumber(int64(proof.Entries))
		if err != nil {
			return err
		}
		data, err := readMemberEntryData(destination, event)
		if err != nil {
			return err
		}
		for field, original := range stored.Fields {
			if ref, ok := data.Bodies[field]; ok {
				if ref.Digest != original.SHA256 || ref.End != int64(original.Bytes) || ref.Start != 0 {
					return ErrAuthorityCorrupt
				}
				continue
			}
			text := data.ActivityJSON
			if field != "activity_json" {
				text, err = canonicalFieldValue(data.Facts, field)
				if err != nil {
					return err
				}
			}
			if len(text) != original.Bytes || StableHash(text) != original.SHA256 {
				return ErrAuthorityCorrupt
			}
		}
		copied, err := restoreMemberEntry(destination, event, data)
		if err != nil {
			return err
		}
		entry.CreatedAt = entry.CreatedAt.UTC()
		original, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		actual, err := json.Marshal(copied)
		if err != nil || !bytes.Equal(original, actual) {
			return fmt.Errorf("compare entry %d (%s): %w", proof.Entries, entry.Type, errors.Join(ErrAuthorityCorrupt, err))
		}
		frame(actual)
		proof.Entries++
		return nil
	})
	if err != nil || actualCount != expected || proof.Entries != expected {
		return OfflineJournalProof{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	copy(proof.Digest[:], hash.Sum(nil))
	if err := transferMemberCheckpoints(source, destination, true); err != nil {
		return OfflineJournalProof{}, err
	}
	return proof, nil
}

func VerifyMemberProviderProjection(ctx context.Context, source spi.ReadTx, destination *member.Journal, leaf string) (int, [32]byte, error) {
	meta, err := readPublishedThreadMeta(source, backendDomainV32Format, destination.Thread())
	if err != nil {
		return 0, [32]byte{}, err
	}
	if leaf == "" {
		leaf = meta.LeafID
	}
	var path []Entry
	if leaf != "" {
		path, err = canonicalCompletePath(ctx, source, meta.ID, leaf)
		if err != nil {
			return 0, [32]byte{}, err
		}
	}
	old, err := BuildContextProjectionChecked(path, ContextProjectionOptions{Purpose: ProjectionProviderRequest})
	if err != nil {
		return 0, [32]byte{}, err
	}
	candidate, err := memberProviderProjection(destination, leaf)
	if err != nil {
		return 0, [32]byte{}, err
	}
	original, err := json.Marshal(old)
	if err != nil {
		return 0, [32]byte{}, err
	}
	actual, err := json.Marshal(candidate)
	if err != nil || !bytes.Equal(original, actual) {
		return 0, [32]byte{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return len(candidate.Messages), sha256.Sum256(actual), nil
}
