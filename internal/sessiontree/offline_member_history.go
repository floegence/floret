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
	return count, destination.SetLeaf(meta.LeafID)
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
		if data.Bodies == nil {
			data.Bodies = map[string]member.BodyRef{}
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
