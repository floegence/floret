package sessiontree

import (
	"errors"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type memberField struct {
	meta   CanonicalBodyMetadata
	class  string
	ref    member.BodyRef
	inline string
}

func locateMemberField(journal *member.Journal, id, field string) (memberField, error) {
	event, err := journal.Entry(id)
	if err != nil {
		return memberField{}, err
	}
	data, err := readMemberEntryData(journal, event)
	if err != nil {
		return memberField{}, err
	}
	value, err := canonicalFieldValue(data.Facts, field)
	if err != nil {
		return memberField{}, err
	}
	if field == "activity_json" {
		value = data.ActivityJSON
	}
	result := memberField{meta: CanonicalBodyMetadata{EntryID: id, Field: field, Bytes: len(value)}, inline: value}
	if ref, ok := data.Bodies[field]; ok {
		if value != "" || ref.Start != 0 || ref.End <= 0 || int64(int(ref.End)) != ref.End {
			return memberField{}, ErrAuthorityCorrupt
		}
		result.class, result.ref = memberBodyClass(EntryType(event.Kind), field), ref
		result.meta.Bytes, result.meta.SHA256 = int(ref.End), ref.Digest
		if _, err := journal.ReadBody(result.class, ref, 0, 0); err != nil {
			return memberField{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	} else {
		result.meta.SHA256 = StableHash(value)
	}
	return result, nil
}

func (field memberField) chunk(journal *member.Journal, offset, limit int) (CanonicalBodyChunk, error) {
	if offset < 0 || offset > field.meta.Bytes {
		return CanonicalBodyChunk{}, spi.ErrInvalidArgument
	}
	var data []byte
	if field.ref.Object != 0 {
		var err error
		data, err = journal.ReadBody(field.class, field.ref, int64(offset), limit)
		if err != nil {
			return CanonicalBodyChunk{}, err
		}
	} else {
		data = []byte(field.inline[offset : offset+min(limit, field.meta.Bytes-offset)])
	}
	text, err := canonicalUTF8Chunk(data, offset, field.meta.Bytes)
	if err != nil {
		return CanonicalBodyChunk{}, err
	}
	return boundBodyChunk(CanonicalBodyChunk{ThreadID: journal.Thread(), EntryID: field.meta.EntryID, Field: field.meta.Field, Offset: offset, Bytes: field.meta.Bytes, SHA256: field.meta.SHA256, Text: text})
}
