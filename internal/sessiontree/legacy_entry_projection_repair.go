package sessiontree

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

const legacyJSONReplacementEscape = `\ufffd`

// repairLegacyUTF8EntryProjections converges tool-result entries written by
// releases that persisted JSON's escaped replacement marker before the
// in-memory string itself was normalized. It accepts only that exact lexical
// difference; every other authority mismatch remains invalid.
func repairLegacyUTF8EntryProjections(repo *MemoryRepo) (bool, error) {
	if repo == nil {
		return false, ErrAuthorityCorrupt
	}
	repaired := false
	for threadID, entries := range repo.entries {
		for index := range entries {
			changed, err := repairLegacyUTF8EntryProjection(&entries[index])
			if err != nil {
				return false, err
			}
			repaired = repaired || changed
		}
		repo.entries[threadID] = entries
	}
	return repaired, nil
}

func repairLegacyUTF8EntryProjection(entry *Entry) (bool, error) {
	if entry == nil {
		return false, ErrAuthorityCorrupt
	}
	current := rawForEntry(*entry)
	if entry.Raw == current {
		return false, nil
	}
	if entry.Type != EntryToolResult || strings.TrimSpace(entry.Raw) == "" ||
		strings.TrimSpace(entry.RawHash) == "" || stableHash(entry.Raw) != entry.RawHash {
		return false, nil
	}
	if err := ValidateEntryMessageAttachments(*entry); err != nil {
		return false, nil
	}
	if err := ValidateEntryMessageReferences(*entry); err != nil {
		return false, nil
	}
	normalized, replacements := normalizeLegacyJSONReplacementEscapes(entry.Raw)
	if replacements == 0 || normalized != current {
		return false, nil
	}
	legacyValue, err := decodeCanonicalEntryRaw(entry.Raw)
	if err != nil {
		return false, nil
	}
	currentValue, err := decodeCanonicalEntryRaw(current)
	if err != nil || !reflect.DeepEqual(legacyValue, currentValue) {
		return false, nil
	}
	entry.Raw = current
	entry.RawHash = stableHash(current)
	return true, nil
}

func normalizeLegacyJSONReplacementEscapes(raw string) (string, int) {
	var normalized strings.Builder
	normalized.Grow(len(raw))
	replacements := 0
	for index := 0; index < len(raw); {
		if raw[index] != '\\' {
			normalized.WriteByte(raw[index])
			index++
			continue
		}
		runEnd := index
		for runEnd < len(raw) && raw[runEnd] == '\\' {
			runEnd++
		}
		runLength := runEnd - index
		if runLength%2 == 1 && strings.HasPrefix(raw[runEnd:], "ufffd") {
			normalized.WriteString(raw[index : runEnd-1])
			normalized.WriteRune('\ufffd')
			index = runEnd + len("ufffd")
			replacements++
			continue
		}
		normalized.WriteString(raw[index:runEnd])
		index = runEnd
	}
	return normalized.String(), replacements
}

func decodeCanonicalEntryRaw(raw string) (canonicalEntryRaw, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var value canonicalEntryRaw
	if err := decoder.Decode(&value); err != nil {
		return canonicalEntryRaw{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("entry Raw contains trailing data")
		}
		return canonicalEntryRaw{}, err
	}
	return value, nil
}
