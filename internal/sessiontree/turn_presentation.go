package sessiontree

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// These optional facts extend the existing journal metadata without rewriting
// released records or changing the domain schema. Missing final references mean
// that the accepted final response boundary was not recorded.
const (
	ResponseSegmentsMetadataKey     = "response_segments"
	FinalResponseEntriesMetadataKey = "final_response_entry_ids"
	OccurrenceTimeMetadataKey       = "occurred_at"
)

func EntryOccurrenceTime(entry Entry) (time.Time, error) {
	value := entry.Metadata[OccurrenceTimeMetadataKey]
	if value == "" {
		return entry.CreatedAt, nil
	}
	at, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid entry occurrence time", ErrAuthorityCorrupt)
	}
	return at, nil
}

func FinalResponseEntryIDs(entry Entry) ([]string, bool, error) {
	value, present := entry.Metadata[FinalResponseEntriesMetadataKey]
	if !present {
		return nil, false, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(value), &ids); err != nil || ids == nil {
		return nil, false, fmt.Errorf("%w: invalid final response references", ErrAuthorityCorrupt)
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) || seen[id] {
			return nil, false, fmt.Errorf("%w: duplicate or empty final response reference", ErrAuthorityCorrupt)
		}
		seen[id] = true
	}
	return ids, true, nil
}
