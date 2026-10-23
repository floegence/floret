package sessiontree

import (
	"fmt"
	"time"
)

// Optional output facts use existing canonical metadata. Historical entries
// remain byte-identical: absence means unmeasured, never a guessed interval.
const OutputStartedAtMetadataKey = "output_started_at"
const OutputEndedAtMetadataKey = "output_ended_at"

func OutputTiming(entry Entry) (started, ended *time.Time, err error) {
	start, hasStart := entry.Metadata[OutputStartedAtMetadataKey]
	end, hasEnd := entry.Metadata[OutputEndedAtMetadataKey]
	if !hasStart && !hasEnd {
		return nil, nil, nil
	}
	invalid := func() (*time.Time, *time.Time, error) {
		return nil, nil, fmt.Errorf("%w: invalid output timing on %s", ErrAuthorityCorrupt, entry.ID)
	}
	if !hasStart || !hasEnd || entry.Type != EntryAssistantMessage ||
		(entry.Message.ContentSourceID == "") == (entry.Message.ReasoningSourceID == "") {
		return invalid()
	}
	a, firstErr := time.Parse(time.RFC3339Nano, start)
	b, lastErr := time.Parse(time.RFC3339Nano, end)
	if firstErr != nil || lastErr != nil || a.IsZero() || b.IsZero() || b.Before(a) {
		return invalid()
	}
	return &a, &b, nil
}
