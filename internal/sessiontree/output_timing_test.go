package sessiontree

import (
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"testing"
	"time"
)

func TestOutputTimingOptionalMetadata(t *testing.T) {
	start := time.Unix(1700000000, 123456789).UTC()
	for _, test := range []struct {
		name, start, end   string
		kind               EntryType
		content, reasoning string
		invalid            bool
	}{
		{"historical", "", "", EntryAssistantMessage, "", "reason", false},
		{"single chunk", start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano), EntryAssistantMessage, "", "reason", false},
		{"answer", start.Format(time.RFC3339Nano), start.Add(time.Second).Format(time.RFC3339Nano), EntryAssistantMessage, "answer", "", false},
		{"missing end", start.Format(time.RFC3339Nano), "", EntryAssistantMessage, "", "reason", true},
		{"missing start", "", start.Format(time.RFC3339Nano), EntryAssistantMessage, "", "reason", true},
		{"malformed", "bad", start.Format(time.RFC3339Nano), EntryAssistantMessage, "", "reason", true},
		{"reversed", start.Format(time.RFC3339Nano), start.Add(-time.Second).Format(time.RFC3339Nano), EntryAssistantMessage, "", "reason", true},
		{"wrong kind", start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano), EntryUserMessage, "answer", "", true},
		{"two sources", start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano), EntryAssistantMessage, "answer", "reason", true},
		{"no source", start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano), EntryAssistantMessage, "", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry := Entry{Type: test.kind, Message: session.Message{ContentSourceID: test.content, ReasoningSourceID: test.reasoning}, Metadata: map[string]string{}}
			if test.start != "" {
				entry.Metadata[OutputStartedAtMetadataKey] = test.start
			}
			if test.end != "" {
				entry.Metadata[OutputEndedAtMetadataKey] = test.end
			}
			a, b, err := OutputTiming(entry)
			if test.invalid {
				if !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("invalid metadata accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.start == "" {
				if a != nil || b != nil {
					t.Fatal("invented historical timing")
				}
				return
			}
			if a == nil || b == nil || !a.Equal(start) || b.Format(time.RFC3339Nano) != test.end {
				t.Fatalf("wrong timing: %v %v", a, b)
			}
		})
	}
}
