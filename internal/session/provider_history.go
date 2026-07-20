package session

import (
	"errors"
	"strings"
)

func IsReferenceOnlyUserMessage(message Message) bool {
	return message.Role == User && strings.TrimSpace(message.Content) == "" && len(message.Attachments) == 0 && len(message.References) > 0
}

// ProjectProviderHistory removes durable reference data from the provider view.
// The returned insertion index is relative to the projected non-system history.
func ProjectProviderHistory(history []Message, supplementalAnchorEntryID string) ([]Message, int, error) {
	anchor := strings.TrimSpace(supplementalAnchorEntryID)
	insertAt := -1
	anchorMatches := 0
	out := make([]Message, 0, len(history))
	for _, original := range history {
		isAnchor := anchor != "" && original.EntryID == anchor
		if isAnchor {
			anchorMatches++
		}
		if IsReferenceOnlyUserMessage(original) {
			if isAnchor {
				insertAt = len(out)
			}
			continue
		}
		message := CloneMessage(original)
		message.References = nil
		out = append(out, message)
		if isAnchor {
			insertAt = len(out)
		}
	}
	if anchor != "" && anchorMatches != 1 {
		return nil, -1, errors.New("supplemental context anchor must match exactly one canonical message")
	}
	return out, insertAt, nil
}
