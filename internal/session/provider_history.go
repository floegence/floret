package session

import (
	"errors"
	"strings"
)

// HasRetryEligibleDurableInput reports whether a canonical user message can be
// replayed without depending on turn-scoped supplemental context.
func HasRetryEligibleDurableInput(message Message) bool {
	return message.Role == User && message.Kind == MessageKindNormal && (strings.TrimSpace(message.Content) != "" || len(message.Attachments) > 0 || len(message.References) > 0 || len(message.Context) > 0)
}

// ProjectProviderHistory derives the complete provider history from canonical facts.
// The returned insertion index is relative to the projected non-system history.
func ProjectProviderHistory(history []Message, supplementalAnchorEntryID string) ([]Message, int, error) {
	anchor := strings.TrimSpace(supplementalAnchorEntryID)
	insertAt := -1
	anchorMatches := 0
	out := make([]Message, 0, len(history))
	var current *ModelContextSnapshot
	for _, original := range history {
		isAnchor := anchor != "" && original.EntryID == anchor
		if isAnchor {
			anchorMatches++
		}
		message := CloneMessage(original)
		message.Content = ProviderContent(message)
		if message.Kind == MessageKindCompactionSummary && message.ModelContextSnapshot != nil {
			current = nil
		}
		update, parts := renderModelContextParts(current, message.ModelContextSnapshot)
		if message.ModelContextSnapshot != nil {
			current = CloneModelContext(message.ModelContextSnapshot)
		}
		if message.Kind == MessageKindModelContext && message.ModelContextSnapshot != nil {
			if update == "" {
				continue
			}
			message.Content = update
			message.ModelContextParts = parts
		}
		message.References = nil
		message.Context = nil
		message.ModelContextSnapshot = nil
		message.Activity = nil
		message.ControlSignal = nil
		out = append(out, message)
		if isAnchor {
			insertAt = len(out)
		}
		if update != "" && message.Kind != MessageKindModelContext {
			out = append(out, Message{Role: User, Kind: MessageKindModelContext, Content: update, ModelContextParts: parts})
		}
	}
	if anchor != "" && anchorMatches != 1 {
		return nil, -1, errors.New("supplemental context anchor must match exactly one canonical message")
	}
	return out, insertAt, nil
}
