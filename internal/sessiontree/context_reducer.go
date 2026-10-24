package sessiontree

import (
	"github.com/floegence/floret/v7/internal/session"
	"strings"
)

// ContextReducer validates a forward-only canonical projection. It releases
// compacted message bodies after each checkpoint; only user sources needed by
// retained-user checkpoint references survive outside the current context.
// Retry selects a different path and must first use retryProjectedContextPath.
// This reducer is never a replacement for that selection boundary.
type ContextReducer struct {
	messages []session.Message
	users    map[string]session.Message
	steering steeringProjection
	controls ControlResultProjection
}

func (projection *ContextReducer) Append(entries []Entry) error {
	for _, source := range entries {
		entry, err := projection.steering.observe(source)
		if err != nil {
			return err
		}
		redundant, err := projection.controls.Observe(entry)
		if err != nil {
			return err
		}
		if entry.Type == EntryCompaction {
			projection.messages, err = projectCheckpointWithSource(projection.messages, entry, func(id string) (session.Message, bool, error) {
				source, found := projection.users[id]
				return source, found, nil
			})
		} else if redundant == "" {
			before := len(projection.messages)
			projection.messages, err = appendProviderVisible(projection.messages, entry)
			if err == nil {
				for _, message := range projection.messages[before:] {
					if message.Role == session.User {
						if projection.users == nil {
							projection.users = map[string]session.Message{}
						}
						if _, exists := projection.users[message.EntryID]; !exists {
							projection.users[message.EntryID] = message
						}
					}
				}
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Messages is owned by the reducer and remains valid until the next Append.
// Callers needing an independent snapshot must clone it.
func (projection *ContextReducer) Messages() []session.Message { return projection.messages }

// ModelContextReducer accepts canonical presentation sources with ordinary
// local tool arguments and result text omitted. Those fields do not participate
// in managed-context selection or checkpoint validation. Control-owned results,
// steering, summaries, retained users and Goal facts must remain complete.
// It uses the same context reducer, but exposes only managed facts: its partial
// messages can never escape as provider history.
type ModelContextReducer struct {
	context ContextReducer
}

func (projection *ModelContextReducer) Append(entries []Entry) error {
	return projection.context.Append(entries)
}

func (projection *ModelContextReducer) ModelContexts() []session.Message {
	var facts []session.Message
	for _, message := range projection.context.Messages() {
		if message.ModelContextSnapshot != nil {
			facts = append(facts, session.Message{EntryID: message.EntryID, ModelContextSnapshot: session.CloneModelContext(message.ModelContextSnapshot)})
		}
	}
	return facts
}

// ContextEntrySelection resolves only canonical identities and tool-pair
// metadata. It never treats body-free entries as provider messages. Complete
// selected entries must still pass ContextReducer before use.
func ContextEntrySelection(metadata []PathMetadata) ([]string, error) {
	// Without retry there is no alternate ordering to construct. Validate the
	// same identities and started-marker contract directly, avoiding two
	// complete Entry arrays for the common append-only path.
	ids := make([]string, len(metadata))
	seen := make(map[string]bool, len(metadata))
	hasRetry := false
	for i, item := range metadata {
		entry := item.Entry
		if strings.TrimSpace(entry.ID) == "" || seen[entry.ID] {
			return nil, ErrAuthorityCorrupt
		}
		seen[entry.ID] = true
		ids[i] = entry.ID
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
			source, err := CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return nil, err
			}
			hasRetry = hasRetry || source != nil
		}
	}
	if !hasRetry {
		return ids, nil
	}
	path := make([]Entry, len(metadata))
	for i, item := range metadata {
		path[i] = item.Entry
	}
	selected, err := retryProjectedContextPath(path)
	if err != nil {
		return nil, err
	}
	ids = make([]string, len(selected))
	for i, entry := range selected {
		ids[i] = entry.ID
	}
	return ids, nil
}
