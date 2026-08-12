package agentharness

import (
	"errors"
	"fmt"
	"sync"

	"github.com/floegence/floret/v3/identity"
)

// unifiedEventKind is the internal, host-neutral event vocabulary used by the
// Phase 1 adapter. Existing journal/projection APIs remain the compatibility
// boundary; new internal writes must append through this log.
type unifiedEventKind string

const (
	unifiedEventUser        unifiedEventKind = "user"
	unifiedEventAssistant   unifiedEventKind = "assistant"
	unifiedEventTurnState   unifiedEventKind = "turn_state"
	unifiedEventInteraction unifiedEventKind = "interaction"
)

type unifiedTimelineEvent struct {
	ID       string
	ThreadID identity.ThreadID
	TurnID   identity.TurnID
	RunID    identity.RunID
	Revision int64
	Kind     unifiedEventKind
	Payload  []byte
}

type unifiedEventLog struct {
	mu         sync.Mutex
	threadID   identity.ThreadID
	eventsByID map[string]unifiedTimelineEvent
	entries    []unifiedTimelineEvent
	revision   int64
}

func newUnifiedEventLog(threadID identity.ThreadID) *unifiedEventLog {
	return &unifiedEventLog{threadID: threadID, eventsByID: make(map[string]unifiedTimelineEvent)}
}

func (log *unifiedEventLog) append(event unifiedTimelineEvent) error {
	if log == nil {
		return errors.New("unified event log is nil")
	}
	if event.ID == "" || event.ThreadID == "" || event.Revision <= 0 || event.Kind == "" {
		return errors.New("unified event requires id, thread, positive revision, and kind")
	}
	if event.ThreadID != log.threadID {
		return fmt.Errorf("unified event thread %q does not match %q", event.ThreadID, log.threadID)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if existing, ok := log.eventsByID[event.ID]; ok {
		if existing.ThreadID == event.ThreadID && existing.Revision == event.Revision && existing.Kind == event.Kind {
			return nil
		}
		return fmt.Errorf("unified event id %q conflicts with existing event", event.ID)
	}
	if event.Revision != log.revision+1 {
		return fmt.Errorf("unified event revision %d is not contiguous after %d", event.Revision, log.revision)
	}
	log.eventsByID[event.ID] = event
	log.entries = append(log.entries, event)
	log.revision = event.Revision
	return nil
}

func (log *unifiedEventLog) currentRevision() int64 {
	if log == nil {
		return 0
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.revision
}

func (log *unifiedEventLog) events() []unifiedTimelineEvent {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	out := make([]unifiedTimelineEvent, len(log.entries))
	copy(out, log.entries)
	return out
}

// eventsAfter returns a copy of the contiguous suffix after revision. The
// caller can use a gap error to trigger a canonical snapshot resync instead
// of guessing missing state.
func (log *unifiedEventLog) eventsAfter(revision int64) ([]unifiedTimelineEvent, error) {
	if log == nil {
		return nil, errors.New("unified event log is nil")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if revision < 0 || revision > log.revision {
		return nil, fmt.Errorf("unified event revision %d is outside 0..%d", revision, log.revision)
	}
	start := int(revision)
	out := make([]unifiedTimelineEvent, len(log.entries)-start)
	copy(out, log.entries[start:])
	return out, nil
}
