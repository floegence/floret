package agentharness

import "testing"

func TestUnifiedEventLogDeduplicatesIDsAndRejectsRevisionGaps(t *testing.T) {
	log := newUnifiedEventLog("thread-a")
	first := unifiedTimelineEvent{ID: "event-1", ThreadID: "thread-a", Revision: 1, Kind: unifiedEventUser}
	if err := log.append(first); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if err := log.append(first); err != nil {
		t.Fatalf("duplicate event should be idempotent: %v", err)
	}
	if got := len(log.events()); got != 1 {
		t.Fatalf("duplicate event count=%d, want 1", got)
	}
	gap := unifiedTimelineEvent{ID: "event-3", ThreadID: "thread-a", Revision: 3, Kind: unifiedEventAssistant}
	if err := log.append(gap); err == nil {
		t.Fatal("revision gap should be rejected")
	}
	second := unifiedTimelineEvent{ID: "event-2", ThreadID: "thread-a", Revision: 2, Kind: unifiedEventAssistant}
	if err := log.append(second); err != nil {
		t.Fatalf("append contiguous event: %v", err)
	}
	if got := log.currentRevision(); got != 2 {
		t.Fatalf("revision=%d, want 2", got)
	}
}

func TestUnifiedEventLogRejectsWrongThreadAndConflictingEventID(t *testing.T) {
	log := newUnifiedEventLog("thread-a")
	if err := log.append(unifiedTimelineEvent{ID: "event-1", ThreadID: "thread-b", Revision: 1, Kind: unifiedEventUser}); err == nil {
		t.Fatal("wrong thread should be rejected")
	}
	if err := log.append(unifiedTimelineEvent{ID: "event-1", ThreadID: "thread-a", Revision: 1, Kind: unifiedEventUser}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := log.append(unifiedTimelineEvent{ID: "event-1", ThreadID: "thread-a", Revision: 2, Kind: unifiedEventAssistant}); err == nil {
		t.Fatal("conflicting event ID should be rejected")
	}
}

func TestUnifiedEventLogReadsAfterRevisionWithoutGuessingGaps(t *testing.T) {
	log := newUnifiedEventLog("thread-a")
	for revision, kind := range []unifiedEventKind{unifiedEventUser, unifiedEventTurnState, unifiedEventAssistant} {
		if err := log.append(unifiedTimelineEvent{ID: string(rune('a' + revision)), ThreadID: "thread-a", Revision: int64(revision + 1), Kind: kind}); err != nil {
			t.Fatalf("append revision %d: %v", revision+1, err)
		}
	}
	events, err := log.eventsAfter(1)
	if err != nil {
		t.Fatalf("events after revision: %v", err)
	}
	if len(events) != 2 || events[0].Revision != 2 || events[1].Revision != 3 {
		t.Fatalf("events after revision = %#v", events)
	}
	if _, err := log.eventsAfter(4); err == nil {
		t.Fatal("future revision should require a snapshot resync")
	}
}
