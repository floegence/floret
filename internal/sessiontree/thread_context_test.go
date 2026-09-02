package sessiontree

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
)

func TestThreadContextEntriesKeepExecutionIdentityOnlyOnCanonicalEntry(t *testing.T) {
	statusEntry, err := NewThreadContextStatusEntry(observation.ContextStatus{
		RunID: "run", ThreadID: "thread", TurnID: "turn",
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_725_273_600, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	compactionEntry, err := NewThreadContextCompactionEntry(ThreadContextCompaction{
		RunID: "run", ThreadID: "thread", TurnID: "turn", OperationID: "compact", RequestID: "request",
		Phase: string(observation.CompactionPhaseNoop), Status: string(observation.CompactionStatusNoop),
	})
	if err != nil {
		t.Fatal(err)
	}
	for label, raw := range map[string]string{
		"status":     statusEntry.Metadata[threadContextStatusKey],
		"compaction": compactionEntry.Metadata[threadContextCompactionKey],
	} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"thread_id", "turn_id", "run_id"} {
			if _, found := fields[field]; found {
				t.Fatalf("%s payload retained %s: %s", label, field, raw)
			}
		}
	}

	statusEntry.ThreadID = "fork"
	status, err := DecodeThreadContextStatusEntry(statusEntry)
	if err != nil {
		t.Fatal(err)
	}
	if status.ThreadID != identity.ThreadID("fork") || status.TurnID != identity.TurnID("turn") || status.RunID != identity.RunID("run") {
		t.Fatalf("decoded status identity=%#v", status)
	}
	compactionEntry.ThreadID = "fork"
	compaction, err := DecodeThreadContextCompactionEntry(compactionEntry)
	if err != nil {
		t.Fatal(err)
	}
	if compaction.ThreadID != "fork" || compaction.TurnID != "turn" || compaction.RunID != "run" {
		t.Fatalf("decoded compaction identity=%#v", compaction)
	}
}
