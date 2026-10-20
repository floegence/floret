package agentharness

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func incrementalContextFixture(t testing.TB) []sessiontree.Entry {
	t.Helper()
	var entries []sessiontree.Entry
	add := func(entry sessiontree.Entry, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		entry.CreatedAt = time.Unix(100+int64(len(entries)), 0).UTC()
		entries = append(entries, entry)
	}
	policy := func(model string) {
		add(sessiontree.NewThreadContextPolicyEntry("thread", "turn", "run", "test", model, contextpolicy.Policy{ContextWindowTokens: 128000}))
	}
	status := func(model string, phase observation.ContextPhase) {
		add(sessiontree.NewThreadContextStatusEntry(observation.ContextStatus{ThreadID: "thread", TurnID: "turn", RunID: "run", Provider: "test", Model: model, Phase: phase, Status: observation.ContextStatusStable, Usage: observation.ProviderUsage{InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5}}))
	}
	compact := func(id, run, phase, status string) {
		add(sessiontree.NewThreadContextCompactionEntry(sessiontree.ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: run, OperationID: id, RequestID: "request", Phase: phase, Status: status, TokensBefore: 1000, TokensAfterEstimate: 100}))
	}
	policy("first")
	status("first", observation.ContextPhaseProjectedRequest)
	status("first", observation.ContextPhaseProviderUsage)
	compact("completed", "run", "start", "running")
	compact("completed", "run", "complete", "compacted")
	compact("cancelled", "run", "start", "running")
	compact("other-run", "other", "start", "running")
	entries = append(entries, sessiontree.Entry{Type: sessiontree.EntryTurnMarker, ThreadID: "thread", TurnID: "turn", RunID: "run", TurnStatus: sessiontree.TurnAborted, CreatedAt: time.Unix(120, 0).UTC(), Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled}})
	policy("second")
	status("second", observation.ContextPhaseProviderUsage)
	compact("other-run", "other", "complete", "compacted")
	return entries
}

func TestIncrementalContextMatchesCanonicalPrefixes(t *testing.T) {
	entries := incrementalContextFixture(t)
	projection := ThreadContextProjection{}
	var frozen []byte
	var prior ThreadContextSnapshot
	for i, entry := range entries {
		if err := projection.Observe(entry); err != nil {
			t.Fatal(err)
		}
		if prior.UsageTotals != nil {
			encoded, _ := json.Marshal(prior)
			if string(encoded) != string(frozen) {
				t.Fatal("later observation mutated prior snapshot")
			}
		}
		got, err := projection.Snapshot(time.Time{})
		want, wantErr := ProjectThreadContext(entries[:i+1])
		if (err == nil) != (wantErr == nil) || !reflect.DeepEqual(got, want) {
			t.Fatalf("prefix %d differs: got=%+v want=%+v err=%v / %v", i, got, want, err, wantErr)
		}
		prior = got
		frozen, _ = json.Marshal(got)
	}
	encoded, _ := json.Marshal(prior)
	const historicalSHA256 = "f72e7eb61faeeafae5c64bf50ca48e379ca9a127f6b838f044237a477cb35572"
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != historicalSHA256 {
		t.Fatalf("historical context digest: %s", got)
	}
	if len(projection.running) != 0 {
		t.Fatal("settled compaction retained as running")
	}
}

func BenchmarkIncrementalContextFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			projection := ThreadContextProjection{}
			entries := incrementalContextFixture(b)
			for _, entry := range entries {
				if err := projection.Observe(entry); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < count; i++ {
				entry, err := sessiontree.NewThreadContextCompactionEntry(sessiontree.ThreadContextCompaction{ThreadID: "thread", TurnID: "history", RunID: "old", RequestID: "request", OperationID: fmt.Sprint(i), Phase: "complete", Status: "compacted"})
				if err != nil {
					b.Fatal(err)
				}
				if err := projection.Observe(entry); err != nil {
					b.Fatal(err)
				}
			}
			entry := entries[len(entries)-2]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := projection.Observe(entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
