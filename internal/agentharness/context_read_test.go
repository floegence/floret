package agentharness

import (
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestThreadDetailContextUsesEntryRunIdentityAndReadsLegacyStatus(t *testing.T) {
	status := observation.ContextStatus{
		RunID: "run-payload", ThreadID: "thread-context", TurnID: "turn-context",
		Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_723_800_000, 0).UTC(),
	}
	policy := sessiontree.Entry{
		ThreadID: "thread-context", TurnID: "turn-context", Type: sessiontree.EntryCustom,
		Metadata: subAgentContextPolicyMetadata("test", "scripted", contextpolicy.Policy{}),
	}
	statusEntry := sessiontree.Entry{
		ThreadID: "thread-context", TurnID: "turn-context", Type: sessiontree.EntryCustom,
		Metadata: map[string]string{
			threadDetailKindKey:      subAgentContextStatusEntryKind,
			subAgentContextStatusKey: mustSubAgentMetadataJSON(status),
		},
	}

	harness := &AgentHarness{}
	legacyEntries := []sessiontree.Entry{policy, statusEntry}
	legacy, err := harness.threadDetailContext(legacyEntries, 1, newThreadDetailActivityContext(legacyEntries), time.Now())
	if err != nil || legacy.Usage == nil || legacy.Usage.RunID != identity.RunID("run-payload") {
		t.Fatalf("legacy context=%#v err=%v", legacy, err)
	}

	statusEntry.RunID = "run-entry"
	entries := []sessiontree.Entry{policy, statusEntry}
	_, err = harness.threadDetailContext(entries, 1, newThreadDetailActivityContext(entries), time.Now())
	if err == nil || !strings.Contains(err.Error(), "run identity mismatch") {
		t.Fatalf("mismatched canonical entry run error=%v", err)
	}
}

func TestThreadDetailContextAccumulatesCanonicalProviderUsageOnly(t *testing.T) {
	policy := sessiontree.Entry{
		ThreadID: "thread-context", TurnID: "turn-one", Type: sessiontree.EntryCustom,
		Metadata: subAgentContextPolicyMetadata("test", "scripted", contextpolicy.Policy{}),
	}
	statusEntry := func(turnID, runID string, status observation.ContextStatus) sessiontree.Entry {
		return sessiontree.Entry{
			ThreadID: "thread-context", TurnID: turnID, RunID: runID, Type: sessiontree.EntryCustom,
			Metadata: map[string]string{
				threadDetailKindKey:      subAgentContextStatusEntryKind,
				subAgentContextStatusKey: mustSubAgentMetadataJSON(status),
			},
		}
	}
	projected := observation.ContextStatus{
		RunID: "run-one", ThreadID: "thread-context", TurnID: "turn-one",
		Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_723_800_000, 0).UTC(),
	}
	first := observation.ContextStatus{
		RunID: "run-one", ThreadID: "thread-context", TurnID: "turn-one",
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_723_800_001, 0).UTC(),
		Usage: observation.ProviderUsage{InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5},
	}
	second := observation.ContextStatus{
		RunID: "run-two", ThreadID: "thread-context", TurnID: "turn-two",
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_723_800_002, 0).UTC(),
		Usage: observation.ProviderUsage{InputTokens: 40, OutputTokens: 10, CacheReadTokens: 30},
	}
	entries := []sessiontree.Entry{
		policy,
		statusEntry("turn-one", "run-one", projected),
		statusEntry("turn-one", "run-one", first),
		statusEntry("turn-two", "run-two", second),
	}

	snapshot, err := (&AgentHarness{}).threadDetailContext(entries, 1, newThreadDetailActivityContext(entries), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.UsageTotals == nil || *snapshot.UsageTotals != (ThreadTokenUsageTotals{
		InputTokens: 120, OutputTokens: 30, CacheReadTokens: 45, CacheWriteTokens: 5,
	}) {
		t.Fatalf("usage totals=%#v", snapshot.UsageTotals)
	}
	if snapshot.Usage == nil || snapshot.Usage.RunID != "run-two" {
		t.Fatalf("latest usage=%#v", snapshot.Usage)
	}
}
