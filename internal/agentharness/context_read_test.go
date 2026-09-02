package agentharness

import (
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
)

func TestThreadDetailContextUsesCanonicalEntryIdentity(t *testing.T) {
	status := observation.ContextStatus{
		RunID: "run-context", ThreadID: "thread-source", TurnID: "turn-context",
		Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", ObservedAt: time.Unix(1_723_800_000, 0).UTC(),
	}
	policy, err := sessiontree.NewThreadContextPolicyEntry("thread-fork", "turn-context", "run-context", "test", "scripted", contextpolicy.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	statusEntry, err := sessiontree.NewThreadContextStatusEntry(status)
	if err != nil {
		t.Fatal(err)
	}
	statusEntry.ThreadID = "thread-fork"

	harness := &AgentHarness{}
	entries := []sessiontree.Entry{policy, statusEntry}
	snapshot, err := harness.threadDetailContext(entries, 1, newThreadDetailActivityContext(entries), time.Now())
	if err != nil || snapshot.Usage == nil || snapshot.Usage.ThreadID != identity.ThreadID("thread-fork") || snapshot.Usage.RunID != identity.RunID("run-context") {
		t.Fatalf("context=%#v err=%v", snapshot, err)
	}
}

func TestThreadDetailContextAccumulatesCanonicalProviderUsageOnly(t *testing.T) {
	policy, err := sessiontree.NewThreadContextPolicyEntry("thread-context", "turn-one", "run-one", "test", "scripted", contextpolicy.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	statusEntry := func(turnID, runID string, status observation.ContextStatus) sessiontree.Entry {
		entry, buildErr := sessiontree.NewThreadContextStatusEntry(status)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		return entry
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

func TestThreadDetailContextStartsNewPolicyWithoutPreviousModelUsage(t *testing.T) {
	policyEntry := func(turnID, providerName, modelName string) sessiontree.Entry {
		entry, err := sessiontree.NewThreadContextPolicyEntry("thread-context", turnID, "run-"+turnID, providerName, modelName, contextpolicy.Policy{ContextWindowTokens: 128_000})
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	statusEntry := func(turnID, runID, providerName, modelName string, inputTokens int) sessiontree.Entry {
		status := observation.ContextStatus{
			RunID: identity.RunID(runID), ThreadID: "thread-context", TurnID: identity.TurnID(turnID),
			Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
			Provider: providerName, Model: modelName, Usage: observation.ProviderUsage{InputTokens: int64(inputTokens)},
		}
		entry, err := sessiontree.NewThreadContextStatusEntry(status)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	flashStatus := statusEntry("turn-flash", "run-flash", "deepseek", "flash", 80)
	entries := []sessiontree.Entry{
		policyEntry("turn-flash", "deepseek", "flash"),
		flashStatus,
		policyEntry("turn-pro", "deepseek", "pro"),
	}

	between, err := (&AgentHarness{}).threadDetailContext(entries, 1, newThreadDetailActivityContext(entries), time.Now())
	if err != nil {
		t.Fatalf("read context between new policy and first usage: %v", err)
	}
	if between.Model.Model != "pro" || between.Usage != nil {
		t.Fatalf("between-model context=%#v, want Pro policy without stale Flash usage", between)
	}
	if between.UsageTotals == nil || between.UsageTotals.InputTokens != 80 {
		t.Fatalf("between-model totals=%#v, want prior committed usage retained", between.UsageTotals)
	}

	entries = append(entries, statusEntry("turn-pro", "run-pro", "deepseek", "pro", 40))
	after, err := (&AgentHarness{}).threadDetailContext(entries, 1, newThreadDetailActivityContext(entries), time.Now())
	if err != nil {
		t.Fatalf("read context after Pro usage: %v", err)
	}
	if after.Usage == nil || after.Usage.Model != "pro" || after.Usage.RunID != "run-pro" {
		t.Fatalf("latest Pro usage=%#v", after.Usage)
	}
	if after.UsageTotals == nil || after.UsageTotals.InputTokens != 120 {
		t.Fatalf("cross-model totals=%#v, want 120 input tokens", after.UsageTotals)
	}
}
