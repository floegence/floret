package agentharness

import (
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/session/contextpolicy"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/observation"
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
