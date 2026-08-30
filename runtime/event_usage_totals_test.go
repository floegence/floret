package runtime

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v6/observation"
)

func TestEventValidateRequiresCommittedFinalUsageForThreadTotals(t *testing.T) {
	status := &observation.ContextStatus{
		RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 1,
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
	}
	valid := Event{
		Type: observation.EventTypeProviderUsage, RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 1,
		ContextStatus: status, ThreadUsageTotals: &ThreadTokenUsageTotals{InputTokens: 80, CacheReadTokens: 15},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid committed usage event: %v", err)
	}

	invalidPhase := valid
	invalidPhase.ContextStatus = &observation.ContextStatus{
		RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 1,
		Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusStable,
	}
	if err := invalidPhase.Validate(); err == nil || !strings.Contains(err.Error(), "require final provider usage") {
		t.Fatalf("invalid phase error=%v", err)
	}

	negative := valid
	negative.ThreadUsageTotals = &ThreadTokenUsageTotals{CacheReadTokens: -1}
	if err := negative.Validate(); err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("negative totals error=%v", err)
	}
}
