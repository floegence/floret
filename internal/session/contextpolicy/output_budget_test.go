package contextpolicy

import "testing"

func TestModelOutputCapabilityDoesNotReserveInputWindow(t *testing.T) {
	p := Policy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216}
	b := ResolveRequestBudget(p, 68400)
	if b.OutputTokens != 393216 || b.SafetyMarginTokens != 4096 || b.ThresholdTokens != 855000 || b.RequestSafeLimit != 945903 {
		t.Fatalf("unexpected automatic budget: %+v", b)
	}
	if b := ResolveRequestBudget(p, 855000); b.OutputTokens != 90904 || !b.CompactionNeeded || b.HardLimitExceeded {
		t.Fatalf("unexpected threshold budget: %+v", b)
	}
	if b := ResolveRequestBudget(p, 945903); b.OutputTokens != 1 || b.HardLimitExceeded {
		t.Fatalf("one output token fits: %+v", b)
	}
	if b := ResolveRequestBudget(p, 945904); !b.HardLimitExceeded {
		t.Fatalf("must leave positive output room: %+v", b)
	}
}

func TestExplicitOutputBudgetRemainsExact(t *testing.T) {
	p := Policy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216, MaxOutputTokens: 200000}
	b := ResolveRequestBudget(p, 745904)
	if b.OutputTokens != 200000 || b.RequestSafeLimit != 745904 || b.ThresholdTokens != 745904 || b.HardLimitExceeded {
		t.Fatalf("unexpected explicit budget: %+v", b)
	}
	if b := ResolveRequestBudget(p, 745905); !b.HardLimitExceeded {
		t.Fatalf("explicit cap must not shrink: %+v", b)
	}
	if b := ResolveRequestBudget(Policy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 100000, MaxOutputTokens: 200000}, 1); !b.Invalid {
		t.Fatalf("cap exceeds capability: %+v", b)
	}
}

func TestLegacyOutputBudgetKeepsPublishedSemantics(t *testing.T) {
	p := Policy{ContextWindowTokens: 950000, MaxOutputTokens: 393216}
	b := ResolveRequestBudget(p, 68400)
	if b.RequestSafeLimit != 556784 || b.ThresholdTokens != 556784 || b.OutputTokens != 393216 || b.SafetyMarginTokens != 0 {
		t.Fatalf("legacy policy changed: %+v", b)
	}
}

func TestReasoningMinimumAndLargeWindow(t *testing.T) {
	p := Policy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216}
	if b := ResolveRequestBudget(p, 945000, 1025); !b.HardLimitExceeded || b.Invalid {
		t.Fatalf("reasoning must fit: %+v", b)
	}
	p.MaxOutputTokens = 1000
	if b := ResolveRequestBudget(p, 1, 1025); !b.Invalid {
		t.Fatalf("explicit limit below reasoning: %+v", b)
	}
	p = Policy{ContextWindowTokens: 1<<63 - 1, ModelMaxOutputTokens: 393216}
	if b := ResolveRequestBudget(p, 1); b.Invalid || b.ThresholdTokens <= 0 || b.HardLimitExceeded {
		t.Fatalf("overflow: %+v", b)
	}
}
