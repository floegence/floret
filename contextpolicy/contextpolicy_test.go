package contextpolicy

import "testing"

func TestNormalizeDefaultsKeepOrdinaryMaxOutputUnset(t *testing.T) {
	policy := Normalize(Policy{})
	if policy.ContextWindowTokens != DefaultContextWindowTokens {
		t.Fatalf("context window = %d, want %d", policy.ContextWindowTokens, DefaultContextWindowTokens)
	}
	if policy.MaxOutputTokens != 0 {
		t.Fatalf("max output = %d, want unset", policy.MaxOutputTokens)
	}
	if policy.ReservedOutputTokens != DefaultReservedOutputTokens {
		t.Fatalf("reserved output = %d, want %d", policy.ReservedOutputTokens, DefaultReservedOutputTokens)
	}
	if policy.ReservedSummaryTokens != DefaultReservedSummaryTokens {
		t.Fatalf("reserved summary = %d, want %d", policy.ReservedSummaryTokens, DefaultReservedSummaryTokens)
	}
	if policy.RecentTailTokens != DefaultRecentTailTokens {
		t.Fatalf("recent tail = %d, want %d", policy.RecentTailTokens, DefaultRecentTailTokens)
	}
	if policy.RecentUserTokens != DefaultRecentUserTokens {
		t.Fatalf("recent users = %d, want %d", policy.RecentUserTokens, DefaultRecentUserTokens)
	}
}

func TestNormalizeUsesSmallMaxOutputAsReservedOutputCeiling(t *testing.T) {
	policy := Normalize(Policy{MaxOutputTokens: 1024})
	if policy.MaxOutputTokens != 1024 {
		t.Fatalf("max output = %d, want explicit value", policy.MaxOutputTokens)
	}
	if policy.ReservedOutputTokens != 1024 {
		t.Fatalf("reserved output = %d, want min(max output, default reserve)", policy.ReservedOutputTokens)
	}
}

func TestNormalizeKeepsExplicitReservedOutputWithUnsetMaxOutput(t *testing.T) {
	policy := Normalize(Policy{MaxOutputTokens: 0, ReservedOutputTokens: 8192})
	if policy.MaxOutputTokens != 0 {
		t.Fatalf("max output = %d, want unset", policy.MaxOutputTokens)
	}
	if policy.ReservedOutputTokens != 8192 {
		t.Fatalf("reserved output = %d, want explicit reserve", policy.ReservedOutputTokens)
	}
}

func TestEstimateMessagesReportsRecentUserBudget(t *testing.T) {
	usage := EstimateMessages("", nil, 0, Policy{RecentUserTokens: 321})
	if usage.RecentUserTokens != 321 {
		t.Fatalf("recent user budget = %d, want 321", usage.RecentUserTokens)
	}
}
