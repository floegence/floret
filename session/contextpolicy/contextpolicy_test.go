package contextpolicy

import (
	"strings"
	"testing"

	"github.com/floegence/floret/session"
)

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

func TestNormalizeScalesDefaultCompactionBudgetsForSmallWindows(t *testing.T) {
	policy := Normalize(Policy{ContextWindowTokens: 8192, MaxOutputTokens: 1024, RecentTailTokens: 1024})
	if policy.ReservedSummaryTokens != 2048 {
		t.Fatalf("reserved summary = %d, want small-window default", policy.ReservedSummaryTokens)
	}
	if policy.RecentUserTokens != 2048 {
		t.Fatalf("recent users = %d, want small-window default", policy.RecentUserTokens)
	}
	if policy.RecentTailTokens != 1024 {
		t.Fatalf("explicit recent tail = %d, want preserved", policy.RecentTailTokens)
	}
	if got := Threshold(policy); got != 7168 {
		t.Fatalf("threshold = %d, want self-consistent small-window threshold", got)
	}
}

func TestMergeDefaultsUsesFallbackMaxOutputOnlyWhenPolicyOmitted(t *testing.T) {
	fallback := Policy{
		ContextWindowTokens:    128000,
		MaxOutputTokens:        64000,
		ReservedOutputTokens:   DefaultReservedOutputTokens,
		ReservedSummaryTokens:  20000,
		RecentTailTokens:       12000,
		RecentUserTokens:       15000,
		EstimatorSource:        "catalog",
		MaxCompactionFailures:  2,
		MicrocompactToolTokens: 4096,
	}
	empty := MergeDefaults(Policy{}, fallback)
	if empty.MaxOutputTokens != 64000 || empty.ContextWindowTokens != 128000 {
		t.Fatalf("empty policy should inherit fallback: %#v", empty)
	}

	explicit := MergeDefaults(Policy{ReservedOutputTokens: 1024}, fallback)
	if explicit.MaxOutputTokens != 0 || explicit.ReservedOutputTokens != 1024 || explicit.ContextWindowTokens != 128000 {
		t.Fatalf("explicit partial policy should keep ordinary max output unset and inherit missing defaults: %#v", explicit)
	}

	smallWindow := MergeDefaults(Policy{ContextWindowTokens: 8192, MaxOutputTokens: 1024, RecentTailTokens: 1024}, fallback)
	if smallWindow.ReservedSummaryTokens != 2048 || smallWindow.RecentUserTokens != 2048 || smallWindow.RecentTailTokens != 1024 {
		t.Fatalf("small-window defaults should scale missing compaction budgets while preserving explicit values: %#v", smallWindow)
	}

	expanded := MergeDefaults(Policy{ContextWindowTokens: 128000}, Policy{ContextWindowTokens: 8192})
	if expanded.ReservedSummaryTokens != DefaultReservedSummaryTokens || expanded.RecentTailTokens != DefaultRecentTailTokens || expanded.RecentUserTokens != DefaultRecentUserTokens {
		t.Fatalf("raw fallback defaults should be derived against the target window: %#v", expanded)
	}
}

func TestEstimateMessagesReportsRecentUserBudget(t *testing.T) {
	usage := EstimateMessages("", nil, 0, Policy{RecentUserTokens: 321})
	if usage.RecentUserTokens != 321 {
		t.Fatalf("recent user budget = %d, want 321", usage.RecentUserTokens)
	}
}

func TestThresholdUsesOutputHeadroomAndRatioLimits(t *testing.T) {
	tests := []struct {
		name   string
		policy Policy
		want   int64
	}{
		{
			name:   "default policy uses request safety",
			policy: Policy{},
			want:   192000,
		},
		{
			name:   "128k with default reserve uses request safety",
			policy: Policy{ContextWindowTokens: 128000, MaxOutputTokens: 0},
			want:   64000,
		},
		{
			name:   "large window with 128k output cap uses request safety",
			policy: Policy{ContextWindowTokens: 1000000, MaxOutputTokens: 128000},
			want:   872000,
		},
		{
			name:   "large window with 384k output cap uses request safety",
			policy: Policy{ContextWindowTokens: 1000000, MaxOutputTokens: 384000},
			want:   616000,
		},
		{
			name:   "128k window with 96k output cap does not lift past request safety",
			policy: Policy{ContextWindowTokens: 128000, MaxOutputTokens: 96000},
			want:   32000,
		},
		{
			name:   "200k window with 128k output cap does not lift past request safety",
			policy: Policy{ContextWindowTokens: 200000, MaxOutputTokens: 128000},
			want:   72000,
		},
		{
			name:   "128k window with 65536 output cap",
			policy: Policy{ContextWindowTokens: 128000, MaxOutputTokens: 65536},
			want:   62464,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Threshold(tt.policy); got != tt.want {
				t.Fatalf("threshold = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEstimateMessagesReportsOutputBudgetFields(t *testing.T) {
	usage := EstimateMessages("system", []session.Message{{Role: session.User, Content: strings.Repeat("x", 40)}}, 1, Policy{
		ContextWindowTokens: 1000000,
		MaxOutputTokens:     384000,
	})
	if usage.MaxOutputTokens != 384000 {
		t.Fatalf("max output tokens = %d, want 384000", usage.MaxOutputTokens)
	}
	if usage.OutputHeadroom != 384000 {
		t.Fatalf("output headroom = %d, want 384000", usage.OutputHeadroom)
	}
	if usage.AutoCompactRatio != DefaultAutoCompactRatioPercent {
		t.Fatalf("auto compact ratio = %d, want %d", usage.AutoCompactRatio, DefaultAutoCompactRatioPercent)
	}
	if usage.ThresholdTokens != 616000 {
		t.Fatalf("threshold = %d, want 616000", usage.ThresholdTokens)
	}
	if usage.RatioLimitTokens != 900000 || usage.RequestSafeLimit != 616000 {
		t.Fatalf("pressure limits = ratio %d request %d, want 900000/616000", usage.RatioLimitTokens, usage.RequestSafeLimit)
	}
}

func TestTokenPressureHighUsesOutputHeadroom(t *testing.T) {
	high := EstimateMessages("", []session.Message{{Role: session.User, Content: strings.Repeat("x", 2464000)}}, 0, Policy{
		ContextWindowTokens: 1000000,
		MaxOutputTokens:     384000,
	})
	if !high.TokenPressureHigh {
		t.Fatalf("expected high pressure with max output headroom, usage=%#v", high)
	}

	unset := EstimateMessages("", []session.Message{{Role: session.User, Content: strings.Repeat("x", 3990000)}}, 0, Policy{
		ContextWindowTokens: 1000000,
		MaxOutputTokens:     0,
	})
	if unset.OutputHeadroom != DefaultReservedOutputTokens {
		t.Fatalf("unset max output headroom = %d, want %d", unset.OutputHeadroom, DefaultReservedOutputTokens)
	}
	if !unset.TokenPressureHigh {
		t.Fatalf("expected high pressure with reserved output headroom, usage=%#v", unset)
	}
}
