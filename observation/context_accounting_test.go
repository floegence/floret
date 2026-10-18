package observation

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/floegence/floret/v7/config"
)

func accountingSample(total, category int64) ContextStatus {
	return ContextStatus{Phase: ContextPhaseProviderUsage, Status: ContextStatusStable, RequestID: "r", PayloadFingerprint: "f", Provider: "openai", Model: "gpt-6.1-sol", Usage: ProviderUsage{Available: true, Source: "native", WindowInputTokens: total}, RequestEstimate: config.RequestEstimate{Breakdown: config.ContextBreakdown{Source: "prepared_content_v4", Quality: "token_estimate", Tokenizer: "proxy_bpe", AttributionCoverage: "complete", UserTokens: category}}}
}

func TestReconcilePreservesEstimatesAndUnattributedRemainder(t *testing.T) {
	for _, tc := range []struct {
		total, category, remainder int64
		details                    bool
	}{{46292, 18363, 27929, true}, {100, 100, 0, true}, {100, 101, 0, false}, {0, 0, 0, true}, {0, 1, 0, false}} {
		got, err := ReconcileContextStatus(accountingSample(tc.total, tc.category))
		if err != nil || !got.Available || !got.Measured || got.InputTokens != tc.total || got.UnattributedTokens != tc.remainder || got.BreakdownAvailable != tc.details {
			t.Fatalf("%+v: %+v %v", tc, got, err)
		}
	}
	for range 2000 {
		total := rand.Int64N(math.MaxInt64)
		category := rand.Int64N(total + 1)
		got, err := ReconcileContextStatus(accountingSample(total, category))
		if err != nil || got.InputTokens != category+got.UnattributedTokens || !got.BreakdownAvailable {
			t.Fatalf("integer reconciliation: %+v %v", got, err)
		}
	}
}

func TestReconcileRejectsUnitsIdentityAndDamagedAttribution(t *testing.T) {
	for _, name := range []string{"legacy", "missing_identity", "partial", "details", "overflow", "negative"} {
		t.Run(name, func(t *testing.T) {
			s := accountingSample(100, 20)
			wantError := false
			switch name {
			case "legacy":
				s.RequestEstimate.Breakdown.Source = "prepared_content_v3"
			case "missing_identity":
				s.PayloadFingerprint = ""
			case "partial":
				s.Phase = ContextPhaseProjectedRequest
				s.RequestEstimate.Breakdown.AttributionCoverage = "partial"
				s.RequestEstimate.Breakdown.Limitation = "hosted_context"
			case "details":
				s.RequestEstimate.Breakdown.Details = &config.ContextBreakdownDetails{Items: []config.ContextTokenDetail{{Category: "tool_definition", Key: "tool", Tokens: 2}}}
				wantError = true
			case "overflow":
				s.RequestEstimate.Breakdown.SystemTokens = math.MaxInt64
				wantError = true
			case "negative":
				s.RequestEstimate.Breakdown.UserTokens = -1
				wantError = true
			}
			got, err := ReconcileContextStatus(s)
			if (err != nil) != wantError || got.BreakdownAvailable || (name != "partial" && (!got.Measured || got.InputTokens != 100)) || (name == "partial" && got.Available) {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
	s := accountingSample(0, 20)
	s.Phase = ContextPhaseProjectedRequest
	s.RequestEstimate.EstimatedInputTokens = 9999
	got, err := ReconcileContextStatus(s)
	if err != nil || !got.Available || got.Measured || got.InputTokens != 20 {
		t.Fatalf("budget entered display: %+v %v", got, err)
	}
}

func TestUsageNormalizationPreservesProtocolRelationships(t *testing.T) {
	for _, tc := range []struct {
		inclusive, present bool
		native, want       int64
	}{{true, false, 0, 120}, {false, false, 0, 125}, {true, true, 0, 0}, {false, true, 987, 987}, {false, false, 987, 987}} {
		u := ProviderUsage{InputTokens: 90, CacheReadTokens: 10, OutputTokens: 20, ReasoningTokens: 5, OutputIncludesReasoning: tc.inclusive, TotalTokensReported: tc.present, TotalTokens: tc.native}.Normalized()
		if u.TotalTokens != tc.want || u.WindowInputTokens != 100 || u.Normalized() != u {
			t.Fatalf("%+v: %+v", tc, u)
		}
	}
}

func TestReconcileCannotQualifyOffsettingAttributionErrors(t *testing.T) {
	s := accountingSample(100, 0)
	s.RequestEstimate.Breakdown.ToolDefinitionTokens = 60
	s.RequestEstimate.Breakdown.UserTokens = 39
	s.RequestEstimate.Breakdown.Source = "prepared_content_v3"
	got, err := ReconcileContextStatus(s)
	if err != nil || got.BreakdownAvailable || got.InputTokens != 100 {
		t.Fatalf("close total qualified legacy units: %+v %v", got, err)
	}
}
