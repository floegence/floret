package observation

import (
	"github.com/floegence/floret/v7/config"
	"math"
	"testing"
)

func TestContextBreakdownMapProjectionAndValidation(t *testing.T) {
	estimate := requestEstimateFromAny(map[string]any{"breakdown": map[string]any{"source": "semantic_input_bpe_v1", "tool_result_tokens": float64(1234), "attachment_tokens": int64(4096)}})
	if estimate.Breakdown.ToolResultTokens != 1234 || estimate.Breakdown.AttachmentTokens != 4096 {
		t.Fatalf("map lost composition: %+v", estimate)
	}
	status := ContextStatus{Phase: ContextPhaseProviderUsage, Status: ContextStatusStable, RequestEstimate: estimate}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, b := range []config.ContextBreakdown{{ToolResultTokens: 1}, {Source: "test", UserTokens: -1}, {Source: "test", UserTokens: math.MaxInt64, ToolResultTokens: 1}} {
		status.RequestEstimate.Breakdown = b
		if status.Validate() == nil {
			t.Fatalf("accepted invalid breakdown: %+v", b)
		}
	}
	status.RequestEstimate.Breakdown = config.ContextBreakdown{}
	if err := status.Validate(); err != nil {
		t.Fatalf("historical sample without breakdown: %v", err)
	}
}
