package observation

import (
	"encoding/json"
	"github.com/floegence/floret/v7/config"
	"math"
	"reflect"
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

func TestContextBreakdownAttributionRoundTripAndValidation(t *testing.T) {
	b := config.ContextBreakdown{Source: "semantic_input_bpe_v2", ToolDefinitionTokens: 17, ModelContextTokens: 9, Details: &config.ContextBreakdownDetails{Items: []config.ContextTokenDetail{{Category: "tool_definition", Key: "docs.read", Source: "mcp", Namespace: "docs", Tokens: 17}, {Category: "model_context", Key: "skills", Tokens: 9}}}}
	raw, _ := json.Marshal(b)
	var mapped map[string]any
	_ = json.Unmarshal(raw, &mapped)
	if got := contextBreakdownFromAny(mapped); !reflect.DeepEqual(got, b) {
		t.Fatalf("lost attribution: %+v", got)
	}
	status := ContextStatus{Phase: ContextPhaseProviderUsage, Status: ContextStatusStable, RequestEstimate: config.RequestEstimate{Breakdown: b}}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*config.ContextBreakdown){
		func(b *config.ContextBreakdown) { b.Details.Items[0].Tokens = -1 },
		func(b *config.ContextBreakdown) { b.Details.Items[0].Tokens = 18 },
		func(b *config.ContextBreakdown) { b.Details.Items[0].Key = "" },
		func(b *config.ContextBreakdown) { b.Details.Items[0].Category = "unknown" },
		func(b *config.ContextBreakdown) { b.Details.Items = append(b.Details.Items, b.Details.Items[0]) },
	} {
		var copy config.ContextBreakdown
		_ = json.Unmarshal(raw, &copy)
		mutate(&copy)
		status.RequestEstimate.Breakdown = copy
		if status.Validate() == nil {
			t.Fatalf("invalid attribution accepted: %+v", copy)
		}
	}
}
