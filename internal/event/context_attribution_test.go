package event

import (
	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/observation"
	"testing"
)

func TestContextAttributionClonesDoNotAlias(t *testing.T) {
	status := &observation.ContextStatus{RequestEstimate: config.RequestEstimate{Breakdown: config.ContextBreakdown{Details: &config.ContextBreakdownDetails{Items: []config.ContextTokenDetail{{Category: "model_context", Key: "skills", Tokens: 9}}}}}}
	original := &ThreadContextUsage{Confirmed: status, Estimate: status}
	cloned := CloneThreadContextUsage(original)
	cloned.Confirmed.RequestEstimate.Breakdown.Details.Items[0].Tokens = 100
	if status.RequestEstimate.Breakdown.Details.Items[0].Tokens != 9 || cloned.Estimate.RequestEstimate.Breakdown.Details.Items[0].Tokens != 9 {
		t.Fatal("attribution aliases another observation")
	}
}
