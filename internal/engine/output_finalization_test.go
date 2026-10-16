package engine

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type budgetControlProvider struct{}

func (budgetControlProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	panic("unexpected dispatch")
}
func (budgetControlProvider) OutputTokenControl() string { return "client" }

type budgetControlPrepared struct{ limit int64 }

func (*budgetControlPrepared) Stream(context.Context) (<-chan provider.StreamEvent, error) {
	panic("unexpected dispatch")
}
func (*budgetControlPrepared) Close() error               { return nil }
func (*budgetControlPrepared) PayloadFingerprint() string { return "private-payload" }
func (*budgetControlPrepared) TokenEstimate() provider.TokenEstimate {
	return provider.TokenEstimate{EstimatedInputTokens: 100, MessageTokens: 100, Source: "fixture", Method: contextpolicy.EstimateMethodProviderRenderedPayload, Confidence: provider.EstimateConservative, Coverage: provider.TokenEstimateCoverageComplete}
}
func (*budgetControlPrepared) WithMaxOutputTokens(_ context.Context, limit int64) (provider.PreparedRequest, error) {
	return &budgetControlPrepared{limit: limit}, nil
}

func TestOutputFinalizationPreservesEphemeralFingerprintFence(t *testing.T) {
	e := &Engine{provider: budgetControlProvider{}}
	req := provider.Request{Prepared: &budgetControlPrepared{}, EphemeralUser: &provider.EphemeralUserMessage{}, ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 950000, ModelMaxOutputTokens: 393216}, ContextPressure: contextpolicy.ContextPressure{ProjectedInputTokens: 100}}
	got, err := e.finalizeRequestOutput(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.RawPlan.PayloadHash != "" || got.MaxOutputTokens != 393216 {
		t.Fatalf("output request=%+v", got)
	}
}
