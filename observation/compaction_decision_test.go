package observation

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/config"
)

func TestCompactionDecisionObservationPreservesMissingAndFrozenScopes(t *testing.T) {
	candidate := &CompactionRequestSnapshot{PayloadFingerprint: "frozen", Provider: "test", Model: "model", Estimate: config.RequestEstimate{Source: "rendered_fixture", Method: config.EstimateMethodProviderRenderedPayload, EstimatedInputTokens: 950}, Pressure: config.ContextPressure{Source: config.PressureSourceFullRequestEstimate, ProjectedInputTokens: 950, ContextWindowTokens: 1000, ThresholdTokens: 900, RequestSafeLimit: 990}}
	decision := &CompactionDecision{Before: candidate, Pressure: candidate.Pressure}
	event := Event{Type: EventTypeContextCompact, Metadata: map[string]any{"phase": "start", "decision": decision}}
	projected, ok := CompactionEventFromEvent(event)
	if !ok || projected.Decision.Before.Estimate.EstimatedInputTokens != 950 {
		t.Fatalf("observation=%+v", projected)
	}
	projected.Decision.Before.Estimate.EstimatedInputTokens = 1
	if candidate.Estimate.EstimatedInputTokens != 950 {
		t.Fatal("observation aliases Engine evidence")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	projected, ok = CompactionEventFromEvent(decoded)
	if !ok || projected.Decision.Before.PayloadFingerprint != "frozen" {
		t.Fatal("JSON observation lost unsent candidate")
	}
	historical, ok := CompactionEventFromEvent(Event{Type: EventTypeContextCompact, Metadata: map[string]any{"phase": "complete", "tokens_before": int64(159143), "tokens_after_estimate": int64(17462)}})
	if !ok || historical.Decision != nil {
		t.Fatal("historical missing evidence was fabricated")
	}
	for _, mutate := range []func(*CompactionDecision){
		func(d *CompactionDecision) { d.Pressure.ThresholdTokens = -1 },
		func(d *CompactionDecision) { d.Before.PayloadFingerprint = "" },
		func(d *CompactionDecision) { d.Before.Estimate.EstimatedInputTokens = -1 },
		func(d *CompactionDecision) { d.Before.CalibrationRequestID = "other" },
	} {
		d := decision.Clone()
		mutate(d)
		if _, ok := CompactionEventFromEvent(Event{Type: EventTypeContextCompact, Metadata: map[string]any{"phase": "start", "decision": d}}); ok {
			t.Fatal("invalid observation accepted")
		}
		if err := d.Validate(); err == nil {
			t.Fatalf("invalid decision accepted: %+v", d)
		}
	}
}
