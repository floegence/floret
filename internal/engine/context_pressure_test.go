package engine

import (
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"testing"
)

func TestCanonicalPressureAnchorBoundaries(t *testing.T) {
	history := []session.Message{{EntryID: "temporary", Role: session.User, Content: "hello"}}
	req := provider.Request{PromptScopeID: "scope", RunID: "run", Provider: "test", Model: "model", Step: 1,
		ContextPolicy:   contextpolicy.Policy{ContextWindowTokens: 1000},
		RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 400, MessageTokens: 100, Source: "test", Method: contextpolicy.EstimateMethodProviderRenderedPayload, Confidence: contextpolicy.EstimateConservative}}
	if err := cache.ValidateCanonicalLineage(t.Context(), nil, "scope", &req.RawPlan, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	tracker := NewContextPressureTracker("scope")
	tracker.ObserveSuccess(req, history, provider.Usage{InputTokens: 100, WindowInputTokens: 100, Available: true, Source: provider.UsageNative})
	tests := []struct {
		name, reason string
		change       func(*provider.Request, *[]session.Message, *ContextPressureTracker)
	}{
		{"canonical IDs", "", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			(*h)[0].EntryID = "durable"
		}},
		{"history edited", "history_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			(*h)[0].Content = "different"
		}},
		{"retry truncated", "history_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) { *h = nil }},
		{"fork", "prompt_scope_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) { r.PromptScopeID = "fork" }},
		{"model", "model_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) { r.Model = "other" }},
		{"config", "execution_surface_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			r.RawPlan.CanonicalEnvelopeHash = "other"
		}},
		{"render", "projection_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			r.RawPlan.Version = "other"
		}},
		{"compaction", "lineage_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			r.RawPlan.CompactionGeneration++
		}},
		{"estimate", "estimator_changed", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			r.RequestEstimate.Source = "other"
		}},
		{"negative delta", "non_monotonic_estimate", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			r.RequestEstimate.EstimatedInputTokens = 399
		}},
		{"missing record", "missing_measurement", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			tr.anchorRequest = cache.ProviderRequestRecord{}
		}},
		{"ephemeral", "ephemeral_input", func(r *provider.Request, h *[]session.Message, tr *ContextPressureTracker) {
			tr.anchorRequest.HasEphemeralOverlay = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tr := *tracker
			r := req
			h := append([]session.Message(nil), history...)
			// Total overhead changes while components remain fixed.
			r.RequestEstimate.EstimatedInputTokens = 450
			test.change(&r, &h, &tr)
			pressure := tr.Project(r, h)
			if tr.anchorReason != test.reason {
				t.Fatalf("reason=%q want=%q", tr.anchorReason, test.reason)
			}
			want := r.RequestEstimate.EstimatedInputTokens
			if test.reason == "" {
				want = 150
			}
			if pressure.ProjectedInputTokens != want {
				t.Fatalf("pressure=%+v want=%d", pressure, want)
			}
			r.ContextPressure = pressure
			missing, _ := tr.ObserveSuccess(r, h, provider.Usage{})
			if missing.ProjectedInputTokens != want || missing.Source != contextpolicy.PressureSourceMissingNativeUsage {
				t.Fatalf("missing usage=%+v", missing)
			}
		})
	}
}
