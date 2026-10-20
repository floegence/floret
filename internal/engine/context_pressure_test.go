package engine

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"strings"
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
	tracker.ObserveSuccess(req, history, provider.Usage{InputTokensReported: true, InputTokenScope: "request_input", InputTokens: 100, WindowInputTokens: 100, Available: true, Source: provider.UsageNative})
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

func TestMeasuredPrefixDoesNotRepeatWholeHistoryMargin(t *testing.T) {
	history := []session.Message{{EntryID: "user", Role: session.User, Content: "synthetic history"}}
	req := provider.Request{PromptScopeID: "scope", RunID: "run", Provider: "test", Model: "model", Step: 1,
		ContextPolicy:   contextpolicy.Policy{ContextWindowTokens: 525000, ReservedOutputTokens: 1000},
		RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 464496, Source: "test", Method: contextpolicy.EstimateMethodProviderRenderedPayload, Confidence: contextpolicy.EstimateConservative, Breakdown: contextpolicy.ContextBreakdown{AttributionCoverage: "partial"}}}
	if err := cache.ValidateCanonicalLineage(t.Context(), nil, "scope", &req.RawPlan, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	tr := NewContextPressureTracker("scope")
	tr.ObserveSuccess(req, history, provider.Usage{InputTokensReported: true, InputTokenScope: "request_input", InputTokens: 5526, CacheReadTokens: 412160, Available: true, Source: provider.UsageNative})
	req.RequestEstimate.EstimatedInputTokens = 474100
	got := tr.Project(req, history)
	if got.ProjectedInputTokens != 427290 || got.CompactionNeeded {
		t.Fatalf("measured prefix repeated estimate margin: %+v reason=%s", got, tr.anchorReason)
	}
	unmeasured := NewContextPressureTracker("scope").Project(req, history)
	if unmeasured.ProjectedInputTokens != 474100 || !unmeasured.CompactionNeeded || unmeasured.ThresholdTokens != 472500 {
		t.Fatalf("missing evidence bypassed estimate: %+v", unmeasured)
	}
}

func TestUnqualifiedNativeUsageNeverCalibratesPressure(t *testing.T) {
	for _, scope := range []string{"", "unknown", "provider_execution", "request_input"} {
		req := provider.Request{RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 100, Breakdown: contextpolicy.ContextBreakdown{AttributionCoverage: "partial", OpaqueBytes: 5000}}, ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 1000}}
		tr := NewContextPressureTracker("scope")
		got, anchor := tr.ObserveSuccess(req, nil, provider.Usage{InputTokensReported: true, InputTokenScope: scope, InputTokens: 999999, Available: true})
		if got.ProjectedInputTokens != 100 || got.Confidence == contextpolicy.EstimateExact || anchor.WindowInputTokens != 0 {
			t.Fatalf("unqualified pressure: %+v %+v", got, anchor)
		}
	}
}

func TestOpaqueReasoningDoesNotTriggerPrematurePressure(t *testing.T) {
	for _, visible := range []bool{false, true} {
		text := "Continue."
		if visible {
			text = strings.Repeat("visible history ", 160000)
		}
		raw, err := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "reasoning", "encrypted_content": strings.Repeat("A", 285984)}, map[string]any{"role": "user", "content": text}}})
		if err != nil {
			t.Fatal(err)
		}
		estimate, err := requestestimate.Count("openai", "gpt-6.1-sol", "openai_responses", raw)
		if err != nil {
			t.Fatal(err)
		}
		req := provider.Request{ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 256000, ModelMaxOutputTokens: 64000}, RequestEstimate: contextpolicy.RequestEstimate{PrefixTokens: estimate.Prefix, MessageTokens: estimate.Messages, ToolDefinitionTokens: estimate.Tools, EstimatedInputTokens: estimate.Prefix + estimate.Messages + estimate.Tools, Source: estimate.Source, Method: contextpolicy.EstimateMethodProviderRenderedPayload, Confidence: contextpolicy.EstimateApproximate, Breakdown: estimate.Breakdown}}
		tracker := NewContextPressureTracker("scope")
		pressure := tracker.Project(req, nil)
		if pressure.CompactionNeeded != visible || pressure.HardLimitExceeded != visible {
			t.Fatalf("visible=%v pressure=%+v", visible, pressure)
		}
		req.ContextPressure = pressure
		observed, anchor := tracker.ObserveSuccess(req, nil, provider.Usage{Source: provider.UsageNative, Available: true, InputTokensReported: true, InputTokenScope: "provider_execution", InputTokens: 159360, WindowInputTokens: 159360})
		if anchor.WindowInputTokens != 0 || observed.ProjectedInputTokens != pressure.ProjectedInputTokens {
			t.Fatalf("unqualified native usage replaced pressure: %+v %+v", observed, anchor)
		}
	}
}
