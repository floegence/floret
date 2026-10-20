package engine

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type measuredPrepared struct {
	payload  []byte
	identity string
}

func (*measuredPrepared) Stream(context.Context) (<-chan provider.StreamEvent, error) {
	panic("must not dispatch")
}
func (*measuredPrepared) TokenEstimate() provider.TokenEstimate { return provider.TokenEstimate{} }
func (*measuredPrepared) PayloadFingerprint() string            { return "frozen" }
func (*measuredPrepared) Close() error                          { return nil }
func (p *measuredPrepared) InputMeasurementPrefix() requestmeasure.Prefix {
	prefix, _ := requestmeasure.RenderedPrefix(p.payload, "openai_responses", p.identity, "input")
	return prefix
}
func (p *measuredPrepared) MeasurementSuffix(prefix requestmeasure.Prefix) ([]byte, bool) {
	return requestmeasure.RenderedSuffix(p.payload, "openai_responses", p.identity, prefix)
}

func measurementFixture(t *testing.T) (provider.Request, []session.Message, provider.Usage) {
	t.Helper()
	history := []session.Message{{EntryID: "user", Role: session.User, Content: "question"}}
	p := &measuredPrepared{payload: []byte(`{"model":"model","input":[{"role":"user","content":"question"}]}`), identity: "account:connection"}
	req := provider.Request{Provider: "test", Model: "model", RunID: "run", Step: 1, AttemptID: "attempt", PromptScopeID: "scope", Prepared: p,
		ContextPolicy:   contextpolicy.Policy{ContextWindowTokens: 525000, ReservedOutputTokens: 1000},
		RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 464496, Source: "rendered_proxy_bpe_content_v5", Method: contextpolicy.EstimateMethodProviderRenderedPayload, Confidence: contextpolicy.EstimateApproximate}}
	if err := cache.ValidateCanonicalLineage(t.Context(), nil, "scope", &req.RawPlan, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	usage := provider.Usage{Source: provider.UsageNative, Available: true, InputTokensReported: true, InputTokenScope: "provider_execution", InputTokens: 5526, CacheReadTokens: 412160,
		OutputTokensReported: true, OutputTokens: 2149, ReasoningTokens: 1345, OutputIncludesReasoning: true,
		Measurement: &requestmeasure.Evidence{AttemptID: "attempt", CompleteInput: true, OpaqueInput: true, Input: p.InputMeasurementPrefix(), ResponseID: "response"}}
	return req, history, usage
}

func TestResponseBoundaryCountsOnlyFrozenSuffix(t *testing.T) {
	for _, inclusive := range []bool{true, false} {
		req, history, usage := measurementFixture(t)
		usage.OutputIncludesReasoning = inclusive
		// Huge ciphertext remains a protocol fact, never a transport-length estimate.
		responsePayload, _ := json.Marshal(map[string]any{"model": "model", "input": []any{map[string]any{"role": "user", "content": "question"}, map[string]any{"type": "reasoning", "encrypted_content": strings.Repeat("A", 300000)}, map[string]any{"type": "function_call", "call_id": "tool", "name": "read", "arguments": "{}"}}})
		usage.Measurement.ResponseEnd, _ = requestmeasure.RenderedPrefix(responsePayload, "openai_responses", "account:connection", "response_end")
		tr := NewContextPressureTracker("scope")
		tr.ObserveSuccess(req, history, usage)
		committed := append(history, session.Message{EntryID: "call", Role: session.Assistant, Content: "tool_call", ToolCallID: "tool", ToolName: "read", ToolArgs: "{}"})
		anchor := tr.ObserveCommittedResponse(req, committed, usage, "response")
		wantBase := int64(419835)
		if !inclusive {
			wantBase += 1345
		}
		if anchor.WindowInputTokens != wantBase || anchor.MeasurementPrefix.Boundary != "response_end" {
			t.Fatalf("anchor=%+v", anchor)
		}
		var body map[string]any
		json.Unmarshal(responsePayload, &body)
		body["input"] = append(body["input"].([]any), map[string]any{"type": "function_call_output", "call_id": "tool", "output": strings.Repeat("large result ", 1000)})
		raw, _ := json.Marshal(body)
		next := req
		next.Prepared = &measuredPrepared{payload: raw, identity: "account:connection"}
		next.RequestEstimate.EstimatedInputTokens = 474100
		suffix, _ := next.Prepared.(requestmeasure.Prepared).MeasurementSuffix(anchor.MeasurementPrefix)
		counted, err := requestestimate.Count(next.Provider, next.Model, "openai_responses", suffix)
		if err != nil {
			t.Fatal(err)
		}
		got := tr.Project(next, committed)
		if got.ProjectedInputTokens != wantBase+counted.Prefix+counted.Messages+counted.Tools || got.CompactionNeeded || got.Confidence == contextpolicy.EstimateExact {
			t.Fatalf("pressure=%+v reason=%s", got, tr.anchorReason)
		}
		// A restarted tracker uses the same durable anchor, not a reconstructed bill.
		restarted := NewContextPressureTracker("scope")
		restarted.SetAnchor(anchor, tr.anchorRequest)
		if after := restarted.Project(next, committed); after != got {
			t.Fatalf("restart changed projection: %+v != %+v", after, got)
		}
		next.Prepared = &measuredPrepared{payload: raw, identity: "different connection"}
		if changed := tr.Project(next, committed); changed.ProjectedInputTokens != 474100 || !changed.CompactionNeeded {
			t.Fatalf("identity mismatch accepted: %+v", changed)
		}
		committed[1].ToolArgs = "changed"
		next.Prepared = &measuredPrepared{payload: raw, identity: "account:connection"}
		if changed := tr.Project(next, committed); changed.ProjectedInputTokens != 474100 || tr.anchorReason != "history_changed" {
			t.Fatalf("edited output accepted: %+v %s", changed, tr.anchorReason)
		}
	}
}

func TestMeasurementQualificationRejectsUnprovenUsage(t *testing.T) {
	for _, name := range []string{"attempt", "scope", "opaque", "negative", "overflow", "aggregated", "missing_output", "negative_output", "output_overflow", "wrong_response", "missing_boundary"} {
		t.Run(name, func(t *testing.T) {
			req, history, usage := measurementFixture(t)
			usage.Measurement.ResponseEnd = usage.Measurement.Input
			usage.Measurement.ResponseEnd.Boundary = "response_end"
			switch name {
			case "attempt":
				usage.Measurement.AttemptID = "failed-attempt"
			case "scope":
				usage.Measurement.CompleteInput = false
			case "opaque":
				req.RequestEstimate.Breakdown.OpaqueBytes = 100
				usage.Measurement.OpaqueInput = false
			case "negative":
				usage.InputTokens = -1
			case "overflow":
				usage.InputTokens = math.MaxInt64
			case "aggregated":
				usage = usage.Add(usage)
			case "missing_output":
				usage.OutputTokensReported = false
			case "negative_output":
				usage.OutputTokens = -1
			case "output_overflow":
				usage.OutputTokens = math.MaxInt64
			case "wrong_response":
				usage.Measurement.ResponseID = "other"
			case "missing_boundary":
				usage.Measurement.ResponseEnd = requestmeasure.Prefix{}
			}
			tr := NewContextPressureTracker("scope")
			tr.ObserveSuccess(req, history, usage)
			if anchor := tr.ObserveCommittedResponse(req, history, usage, "response"); anchor.InputMeasurementQualified {
				t.Fatalf("invalid response measurement accepted: %+v", anchor)
			}
		})
	}
}

func TestInputBoundaryRejectsNewOpaqueSuffix(t *testing.T) {
	req, history, usage := measurementFixture(t)
	tr := NewContextPressureTracker("scope")
	tr.ObserveSuccess(req, history, usage)
	req.RequestEstimate.EstimatedInputTokens = 474100
	req.Prepared = &measuredPrepared{identity: "account:connection", payload: []byte(`{"model":"model","input":[{"role":"user","content":"question"},{"type":"reasoning","encrypted_content":"not-tokenizable"}]}`)}
	if got := tr.Project(req, history); got.ProjectedInputTokens != 474100 || tr.anchorReason != "unmeasured_opaque_suffix" {
		t.Fatalf("opaque suffix lost: %+v %s", got, tr.anchorReason)
	}
}

func TestMeasuredZeroAndThresholdBoundaries(t *testing.T) {
	for _, tokens := range []int64{0, 472499, 472500, 524000, 524001} {
		req, history, usage := measurementFixture(t)
		usage.InputTokens, usage.CacheReadTokens, usage.WindowInputTokens = tokens, 0, tokens
		tr := NewContextPressureTracker("scope")
		_, anchor := tr.ObserveSuccess(req, history, usage)
		if !anchor.InputMeasurementQualified {
			t.Fatal("explicit input zero or valid count rejected")
		}
		got := tr.Project(req, history)
		if got.ProjectedInputTokens != tokens || got.CompactionNeeded != (tokens >= 472500) || got.HardLimitExceeded != (tokens >= 524000) {
			t.Fatalf("tokens=%d got=%+v", tokens, got)
		}
	}
}

func TestRestoredMeasurementRequiresCommittedMatchingAttempt(t *testing.T) {
	req, history, usage := measurementFixture(t)
	req.RawPlan.MeasurementPrefix = req.Prepared.(requestmeasure.Prepared).InputMeasurementPrefix()
	usage.Measurement.ResponseEnd = usage.Measurement.Input
	usage.Measurement.ResponseEnd.Boundary = "response_end"
	tr := NewContextPressureTracker("scope")
	tr.ObserveSuccess(req, history, usage)
	anchor := tr.ObserveCommittedResponse(req, history, usage, "response")
	request := cache.ProviderRequestRecord{ID: anchor.RequestID, PromptScopeID: "scope", AttemptID: req.AttemptID, MeasurementPrefix: usage.Measurement.Input}
	response := cache.ProviderResponseRecord{RequestID: anchor.RequestID, PromptScopeID: "scope", AttemptID: req.AttemptID, ProviderResponseID: "response", Measurement: usage.Measurement, PressureAnchor: anchor, WindowInputTokens: usage.Normalized().WindowInputTokens, InputTokens: usage.InputTokens, CacheReadTokens: usage.CacheReadTokens, OutputTokens: usage.OutputTokens, ReasoningTokens: usage.ReasoningTokens, OutputTokensReported: true, InputTokensReported: true, InputTokenScope: usage.InputTokenScope, OutputIncludesReasoning: true, UsageAvailable: true, UsageSource: "native", MeasurementCommitted: true}
	for _, name := range []string{"valid", "attempt", "commit", "budget", "evidence"} {
		t.Run(name, func(t *testing.T) {
			current := response
			evidence := *response.Measurement
			current.Measurement = &evidence
			switch name {
			case "attempt":
				current.AttemptID = "failed"
			case "commit":
				current.MeasurementCommitted = false
			case "budget":
				current.OutputTokens++
			case "evidence":
				current.Measurement.CompleteInput = false
			}
			store := cache.NewMemoryStore()
			store.AppendProviderRequest(t.Context(), request)
			store.AppendProviderResponse(t.Context(), current)
			restored := NewContextPressureTracker("scope")
			e := &Engine{prompt: store}
			if err := e.restorePressureAnchor(t.Context(), restored, Options{PromptScopeID: "scope", ProviderName: req.Provider, Model: req.Model}); err != nil {
				t.Fatal(err)
			}
			if restored.anchor.InputMeasurementQualified != (name == "valid") {
				t.Fatalf("restored invalid measurement: %+v", restored.anchor)
			}
		})
	}
}
