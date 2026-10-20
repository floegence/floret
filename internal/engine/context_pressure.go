package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/requestestimate"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

var ErrContextWouldOverflow = errors.New("provider request would exceed context window")

type RequestShapeHashes = cache.RequestShapeHashes
type PressureAnchorState = cache.PressureAnchorState

type ContextPressureTracker struct {
	softNoopFingerprint string
	promptScopeID       string
	anchor              PressureAnchorState
	anchorRequest       cache.ProviderRequestRecord
	anchorReason        string
}

func NewContextPressureTracker(promptScopeID string) *ContextPressureTracker {
	return &ContextPressureTracker{promptScopeID: promptScopeID}
}

func (t *ContextPressureTracker) SetAnchor(anchor PressureAnchorState, request cache.ProviderRequestRecord) {
	if t == nil || !anchor.InputMeasurementQualified || anchor.WindowInputTokens < 0 {
		return
	}
	t.anchor = anchor
	t.anchorRequest = request
}

func (t *ContextPressureTracker) Project(req provider.Request, history []session.Message) contextpolicy.ContextPressure {
	if t == nil {
		return contextpolicy.PressureFromProjectedRequest(req.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
	}
	estimate := req.RequestEstimate.Normalized(req.ContextPolicy)
	t.anchorReason = pressureAnchorInvalidReason(t.anchor, t.anchorRequest, req, history)
	if t.anchorReason == "" {
		difference := estimate.EstimatedInputTokens - t.anchorRequest.RequestEstimate.EstimatedInputTokens
		if t.anchor.MeasurementPrefix.Valid() {
			prepared, ok := req.Prepared.(requestmeasure.Prepared)
			if !ok {
				t.anchorReason = "missing_frozen_boundary"
			} else if suffix, matched := prepared.MeasurementSuffix(t.anchor.MeasurementPrefix); !matched {
				t.anchorReason = "protocol_prefix_changed"
			} else if len(suffix) == 0 {
				difference = 0
			} else if counted, err := requestestimate.Count(req.Provider, req.Model, t.anchor.MeasurementPrefix.Format, suffix); err != nil || counted.Breakdown.OpaqueBytes > 0 {
				t.anchorReason = "unmeasured_opaque_suffix"
			} else {
				difference = counted.Prefix + counted.Messages + counted.Tools
			}
		}
		if t.anchorReason != "" || difference < 0 || difference > math.MaxInt64-t.anchor.WindowInputTokens {
			if t.anchorReason == "" {
				t.anchorReason = "invalid_suffix_budget"
			}
			return contextpolicy.PressureFromProjectedRequest(estimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
		}
		delta := contextpolicy.RequestDeltaEstimate{
			EstimatedDeltaTokens: difference,
			Source:               estimate.Source, Method: estimate.Method, Confidence: contextpolicy.EstimateConservative,
		}
		base := estimate
		base.EstimatedInputTokens = t.anchor.WindowInputTokens
		base.PrefixTokens, base.MessageTokens, base.ToolDefinitionTokens = 0, 0, 0
		return contextpolicy.PressureFromProjectedRequest(base, delta, req.ContextPolicy)
	}
	return contextpolicy.PressureFromProjectedRequest(estimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
}

func (t *ContextPressureTracker) ObserveSuccess(req provider.Request, history []session.Message, usage provider.Usage) (contextpolicy.ContextPressure, PressureAnchorState) {
	if t == nil {
		normalized := usage.Normalized()
		if !qualifiedPressureUsage(req, normalized) {
			return contextpolicy.PressureFromProjectedRequest(req.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy), PressureAnchorState{}
		}
		return contextpolicy.PressureFromNativeUsage(nativeUsageForPressure(normalized), req.ContextPolicy), PressureAnchorState{}
	}
	normalized := usage.Normalized()
	if !qualifiedPressureUsage(req, normalized) {
		pressure := req.ContextPressure
		if pressure == (contextpolicy.ContextPressure{}) {
			pressure = t.Project(req, history)
		}
		pressure.Source = contextpolicy.PressureSourceMissingNativeUsage
		return pressure, PressureAnchorState{}
	}
	pressure := contextpolicy.PressureFromNativeUsage(nativeUsageForPressure(normalized), req.ContextPolicy)
	if req.ContextPolicy.ModelMaxOutputTokens > 0 {
		pressure.RequestSafeLimit = req.ContextPressure.RequestSafeLimit
		pressure.ThresholdTokens = req.ContextPressure.ThresholdTokens
		pressure.CompactionNeeded = pressure.WindowInputTokens >= pressure.ThresholdTokens
		pressure.HardLimitExceeded = pressure.WindowInputTokens > pressure.RequestSafeLimit
	}
	pressure.RequestOutputTokens = req.ContextPressure.RequestOutputTokens
	pressure.OutputControl = req.ContextPressure.OutputControl
	pressure.SafetyMarginTokens = req.ContextPressure.SafetyMarginTokens
	anchor := PressureAnchorState{}
	if normalized.Available {
		anchor = pressureAnchorForRequest(req, history, normalized, pressure)
		if anchor.InputMeasurementQualified {
			t.SetAnchor(anchor, cache.ProviderRequestRecord{
				ID: anchor.RequestID, PromptScopeID: req.PromptScopeID, Provider: req.Provider, Model: req.Model,
				CanonicalEnvelopeHash:      req.RawPlan.CanonicalEnvelopeHash,
				CanonicalMessageCount:      req.RawPlan.CanonicalMessageCount,
				CanonicalHistoryPrefixHash: req.RawPlan.CanonicalHistoryPrefixHash,
				RenderLineageKey:           req.RawPlan.RenderLineageKey, ContextProjectionRevision: req.RawPlan.ContextProjectionRevision,
				RequestEstimate: req.RequestEstimate, HasEphemeralOverlay: req.EphemeralUser != nil,
			})
		}
	} else {
		pressure = req.ContextPressure
		pressure.Source = contextpolicy.PressureSourceMissingNativeUsage
	}
	return pressure, anchor
}

func softNoopIdentity(req provider.Request) string {
	return fmt.Sprintf("%s:%s:%d:%d:%d:%d:%d:%s:%s:%s", req.RawPlan.PayloadHash, req.RawPlan.CanonicalEnvelopeHash, req.ContextPressure.ProjectedInputTokens, req.ContextPressure.ThresholdTokens, req.ContextPressure.RequestSafeLimit, req.MaxOutputTokens, req.ContextPolicy.ModelMaxOutputTokens, req.RequestEstimate.Source, req.RequestEstimate.Method, req.PressureAnchorReason)
}

func (t *ContextPressureTracker) Overflow(policy contextpolicy.Policy) contextpolicy.ContextPressure {
	return contextpolicy.PressureFromOverflow(policy)
}

// A calibration belongs to the canonical request prefix, not transient Engine
// message identifiers, which the durable harness replaces when committing output.
func pressureAnchorInvalidReason(anchor PressureAnchorState, previous cache.ProviderRequestRecord, req provider.Request, history []session.Message) string {
	if anchor.MeasurementPrefix != (requestmeasure.Prefix{}) && !anchor.MeasurementPrefix.Valid() {
		return "invalid_measurement_boundary"
	}
	if !anchor.MeasurementPrefix.Valid() && (req.RequestEstimate.Breakdown.OpaqueBytes > 0 || len(req.HostedTools) > 0) {
		return "incomplete_request_coverage"
	}
	if !anchor.InputMeasurementQualified {
		return "unqualified_measurement"
	}
	if anchor.WindowInputTokens < 0 || previous.ID == "" || previous.ID != anchor.RequestID {
		return "missing_measurement"
	}
	if anchor.PromptScopeID != req.PromptScopeID || previous.PromptScopeID != req.PromptScopeID {
		return "prompt_scope_changed"
	}
	if anchor.Provider != req.Provider || anchor.Model != req.Model || previous.Provider != req.Provider || previous.Model != req.Model {
		return "model_changed"
	}
	if anchor.AdapterVersion != req.RawPlan.Version || previous.ContextProjectionRevision != req.RawPlan.ContextProjectionRevision {
		return "projection_changed"
	}
	if anchor.CompactionGeneration != req.RawPlan.CompactionGeneration || anchor.CompactionWindowID != req.RawPlan.CompactionWindowID || req.RawPlan.CanonicalLineageReset {
		return "lineage_changed"
	}
	if previous.HasEphemeralOverlay || req.EphemeralUser != nil {
		return "ephemeral_input"
	}
	if anchor.Shape.CacheShapeHash != req.Cache.Namespace || previous.CanonicalEnvelopeHash != req.RawPlan.CanonicalEnvelopeHash || previous.RenderLineageKey != req.RawPlan.RenderLineageKey {
		return "execution_surface_changed"
	}
	if anchor.EstimateSource != req.RequestEstimate.Source || anchor.EstimateMethod != req.RequestEstimate.Method || previous.RequestEstimate.Confidence != req.RequestEstimate.Confidence {
		return "estimator_changed"
	}
	boundary := previous
	if anchor.MeasurementPrefix.Boundary == "response_end" {
		boundary.CanonicalMessageCount = anchor.CanonicalMessageCount
		boundary.CanonicalHistoryPrefixHash = anchor.CanonicalHistoryPrefixHash
	}
	if !cache.MatchesCanonicalHistoryPrefix(boundary, history) {
		return "history_changed"
	}
	if !anchor.MeasurementPrefix.Valid() && (previous.RequestEstimate.EstimatedInputTokens <= 0 || req.RequestEstimate.EstimatedInputTokens < previous.RequestEstimate.EstimatedInputTokens) {
		return "non_monotonic_estimate"
	}
	return ""
}

func (e *Engine) restorePressureAnchor(ctx context.Context, tracker *ContextPressureTracker, opts Options) error {
	anchor, ok, err := e.prompt.LatestPressureAnchor(ctx, opts.PromptScopeID, opts.ProviderName, opts.Model)
	if err != nil || !ok {
		return err
	}
	requests, err := e.prompt.ProviderRequests(ctx, opts.PromptScopeID)
	if err != nil {
		return err
	}
	var response *cache.ProviderResponseRecord
	if anchor.MeasurementPrefix != (requestmeasure.Prefix{}) {
		responses, err := e.prompt.ProviderResponses(ctx, opts.PromptScopeID)
		if err != nil {
			return err
		}
		for i := len(responses) - 1; i >= 0; i-- {
			if responses[i].PressureAnchor == anchor {
				response = &responses[i]
				break
			}
		}
		if response == nil {
			return nil
		}
	}
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].ID != anchor.RequestID || response != nil && requests[i].AttemptID != response.AttemptID {
			continue
		}
		if response != nil && !validStoredMeasurement(anchor, requests[i], *response) {
			return nil
		}
		tracker.SetAnchor(anchor, requests[i])
		break
	}
	return nil
}

func validStoredMeasurement(anchor PressureAnchorState, request cache.ProviderRequestRecord, response cache.ProviderResponseRecord) bool {
	prefix, evidence := anchor.MeasurementPrefix, response.Measurement
	if !response.MeasurementCommitted || evidence == nil || response.AttemptID == "" || evidence.AttemptID != response.AttemptID || !evidence.CompleteInput || !prefix.Valid() || !request.MeasurementPrefix.Valid() || evidence.Input != request.MeasurementPrefix || prefix.ExecutionIdentity != evidence.Input.ExecutionIdentity || prefix.Format != evidence.Input.Format || request.RequestEstimate.Breakdown.OpaqueBytes > 0 && !evidence.OpaqueInput {
		return false
	}
	usage := provider.Usage{Source: provider.UsageSource(response.UsageSource), Available: response.UsageAvailable, InputTokensReported: response.InputTokensReported, InputTokens: response.InputTokens, CacheReadTokens: response.CacheReadTokens, CacheWriteTokens: response.CacheWriteTokens, WindowInputTokens: response.WindowInputTokens}
	if usage.Source != provider.UsageNative || !usage.Available || !usage.InputTokensReported || !validNativeInput(usage) {
		return false
	}
	if prefix.Boundary == "input" {
		return prefix == evidence.Input && anchor.WindowInputTokens == usage.WindowInputTokens
	}
	output, ok := nativeReplayOutput(provider.Usage{OutputTokensReported: response.OutputTokensReported, OutputTokens: response.OutputTokens, ReasoningTokens: response.ReasoningTokens, OutputIncludesReasoning: response.OutputIncludesReasoning})
	return ok && output <= math.MaxInt64-usage.WindowInputTokens && anchor.WindowInputTokens == usage.WindowInputTokens+output && evidence.ResponseID != "" && evidence.ResponseID == response.ProviderResponseID && prefix == evidence.ResponseEnd && prefix.ItemCount >= evidence.Input.ItemCount && anchor.CanonicalMessageCount >= request.CanonicalMessageCount && anchor.CanonicalHistoryPrefixHash != ""
}

func pressureAnchorForRequest(req provider.Request, history []session.Message, usage provider.Usage, pressure contextpolicy.ContextPressure) PressureAnchorState {
	lastIndex := -1
	lastEntryID := ""
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].EntryID != "" {
			lastIndex = i
			lastEntryID = history[i].EntryID
			break
		}
	}
	if lastEntryID == "" {
		return PressureAnchorState{}
	}
	return PressureAnchorState{
		MeasurementPrefix:         qualifiedInputPrefix(req, usage),
		InputMeasurementQualified: true,
		PromptScopeID:             req.PromptScopeID,
		ThreadID:                  req.ThreadID,
		Provider:                  req.Provider,
		Model:                     req.Model,
		AdapterVersion:            req.RawPlan.Version,
		RequestID:                 requestID(req.RunID, req.Step),
		RunID:                     req.RunID,
		LogicalRequestID:          req.LogicalRequestID,
		LastMessageEntryID:        lastEntryID,
		LastMessageIndex:          lastIndex,
		CompactionGeneration:      req.RawPlan.CompactionGeneration,
		CompactionWindowID:        req.RawPlan.CompactionWindowID,
		Shape:                     requestShapeHashes(req),
		WindowInputTokens:         usage.WindowInputTokens,
		PrefixTokens:              req.RequestEstimate.PrefixTokens,
		MessageTokens:             req.RequestEstimate.MessageTokens,
		ToolDefinitionTokens:      req.RequestEstimate.ToolDefinitionTokens,
		ContextWindowTokens:       pressure.ContextWindowTokens,
		EstimateSource:            req.RequestEstimate.Source,
		EstimateMethod:            req.RequestEstimate.Method,
		Confidence:                req.RequestEstimate.Confidence,
		PressureSource:            pressure.Source,
		CreatedAt:                 time.Now(),
	}
}

func requestShapeHashes(req provider.Request) RequestShapeHashes {
	return RequestShapeHashes{
		SystemPrefixHash:    req.RawPlan.PrefixHash,
		MessagePayloadHash:  req.RawPlan.PayloadHash,
		LocalToolsetHash:    req.RawPlan.ToolsetID,
		HostedToolsetHash:   req.RawPlan.HostedToolsetHash,
		ProviderPayloadHash: req.RawPlan.PayloadHash,
		CacheShapeHash:      req.Cache.Namespace,
	}
}

func nativeUsageForPressure(usage provider.Usage) contextpolicy.NativeUsage {
	return contextpolicy.NativeUsage{
		InputTokens:       usage.InputTokens,
		CacheReadTokens:   usage.CacheReadTokens,
		CacheWriteTokens:  usage.CacheWriteTokens,
		WindowInputTokens: usage.WindowInputTokens,
		Available:         usage.Available,
	}
}

func requestID(runID string, step int) string {
	return fmt.Sprintf("%s:req:%d", runID, step)
}

// Native billing input is an anchor only when its protocol scope and visible
// request coverage establish that it measures this prepared input.
func qualifiedPressureUsage(req provider.Request, usage provider.Usage) bool {
	if usage.Aggregated || usage.Source != provider.UsageNative || !usage.Available || !usage.InputTokensReported {
		return false
	}
	if !validNativeInput(usage) {
		return false
	}
	if qualifiedInputPrefix(req, usage).Valid() {
		return true
	}
	return usage.Measurement == nil && usage.InputTokenScope == "request_input" && len(req.HostedTools) == 0 && req.RequestEstimate.Breakdown.OpaqueBytes == 0
}

func validNativeInput(usage provider.Usage) bool {
	remaining := int64(math.MaxInt64)
	for _, n := range []int64{usage.InputTokens, usage.CacheReadTokens, usage.CacheWriteTokens} {
		if n < 0 || n > remaining {
			return false
		}
		remaining -= n
	}
	if usage.WindowInputTokens < 0 || usage.WindowInputTokens < math.MaxInt64-remaining {
		return false
	}
	return true
}

func nativeReplayOutput(usage provider.Usage) (int64, bool) {
	if !usage.OutputTokensReported || usage.OutputTokens < 0 || usage.ReasoningTokens < 0 {
		return 0, false
	}
	output := usage.OutputTokens
	if !usage.OutputIncludesReasoning {
		if usage.ReasoningTokens > math.MaxInt64-output {
			return 0, false
		}
		output += usage.ReasoningTokens
	} else if usage.ReasoningTokens > output {
		return 0, false
	}
	return output, true
}

func qualifiedInputPrefix(req provider.Request, usage provider.Usage) requestmeasure.Prefix {
	evidence := usage.Measurement
	prepared, ok := req.Prepared.(requestmeasure.Prepared)
	if !ok || evidence == nil || evidence.AttemptID == "" || evidence.AttemptID != req.AttemptID || !evidence.CompleteInput || !evidence.Input.Valid() || evidence.Input.Boundary != "input" || evidence.Input != prepared.InputMeasurementPrefix() || req.RequestEstimate.Breakdown.OpaqueBytes > 0 && !evidence.OpaqueInput {
		return requestmeasure.Prefix{}
	}
	return evidence.Input
}

// ObserveCommittedResponse advances the same anchor only after canonical output
// commits. Native output is a conservative replay budget, never window occupancy.
func (t *ContextPressureTracker) ObserveCommittedResponse(req provider.Request, history []session.Message, usage provider.Usage, responseID string) PressureAnchorState {
	usage = usage.Normalized()
	if t == nil || !qualifiedPressureUsage(req, usage) || !usage.OutputTokensReported || usage.OutputTokens < 0 || usage.ReasoningTokens < 0 || !qualifiedInputPrefix(req, usage).Valid() {
		return PressureAnchorState{}
	}
	evidence := usage.Measurement
	prefix := evidence.ResponseEnd
	if responseID == "" || evidence.ResponseID != responseID || !prefix.Valid() || prefix.Boundary != "response_end" || prefix.ExecutionIdentity != evidence.Input.ExecutionIdentity || prefix.Format != evidence.Input.Format || prefix.ItemCount < evidence.Input.ItemCount {
		return PressureAnchorState{}
	}
	output, ok := nativeReplayOutput(usage)
	if !ok {
		return PressureAnchorState{}
	}
	if output > math.MaxInt64-usage.WindowInputTokens {
		return PressureAnchorState{}
	}
	anchor := t.anchor
	if anchor.RequestID != requestID(req.RunID, req.Step) {
		return PressureAnchorState{}
	}
	anchor.MeasurementPrefix = prefix
	anchor.WindowInputTokens = usage.WindowInputTokens + output
	var plan cache.RawPlan
	if err := cache.ValidateCanonicalLineage(context.Background(), nil, req.PromptScopeID, &plan, "", nil, nil, nil, history); err != nil {
		return PressureAnchorState{}
	}
	anchor.CanonicalMessageCount, anchor.CanonicalHistoryPrefixHash = plan.CanonicalMessageCount, plan.CanonicalHistoryPrefixHash
	t.anchor = anchor
	return anchor
}
