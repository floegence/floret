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

// Recheck only the ordinary threshold decision. Never replace the estimate in
// the request ledger: a later calibrated delta needs comparable estimates.
func verifyCompactionPressure(ctx context.Context, req *provider.Request) error {
	p := req.ContextPressure
	if p.Source != contextpolicy.PressureSourceFullRequestEstimate || p.Confidence == contextpolicy.EstimateExact || req.EphemeralUser != nil || req.Reasoning.BudgetTokens == math.MaxInt64 {
		return nil
	}
	budget := contextpolicy.ResolveRequestBudget(req.ContextPolicy, p.ProjectedInputTokens, max(1, req.Reasoning.BudgetTokens+1))
	if budget.Invalid || !budget.CompactionNeeded {
		return nil
	}
	c, ok := req.Prepared.(interface {
		CountPreparedInput(context.Context) (int64, bool)
	})
	if !ok {
		return nil
	}
	check, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, available := c.CountPreparedInput(check)
	if err := ctx.Err(); err != nil {
		return err
	}
	if check.Err() != nil || !available || n < 0 {
		return nil
	}
	budget = contextpolicy.ResolveRequestBudget(req.ContextPolicy, n, max(1, req.Reasoning.BudgetTokens+1))
	p.ProjectedInputTokens, p.CompactionNeeded, p.HardLimitExceeded = n, budget.CompactionNeeded, budget.HardLimitExceeded
	p.EstimateMethod, p.Confidence = contextpolicy.EstimateMethodOfficialPreflightCount, contextpolicy.EstimateExact
	req.ContextPressure = p
	return nil
}

type RequestShapeHashes = cache.RequestShapeHashes
type PressureAnchorState = cache.PressureAnchorState

type ContextPressureTracker struct {
	softNoopFingerprint  string
	promptScopeID        string
	anchor               PressureAnchorState
	anchorRequest        cache.ProviderRequestRecord
	anchorReason         string
	inputReferenceTokens int64
	projectedBoundary    string
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
	t.inputReferenceTokens = -1
}

func (t *ContextPressureTracker) Project(req provider.Request, history []session.Message) contextpolicy.ContextPressure {
	if t == nil {
		return contextpolicy.PressureFromProjectedRequest(req.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
	}
	estimate := req.RequestEstimate.Normalized(req.ContextPolicy)
	t.projectedBoundary = t.anchor.MeasurementPrefix.Boundary
	t.anchorReason = pressureAnchorInvalidReason(t.anchor, t.anchorRequest, req, history)
	if t.anchorReason == "" {
		difference := estimate.EstimatedInputTokens - t.anchorRequest.RequestEstimate.EstimatedInputTokens
		baseTokens := t.anchor.WindowInputTokens
		approximate := t.anchor.PressureSource == contextpolicy.PressureSourceExecutionReference || !t.anchor.MeasurementPrefix.Valid() && estimate.Breakdown.OpaqueBytes > 0
		if t.anchor.MeasurementPrefix.Valid() {
			prepared, ok := req.Prepared.(requestmeasure.Prepared)
			if !ok {
				t.anchorReason = "missing_frozen_boundary"
			} else {
				suffix, matched := prepared.MeasurementSuffix(t.anchor.MeasurementPrefix)
				if !matched && t.projectedBoundary == "response_end" && t.inputReferenceTokens >= 0 && t.anchorRequest.MeasurementPrefix.Valid() {
					suffix, matched = prepared.MeasurementSuffix(t.anchorRequest.MeasurementPrefix)
					baseTokens, t.projectedBoundary = t.inputReferenceTokens, "input"
				}
				switch {
				case !matched:
					t.anchorReason = "protocol_prefix_changed"
				case len(suffix) == 0:
					difference = 0
				default:
					counted, err := requestestimate.Count(req.Provider, req.Model, t.anchor.MeasurementPrefix.Format, suffix)
					if err != nil {
						t.anchorReason = "suffix_estimate_unavailable"
					} else {
						difference = counted.Prefix + counted.Messages + counted.Tools
						approximate = approximate || counted.Breakdown.OpaqueBytes > 0
					}
				}
			}
		}
		if t.anchorReason != "" || difference < 0 || difference > math.MaxInt64-baseTokens {
			if t.anchorReason == "" {
				t.anchorReason = "invalid_suffix_budget"
			}
			return contextpolicy.PressureFromProjectedRequest(estimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
		}
		delta := contextpolicy.RequestDeltaEstimate{
			EstimatedDeltaTokens: difference,
			Source:               estimate.Source, Method: estimate.Method, Confidence: contextpolicy.EstimateConservative,
		}
		if approximate {
			delta.Confidence = contextpolicy.EstimateApproximate
		}
		base := estimate
		base.EstimatedInputTokens = baseTokens
		base.PrefixTokens, base.MessageTokens, base.ToolDefinitionTokens = 0, 0, 0
		return contextpolicy.PressureFromProjectedRequest(base, delta, req.ContextPolicy)
	}
	return contextpolicy.PressureFromProjectedRequest(estimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
}

func (t *ContextPressureTracker) ObserveSuccess(req provider.Request, history []session.Message, usage provider.Usage, responseID string) (contextpolicy.ContextPressure, PressureAnchorState) {
	normalized := usage.Normalized()
	prefix, source := pressureInputBoundary(req, normalized, responseID)
	if source == "" {
		pressure := req.ContextPressure
		if pressure == (contextpolicy.ContextPressure{}) {
			pressure = t.Project(req, history)
		}
		pressure.Source = contextpolicy.PressureSourceMissingNativeUsage
		return pressure, PressureAnchorState{}
	}
	pressure := contextpolicy.PressureFromNativeUsage(nativeUsageForPressure(normalized), req.ContextPolicy)
	if source == contextpolicy.PressureSourceExecutionReference {
		pressure.Source, pressure.Confidence = source, contextpolicy.EstimateApproximate
	}
	if t == nil {
		return pressure, PressureAnchorState{}
	}
	if req.ContextPolicy.ModelMaxOutputTokens > 0 {
		pressure.RequestSafeLimit = req.ContextPressure.RequestSafeLimit
		pressure.ThresholdTokens = req.ContextPressure.ThresholdTokens
		pressure.CompactionNeeded = pressure.WindowInputTokens >= pressure.ThresholdTokens
		pressure.HardLimitExceeded = pressure.WindowInputTokens > pressure.RequestSafeLimit
	}
	pressure.RequestOutputTokens = req.ContextPressure.RequestOutputTokens
	pressure.OutputControl = req.ContextPressure.OutputControl
	pressure.SafetyMarginTokens = req.ContextPressure.SafetyMarginTokens
	anchor := pressureAnchorForRequest(req, history, normalized, pressure, prefix)
	if anchor.InputMeasurementQualified {
		t.SetAnchor(anchor, cache.ProviderRequestRecord{
			MeasurementPrefix: anchor.MeasurementPrefix,
			ID:                anchor.RequestID, PromptScopeID: req.PromptScopeID, Provider: req.Provider, Model: req.Model,
			CanonicalEnvelopeHash:      req.RawPlan.CanonicalEnvelopeHash,
			CanonicalMessageCount:      req.RawPlan.CanonicalMessageCount,
			CanonicalHistoryPrefixHash: req.RawPlan.CanonicalHistoryPrefixHash,
			RenderLineageKey:           req.RawPlan.RenderLineageKey, ContextProjectionRevision: req.RawPlan.ContextProjectionRevision,
			RequestEstimate: req.RequestEstimate, HasEphemeralOverlay: req.EphemeralUser != nil,
		})
		t.inputReferenceTokens = normalized.WindowInputTokens
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
	evidence, found, err := cache.ReadPressureEvidence(ctx, e.prompt, opts.PromptScopeID, opts.ProviderName, opts.Model)
	if err != nil || !found {
		return err
	}
	if evidence.Anchor.RequiresCommittedResponse() && (evidence.Response == nil || !validStoredMeasurement(evidence.Anchor, evidence.Request, *evidence.Response)) {
		return nil
	}
	tracker.SetAnchor(evidence.Anchor, evidence.Request)
	if evidence.Response != nil {
		tracker.inputReferenceTokens = evidence.Response.WindowInputTokens
	}
	return nil
}

func validStoredMeasurement(anchor PressureAnchorState, request cache.ProviderRequestRecord, response cache.ProviderResponseRecord) bool {
	prefix, evidence := anchor.MeasurementPrefix, response.Measurement
	if !response.MeasurementCommitted || request.AttemptID == "" || request.AttemptID != response.AttemptID || request.ID != response.RequestID || request.ID != anchor.RequestID {
		return false
	}
	usage := provider.Usage{Source: provider.UsageSource(response.UsageSource), Available: response.UsageAvailable, InputTokensReported: response.InputTokensReported, InputTokenScope: response.InputTokenScope, InputTokens: response.InputTokens, CacheReadTokens: response.CacheReadTokens, CacheWriteTokens: response.CacheWriteTokens, WindowInputTokens: response.WindowInputTokens}
	if !nativeHistoryInput(usage) {
		return false
	}
	if evidence == nil {
		return anchor.PressureSource == contextpolicy.PressureSourceExecutionReference && prefix == request.MeasurementPrefix && (prefix == (requestmeasure.Prefix{}) || prefix.Valid() && prefix.Boundary == "input") && anchor.WindowInputTokens == usage.WindowInputTokens
	}
	if !prefix.Valid() || !matchesInputEvidence(evidence, response.AttemptID, response.InputTokenScope, request.MeasurementPrefix, request.RequestEstimate.Breakdown.OpaqueBytes) {
		return false
	}
	reference := evidence.Input.Boundary == requestmeasure.ExecutionInputBoundary
	if prefix.ExecutionIdentity != evidence.Input.ExecutionIdentity || prefix.Format != evidence.Input.Format || (anchor.PressureSource == contextpolicy.PressureSourceExecutionReference) != reference || reference && evidence.ResponseID != response.ProviderResponseID {
		return false
	}
	if prefix.Boundary == "input" {
		return prefix == request.MeasurementPrefix && anchor.WindowInputTokens == usage.WindowInputTokens
	}
	output, ok := nativeReplayOutput(provider.Usage{OutputTokensReported: response.OutputTokensReported, OutputTokens: response.OutputTokens, ReasoningTokens: response.ReasoningTokens, OutputIncludesReasoning: response.OutputIncludesReasoning})
	return prefix.Boundary == "response_end" && ok && output <= math.MaxInt64-usage.WindowInputTokens && anchor.WindowInputTokens == usage.WindowInputTokens+output && evidence.ResponseID != "" && evidence.ResponseID == response.ProviderResponseID && prefix == evidence.ResponseEnd && prefix.ItemCount >= evidence.Input.ItemCount && anchor.CanonicalMessageCount >= request.CanonicalMessageCount && anchor.CanonicalHistoryPrefixHash != ""
}

func pressureAnchorForRequest(req provider.Request, history []session.Message, usage provider.Usage, pressure contextpolicy.ContextPressure, prefix requestmeasure.Prefix) PressureAnchorState {
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
		MeasurementPrefix:         prefix,
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

func nativeHistoryInput(usage provider.Usage) bool {
	positiveLegacyInput := usage.InputTokenScope == "" && (usage.InputTokens > 0 || usage.CacheReadTokens > 0 || usage.CacheWriteTokens > 0 || usage.WindowInputTokens > 0)
	return !usage.Aggregated && usage.Source == provider.UsageNative && usage.Available && (usage.InputTokensReported || positiveLegacyInput) && validNativeInput(usage)
}

func pressureInputBoundary(req provider.Request, usage provider.Usage, responseID string) (requestmeasure.Prefix, contextpolicy.PressureSource) {
	if !nativeHistoryInput(usage) {
		return requestmeasure.Prefix{}, ""
	}
	if usage.Measurement != nil {
		prefix := qualifiedInputPrefix(req, usage)
		if !prefix.Valid() {
			return requestmeasure.Prefix{}, ""
		}
		if usage.Measurement.Input.Boundary == requestmeasure.ExecutionInputBoundary {
			if usage.Measurement.ResponseID != responseID {
				return requestmeasure.Prefix{}, ""
			}
			return prefix, contextpolicy.PressureSourceExecutionReference
		}
		return prefix, contextpolicy.PressureSourceProviderUsage
	}
	if usage.InputTokenScope == "request_input" && len(req.HostedTools) == 0 && req.RequestEstimate.Breakdown.OpaqueBytes == 0 {
		return requestmeasure.Prefix{}, contextpolicy.PressureSourceProviderUsage
	}
	return req.RawPlan.MeasurementPrefix, contextpolicy.PressureSourceExecutionReference
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
	prepared, ok := req.Prepared.(requestmeasure.Prepared)
	if !ok || usage.Measurement == nil {
		return requestmeasure.Prefix{}
	}
	prefix := req.RawPlan.MeasurementPrefix
	if !prefix.Valid() {
		prefix = prepared.InputMeasurementPrefix()
	}
	if !matchesInputEvidence(usage.Measurement, req.AttemptID, usage.InputTokenScope, prefix, req.RequestEstimate.Breakdown.OpaqueBytes) {
		return requestmeasure.Prefix{}
	}
	return prefix
}

func matchesInputEvidence(evidence *requestmeasure.Evidence, attempt, scope string, frozen requestmeasure.Prefix, opaqueBytes int64) bool {
	if evidence == nil || attempt == "" || evidence.AttemptID != attempt || !frozen.Valid() || frozen.Boundary != "input" {
		return false
	}
	input := evidence.Input
	if input.Boundary == requestmeasure.ExecutionInputBoundary {
		input.Boundary = "input"
		return input == frozen && scope == "provider_execution" && evidence.ResponseID != "" && !evidence.CompleteInput && !evidence.OpaqueInput
	}
	return input == frozen && evidence.CompleteInput && (opaqueBytes == 0 || evidence.OpaqueInput)
}

// ObserveCommittedResponse advances the same anchor only after canonical output
// commits. Native output is a conservative replay budget, never window occupancy.
func (t *ContextPressureTracker) ObserveCommittedResponse(req provider.Request, history []session.Message, usage provider.Usage, responseID string) PressureAnchorState {
	usage = usage.Normalized()
	if t == nil || usage.Measurement == nil || !nativeHistoryInput(usage) || !qualifiedInputPrefix(req, usage).Valid() {
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
	if err := cache.ValidateCanonicalLineage(context.Background(), nil, req.PromptScopeID, &plan, "", nil, nil, nil, history, nil); err != nil {
		return PressureAnchorState{}
	}
	anchor.CanonicalMessageCount, anchor.CanonicalHistoryPrefixHash = plan.CanonicalMessageCount, plan.CanonicalHistoryPrefixHash
	t.anchor = anchor
	return anchor
}
