package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

var ErrContextWouldOverflow = errors.New("provider request would exceed context window")

type RequestShapeHashes = cache.RequestShapeHashes
type PressureAnchorState = cache.PressureAnchorState

type ContextPressureTracker struct {
	promptScopeID     string
	anchor            PressureAnchorState
	anchorRequest     cache.ProviderRequestRecord
	anchorReason      string
	pendingCompaction bool
	pendingPressure   contextpolicy.ContextPressure
}

func NewContextPressureTracker(promptScopeID string) *ContextPressureTracker {
	return &ContextPressureTracker{promptScopeID: promptScopeID}
}

func (t *ContextPressureTracker) SetAnchor(anchor PressureAnchorState, request cache.ProviderRequestRecord) {
	if t == nil || anchor.WindowInputTokens <= 0 {
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
		delta := contextpolicy.RequestDeltaEstimate{
			EstimatedDeltaTokens: estimate.EstimatedInputTokens - t.anchorRequest.RequestEstimate.EstimatedInputTokens,
			Source:               estimate.Source, Method: estimate.Method, Confidence: estimate.Confidence,
		}
		base := estimate
		base.EstimatedInputTokens = t.anchor.WindowInputTokens
		return contextpolicy.PressureFromProjectedRequest(base, delta, req.ContextPolicy)
	}
	return contextpolicy.PressureFromProjectedRequest(estimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
}

func (t *ContextPressureTracker) ObserveSuccess(req provider.Request, history []session.Message, usage provider.Usage) (contextpolicy.ContextPressure, PressureAnchorState) {
	if t == nil {
		normalized := usage.Normalized()
		return contextpolicy.PressureFromNativeUsage(nativeUsageForPressure(normalized), req.ContextPolicy), PressureAnchorState{}
	}
	normalized := usage.Normalized()
	pressure := contextpolicy.PressureFromNativeUsage(nativeUsageForPressure(normalized), req.ContextPolicy)
	anchor := PressureAnchorState{}
	if normalized.Available {
		anchor = pressureAnchorForRequest(req, history, normalized, pressure)
		if anchor.WindowInputTokens > 0 {
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
	if pressure.CompactionNeeded {
		t.pendingCompaction = true
		t.pendingPressure = pressure
	}
	return pressure, anchor
}

func (t *ContextPressureTracker) Overflow(policy contextpolicy.Policy) contextpolicy.ContextPressure {
	return contextpolicy.PressureFromOverflow(policy)
}

func (t *ContextPressureTracker) ConsumePendingCompaction() (contextpolicy.ContextPressure, bool) {
	if t == nil || !t.pendingCompaction {
		return contextpolicy.ContextPressure{}, false
	}
	pressure := t.pendingPressure
	t.pendingCompaction = false
	t.pendingPressure = contextpolicy.ContextPressure{}
	return pressure, true
}

// A calibration belongs to the canonical request prefix, not transient Engine
// message identifiers, which the durable harness replaces when committing output.
func pressureAnchorInvalidReason(anchor PressureAnchorState, previous cache.ProviderRequestRecord, req provider.Request, history []session.Message) string {
	if anchor.WindowInputTokens <= 0 || previous.ID == "" || previous.ID != anchor.RequestID {
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
	if !cache.MatchesCanonicalHistoryPrefix(previous, history) {
		return "history_changed"
	}
	if previous.RequestEstimate.EstimatedInputTokens <= 0 || req.RequestEstimate.EstimatedInputTokens < previous.RequestEstimate.EstimatedInputTokens {
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
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].ID == anchor.RequestID {
			tracker.SetAnchor(anchor, requests[i])
			break
		}
	}
	return nil
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
		PromptScopeID:        req.PromptScopeID,
		ThreadID:             req.ThreadID,
		Provider:             req.Provider,
		Model:                req.Model,
		AdapterVersion:       req.RawPlan.Version,
		RequestID:            requestID(req.RunID, req.Step),
		RunID:                req.RunID,
		LogicalRequestID:     req.LogicalRequestID,
		LastMessageEntryID:   lastEntryID,
		LastMessageIndex:     lastIndex,
		CompactionGeneration: req.RawPlan.CompactionGeneration,
		CompactionWindowID:   req.RawPlan.CompactionWindowID,
		Shape:                requestShapeHashes(req),
		WindowInputTokens:    usage.WindowInputTokens,
		PrefixTokens:         req.RequestEstimate.PrefixTokens,
		MessageTokens:        req.RequestEstimate.MessageTokens,
		ToolDefinitionTokens: req.RequestEstimate.ToolDefinitionTokens,
		ContextWindowTokens:  pressure.ContextWindowTokens,
		EstimateSource:       req.RequestEstimate.Source,
		EstimateMethod:       req.RequestEstimate.Method,
		Confidence:           req.RequestEstimate.Confidence,
		PressureSource:       pressure.Source,
		CreatedAt:            time.Now(),
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
