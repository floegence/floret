package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/contextpolicy"
)

var ErrContextWouldOverflow = errors.New("provider request would exceed context window")

type RequestShapeHashes = cache.RequestShapeHashes
type PressureAnchorState = cache.PressureAnchorState

type ContextPressureTracker struct {
	sessionID         string
	anchor            PressureAnchorState
	pendingCompaction bool
	pendingPressure   contextpolicy.ContextPressure
}

func NewContextPressureTracker(sessionID string) *ContextPressureTracker {
	return &ContextPressureTracker{sessionID: sessionID}
}

func (t *ContextPressureTracker) SetAnchor(anchor PressureAnchorState) {
	if t == nil || anchor.WindowInputTokens <= 0 {
		return
	}
	t.anchor = anchor
}

func (t *ContextPressureTracker) Project(req provider.Request, history []session.Message) contextpolicy.ContextPressure {
	if t == nil {
		return contextpolicy.PressureFromProjectedRequest(req.RequestEstimate, contextpolicy.RequestDeltaEstimate{}, req.ContextPolicy)
	}
	estimate := req.RequestEstimate.Normalized(req.ContextPolicy)
	if validPressureAnchor(t.anchor, req, history) {
		delta := contextpolicy.RequestDeltaEstimate{
			MessageDeltaTokens:        estimate.MessageTokens - t.anchor.MessageTokens,
			PrefixDeltaTokens:         estimate.PrefixTokens - t.anchor.PrefixTokens,
			ToolDefinitionDeltaTokens: estimate.ToolDefinitionTokens - t.anchor.ToolDefinitionTokens,
			Source:                    estimate.Source,
			Method:                    estimate.Method,
			Confidence:                estimate.Confidence,
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
			t.anchor = anchor
		}
	} else {
		pressure = contextpolicy.PressureFromMissingNativeUsage(req.RequestEstimate, req.ContextPolicy)
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

func validPressureAnchor(anchor PressureAnchorState, req provider.Request, history []session.Message) bool {
	if anchor.WindowInputTokens <= 0 {
		return false
	}
	if anchor.SessionID != "" && anchor.SessionID != req.SessionID {
		return false
	}
	if anchor.Provider != "" && anchor.Provider != req.Provider {
		return false
	}
	if anchor.Model != "" && anchor.Model != req.Model {
		return false
	}
	if anchor.AdapterVersion != "" && anchor.AdapterVersion != req.RawPlan.Version {
		return false
	}
	if anchor.CompactionGeneration != req.RawPlan.CompactionGeneration || anchor.CompactionWindowID != req.RawPlan.CompactionWindowID {
		return false
	}
	if anchor.Shape.CacheShapeHash != req.Cache.Namespace {
		return false
	}
	if anchor.EstimateSource != "" && anchor.EstimateSource != req.RequestEstimate.Source {
		return false
	}
	if anchor.EstimateMethod != "" && anchor.EstimateMethod != req.RequestEstimate.Method {
		return false
	}
	if anchor.LastMessageEntryID == "" {
		return false
	}
	if anchor.LastMessageIndex < 0 || anchor.LastMessageIndex >= len(history) {
		return false
	}
	return history[anchor.LastMessageIndex].EntryID == anchor.LastMessageEntryID
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
		SessionID:            req.SessionID,
		ThreadID:             req.SessionID,
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
