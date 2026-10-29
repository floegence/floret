package engine

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/floegence/floret/v7/internal/configbridge"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/observation"
)

const (
	ProviderUsagePhaseStreamUsage        = "stream_usage"
	ProviderUsagePhaseFinalContextStatus = "final_context_status"

	ContextStatusStable        = "stable"
	ContextStatusNearThreshold = "near_threshold"
	ContextStatusWillCompact   = "will_compact"
	ContextStatusHardLimit     = "hard_limit"
	ContextStatusEstimated     = "estimated"

	ContextCompactPhaseStart     = "start"
	ContextCompactPhaseComplete  = "complete"
	ContextCompactPhaseFailed    = "failed"
	ContextCompactPhaseCancelled = "cancelled"
	ContextCompactPhaseNoop      = "noop"

	ContextCompactDebugStageBegin                   = "begin"
	ContextCompactDebugStagePoll                    = "poll"
	ContextCompactDebugStagePreflight               = "preflight"
	ContextCompactDebugStageGenerateAttemptStart    = "generate_attempt_start"
	ContextCompactDebugStageGenerateAttemptComplete = "generate_attempt_complete"
	ContextCompactDebugStageRequestRebuildStart     = "request_rebuild_start"
	ContextCompactDebugStageRequestRebuildComplete  = "request_rebuild_complete"
	ContextCompactDebugStageRequestValidation       = "request_validation"
	ContextCompactDebugStageInstallStart            = "install_start"
	ContextCompactDebugStageInstallComplete         = "install_complete"

	ContextCompactDebugStatusRunning   = "running"
	ContextCompactDebugStatusOK        = "ok"
	ContextCompactDebugStatusRetrying  = "retrying"
	ContextCompactDebugStatusFailed    = "failed"
	ContextCompactDebugStatusCancelled = "cancelled"

	ContextCompactDebugNextActionProviderRequest        = "provider_request"
	ContextCompactDebugNextActionReturnCompactedContext = "return_compacted_context"
	ContextCompactDebugNextActionFailTurn               = "fail_turn"
)

type ProviderUsageContextStatus struct {
	PayloadFingerprint   string                        `json:"payload_fingerprint,omitempty"`
	AttemptID            string                        `json:"attempt_id,omitempty"`
	AttemptEpoch         int                           `json:"attempt_epoch,omitempty"`
	Phase                string                        `json:"phase"`
	RequestID            string                        `json:"request_id,omitempty"`
	LogicalRequestID     string                        `json:"logical_request_id,omitempty"`
	Attempt              int                           `json:"attempt,omitempty"`
	Usage                provider.Usage                `json:"usage"`
	RequestEstimate      contextpolicy.RequestEstimate `json:"request_estimate"`
	ContextPressure      contextpolicy.ContextPressure `json:"context_pressure"`
	UsedRatio            float64                       `json:"used_ratio,omitempty"`
	ThresholdRatio       float64                       `json:"threshold_ratio,omitempty"`
	Status               string                        `json:"status"`
	CompactionGeneration int                           `json:"compaction_generation,omitempty"`
	CompactionWindowID   string                        `json:"compaction_window_id,omitempty"`
}

func providerUsageContextStatus(req provider.Request, usage provider.Usage, pressure contextpolicy.ContextPressure) ProviderUsageContextStatus {
	normalized := usage.Normalized()
	return ProviderUsageContextStatus{
		PayloadFingerprint: req.RawPlan.PayloadHash, AttemptID: req.AttemptID, AttemptEpoch: req.AttemptEpoch,
		Phase:                ProviderUsagePhaseFinalContextStatus,
		RequestID:            requestID(req.RunID, req.Step),
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Usage:                normalized,
		RequestEstimate:      req.RequestEstimate.Normalized(req.ContextPolicy),
		ContextPressure:      pressure,
		UsedRatio:            ContextPressureUsedRatio(pressure),
		ThresholdRatio:       ContextPressureThresholdRatio(pressure),
		Status:               ContextPressureDisplayStatus(pressure),
		CompactionGeneration: req.RawPlan.CompactionGeneration,
		CompactionWindowID:   req.RawPlan.CompactionWindowID,
	}
}

// ProviderUsageContextStatusFromMetadata reads the final provider-usage
// payload from the attempt-scoped event metadata emitted by Engine.
func ProviderUsageContextStatusFromMetadata(metadata any) (ProviderUsageContextStatus, bool) {
	values, ok := metadata.(map[string]any)
	if !ok {
		return ProviderUsageContextStatus{}, false
	}
	status, ok := values["details"].(ProviderUsageContextStatus)
	if !ok || status.Phase != ProviderUsagePhaseFinalContextStatus {
		return ProviderUsageContextStatus{}, false
	}
	logicalRequestID, logicalOK := values["logical_request_id"].(string)
	attemptID, attemptIDOK := values["attempt_id"].(string)
	attemptEpoch, attemptEpochOK := values["attempt_epoch"].(int)
	attempt, attemptOK := values["attempt"].(int)
	if !logicalOK || !attemptIDOK || !attemptEpochOK || !attemptOK ||
		strings.TrimSpace(logicalRequestID) == "" || strings.TrimSpace(attemptID) == "" || attemptEpoch <= 0 || attempt <= 0 ||
		status.LogicalRequestID != logicalRequestID || status.Attempt != attempt ||
		(status.AttemptID != "" && status.AttemptID != attemptID) || (status.AttemptEpoch != 0 && status.AttemptEpoch != attemptEpoch) {
		return ProviderUsageContextStatus{}, false
	}
	return status, true
}

func streamUsageMetadata() map[string]any {
	return map[string]any{"phase": ProviderUsagePhaseStreamUsage}
}

func ContextPressureDisplayStatus(pressure contextpolicy.ContextPressure) string {
	if pressure.HardLimitExceeded {
		return ContextStatusHardLimit
	}
	if pressure.CompactionNeeded {
		return ContextStatusWillCompact
	}
	if pressure.Source == contextpolicy.PressureSourceMissingNativeUsage {
		return ContextStatusEstimated
	}
	used := ContextPressureUsedRatio(pressure)
	threshold := ContextPressureThresholdRatio(pressure)
	if used > 0 && threshold > 0 && used >= threshold*0.9 {
		return ContextStatusNearThreshold
	}
	return ContextStatusStable
}

func ContextPressureUsedRatio(pressure contextpolicy.ContextPressure) float64 {
	if pressure.ContextWindowTokens <= 0 {
		return 0
	}
	used := pressure.WindowInputTokens
	if used <= 0 {
		used = pressure.ProjectedInputTokens
	}
	return cleanRatio(float64(used) / float64(pressure.ContextWindowTokens))
}

func ContextPressureThresholdRatio(pressure contextpolicy.ContextPressure) float64 {
	if pressure.ContextWindowTokens <= 0 || pressure.ThresholdTokens <= 0 {
		return 0
	}
	return cleanRatio(float64(pressure.ThresholdTokens) / float64(pressure.ContextWindowTokens))
}

func cleanRatio(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	return value
}

// CompactionOperationID returns the engine identity used to correlate one
// logical compaction lifecycle across start, debug, complete, and failed events.
func CompactionOperationID(runID string, step int, trigger compaction.Trigger, reason compaction.Reason, requestID string) string {
	runID = strings.TrimSpace(runID)
	requestID = strings.TrimSpace(requestID)
	if requestID != "" {
		return fmt.Sprintf("%s:compact:%d:%s:%s:%s", runID, step, trigger, reason, requestID)
	}
	return fmt.Sprintf("%s:compact:%d:%s:%s", runID, step, trigger, reason)
}

func compactionOperationID(runID string, step int, trigger compaction.Trigger, reason compaction.Reason, manual ManualCompactionRequest) string {
	return CompactionOperationID(runID, step, trigger, reason, manual.RequestID)
}

func compactionLifecycleRequest(operationID string, trigger compaction.Trigger, request ManualCompactionRequest) ManualCompactionRequest {
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.Source = strings.TrimSpace(request.Source)
	if request.RequestID == "" {
		request.RequestID = operationID + ":request"
	}
	if request.Source == "" && trigger != compaction.TriggerManual {
		request.Source = "engine"
	}
	return request
}

func compactionStartMetadata(operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"phase":                  ContextCompactPhaseStart,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionFailedMetadata(operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"phase":                  ContextCompactPhaseFailed,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionCancelledMetadata(operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"phase":                  ContextCompactPhaseCancelled,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionNoopMetadata(operationID string, trigger compaction.Trigger, noopReason string, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"phase":                  ContextCompactPhaseNoop,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 strings.TrimSpace(noopReason),
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionCompleteMetadata(operationID string, result compaction.Result, validation compactedRequestValidation, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"phase":                      ContextCompactPhaseComplete,
		"operation_id":               operationID,
		"trigger":                    result.Trigger,
		"reason":                     result.Reason,
		"compaction_id":              result.CompactionID,
		"compaction_generation":      result.CompactionGeneration,
		"compaction_window_id":       result.CompactionWindowID,
		"previous_compaction_id":     result.PreviousCompactionID,
		"first_kept_entry_id":        result.FirstKeptEntryID,
		"compacted_through_entry_id": result.CompactedThroughEntryID,
		"summary_schema_version":     result.SummarySchemaVersion,
		"compaction_phase":           result.Phase,
		"tokens_before":              result.TokensBefore,
		"tokens_after_estimate":      result.TokensAfterEstimate,
		"context_before":             result.UsageBefore,
		"context_after":              result.UsageAfter,
		"validated_request_estimate": validation.RequestEstimate,
		"validated_context_pressure": validation.ContextPressure,
		"validated_message_context":  validation.MessageContextUsage,
		"fixed_input_tokens":         validation.FixedInputTokens,
		"reducible_input_tokens":     validation.ReducibleInputTokens,
		"request_safe_limit":         validation.RequestSafeLimit,
	}, manual)
}

func compactionDebugMetadata(operationID string, stage string, status string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withCompactionIdentityMetadata(map[string]any{
		"stage":                  stage,
		"status":                 status,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"context_before":         usage,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionRequestSnapshot(req provider.Request, tracker *ContextPressureTracker) *observation.CompactionRequestSnapshot {
	estimate := configbridge.RequestEstimate(req.RequestEstimate)
	estimate.Characters, estimate.Breakdown.Details = nil, nil
	snapshot := &observation.CompactionRequestSnapshot{PayloadFingerprint: req.RawPlan.PayloadHash,
		Provider: req.Provider, Model: req.Model, Estimate: estimate,
		Pressure: configbridge.PublicContextPressure(req.ContextPressure)}
	if tracker != nil && req.ContextPressure.Source == contextpolicy.PressureSourceUsageAnchoredDelta {
		snapshot.CalibrationRequestID = tracker.anchor.RequestID
		snapshot.CalibrationBoundary = tracker.anchor.MeasurementPrefix.Boundary
		if snapshot.CalibrationBoundary == "" {
			snapshot.CalibrationBoundary = "input"
		}
	}
	if tracker != nil {
		snapshot.CalibrationInvalidReason = tracker.anchorReason
	}
	return snapshot
}

func withCompactionIdentityMetadata(meta map[string]any, manual ManualCompactionRequest) map[string]any {
	if d := manual.decision; d != nil && (d.Before != nil || d.After != nil || d.Capacity != nil) {
		meta["decision"] = d.Clone()
	}
	if requestID := strings.TrimSpace(manual.RequestID); requestID != "" {
		meta["request_id"] = requestID
	}
	if source := strings.TrimSpace(manual.Source); source != "" {
		meta["source"] = source
	}
	return meta
}

func compactionOverflowCause(err error, tracker *ContextPressureTracker, policy contextpolicy.Policy, stage string) (contextpolicy.ContextPressure, compaction.Reason, *observation.CompactionCapacity) {
	if kind, actual, limit := provider.CapacityLimit(err); kind != "" {
		pressure := contextpolicy.PressureFromManual(contextpolicy.Usage{}, policy)
		pressure.Signal, pressure.Source, pressure.Confidence = contextpolicy.PressureSignal(kind), "request_capacity", contextpolicy.EstimateConservative
		capacity := &observation.CompactionCapacity{Kind: kind, Stage: stage, Source: "provider"}
		if kind == "image_count" {
			capacity.ActualImages, capacity.LimitImages = actual, limit
		} else {
			capacity.ActualBytes, capacity.LimitBytes = actual, limit
		}
		return pressure, compaction.Reason(kind), capacity
	}
	var bytes interface{ RequestByteLimit() (int64, int64) }
	if errors.As(err, &bytes) {
		actual, limit := bytes.RequestByteLimit()
		pressure := contextpolicy.PressureFromManual(contextpolicy.Usage{}, policy)
		pressure.Signal = "request_bytes"
		pressure.Source = "request_capacity"
		pressure.Confidence = contextpolicy.EstimateConservative
		return pressure, compaction.Reason("request_bytes"), &observation.CompactionCapacity{Kind: "request_bytes", Stage: "prepare", Source: "host_policy", ActualBytes: actual, LimitBytes: limit}
	}
	var rejected interface{ RequestBodyRejected() bool }
	if errors.As(err, &rejected) && rejected.RequestBodyRejected() {
		pressure := contextpolicy.PressureFromManual(contextpolicy.Usage{}, policy)
		pressure.Signal, pressure.Source, pressure.Confidence = "request_bytes", "request_capacity", contextpolicy.EstimateConservative
		return pressure, compaction.Reason("request_bytes"), &observation.CompactionCapacity{Kind: "request_bytes", Stage: "response", Source: "provider"}
	}
	return tracker.Overflow(policy), compaction.ReasonProviderOverflow, nil
}
