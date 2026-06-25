package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
)

const (
	ProviderUsagePhaseStreamUsage        = "stream_usage"
	ProviderUsagePhaseFinalContextStatus = "final_context_status"

	ContextStatusStable        = "stable"
	ContextStatusNearThreshold = "near_threshold"
	ContextStatusWillCompact   = "will_compact"
	ContextStatusHardLimit     = "hard_limit"
	ContextStatusEstimated     = "estimated"

	ContextCompactPhaseStart    = "start"
	ContextCompactPhaseComplete = "complete"
	ContextCompactPhaseFailed   = "failed"

	ContextCompactDebugStageBegin                   = "begin"
	ContextCompactDebugStagePreflight               = "preflight"
	ContextCompactDebugStageGenerateAttemptStart    = "generate_attempt_start"
	ContextCompactDebugStageGenerateAttemptComplete = "generate_attempt_complete"
	ContextCompactDebugStageRequestRebuildStart     = "request_rebuild_start"
	ContextCompactDebugStageRequestRebuildComplete  = "request_rebuild_complete"
	ContextCompactDebugStageRequestValidation       = "request_validation"
	ContextCompactDebugStageInstallStart            = "install_start"
	ContextCompactDebugStageInstallComplete         = "install_complete"

	ContextCompactDebugStatusRunning  = "running"
	ContextCompactDebugStatusOK       = "ok"
	ContextCompactDebugStatusRetrying = "retrying"
	ContextCompactDebugStatusFailed   = "failed"

	ContextCompactDebugNextActionProviderRequest        = "provider_request"
	ContextCompactDebugNextActionReturnCompactedContext = "return_compacted_context"
)

type ProviderUsageContextStatus struct {
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

func compactionOperationID(runID string, step int, trigger compaction.Trigger, reason compaction.Reason, manual ManualCompactionRequest) string {
	if requestID := strings.TrimSpace(manual.RequestID); requestID != "" {
		return fmt.Sprintf("%s:compact:%d:%s:%s:%s", runID, step, trigger, reason, requestID)
	}
	return fmt.Sprintf("%s:compact:%d:%s:%s", runID, step, trigger, reason)
}

func compactionStartMetadata(operationID string, trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage, manual ManualCompactionRequest) map[string]any {
	return withManualCompactionMetadata(map[string]any{
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
	return withManualCompactionMetadata(map[string]any{
		"phase":                  ContextCompactPhaseFailed,
		"operation_id":           operationID,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}, manual)
}

func compactionCompleteMetadata(operationID string, result compaction.Result, validation compactedRequestValidation, manual ManualCompactionRequest) map[string]any {
	return withManualCompactionMetadata(map[string]any{
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
	return withManualCompactionMetadata(map[string]any{
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

func withManualCompactionMetadata(meta map[string]any, manual ManualCompactionRequest) map[string]any {
	if requestID := strings.TrimSpace(manual.RequestID); requestID != "" {
		meta["request_id"] = requestID
	}
	if source := strings.TrimSpace(manual.Source); source != "" {
		meta["source"] = source
	}
	return meta
}
