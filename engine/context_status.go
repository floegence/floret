package engine

import (
	"math"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
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

func compactionStartMetadata(trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage) map[string]any {
	return map[string]any{
		"phase":                  ContextCompactPhaseStart,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}
}

func compactionFailedMetadata(trigger compaction.Trigger, reason compaction.Reason, beforePressure contextpolicy.ContextPressure, usage contextpolicy.Usage) map[string]any {
	return map[string]any{
		"phase":                  ContextCompactPhaseFailed,
		"trigger":                trigger,
		"reason":                 reason,
		"before_pressure":        beforePressure,
		"message_context_before": usage,
		"tokens_before":          usage.InputTokens,
	}
}

func compactionCompleteMetadata(result compaction.Result) map[string]any {
	return map[string]any{
		"phase":                      ContextCompactPhaseComplete,
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
	}
}
