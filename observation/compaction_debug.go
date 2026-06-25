package observation

import (
	"time"

	"github.com/floegence/floret/config"
)

const (
	EventTypeContextCompactDebug = "context_compact_debug"

	CompactionDebugStageBegin                   = "begin"
	CompactionDebugStagePoll                    = "poll"
	CompactionDebugStagePreflight               = "preflight"
	CompactionDebugStageGenerateAttemptStart    = "generate_attempt_start"
	CompactionDebugStageGenerateAttemptComplete = "generate_attempt_complete"
	CompactionDebugStageRequestRebuildStart     = "request_rebuild_start"
	CompactionDebugStageRequestRebuildComplete  = "request_rebuild_complete"
	CompactionDebugStageRequestValidation       = "request_validation"
	CompactionDebugStageInstallStart            = "install_start"
	CompactionDebugStageInstallComplete         = "install_complete"

	CompactionDebugStatusRunning   = "running"
	CompactionDebugStatusOK        = "ok"
	CompactionDebugStatusRetrying  = "retrying"
	CompactionDebugStatusFailed    = "failed"
	CompactionDebugStatusCancelled = "cancelled"
)

type CompactionDebugEvent struct {
	RunID                            string                 `json:"run_id,omitempty"`
	ThreadID                         string                 `json:"thread_id,omitempty"`
	TurnID                           string                 `json:"turn_id,omitempty"`
	Step                             int                    `json:"step,omitempty"`
	OperationID                      string                 `json:"operation_id,omitempty"`
	RequestID                        string                 `json:"request_id,omitempty"`
	Stage                            string                 `json:"stage"`
	Status                           string                 `json:"status"`
	Trigger                          string                 `json:"trigger,omitempty"`
	Reason                           string                 `json:"reason,omitempty"`
	Source                           string                 `json:"source,omitempty"`
	CompactionConvergenceAttempt     int                    `json:"compaction_convergence_attempt,omitempty"`
	HistoryMessageCount              int                    `json:"history_message_count,omitempty"`
	ActiveMessageCount               int                    `json:"active_message_count,omitempty"`
	CompactionID                     string                 `json:"compaction_id,omitempty"`
	CompactionGeneration             int                    `json:"compaction_generation,omitempty"`
	CompactionWindowID               string                 `json:"compaction_window_id,omitempty"`
	TokensBefore                     int64                  `json:"tokens_before,omitempty"`
	TokensAfterEstimate              int64                  `json:"tokens_after_estimate,omitempty"`
	ContextBefore                    config.ContextUsage    `json:"context_before,omitempty"`
	ContextAfter                     config.ContextUsage    `json:"context_after,omitempty"`
	BeforePressure                   config.ContextPressure `json:"before_pressure,omitempty"`
	RequestEstimate                  config.RequestEstimate `json:"request_estimate,omitempty"`
	ValidatedContextPressure         config.ContextPressure `json:"validated_context_pressure,omitempty"`
	HardLimitExceeded                bool                   `json:"hard_limit_exceeded,omitempty"`
	FixedInputTokens                 int64                  `json:"fixed_input_tokens,omitempty"`
	ReducibleInputTokens             int64                  `json:"reducible_input_tokens,omitempty"`
	RequestSafeLimit                 int64                  `json:"request_safe_limit,omitempty"`
	CompactedContextTargetTokens     int64                  `json:"compacted_context_target_tokens,omitempty"`
	NextCompactedContextTargetTokens int64                  `json:"next_compacted_context_target_tokens,omitempty"`
	ConsecutiveFailures              int                    `json:"consecutive_failures,omitempty"`
	DurationMS                       int64                  `json:"duration_ms,omitempty"`
	ProviderStateKind                string                 `json:"provider_state_kind,omitempty"`
	NextAction                       string                 `json:"next_action,omitempty"`
	Error                            string                 `json:"error,omitempty"`
	ObservedAt                       time.Time              `json:"observed_at"`
}

func CompactionDebugEventFromEvent(ev Event) (CompactionDebugEvent, bool) {
	if ev.CompactionDebug != nil {
		return *ev.CompactionDebug, true
	}
	if ev.Type != EventTypeContextCompactDebug {
		return CompactionDebugEvent{}, false
	}
	meta := ev.Metadata
	stage := stringFromAny(meta["stage"])
	status := stringFromAny(meta["status"])
	if stage == "" || status == "" {
		return CompactionDebugEvent{}, false
	}
	out := CompactionDebugEvent{
		RunID:                            ev.RunID,
		ThreadID:                         ev.ThreadID,
		TurnID:                           ev.TurnID,
		Step:                             ev.Step,
		OperationID:                      stringFromAny(meta["operation_id"]),
		RequestID:                        stringFromAny(meta["request_id"]),
		Stage:                            stage,
		Status:                           status,
		Trigger:                          stringFromAny(meta["trigger"]),
		Reason:                           stringFromAny(meta["reason"]),
		Source:                           stringFromAny(meta["source"]),
		CompactionConvergenceAttempt:     intFromAny(meta["compaction_convergence_attempt"], 0),
		HistoryMessageCount:              intFromAny(meta["history_message_count"], 0),
		ActiveMessageCount:               intFromAny(meta["active_message_count"], 0),
		CompactionID:                     stringFromAny(meta["compaction_id"]),
		CompactionGeneration:             intFromAny(meta["compaction_generation"], 0),
		CompactionWindowID:               stringFromAny(meta["compaction_window_id"]),
		TokensBefore:                     int64FromAny(meta["tokens_before"], 0),
		TokensAfterEstimate:              int64FromAny(meta["tokens_after_estimate"], 0),
		HardLimitExceeded:                boolFromAny(meta["hard_limit_exceeded"], false),
		FixedInputTokens:                 int64FromAny(meta["fixed_input_tokens"], 0),
		ReducibleInputTokens:             int64FromAny(meta["reducible_input_tokens"], 0),
		RequestSafeLimit:                 int64FromAny(meta["request_safe_limit"], 0),
		CompactedContextTargetTokens:     int64FromAny(meta["compacted_context_target_tokens"], 0),
		NextCompactedContextTargetTokens: int64FromAny(meta["next_compacted_context_target_tokens"], 0),
		ConsecutiveFailures:              intFromAny(meta["consecutive_failures"], 0),
		DurationMS:                       int64FromAny(meta["duration_ms"], ev.DurationMS),
		ProviderStateKind:                stringFromAny(meta["provider_state_kind"]),
		NextAction:                       stringFromAny(meta["next_action"]),
		Error:                            ev.Error,
		ObservedAt:                       ev.ObservedAt,
	}
	if usage, ok := contextUsageFromAny(meta["context_before"]); ok {
		out.ContextBefore = usage
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := contextUsageFromAny(meta["message_context_before"]); ok {
		out.ContextBefore = usage
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := contextUsageFromAny(meta["context_after"]); ok {
		out.ContextAfter = usage
	}
	if pressure, ok := contextPressureFromAny(meta["before_pressure"]); ok {
		out.BeforePressure = pressure
	}
	if pressure, ok := contextPressureFromAny(meta["validated_context_pressure"]); ok {
		out.ValidatedContextPressure = pressure
		if out.HardLimitExceeded == false {
			out.HardLimitExceeded = pressure.HardLimitExceeded
		}
	}
	out.RequestEstimate = requestEstimateFromAny(meta["request_estimate"])
	if out.Status != CompactionDebugStatusRunning &&
		out.Status != CompactionDebugStatusOK &&
		out.Status != CompactionDebugStatusRetrying &&
		out.Status != CompactionDebugStatusFailed &&
		out.Status != CompactionDebugStatusCancelled {
		return CompactionDebugEvent{}, false
	}
	return out, true
}

func CompactionDebugEventsFromEvents(events []Event) []CompactionDebugEvent {
	out := []CompactionDebugEvent{}
	for _, ev := range events {
		if debug, ok := CompactionDebugEventFromEvent(ev); ok {
			out = append(out, debug)
		}
	}
	return out
}
