package config

import "github.com/floegence/floret/internal/session/contextpolicy"

const (
	MinSupportedContextWindowTokens     int64 = 256000
	DefaultContextWindowTokens          int64 = MinSupportedContextWindowTokens
	DefaultMaxOutputTokens              int64 = 0
	DefaultReservedOutputTokens         int64 = 64000
	DefaultAutoCompactRatioPercent      int64 = 90
	DefaultCompactedContextTargetTokens int64 = 50000
	DefaultReservedSummaryTokens        int64 = 20000
	DefaultRecentTailTokens             int64 = 12000
	DefaultRecentUserTokens             int64 = 15000
	DefaultCheckpointOverheadTokens     int64 = 2000
	DefaultEstimatorSource                    = "generic_conservative"
	DefaultEstimatorMethod                    = EstimateMethodMessageContext
)

type EstimateConfidence string

const (
	EstimateExact        EstimateConfidence = "exact"
	EstimateApproximate  EstimateConfidence = "approximate"
	EstimateConservative EstimateConfidence = "conservative"
)

type EstimateMethod string

const (
	EstimateMethodGenericPayload          EstimateMethod = "generic_payload_estimate"
	EstimateMethodProviderRenderedPayload EstimateMethod = "provider_rendered_payload_estimate"
	EstimateMethodMessageContext          EstimateMethod = "message_context_estimate"
	EstimateMethodOfficialPreflightCount  EstimateMethod = "official_preflight_count"
	EstimateMethodUnknown                 EstimateMethod = "unknown_estimate_method"
)

type ContextPolicy struct {
	ContextWindowTokens          int64          `json:"context_window_tokens,omitempty"`
	MaxOutputTokens              int64          `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens         int64          `json:"reserved_output_tokens,omitempty"`
	ReservedSummaryTokens        int64          `json:"reserved_summary_tokens,omitempty"`
	RecentTailTokens             int64          `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens             int64          `json:"recent_user_tokens,omitempty"`
	CompactedContextTargetTokens int64          `json:"compacted_context_target_tokens,omitempty"`
	EstimatorSource              string         `json:"estimator_source,omitempty"`
	EstimatorMethod              EstimateMethod `json:"estimator_method,omitempty"`
	MaxCompactionFailures        int            `json:"max_compaction_failures,omitempty"`
}

type RequestEstimate struct {
	PrefixTokens         int64              `json:"prefix_tokens,omitempty"`
	MessageTokens        int64              `json:"message_tokens,omitempty"`
	ToolDefinitionTokens int64              `json:"tool_definition_tokens,omitempty"`
	EstimatedInputTokens int64              `json:"estimated_input_tokens,omitempty"`
	Source               string             `json:"source,omitempty"`
	Method               EstimateMethod     `json:"method,omitempty"`
	Confidence           EstimateConfidence `json:"confidence,omitempty"`
}

type PressureSignal string

const (
	PressureSignalNativeUsage PressureSignal = "native_usage"
	PressureSignalProjected   PressureSignal = "projected_request"
	PressureSignalOverflow    PressureSignal = "provider_overflow"
	PressureSignalManual      PressureSignal = "manual"
)

type PressureSource string

const (
	PressureSourceProviderUsage       PressureSource = "provider_usage"
	PressureSourceUsageAnchoredDelta  PressureSource = "usage_anchored_delta"
	PressureSourceFullRequestEstimate PressureSource = "full_request_estimate"
	PressureSourceMissingNativeUsage  PressureSource = "missing_native_usage_request_estimate"
	PressureSourceManual              PressureSource = "manual"
)

type ContextPressure struct {
	WindowInputTokens    int64              `json:"window_input_tokens,omitempty"`
	ProjectedInputTokens int64              `json:"projected_input_tokens,omitempty"`
	ContextWindowTokens  int64              `json:"context_window_tokens,omitempty"`
	ThresholdTokens      int64              `json:"threshold_tokens,omitempty"`
	RequestSafeLimit     int64              `json:"request_safe_limit_tokens,omitempty"`
	OutputHeadroomTokens int64              `json:"output_headroom_tokens,omitempty"`
	Signal               PressureSignal     `json:"pressure_signal,omitempty"`
	Source               PressureSource     `json:"pressure_source,omitempty"`
	EstimateMethod       EstimateMethod     `json:"estimate_method,omitempty"`
	Confidence           EstimateConfidence `json:"confidence,omitempty"`
	CompactionNeeded     bool               `json:"compaction_needed,omitempty"`
	HardLimitExceeded    bool               `json:"hard_limit_exceeded,omitempty"`
}

type ContextUsage struct {
	PrefixTokens      int64              `json:"prefix_tokens,omitempty"`
	MessageTokens     int64              `json:"message_tokens,omitempty"`
	InputTokens       int64              `json:"input_tokens,omitempty"`
	ContextWindow     int64              `json:"context_window,omitempty"`
	ThresholdTokens   int64              `json:"threshold_tokens,omitempty"`
	RatioLimitTokens  int64              `json:"ratio_limit_tokens,omitempty"`
	RequestSafeLimit  int64              `json:"request_safe_limit_tokens,omitempty"`
	MaxOutputTokens   int64              `json:"max_output_tokens,omitempty"`
	ReservedOutput    int64              `json:"reserved_output,omitempty"`
	ReservedSummary   int64              `json:"reserved_summary,omitempty"`
	OutputHeadroom    int64              `json:"output_headroom_tokens,omitempty"`
	AutoCompactRatio  int64              `json:"auto_compact_ratio_pct,omitempty"`
	RecentTailTokens  int64              `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens  int64              `json:"recent_user_tokens,omitempty"`
	Source            string             `json:"source,omitempty"`
	Method            EstimateMethod     `json:"method,omitempty"`
	Confidence        EstimateConfidence `json:"confidence,omitempty"`
	CompactionNeeded  bool               `json:"compaction_needed,omitempty"`
	HardLimitExceeded bool               `json:"hard_limit_exceeded,omitempty"`
}

func (p ContextPolicy) internal() contextpolicy.Policy {
	return contextpolicy.Policy{
		ContextWindowTokens:          p.ContextWindowTokens,
		MaxOutputTokens:              p.MaxOutputTokens,
		ReservedOutputTokens:         p.ReservedOutputTokens,
		ReservedSummaryTokens:        p.ReservedSummaryTokens,
		RecentTailTokens:             p.RecentTailTokens,
		RecentUserTokens:             p.RecentUserTokens,
		CompactedContextTargetTokens: p.CompactedContextTargetTokens,
		EstimatorSource:              p.EstimatorSource,
		EstimatorMethod:              contextpolicy.EstimateMethod(p.EstimatorMethod),
		MaxCompactionFailures:        p.MaxCompactionFailures,
	}
}

func contextPolicyFromInternal(p contextpolicy.Policy) ContextPolicy {
	return ContextPolicy{
		ContextWindowTokens:          p.ContextWindowTokens,
		MaxOutputTokens:              p.MaxOutputTokens,
		ReservedOutputTokens:         p.ReservedOutputTokens,
		ReservedSummaryTokens:        p.ReservedSummaryTokens,
		RecentTailTokens:             p.RecentTailTokens,
		RecentUserTokens:             p.RecentUserTokens,
		CompactedContextTargetTokens: p.CompactedContextTargetTokens,
		EstimatorSource:              p.EstimatorSource,
		EstimatorMethod:              EstimateMethod(p.EstimatorMethod),
		MaxCompactionFailures:        p.MaxCompactionFailures,
	}
}
