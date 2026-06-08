package contextpolicy

import (
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/session"
)

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
)

type EstimateConfidence string

const (
	EstimateExact        EstimateConfidence = "exact"
	EstimateApproximate  EstimateConfidence = "approximate"
	EstimateConservative EstimateConfidence = "conservative"
)

type Estimate struct {
	PrefixTokens  int64              `json:"prefix_tokens,omitempty"`
	HistoryTokens int64              `json:"history_tokens,omitempty"`
	ToolTokens    int64              `json:"tool_tokens,omitempty"`
	InputTokens   int64              `json:"input_tokens,omitempty"`
	Source        string             `json:"source,omitempty"`
	Confidence    EstimateConfidence `json:"confidence,omitempty"`
}

type Policy struct {
	ContextWindowTokens   int64  `json:"context_window_tokens,omitempty"`
	MaxOutputTokens       int64  `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens  int64  `json:"reserved_output_tokens,omitempty"`
	ReservedSummaryTokens int64  `json:"reserved_summary_tokens,omitempty"`
	RecentTailTokens      int64  `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens      int64  `json:"recent_user_tokens,omitempty"`
	EstimatorSource       string `json:"estimator_source,omitempty"`
	MaxCompactionFailures int    `json:"max_compaction_failures,omitempty"`
}

type Usage struct {
	ActiveTokens        int64  `json:"active_tokens,omitempty"`
	HistoryTokens       int64  `json:"history_tokens,omitempty"`
	PrefixTokens        int64  `json:"prefix_tokens,omitempty"`
	ToolTokens          int64  `json:"tool_tokens,omitempty"`
	CacheReadTokens     int64  `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens    int64  `json:"cache_write_tokens,omitempty"`
	InputTokens         int64  `json:"input_tokens,omitempty"`
	OutputTokens        int64  `json:"output_tokens,omitempty"`
	TotalTokens         int64  `json:"total_tokens,omitempty"`
	ContextWindow       int64  `json:"context_window,omitempty"`
	ThresholdTokens     int64  `json:"threshold_tokens,omitempty"`
	RatioLimitTokens    int64  `json:"ratio_limit_tokens,omitempty"`
	RequestSafeLimit    int64  `json:"request_safe_limit_tokens,omitempty"`
	MaxOutputTokens     int64  `json:"max_output_tokens,omitempty"`
	ReservedOutput      int64  `json:"reserved_output,omitempty"`
	ReservedSummary     int64  `json:"reserved_summary,omitempty"`
	OutputHeadroom      int64  `json:"output_headroom_tokens,omitempty"`
	AutoCompactRatio    int64  `json:"auto_compact_ratio_pct,omitempty"`
	RecentTailTokens    int64  `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens    int64  `json:"recent_user_tokens,omitempty"`
	EstimatorSource     string `json:"estimator_source,omitempty"`
	EstimatorConfidence string `json:"estimator_confidence,omitempty"`
	CompactionNeeded    bool   `json:"compaction_needed,omitempty"`
	TokenPressureHigh   bool   `json:"token_pressure_high,omitempty"`
}

func HasValues(policy Policy) bool {
	return policy.ContextWindowTokens > 0 ||
		policy.MaxOutputTokens > 0 ||
		policy.ReservedOutputTokens > 0 ||
		policy.ReservedSummaryTokens > 0 ||
		policy.RecentTailTokens > 0 ||
		policy.RecentUserTokens > 0 ||
		policy.EstimatorSource != "" ||
		policy.MaxCompactionFailures > 0
}

func MergeDefaults(policy, defaults Policy) Policy {
	provided := HasValues(policy)
	if policy.ContextWindowTokens <= 0 {
		policy.ContextWindowTokens = defaults.ContextWindowTokens
		if policy.ContextWindowTokens <= 0 {
			policy.ContextWindowTokens = DefaultContextWindowTokens
		}
	}
	if policy.MaxOutputTokens <= 0 && !provided {
		policy.MaxOutputTokens = defaults.MaxOutputTokens
	}
	if policy.ReservedOutputTokens <= 0 {
		policy.ReservedOutputTokens = defaults.ReservedOutputTokens
		if policy.ReservedOutputTokens <= 0 {
			policy.ReservedOutputTokens = DefaultReservedOutputTokens
		}
		if policy.MaxOutputTokens > 0 {
			policy.ReservedOutputTokens = min64(policy.MaxOutputTokens, policy.ReservedOutputTokens)
		}
	}
	if policy.ReservedSummaryTokens <= 0 {
		policy.ReservedSummaryTokens = defaultWindowBudget(defaults.ReservedSummaryTokens, DefaultReservedSummaryTokens, policy.ContextWindowTokens)
	}
	if policy.RecentTailTokens <= 0 {
		policy.RecentTailTokens = defaultWindowBudget(defaults.RecentTailTokens, DefaultRecentTailTokens, policy.ContextWindowTokens)
	}
	if policy.RecentUserTokens <= 0 {
		policy.RecentUserTokens = defaultWindowBudget(defaults.RecentUserTokens, DefaultRecentUserTokens, policy.ContextWindowTokens)
	}
	if policy.EstimatorSource == "" {
		policy.EstimatorSource = defaults.EstimatorSource
		if policy.EstimatorSource == "" {
			policy.EstimatorSource = DefaultEstimatorSource
		}
	}
	if policy.MaxCompactionFailures <= 0 {
		policy.MaxCompactionFailures = defaults.MaxCompactionFailures
		if policy.MaxCompactionFailures <= 0 {
			policy.MaxCompactionFailures = 2
		}
	}
	return policy
}

func Normalize(policy Policy) Policy {
	if policy.ContextWindowTokens <= 0 {
		policy.ContextWindowTokens = DefaultContextWindowTokens
	}
	if policy.ReservedOutputTokens <= 0 {
		policy.ReservedOutputTokens = DefaultReservedOutputTokens
		if policy.MaxOutputTokens > 0 {
			policy.ReservedOutputTokens = min64(policy.MaxOutputTokens, DefaultReservedOutputTokens)
		}
	}
	if policy.ReservedSummaryTokens <= 0 {
		policy.ReservedSummaryTokens = defaultWindowBudget(DefaultReservedSummaryTokens, DefaultReservedSummaryTokens, policy.ContextWindowTokens)
	}
	if policy.RecentTailTokens <= 0 {
		policy.RecentTailTokens = defaultWindowBudget(DefaultRecentTailTokens, DefaultRecentTailTokens, policy.ContextWindowTokens)
	}
	if policy.RecentUserTokens <= 0 {
		policy.RecentUserTokens = defaultWindowBudget(DefaultRecentUserTokens, DefaultRecentUserTokens, policy.ContextWindowTokens)
	}
	if policy.EstimatorSource == "" {
		policy.EstimatorSource = DefaultEstimatorSource
	}
	if policy.MaxCompactionFailures <= 0 {
		policy.MaxCompactionFailures = 2
	}
	return policy
}

func Threshold(policy Policy) int64 {
	policy = Normalize(policy)
	threshold := min64(ratioLimit(policy), requestSafeLimit(policy))
	if threshold < 1 {
		threshold = 1
	}
	return threshold
}

func OutputHeadroom(policy Policy) int64 {
	policy = Normalize(policy)
	if policy.MaxOutputTokens > 0 {
		return policy.MaxOutputTokens
	}
	return policy.ReservedOutputTokens
}

func ratioLimit(policy Policy) int64 {
	return policy.ContextWindowTokens * DefaultAutoCompactRatioPercent / 100
}

func requestSafeLimit(policy Policy) int64 {
	return policy.ContextWindowTokens - OutputHeadroom(policy)
}

func defaultWindowBudget(value, fallback, contextWindow int64) int64 {
	if value <= 0 {
		value = fallback
	}
	if contextWindow <= 0 {
		return value
	}
	limit := contextWindow / 4
	if limit < 1 {
		limit = 1
	}
	return min64(value, limit)
}

func UsageFromEstimate(estimate Estimate, policy Policy) Usage {
	policy = Normalize(policy)
	ratioLimitTokens := ratioLimit(policy)
	requestSafeLimitTokens := requestSafeLimit(policy)
	thresholdTokens := min64(ratioLimitTokens, requestSafeLimitTokens)
	if thresholdTokens < 1 {
		thresholdTokens = 1
	}
	if estimate.InputTokens <= 0 {
		estimate.InputTokens = estimate.PrefixTokens + estimate.HistoryTokens + estimate.ToolTokens
	}
	source := estimate.Source
	if source == "" {
		source = policy.EstimatorSource
	}
	confidence := estimate.Confidence
	if confidence == "" {
		confidence = EstimateConservative
	}
	usage := Usage{
		ActiveTokens:        estimate.HistoryTokens,
		HistoryTokens:       estimate.HistoryTokens,
		PrefixTokens:        estimate.PrefixTokens,
		ToolTokens:          estimate.ToolTokens,
		InputTokens:         estimate.InputTokens,
		TotalTokens:         estimate.InputTokens,
		ContextWindow:       policy.ContextWindowTokens,
		ThresholdTokens:     thresholdTokens,
		RatioLimitTokens:    ratioLimitTokens,
		RequestSafeLimit:    requestSafeLimitTokens,
		MaxOutputTokens:     policy.MaxOutputTokens,
		ReservedOutput:      policy.ReservedOutputTokens,
		ReservedSummary:     policy.ReservedSummaryTokens,
		OutputHeadroom:      OutputHeadroom(policy),
		AutoCompactRatio:    DefaultAutoCompactRatioPercent,
		RecentTailTokens:    policy.RecentTailTokens,
		RecentUserTokens:    policy.RecentUserTokens,
		EstimatorSource:     source,
		EstimatorConfidence: string(confidence),
	}
	usage.CompactionNeeded = usage.InputTokens >= usage.ThresholdTokens
	usage.TokenPressureHigh = usage.InputTokens >= usage.RequestSafeLimit
	return usage
}

func EstimateMessages(systemPrompt string, history []session.Message, policy Policy) Usage {
	estimate := Estimate{
		Source:     DefaultEstimatorSource,
		Confidence: EstimateConservative,
	}
	if systemPrompt != "" {
		estimate.PrefixTokens += EstimateText(systemPrompt)
	}
	for _, msg := range history {
		tokens := EstimateMessage(msg)
		estimate.HistoryTokens += tokens
	}
	estimate.InputTokens = estimate.PrefixTokens + estimate.HistoryTokens + estimate.ToolTokens
	return UsageFromEstimate(estimate, policy)
}

func EstimateMessage(msg session.Message) int64 {
	tokens := EstimateText(msg.Content) + EstimateText(msg.ToolName) + EstimateText(msg.ToolArgs) + EstimateText(msg.ToolCallID) + 8
	if msg.Kind != "" {
		tokens += EstimateText(string(msg.Kind))
	}
	return tokens
}

func EstimateText(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	var ascii, nonASCII int64
	for _, r := range value {
		if r < utf8.RuneSelf {
			ascii++
		} else {
			nonASCII++
		}
	}
	tokens := ceilDiv(ascii, 3) + nonASCII
	if tokens < 1 {
		return 1
	}
	return tokens
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return (value + divisor - 1) / divisor
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
