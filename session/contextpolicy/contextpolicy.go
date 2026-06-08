package contextpolicy

import (
	"strings"

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
	DefaultEstimatorSource                    = "chars_per_token"
)

type Policy struct {
	ContextWindowTokens    int64  `json:"context_window_tokens,omitempty"`
	MaxOutputTokens        int64  `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens   int64  `json:"reserved_output_tokens,omitempty"`
	ReservedSummaryTokens  int64  `json:"reserved_summary_tokens,omitempty"`
	RecentTailTokens       int64  `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens       int64  `json:"recent_user_tokens,omitempty"`
	EstimatorSource        string `json:"estimator_source,omitempty"`
	MaxCompactionFailures  int    `json:"max_compaction_failures,omitempty"`
	MicrocompactToolTokens int64  `json:"microcompact_tool_tokens,omitempty"`
}

type Usage struct {
	ActiveTokens      int64  `json:"active_tokens,omitempty"`
	HistoryTokens     int64  `json:"history_tokens,omitempty"`
	PrefixTokens      int64  `json:"prefix_tokens,omitempty"`
	ToolTokens        int64  `json:"tool_tokens,omitempty"`
	CacheReadTokens   int64  `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens  int64  `json:"cache_write_tokens,omitempty"`
	InputTokens       int64  `json:"input_tokens,omitempty"`
	OutputTokens      int64  `json:"output_tokens,omitempty"`
	TotalTokens       int64  `json:"total_tokens,omitempty"`
	ContextWindow     int64  `json:"context_window,omitempty"`
	ThresholdTokens   int64  `json:"threshold_tokens,omitempty"`
	MaxOutputTokens   int64  `json:"max_output_tokens,omitempty"`
	ReservedOutput    int64  `json:"reserved_output,omitempty"`
	ReservedSummary   int64  `json:"reserved_summary,omitempty"`
	OutputHeadroom    int64  `json:"output_headroom_tokens,omitempty"`
	AutoCompactRatio  int64  `json:"auto_compact_ratio_pct,omitempty"`
	RecentTailTokens  int64  `json:"recent_tail_tokens,omitempty"`
	RecentUserTokens  int64  `json:"recent_user_tokens,omitempty"`
	EstimatorSource   string `json:"estimator_source,omitempty"`
	CompactionNeeded  bool   `json:"compaction_needed,omitempty"`
	TokenPressureHigh bool   `json:"token_pressure_high,omitempty"`
}

func HasValues(policy Policy) bool {
	return policy.ContextWindowTokens > 0 ||
		policy.MaxOutputTokens > 0 ||
		policy.ReservedOutputTokens > 0 ||
		policy.ReservedSummaryTokens > 0 ||
		policy.RecentTailTokens > 0 ||
		policy.RecentUserTokens > 0 ||
		policy.EstimatorSource != "" ||
		policy.MaxCompactionFailures > 0 ||
		policy.MicrocompactToolTokens > 0
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
	if policy.MicrocompactToolTokens <= 0 {
		policy.MicrocompactToolTokens = defaults.MicrocompactToolTokens
		if policy.MicrocompactToolTokens <= 0 {
			policy.MicrocompactToolTokens = 4096
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
	if policy.MicrocompactToolTokens <= 0 {
		policy.MicrocompactToolTokens = 4096
	}
	return policy
}

func Threshold(policy Policy) int64 {
	policy = Normalize(policy)
	threshold := min64(
		ratioLimit(policy),
		min64(requestSafeLimit(policy), summarySafeLimit(policy)),
	)
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

func summarySafeLimit(policy Policy) int64 {
	return policy.ContextWindowTokens - policy.ReservedOutputTokens - policy.ReservedSummaryTokens
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

func EstimateMessages(systemPrompt string, history []session.Message, toolCount int, policy Policy) Usage {
	policy = Normalize(policy)
	usage := Usage{
		ContextWindow:    policy.ContextWindowTokens,
		ThresholdTokens:  Threshold(policy),
		MaxOutputTokens:  policy.MaxOutputTokens,
		ReservedOutput:   policy.ReservedOutputTokens,
		ReservedSummary:  policy.ReservedSummaryTokens,
		OutputHeadroom:   OutputHeadroom(policy),
		AutoCompactRatio: DefaultAutoCompactRatioPercent,
		RecentTailTokens: policy.RecentTailTokens,
		RecentUserTokens: policy.RecentUserTokens,
		EstimatorSource:  policy.EstimatorSource,
	}
	if systemPrompt != "" {
		usage.PrefixTokens += EstimateText(systemPrompt)
	}
	for _, msg := range history {
		tokens := EstimateMessage(msg)
		usage.HistoryTokens += tokens
		usage.ActiveTokens += tokens
	}
	if toolCount > 0 {
		usage.ToolTokens = int64(toolCount) * 96
	}
	usage.InputTokens = usage.PrefixTokens + usage.ActiveTokens + usage.ToolTokens
	usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	usage.CompactionNeeded = usage.InputTokens >= usage.ThresholdTokens
	usage.TokenPressureHigh = usage.InputTokens+usage.OutputHeadroom >= policy.ContextWindowTokens
	return usage
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
	runes := int64(len([]rune(value)))
	tokens := runes / 4
	if runes%4 != 0 {
		tokens++
	}
	if tokens < 1 {
		return 1
	}
	return tokens
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
