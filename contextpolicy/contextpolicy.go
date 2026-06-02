package contextpolicy

import (
	"strings"

	"github.com/floegence/floret/session"
)

const (
	DefaultContextWindowTokens   int64 = 128000
	DefaultMaxOutputTokens       int64 = 4096
	DefaultReservedOutputTokens  int64 = 4096
	DefaultReservedSummaryTokens int64 = 2048
	DefaultRecentTailTokens      int64 = 12000
	DefaultEstimatorSource             = "chars_per_token"
)

type Policy struct {
	ContextWindowTokens    int64  `json:"context_window_tokens,omitempty"`
	MaxOutputTokens        int64  `json:"max_output_tokens,omitempty"`
	ReservedOutputTokens   int64  `json:"reserved_output_tokens,omitempty"`
	ReservedSummaryTokens  int64  `json:"reserved_summary_tokens,omitempty"`
	RecentTailTokens       int64  `json:"recent_tail_tokens,omitempty"`
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
	ReservedOutput    int64  `json:"reserved_output,omitempty"`
	ReservedSummary   int64  `json:"reserved_summary,omitempty"`
	RecentTailTokens  int64  `json:"recent_tail_tokens,omitempty"`
	EstimatorSource   string `json:"estimator_source,omitempty"`
	CompactionNeeded  bool   `json:"compaction_needed,omitempty"`
	TokenPressureHigh bool   `json:"token_pressure_high,omitempty"`
}

func Normalize(policy Policy) Policy {
	if policy.ContextWindowTokens <= 0 {
		policy.ContextWindowTokens = DefaultContextWindowTokens
	}
	if policy.MaxOutputTokens <= 0 {
		policy.MaxOutputTokens = DefaultMaxOutputTokens
	}
	if policy.ReservedOutputTokens <= 0 {
		policy.ReservedOutputTokens = min64(policy.MaxOutputTokens, DefaultReservedOutputTokens)
	}
	if policy.ReservedSummaryTokens <= 0 {
		policy.ReservedSummaryTokens = DefaultReservedSummaryTokens
	}
	if policy.RecentTailTokens <= 0 {
		policy.RecentTailTokens = DefaultRecentTailTokens
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
	threshold := policy.ContextWindowTokens - policy.ReservedOutputTokens - policy.ReservedSummaryTokens
	if threshold < policy.ContextWindowTokens/2 {
		threshold = policy.ContextWindowTokens / 2
	}
	if threshold < 1 {
		threshold = 1
	}
	return threshold
}

func EstimateMessages(systemPrompt string, history []session.Message, toolCount int, policy Policy) Usage {
	policy = Normalize(policy)
	usage := Usage{
		ContextWindow:    policy.ContextWindowTokens,
		ThresholdTokens:  Threshold(policy),
		ReservedOutput:   policy.ReservedOutputTokens,
		ReservedSummary:  policy.ReservedSummaryTokens,
		RecentTailTokens: policy.RecentTailTokens,
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
	usage.TokenPressureHigh = usage.InputTokens+policy.ReservedOutputTokens >= policy.ContextWindowTokens
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
