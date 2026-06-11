package observation

import (
	"github.com/floegence/floret/config"
)

func providerUsageFromAny(value any) ProviderUsage {
	switch v := value.(type) {
	case ProviderUsage:
		return v
	case *ProviderUsage:
		if v == nil {
			return ProviderUsage{}
		}
		return *v
	case map[string]any:
		return ProviderUsage{
			InputTokens:       int64FromAny(v["input_tokens"], 0),
			OutputTokens:      int64FromAny(v["output_tokens"], 0),
			ReasoningTokens:   int64FromAny(v["reasoning_tokens"], 0),
			CacheReadTokens:   int64FromAny(v["cache_read_tokens"], 0),
			CacheWriteTokens:  int64FromAny(v["cache_write_tokens"], 0),
			TotalTokens:       int64FromAny(v["total_tokens"], 0),
			WindowInputTokens: int64FromAny(v["window_input_tokens"], 0),
			CostUSD:           float64FromAny(v["cost_usd"], 0),
			Source:            stringFromAny(v["source"]),
			Available:         boolFromAny(v["available"], false),
		}.Normalized()
	default:
		return ProviderUsage{}
	}
}

func requestEstimateFromAny(value any) config.RequestEstimate {
	switch v := value.(type) {
	case config.RequestEstimate:
		return v
	case *config.RequestEstimate:
		if v == nil {
			return config.RequestEstimate{}
		}
		return *v
	case map[string]any:
		return config.RequestEstimate{
			PrefixTokens:         int64FromAny(v["prefix_tokens"], 0),
			MessageTokens:        int64FromAny(v["message_tokens"], 0),
			ToolDefinitionTokens: int64FromAny(v["tool_definition_tokens"], 0),
			EstimatedInputTokens: int64FromAny(v["estimated_input_tokens"], 0),
			Source:               stringFromAny(v["source"]),
			Method:               config.EstimateMethod(stringFromAny(v["method"])),
			Confidence:           config.EstimateConfidence(stringFromAny(v["confidence"])),
		}
	default:
		return config.RequestEstimate{}
	}
}

func contextPressureFromAny(value any) (config.ContextPressure, bool) {
	switch v := value.(type) {
	case config.ContextPressure:
		return v, hasContextPressure(v)
	case *config.ContextPressure:
		if v == nil {
			return config.ContextPressure{}, false
		}
		return *v, hasContextPressure(*v)
	case map[string]any:
		out := config.ContextPressure{
			WindowInputTokens:    int64FromAny(v["window_input_tokens"], 0),
			ProjectedInputTokens: int64FromAny(v["projected_input_tokens"], 0),
			ContextWindowTokens:  int64FromAny(v["context_window_tokens"], 0),
			ThresholdTokens:      int64FromAny(v["threshold_tokens"], 0),
			RequestSafeLimit:     int64FromAny(v["request_safe_limit_tokens"], 0),
			OutputHeadroomTokens: int64FromAny(v["output_headroom_tokens"], 0),
			Signal:               config.PressureSignal(stringFromAny(v["pressure_signal"])),
			Source:               config.PressureSource(stringFromAny(v["pressure_source"])),
			EstimateMethod:       config.EstimateMethod(stringFromAny(v["estimate_method"])),
			Confidence:           config.EstimateConfidence(stringFromAny(v["confidence"])),
			CompactionNeeded:     boolFromAny(v["compaction_needed"], false),
			HardLimitExceeded:    boolFromAny(v["hard_limit_exceeded"], false),
		}
		return out, hasContextPressure(out)
	default:
		return config.ContextPressure{}, false
	}
}

func contextUsageFromAny(value any) (config.ContextUsage, bool) {
	switch v := value.(type) {
	case config.ContextUsage:
		return v, hasContextUsage(v)
	case *config.ContextUsage:
		if v == nil {
			return config.ContextUsage{}, false
		}
		return *v, hasContextUsage(*v)
	case map[string]any:
		out := config.ContextUsage{
			PrefixTokens:      int64FromAny(v["prefix_tokens"], 0),
			MessageTokens:     int64FromAny(v["message_tokens"], 0),
			InputTokens:       int64FromAny(v["input_tokens"], 0),
			ContextWindow:     int64FromAny(v["context_window"], 0),
			ThresholdTokens:   int64FromAny(v["threshold_tokens"], 0),
			RatioLimitTokens:  int64FromAny(v["ratio_limit_tokens"], 0),
			RequestSafeLimit:  int64FromAny(v["request_safe_limit_tokens"], 0),
			MaxOutputTokens:   int64FromAny(v["max_output_tokens"], 0),
			ReservedOutput:    int64FromAny(v["reserved_output"], 0),
			ReservedSummary:   int64FromAny(v["reserved_summary"], 0),
			OutputHeadroom:    int64FromAny(v["output_headroom_tokens"], 0),
			AutoCompactRatio:  int64FromAny(v["auto_compact_ratio_pct"], 0),
			RecentTailTokens:  int64FromAny(v["recent_tail_tokens"], 0),
			RecentUserTokens:  int64FromAny(v["recent_user_tokens"], 0),
			Source:            stringFromAny(v["source"]),
			Method:            config.EstimateMethod(stringFromAny(v["method"])),
			Confidence:        config.EstimateConfidence(stringFromAny(v["confidence"])),
			CompactionNeeded:  boolFromAny(v["compaction_needed"], false),
			HardLimitExceeded: boolFromAny(v["hard_limit_exceeded"], false),
		}
		return out, hasContextUsage(out)
	default:
		return config.ContextUsage{}, false
	}
}

func hasContextUsage(usage config.ContextUsage) bool {
	return usage.PrefixTokens > 0 ||
		usage.MessageTokens > 0 ||
		usage.InputTokens > 0 ||
		usage.ContextWindow > 0 ||
		usage.ThresholdTokens > 0 ||
		usage.RequestSafeLimit > 0 ||
		usage.Source != "" ||
		usage.Method != ""
}

func boolFromAny(value any, defaultValue bool) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		switch v {
		case "true":
			return true
		case "false":
			return false
		default:
			return defaultValue
		}
	default:
		return defaultValue
	}
}

func float64FromAny(value any, defaultValue float64) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	default:
		return defaultValue
	}
}
