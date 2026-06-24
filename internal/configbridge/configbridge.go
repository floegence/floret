package configbridge

import (
	"strings"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session/contextpolicy"
)

func ContextPolicy(policy config.ContextPolicy) contextpolicy.Policy {
	return contextpolicy.Policy{
		ContextWindowTokens:          policy.ContextWindowTokens,
		MaxOutputTokens:              policy.MaxOutputTokens,
		ReservedOutputTokens:         policy.ReservedOutputTokens,
		ReservedSummaryTokens:        policy.ReservedSummaryTokens,
		RecentTailTokens:             policy.RecentTailTokens,
		RecentUserTokens:             policy.RecentUserTokens,
		CompactedContextTargetTokens: policy.CompactedContextTargetTokens,
		EstimatorSource:              policy.EstimatorSource,
		EstimatorMethod:              contextpolicy.EstimateMethod(policy.EstimatorMethod),
		MaxCompactionFailures:        policy.MaxCompactionFailures,
	}
}

func PublicContextPolicy(policy contextpolicy.Policy) config.ContextPolicy {
	return config.ContextPolicy{
		ContextWindowTokens:          policy.ContextWindowTokens,
		MaxOutputTokens:              policy.MaxOutputTokens,
		ReservedOutputTokens:         policy.ReservedOutputTokens,
		ReservedSummaryTokens:        policy.ReservedSummaryTokens,
		RecentTailTokens:             policy.RecentTailTokens,
		RecentUserTokens:             policy.RecentUserTokens,
		CompactedContextTargetTokens: policy.CompactedContextTargetTokens,
		EstimatorSource:              policy.EstimatorSource,
		EstimatorMethod:              config.EstimateMethod(policy.EstimatorMethod),
		MaxCompactionFailures:        policy.MaxCompactionFailures,
	}
}

func NormalizeContextPolicy(policy config.ContextPolicy) config.ContextPolicy {
	return PublicContextPolicy(contextpolicy.Normalize(ContextPolicy(policy)))
}

func ReasoningSelection(selection config.ReasoningSelection) provider.ReasoningSelection {
	selection = config.NormalizeReasoningSelection(selection)
	return provider.ReasoningSelection{
		Level:        provider.ReasoningLevel(selection.Level),
		BudgetTokens: selection.BudgetTokens,
	}
}

func PublicReasoningSelection(selection provider.ReasoningSelection) config.ReasoningSelection {
	selection = provider.NormalizeReasoningSelection(selection)
	return config.ReasoningSelection{
		Level:        config.ReasoningLevel(selection.Level),
		BudgetTokens: selection.BudgetTokens,
	}
}

func RequestEstimate(estimate contextpolicy.RequestEstimate) config.RequestEstimate {
	return config.RequestEstimate{
		PrefixTokens:         estimate.PrefixTokens,
		MessageTokens:        estimate.MessageTokens,
		ToolDefinitionTokens: estimate.ToolDefinitionTokens,
		EstimatedInputTokens: estimate.EstimatedInputTokens,
		Source:               estimate.Source,
		Method:               config.EstimateMethod(estimate.Method),
		Confidence:           config.EstimateConfidence(estimate.Confidence),
	}
}

func PublicContextPressure(pressure contextpolicy.ContextPressure) config.ContextPressure {
	return config.ContextPressure{
		WindowInputTokens:    pressure.WindowInputTokens,
		ProjectedInputTokens: pressure.ProjectedInputTokens,
		ContextWindowTokens:  pressure.ContextWindowTokens,
		ThresholdTokens:      pressure.ThresholdTokens,
		RequestSafeLimit:     pressure.RequestSafeLimit,
		OutputHeadroomTokens: pressure.OutputHeadroomTokens,
		Signal:               config.PressureSignal(pressure.Signal),
		Source:               config.PressureSource(pressure.Source),
		EstimateMethod:       config.EstimateMethod(pressure.EstimateMethod),
		Confidence:           config.EstimateConfidence(pressure.Confidence),
		CompactionNeeded:     pressure.CompactionNeeded,
		HardLimitExceeded:    pressure.HardLimitExceeded,
	}
}

func PublicContextUsage(usage contextpolicy.Usage) config.ContextUsage {
	return config.ContextUsage{
		PrefixTokens:      usage.PrefixTokens,
		MessageTokens:     usage.MessageTokens,
		InputTokens:       usage.InputTokens,
		ContextWindow:     usage.ContextWindow,
		ThresholdTokens:   usage.ThresholdTokens,
		RatioLimitTokens:  usage.RatioLimitTokens,
		RequestSafeLimit:  usage.RequestSafeLimit,
		MaxOutputTokens:   usage.MaxOutputTokens,
		ReservedOutput:    usage.ReservedOutput,
		ReservedSummary:   usage.ReservedSummary,
		OutputHeadroom:    usage.OutputHeadroom,
		AutoCompactRatio:  usage.AutoCompactRatio,
		RecentTailTokens:  usage.RecentTailTokens,
		RecentUserTokens:  usage.RecentUserTokens,
		Source:            usage.Source,
		Method:            config.EstimateMethod(usage.Method),
		Confidence:        config.EstimateConfidence(usage.Confidence),
		CompactionNeeded:  usage.CompactionNeeded,
		HardLimitExceeded: usage.HardLimitExceeded,
	}
}

func CacheRetention(value string) cache.Retention {
	switch cache.Retention(strings.TrimSpace(value)) {
	case cache.RetentionNone:
		return cache.RetentionNone
	case cache.RetentionShort:
		return cache.RetentionShort
	case cache.RetentionLong:
		return cache.RetentionLong
	case cache.RetentionDay:
		return cache.RetentionDay
	default:
		return cache.RetentionInMemory
	}
}
