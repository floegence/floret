package observation

// FinishReason is Floret's provider-neutral normalized model finish reason.
type FinishReason string

const (
	FinishReasonUnknown       FinishReason = "unknown"
	FinishReasonStop          FinishReason = "stop"
	FinishReasonToolCalls     FinishReason = "tool_calls"
	FinishReasonLength        FinishReason = "length"
	FinishReasonContentFilter FinishReason = "content_filter"
	FinishReasonError         FinishReason = "error"
	FinishReasonCancelled     FinishReason = "cancelled"
)

func (r FinishReason) Valid() bool {
	switch r {
	case FinishReasonUnknown, FinishReasonStop, FinishReasonToolCalls, FinishReasonLength, FinishReasonContentFilter, FinishReasonError, FinishReasonCancelled:
		return true
	default:
		return false
	}
}

// CompletionReason explains why a provider-loop decision completed a run.
type CompletionReason string

const (
	CompletionReasonNaturalStop CompletionReason = "natural_stop"
	CompletionReasonToolSignal  CompletionReason = "tool_signal"
	CompletionReasonHookStop    CompletionReason = "hook_stop"
)

func (r CompletionReason) Valid() bool {
	switch r {
	case CompletionReasonNaturalStop, CompletionReasonToolSignal, CompletionReasonHookStop:
		return true
	default:
		return false
	}
}

// ContinuationReason explains why a provider-loop decision continued.
type ContinuationReason string

const (
	ContinuationReasonToolResults       ContinuationReason = "tool_results"
	ContinuationReasonCompaction        ContinuationReason = "compaction"
	ContinuationReasonProviderTruncated ContinuationReason = "provider_truncated"
	ContinuationReasonRetryEmpty        ContinuationReason = "retry_empty"
	ContinuationReasonNoProgress        ContinuationReason = "no_progress"
	ContinuationReasonHook              ContinuationReason = "hook"
)

func (r ContinuationReason) Valid() bool {
	switch r {
	case ContinuationReasonToolResults, ContinuationReasonCompaction, ContinuationReasonProviderTruncated, ContinuationReasonRetryEmpty, ContinuationReasonNoProgress, ContinuationReasonHook:
		return true
	default:
		return false
	}
}
