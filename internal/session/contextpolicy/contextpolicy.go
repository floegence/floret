package contextpolicy

import (
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
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

// ContextBreakdown contains approximate, disjoint model-input token weights.
// It describes the prepared request, never cumulative billing usage or original
// tool output discarded before model input. Empty Source means unavailable.
type ContextBreakdown struct {
	// AttributionCoverage describes token attribution, independently of preparation coverage.
	AttributionCoverage string `json:"attribution_coverage,omitempty"`
	Limitation          string `json:"limitation,omitempty"`
	// OpaqueBytes is transport diagnostics, never a token category.
	OpaqueBytes          int64                    `json:"opaque_bytes,omitempty"`
	Tokenizer            string                   `json:"tokenizer,omitempty"`
	Quality              string                   `json:"quality,omitempty"`
	FramingTokens        int64                    `json:"framing_tokens,omitempty"`
	Diagnostic           string                   `json:"diagnostic,omitempty"`
	Details              *ContextBreakdownDetails `json:"details,omitempty"`
	Source               string                   `json:"source,omitempty"`
	SystemTokens         int64                    `json:"system_tokens,omitempty"`
	UserTokens           int64                    `json:"user_tokens,omitempty"`
	AssistantTokens      int64                    `json:"assistant_tokens,omitempty"`
	ToolDefinitionTokens int64                    `json:"tool_definition_tokens,omitempty"`
	ToolCallTokens       int64                    `json:"tool_call_tokens,omitempty"`
	ToolResultTokens     int64                    `json:"tool_result_tokens,omitempty"`
	ModelContextTokens   int64                    `json:"model_context_tokens,omitempty"`
	CompactionTokens     int64                    `json:"compaction_tokens,omitempty"`
	ReasoningTokens      int64                    `json:"reasoning_tokens,omitempty"`
	AttachmentTokens     int64                    `json:"attachment_tokens,omitempty"`
}

// ContextTokenDetail attributes one retained fragment to its actual origin.
// Category is tool_definition or model_context; Key is a tool name or section
// key. Source and Namespace come from tool definition annotations, never prose.
type ContextTokenDetail struct {
	Category  string `json:"category"`
	Key       string `json:"key"`
	Source    string `json:"source,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Tokens    int64  `json:"tokens"`
}

// ContextBreakdownDetails contains optional request-scoped attribution. It
// contains no prompt text, arguments, results, or resource paths.
type ContextBreakdownDetails struct {
	Items []ContextTokenDetail `json:"items"`
}

type RequestEstimate struct {
	Characters           *ContextCharacterBreakdown `json:"characters,omitempty"`
	Breakdown            ContextBreakdown           `json:"breakdown,omitzero"`
	PrefixTokens         int64                      `json:"prefix_tokens,omitempty"`
	MessageTokens        int64                      `json:"message_tokens,omitempty"`
	ToolDefinitionTokens int64                      `json:"tool_definition_tokens,omitempty"`
	EstimatedInputTokens int64                      `json:"estimated_input_tokens,omitempty"`
	Source               string                     `json:"source,omitempty"`
	Method               EstimateMethod             `json:"method,omitempty"`
	Confidence           EstimateConfidence         `json:"confidence,omitempty"`
}

func (e RequestEstimate) Normalized(policy Policy) RequestEstimate {
	if e.EstimatedInputTokens <= 0 {
		e.EstimatedInputTokens = e.PrefixTokens + e.MessageTokens + e.ToolDefinitionTokens
	}
	if e.Source == "" {
		e.Source = Normalize(policy).EstimatorSource
	}
	if e.Method == "" {
		e.Method = MethodForEstimateSource(e.Source, EstimateMethodGenericPayload)
	}
	if e.Confidence == "" {
		e.Confidence = EstimateConservative
	}
	return e
}

type RequestDeltaEstimate struct {
	MessageDeltaTokens        int64              `json:"message_delta_tokens,omitempty"`
	PrefixDeltaTokens         int64              `json:"prefix_delta_tokens,omitempty"`
	ToolDefinitionDeltaTokens int64              `json:"tool_definition_delta_tokens,omitempty"`
	EstimatedDeltaTokens      int64              `json:"estimated_delta_tokens,omitempty"`
	Source                    string             `json:"source,omitempty"`
	Method                    EstimateMethod     `json:"method,omitempty"`
	Confidence                EstimateConfidence `json:"confidence,omitempty"`
}

func (e RequestDeltaEstimate) Normalized() RequestDeltaEstimate {
	if e.EstimatedDeltaTokens == 0 {
		e.EstimatedDeltaTokens = e.MessageDeltaTokens + e.PrefixDeltaTokens + e.ToolDefinitionDeltaTokens
	}
	if e.Method == "" && e.Source != "" {
		e.Method = MethodForEstimateSource(e.Source, EstimateMethodGenericPayload)
	}
	if e.Confidence == "" {
		e.Confidence = EstimateConservative
	}
	return e
}

type MessageContextEstimate struct {
	PrefixTokens  int64              `json:"prefix_tokens,omitempty"`
	MessageTokens int64              `json:"message_tokens,omitempty"`
	InputTokens   int64              `json:"input_tokens,omitempty"`
	Source        string             `json:"source,omitempty"`
	Method        EstimateMethod     `json:"method,omitempty"`
	Confidence    EstimateConfidence `json:"confidence,omitempty"`
}

func (e MessageContextEstimate) Normalized(policy Policy) MessageContextEstimate {
	if e.InputTokens <= 0 {
		e.InputTokens = e.PrefixTokens + e.MessageTokens
	}
	if e.Source == "" {
		e.Source = Normalize(policy).EstimatorSource
	}
	if e.Method == "" {
		e.Method = MethodForEstimateSource(e.Source, EstimateMethodMessageContext)
	}
	if e.Confidence == "" {
		e.Confidence = EstimateConservative
	}
	return e
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
	RequestOutputTokens  int64              `json:"request_output_tokens,omitempty"`
	OutputControl        string             `json:"output_control,omitempty"`
	SafetyMarginTokens   int64              `json:"safety_margin_tokens,omitempty"`
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

type NativeUsage struct {
	InputTokens       int64
	CacheReadTokens   int64
	CacheWriteTokens  int64
	WindowInputTokens int64
	Available         bool
}

type Policy struct {
	// ModelMaxOutputTokens enables request-fitted budgeting when positive.
	ModelMaxOutputTokens         int64          `json:"model_max_output_tokens,omitempty"`
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

// Usage is the compaction-internal message-context budget. It intentionally
// excludes request tool schemas, native usage, projected pressure, and triggers.
type Usage struct {
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

func HasValues(policy Policy) bool {
	return policy.ModelMaxOutputTokens > 0 || policy.ContextWindowTokens > 0 ||
		policy.MaxOutputTokens > 0 ||
		policy.ReservedOutputTokens > 0 ||
		policy.ReservedSummaryTokens > 0 ||
		policy.RecentTailTokens > 0 ||
		policy.RecentUserTokens > 0 ||
		policy.CompactedContextTargetTokens > 0 ||
		policy.EstimatorSource != "" ||
		policy.EstimatorMethod != "" ||
		policy.MaxCompactionFailures > 0
}

func MergeDefaults(policy, defaults Policy) Policy {
	provided := HasValues(policy)
	if !provided {
		policy.ModelMaxOutputTokens = defaults.ModelMaxOutputTokens
	}
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
	if policy.CompactedContextTargetTokens <= 0 {
		policy.CompactedContextTargetTokens = defaults.CompactedContextTargetTokens
	}
	if policy.EstimatorSource == "" {
		policy.EstimatorSource = defaults.EstimatorSource
		if policy.EstimatorSource == "" {
			policy.EstimatorSource = DefaultEstimatorSource
		}
	}
	if policy.EstimatorMethod == "" {
		policy.EstimatorMethod = defaults.EstimatorMethod
		if policy.EstimatorMethod == "" {
			policy.EstimatorMethod = MethodForEstimateSource(policy.EstimatorSource, DefaultEstimatorMethod)
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
	if policy.CompactedContextTargetTokens <= 0 {
		policy.CompactedContextTargetTokens = DefaultCompactedContextTargetTokens
	}
	if policy.EstimatorSource == "" {
		policy.EstimatorSource = DefaultEstimatorSource
	}
	if policy.EstimatorMethod == "" {
		policy.EstimatorMethod = MethodForEstimateSource(policy.EstimatorSource, DefaultEstimatorMethod)
	}
	if policy.MaxCompactionFailures <= 0 {
		policy.MaxCompactionFailures = 2
	}
	return policy
}

func Threshold(policy Policy) int64      { return ResolveRequestBudget(policy, 0).ThresholdTokens }
func OutputHeadroom(policy Policy) int64 { return ResolveRequestBudget(policy, 0).HeadroomTokens }
func RatioLimitTokens(policy Policy) int64 {
	policy = Normalize(policy)
	return policy.ContextWindowTokens/100*DefaultAutoCompactRatioPercent + policy.ContextWindowTokens%100*DefaultAutoCompactRatioPercent/100
}
func RequestSafeLimitTokens(policy Policy) int64 {
	return ResolveRequestBudget(policy, 0).RequestSafeLimit
}

func PressureFromNativeUsage(usage NativeUsage, policy Policy) ContextPressure {
	policy = Normalize(policy)
	pressure := basePressure(policy)
	pressure.Signal = PressureSignalNativeUsage
	pressure.Source = PressureSourceProviderUsage
	pressure.Confidence = EstimateExact
	if !usage.Available {
		pressure.Confidence = EstimateConservative
		return pressure
	}
	windowInput := usage.WindowInputTokens
	if windowInput <= 0 {
		windowInput = usage.InputTokens + usage.CacheReadTokens + usage.CacheWriteTokens
	}
	pressure.WindowInputTokens = windowInput
	budget := ResolveRequestBudget(policy, windowInput)
	pressure.CompactionNeeded = budget.CompactionNeeded
	pressure.HardLimitExceeded = budget.HardLimitExceeded
	return pressure
}

func PressureFromProjectedRequest(estimate RequestEstimate, delta RequestDeltaEstimate, policy Policy) ContextPressure {
	estimate = estimate.Normalized(policy)
	policy = Normalize(policy)
	pressure := basePressure(policy)
	pressure.Signal = PressureSignalProjected
	pressure.Source = PressureSourceFullRequestEstimate
	pressure.EstimateMethod = estimate.Method
	pressure.Confidence = estimate.Confidence
	projected := estimate.EstimatedInputTokens
	delta = delta.Normalized()
	if delta.Source != "" {
		projected += delta.EstimatedDeltaTokens
		pressure.Source = PressureSourceUsageAnchoredDelta
		if delta.Method != "" {
			pressure.EstimateMethod = delta.Method
		}
		if delta.Confidence != "" {
			pressure.Confidence = delta.Confidence
		}
	}
	if projected < 0 {
		projected = 0
	}
	pressure.ProjectedInputTokens = projected
	budget := ResolveRequestBudget(policy, projected)
	pressure.HardLimitExceeded = budget.HardLimitExceeded
	if policy.ModelMaxOutputTokens > 0 {
		pressure.CompactionNeeded = budget.CompactionNeeded
	}
	return pressure
}

func PressureFromMissingNativeUsage(estimate RequestEstimate, policy Policy) ContextPressure {
	pressure := PressureFromProjectedRequest(estimate, RequestDeltaEstimate{}, policy)
	pressure.Source = PressureSourceMissingNativeUsage
	return pressure
}

func PressureFromOverflow(policy Policy) ContextPressure {
	pressure := basePressure(policy)
	pressure.Signal = PressureSignalOverflow
	pressure.Source = PressureSourceProviderUsage
	pressure.Confidence = EstimateExact
	pressure.CompactionNeeded = true
	pressure.HardLimitExceeded = true
	return pressure
}

func PressureFromManual(usage Usage, policy Policy) ContextPressure {
	policy = Normalize(policy)
	pressure := basePressure(policy)
	pressure.Signal = PressureSignalManual
	pressure.Source = PressureSourceManual
	pressure.Confidence = EstimateExact
	if usage.InputTokens > 0 {
		pressure.WindowInputTokens = usage.InputTokens
		pressure.ProjectedInputTokens = usage.InputTokens
	}
	pressure.CompactionNeeded = true
	pressure.HardLimitExceeded = ResolveRequestBudget(policy, pressure.ProjectedInputTokens).HardLimitExceeded
	return pressure
}

func UsageFromMessageContextEstimate(estimate MessageContextEstimate, policy Policy) Usage {
	policy = Normalize(policy)
	estimate = estimate.Normalized(policy)
	usage := Usage{
		PrefixTokens:     estimate.PrefixTokens,
		MessageTokens:    estimate.MessageTokens,
		InputTokens:      estimate.InputTokens,
		ContextWindow:    policy.ContextWindowTokens,
		ThresholdTokens:  Threshold(policy),
		RatioLimitTokens: RatioLimitTokens(policy),
		RequestSafeLimit: RequestSafeLimitTokens(policy),
		MaxOutputTokens:  policy.MaxOutputTokens,
		ReservedOutput:   policy.ReservedOutputTokens,
		ReservedSummary:  policy.ReservedSummaryTokens,
		OutputHeadroom:   OutputHeadroom(policy),
		AutoCompactRatio: DefaultAutoCompactRatioPercent,
		RecentTailTokens: policy.RecentTailTokens,
		RecentUserTokens: policy.RecentUserTokens,
		Source:           estimate.Source,
		Method:           estimate.Method,
		Confidence:       estimate.Confidence,
	}
	budget := ResolveRequestBudget(policy, usage.InputTokens)
	usage.CompactionNeeded = budget.CompactionNeeded
	usage.HardLimitExceeded = budget.HardLimitExceeded
	return usage
}

func UsageFromEstimate(estimate MessageContextEstimate, policy Policy) Usage {
	return UsageFromMessageContextEstimate(estimate, policy)
}

func EstimateMessageContext(systemPrompt string, history []session.Message, policy Policy) Usage {
	policy = Normalize(policy)
	estimate := MessageContextEstimate{
		Source:     policy.EstimatorSource,
		Method:     policy.EstimatorMethod,
		Confidence: EstimateConservative,
	}
	if systemPrompt != "" {
		estimate.PrefixTokens += EstimateTextTokens(systemPrompt)
	}
	projected, _, _ := session.ProjectProviderHistory(history, "")
	for _, msg := range projected {
		estimate.MessageTokens += EstimateMessageTokens(msg)
	}
	estimate.InputTokens = estimate.PrefixTokens + estimate.MessageTokens
	return UsageFromMessageContextEstimate(estimate, policy)
}

func MethodForEstimateSource(source string, defaultMethod EstimateMethod) EstimateMethod {
	source = strings.TrimSpace(source)
	if source == "" {
		if defaultMethod != "" {
			return defaultMethod
		}
		return EstimateMethodUnknown
	}
	if strings.HasSuffix(source, "_rendered_json") {
		return EstimateMethodProviderRenderedPayload
	}
	switch source {
	case "generic_request_json":
		return EstimateMethodGenericPayload
	case "generic_conservative", "message_context_test":
		if defaultMethod != "" {
			return defaultMethod
		}
		return EstimateMethodMessageContext
	case "official_preflight_count":
		return EstimateMethodOfficialPreflightCount
	default:
		return EstimateMethodUnknown
	}
}

func EstimateMessageTokens(msg session.Message) int64 {
	tokens := EstimateTextTokens(session.ProviderContent(msg)) + EstimateTextTokens(msg.ToolName) + EstimateTextTokens(msg.ToolArgs) + EstimateTextTokens(msg.ToolCallID) + 8
	if msg.Kind != "" {
		tokens += EstimateTextTokens(string(msg.Kind))
	}
	return tokens
}

func EstimateTextTokens(value string) int64 {
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

func basePressure(policy Policy) ContextPressure {
	policy = Normalize(policy)
	return ContextPressure{
		ContextWindowTokens:  policy.ContextWindowTokens,
		ThresholdTokens:      Threshold(policy),
		RequestSafeLimit:     RequestSafeLimitTokens(policy),
		OutputHeadroomTokens: OutputHeadroom(policy),
	}
}

func defaultWindowBudget(value, defaultValue, contextWindow int64) int64 {
	if value <= 0 {
		value = defaultValue
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
