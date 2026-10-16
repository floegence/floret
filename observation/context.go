package observation

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/tools"
)

const (
	ContextPhaseProjectedRequest ContextPhase = "projected_request"
	ContextPhaseProviderUsage    ContextPhase = "provider_usage"

	ContextStatusStable        ContextDisplayStatus = "stable"
	ContextStatusNearThreshold ContextDisplayStatus = "near_threshold"
	ContextStatusWillCompact   ContextDisplayStatus = "will_compact"
	ContextStatusHardLimit     ContextDisplayStatus = "hard_limit"
	ContextStatusEstimated     ContextDisplayStatus = "estimated"

	ProviderUsagePhaseStreamUsage        = "stream_usage"
	ProviderUsagePhaseFinalContextStatus = "final_context_status"
)

type ContextPhase string

func (p ContextPhase) Valid() bool {
	return p == ContextPhaseProjectedRequest || p == ContextPhaseProviderUsage
}

type ContextDisplayStatus string

func (s ContextDisplayStatus) Valid() bool {
	switch s {
	case ContextStatusStable, ContextStatusNearThreshold, ContextStatusWillCompact, ContextStatusHardLimit, ContextStatusEstimated:
		return true
	default:
		return false
	}
}

// ProviderRetryProgress is memory-only progress for a scheduled model attempt.
type ProviderRetryProgress struct {
	Retry                int       `json:"retry"`
	MaxRetries           int       `json:"max_retries"`
	NextAttemptAt        time.Time `json:"next_attempt_at"`
	Reason               string    `json:"reason"`
	ReplacedAttemptID    string    `json:"replaced_attempt_id,omitempty"`
	ReplacedAttemptEpoch int       `json:"replaced_attempt_epoch,omitempty"`
}

type Event struct {
	Retry              *ProviderRetryProgress      `json:"retry,omitempty"`
	Type               EventType                   `json:"type"`
	TraceID            identity.TraceID            `json:"trace_id,omitempty"`
	RunID              identity.RunID              `json:"run_id,omitempty"`
	ThreadID           identity.ThreadID           `json:"thread_id,omitempty"`
	TurnID             identity.TurnID             `json:"turn_id,omitempty"`
	Step               int                         `json:"step,omitempty"`
	Provider           string                      `json:"provider,omitempty"`
	Model              string                      `json:"model,omitempty"`
	Message            string                      `json:"message,omitempty"`
	Result             string                      `json:"result,omitempty"`
	Error              string                      `json:"error,omitempty"`
	ToolID             string                      `json:"tool_id,omitempty"`
	ToolName           string                      `json:"tool_name,omitempty"`
	ToolKind           string                      `json:"tool_kind,omitempty"`
	ArgsHash           string                      `json:"args_hash,omitempty"`
	DurationMS         int64                       `json:"duration_ms,omitempty"`
	FinishReason       FinishReason                `json:"finish_reason,omitempty"`
	RawFinishReason    string                      `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                        `json:"finish_inferred,omitempty"`
	CompletionReason   CompletionReason            `json:"completion_reason,omitempty"`
	ContinuationReason ContinuationReason          `json:"continuation_reason,omitempty"`
	Activity           *tools.ActivityPresentation `json:"activity,omitempty"`
	Compaction         *CompactionEvent            `json:"compaction,omitempty"`
	CompactionDebug    *CompactionDebugEvent       `json:"compaction_debug,omitempty"`
	Metadata           map[string]any              `json:"metadata,omitempty"`
	ObservedAt         time.Time                   `json:"observed_at"`
}

type ProviderUsage struct {
	InputTokens       int64   `json:"input_tokens,omitempty"`
	OutputTokens      int64   `json:"output_tokens,omitempty"`
	ReasoningTokens   int64   `json:"reasoning_tokens,omitempty"`
	CacheReadTokens   int64   `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens  int64   `json:"cache_write_tokens,omitempty"`
	TotalTokens       int64   `json:"total_tokens,omitempty"`
	WindowInputTokens int64   `json:"window_input_tokens,omitempty"`
	CostUSD           float64 `json:"cost_usd,omitempty"`
	Source            string  `json:"source,omitempty"`
	Available         bool    `json:"available,omitempty"`
}

func (u ProviderUsage) Normalized() ProviderUsage {
	if u.WindowInputTokens <= 0 {
		u.WindowInputTokens = u.InputTokens + u.CacheReadTokens + u.CacheWriteTokens
	}
	if u.TotalTokens <= 0 {
		u.TotalTokens = u.InputTokens + u.OutputTokens + u.ReasoningTokens + u.CacheReadTokens + u.CacheWriteTokens
	}
	return u
}

type RequestObservation struct {
	RunID             identity.RunID            `json:"run_id,omitempty"`
	ThreadID          identity.ThreadID         `json:"thread_id,omitempty"`
	TurnID            identity.TurnID           `json:"turn_id,omitempty"`
	Step              int                       `json:"step"`
	RequestID         string                    `json:"request_id,omitempty"`
	LogicalRequestID  identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	Attempt           int                       `json:"attempt,omitempty"`
	Provider          string                    `json:"provider"`
	Model             string                    `json:"model"`
	ObservedAt        time.Time                 `json:"observed_at"`
	RequestEstimate   config.RequestEstimate    `json:"request_estimate,omitempty"`
	ProjectedPressure config.ContextPressure    `json:"projected_context_pressure,omitempty"`
}

type ProviderUsageObservation struct {
	RunID            identity.RunID            `json:"run_id,omitempty"`
	ThreadID         identity.ThreadID         `json:"thread_id,omitempty"`
	TurnID           identity.TurnID           `json:"turn_id,omitempty"`
	Step             int                       `json:"step,omitempty"`
	RequestID        string                    `json:"request_id,omitempty"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	Attempt          int                       `json:"attempt,omitempty"`
	Provider         string                    `json:"provider,omitempty"`
	Model            string                    `json:"model,omitempty"`
	ObservedAt       time.Time                 `json:"observed_at"`
	Usage            ProviderUsage             `json:"usage,omitempty"`
	RequestEstimate  config.RequestEstimate    `json:"request_estimate,omitempty"`
	ContextPressure  config.ContextPressure    `json:"context_pressure,omitempty"`
}

type ContextStatus struct {
	RunID            identity.RunID            `json:"run_id,omitempty"`
	ThreadID         identity.ThreadID         `json:"thread_id,omitempty"`
	TurnID           identity.TurnID           `json:"turn_id,omitempty"`
	Step             int                       `json:"step,omitempty"`
	RequestID        string                    `json:"request_id,omitempty"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	Attempt          int                       `json:"attempt,omitempty"`
	Phase            ContextPhase              `json:"phase"`
	Provider         string                    `json:"provider,omitempty"`
	Model            string                    `json:"model,omitempty"`
	ObservedAt       time.Time                 `json:"observed_at"`
	Usage            ProviderUsage             `json:"usage,omitempty"`
	RequestEstimate  config.RequestEstimate    `json:"request_estimate,omitempty"`
	ContextPressure  config.ContextPressure    `json:"context_pressure,omitempty"`
	UsedRatio        float64                   `json:"used_ratio,omitempty"`
	ThresholdRatio   float64                   `json:"threshold_ratio,omitempty"`
	Status           ContextDisplayStatus      `json:"status"`
}

func (s ContextStatus) Validate() error {
	p := s.ContextPressure
	if p.RequestOutputTokens < 0 || p.SafetyMarginTokens < 0 || p.SafetyMarginTokens > 4096 {
		return fmt.Errorf("invalid request output budget")
	}
	switch p.OutputControl {
	case "":
		if p.RequestOutputTokens != 0 || p.SafetyMarginTokens != 0 {
			return fmt.Errorf("request output budget requires control mode")
		}
	case "client":
	case "server":
		if p.RequestOutputTokens != 0 {
			return fmt.Errorf("server-managed output cannot claim a client limit")
		}
	default:
		return fmt.Errorf("unsupported output control %q", p.OutputControl)
	}

	if !s.Phase.Valid() {
		return fmt.Errorf("unsupported context phase %q", s.Phase)
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported context display status %q", s.Status)
	}

	b := s.RequestEstimate.Breakdown
	if b.Quality != "" && b.Quality != "token_estimate" && b.Quality != "relative_weight" {
		return fmt.Errorf("invalid context breakdown quality")
	}
	if b.Quality == "token_estimate" && (b.Tokenizer == "" || b.AttachmentTokens > 0) {
		return fmt.Errorf("context token estimate requires complete text vocabulary coverage")
	}
	if b.Diagnostic != "" && b.Diagnostic != "attribution_mismatch" {
		return fmt.Errorf("invalid context breakdown diagnostic")
	}
	total := int64(0)
	for _, n := range []int64{b.FramingTokens, b.SystemTokens, b.UserTokens, b.AssistantTokens, b.ToolDefinitionTokens, b.ToolCallTokens, b.ToolResultTokens, b.ModelContextTokens, b.CompactionTokens, b.ReasoningTokens, b.AttachmentTokens} {
		if n < 0 || n > int64(^uint64(0)>>1)-total {
			return fmt.Errorf("invalid context breakdown token counts")
		}
		total += n
	}
	if total > 0 && strings.TrimSpace(b.Source) == "" {
		return fmt.Errorf("context breakdown requires an estimate source")
	}
	if b.Details != nil {
		totals := map[string]int64{"tool_definition": 0, "model_context": 0}
		seen := map[[4]string]bool{}
		for _, item := range b.Details.Items {
			key := [4]string{item.Category, item.Key, item.Source, item.Namespace}
			subtotal, ok := totals[item.Category]
			if !ok || item.Tokens < 0 || item.Tokens > int64(^uint64(0)>>1)-subtotal || seen[key] ||
				(item.Category == "tool_definition" && item.Key == "") ||
				(item.Category == "model_context" && (item.Source != "" || item.Namespace != "")) {
				return fmt.Errorf("invalid context breakdown attribution")
			}
			seen[key] = true
			totals[item.Category] += item.Tokens
		}
		if strings.TrimSpace(b.Source) == "" || totals["tool_definition"] != b.ToolDefinitionTokens || totals["model_context"] != b.ModelContextTokens {
			return fmt.Errorf("context breakdown attribution does not match category totals")
		}
	}
	return nil
}

func ContextStatusFromRequest(req RequestObservation) ContextStatus {
	return ContextStatus{
		RunID:            req.RunID,
		ThreadID:         req.ThreadID,
		TurnID:           req.TurnID,
		Step:             req.Step,
		RequestID:        requestIDOrDefault(req.RequestID, req.RunID.String(), req.Step),
		LogicalRequestID: req.LogicalRequestID,
		Attempt:          req.Attempt,
		Phase:            ContextPhaseProjectedRequest,
		Provider:         req.Provider,
		Model:            req.Model,
		ObservedAt:       req.ObservedAt,
		RequestEstimate:  req.RequestEstimate,
		ContextPressure:  req.ProjectedPressure,
		UsedRatio:        ContextPressureUsedRatio(req.ProjectedPressure),
		ThresholdRatio:   ContextPressureThresholdRatio(req.ProjectedPressure),
		Status:           ContextPressureDisplayStatus(req.ProjectedPressure),
	}
}

func ContextStatusFromProviderUsage(usage ProviderUsageObservation) (ContextStatus, bool) {
	if !hasContextPressure(usage.ContextPressure) {
		return ContextStatus{}, false
	}
	return ContextStatus{
		RunID:            usage.RunID,
		ThreadID:         usage.ThreadID,
		TurnID:           usage.TurnID,
		Step:             usage.Step,
		RequestID:        usage.RequestID,
		LogicalRequestID: usage.LogicalRequestID,
		Attempt:          usage.Attempt,
		Phase:            ContextPhaseProviderUsage,
		Provider:         usage.Provider,
		Model:            usage.Model,
		ObservedAt:       usage.ObservedAt,
		Usage:            usage.Usage.Normalized(),
		RequestEstimate:  usage.RequestEstimate,
		ContextPressure:  usage.ContextPressure,
		UsedRatio:        ContextPressureUsedRatio(usage.ContextPressure),
		ThresholdRatio:   ContextPressureThresholdRatio(usage.ContextPressure),
		Status:           ContextPressureDisplayStatus(usage.ContextPressure),
	}, true
}

func ContextStatusFromProviderUsageEvent(ev Event) (ContextStatus, bool) {
	if ev.Type != EventTypeProviderUsage {
		return ContextStatus{}, false
	}
	status, ok := providerUsageContextStatusFromMetadata(ev.Metadata)
	if !ok || status.Phase != ProviderUsagePhaseFinalContextStatus {
		return ContextStatus{}, false
	}
	return ContextStatusFromProviderUsage(ProviderUsageObservation{
		RunID:            ev.RunID,
		ThreadID:         ev.ThreadID,
		TurnID:           ev.TurnID,
		Step:             ev.Step,
		RequestID:        status.RequestID,
		LogicalRequestID: status.LogicalRequestID,
		Attempt:          status.Attempt,
		Provider:         ev.Provider,
		Model:            ev.Model,
		ObservedAt:       ev.ObservedAt,
		Usage:            status.Usage,
		RequestEstimate:  status.RequestEstimate,
		ContextPressure:  status.ContextPressure,
	})
}

func ContextStatusesFromRequests(requests []RequestObservation, events []Event) []ContextStatus {
	return ContextStatusesFromObservations(requests, nil, events)
}

func ContextStatusesFromObservations(requests []RequestObservation, usages []ProviderUsageObservation, events []Event) []ContextStatus {
	out := make([]ContextStatus, 0, len(requests)+len(usages)+len(events))
	for _, req := range requests {
		out = append(out, ContextStatusFromRequest(req))
	}
	for _, usage := range usages {
		if status, ok := ContextStatusFromProviderUsage(usage); ok {
			out = append(out, status)
		}
	}
	for _, ev := range events {
		if status, ok := ContextStatusFromProviderUsageEvent(ev); ok {
			out = append(out, status)
		}
	}
	SortContextStatuses(out)
	return out
}

func SortContextStatuses(statuses []ContextStatus) {
	slices.SortStableFunc(statuses, func(a, b ContextStatus) int {
		if a.ObservedAt.Equal(b.ObservedAt) {
			if a.Step == b.Step {
				return a.PhaseOrder() - b.PhaseOrder()
			}
			return a.Step - b.Step
		}
		if a.ObservedAt.Before(b.ObservedAt) {
			return -1
		}
		return 1
	})
}

func (s ContextStatus) PhaseOrder() int {
	switch s.Phase {
	case ContextPhaseProjectedRequest:
		return 1
	case ContextPhaseProviderUsage:
		return 2
	default:
		return 9
	}
}

func RequestID(runID string, step int) string {
	if step <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:req:%d", runID, step)
}

func ContextPressureDisplayStatus(pressure config.ContextPressure) ContextDisplayStatus {
	if pressure.HardLimitExceeded {
		return ContextStatusHardLimit
	}
	if pressure.CompactionNeeded {
		return ContextStatusWillCompact
	}
	if pressure.Source == config.PressureSourceMissingNativeUsage {
		return ContextStatusEstimated
	}
	used := ContextPressureUsedRatio(pressure)
	threshold := ContextPressureThresholdRatio(pressure)
	if used > 0 && threshold > 0 && used >= threshold*0.9 {
		return ContextStatusNearThreshold
	}
	return ContextStatusStable
}

func ContextPressureUsedRatio(pressure config.ContextPressure) float64 {
	if pressure.ContextWindowTokens <= 0 {
		return 0
	}
	used := pressure.WindowInputTokens
	if used <= 0 {
		used = pressure.ProjectedInputTokens
	}
	return cleanRatio(float64(used) / float64(pressure.ContextWindowTokens))
}

func ContextPressureThresholdRatio(pressure config.ContextPressure) float64 {
	if pressure.ContextWindowTokens <= 0 || pressure.ThresholdTokens <= 0 {
		return 0
	}
	return cleanRatio(float64(pressure.ThresholdTokens) / float64(pressure.ContextWindowTokens))
}

type providerUsageContextStatus struct {
	Phase            string
	RequestID        string
	LogicalRequestID identity.LogicalRequestID
	Attempt          int
	Usage            ProviderUsage
	RequestEstimate  config.RequestEstimate
	ContextPressure  config.ContextPressure
}

func providerUsageContextStatusFromMetadata(meta map[string]any) (providerUsageContextStatus, bool) {
	if len(meta) == 0 {
		return providerUsageContextStatus{}, false
	}
	pressure, _ := contextPressureFromAny(meta["context_pressure"])
	phase := stringFromAny(meta["phase"])
	if phase == "" {
		return providerUsageContextStatus{}, false
	}
	return providerUsageContextStatus{
		Phase:            phase,
		RequestID:        stringFromAny(meta["request_id"]),
		LogicalRequestID: identity.LogicalRequestID(stringFromAny(meta["logical_request_id"])),
		Attempt:          intFromAny(meta["attempt"], 0),
		Usage:            providerUsageFromAny(meta["usage"]),
		RequestEstimate:  requestEstimateFromAny(meta["request_estimate"]),
		ContextPressure:  pressure,
	}, true
}

func hasContextPressure(pressure config.ContextPressure) bool {
	return pressure.ContextWindowTokens > 0 ||
		pressure.ThresholdTokens > 0 ||
		pressure.ProjectedInputTokens > 0 ||
		pressure.WindowInputTokens > 0 ||
		pressure.RequestSafeLimit > 0 ||
		pressure.OutputHeadroomTokens > 0 ||
		pressure.Source != "" ||
		pressure.Signal != ""
}

func cleanRatio(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	return value
}

func requestIDOrDefault(requestID, runID string, step int) string {
	if requestID != "" {
		return requestID
	}
	return RequestID(runID, step)
}
