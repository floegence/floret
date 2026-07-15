package observation

import (
	"fmt"
	"time"

	"github.com/floegence/floret/config"
)

const (
	CompactionPhaseStart     CompactionPhase = "start"
	CompactionPhaseComplete  CompactionPhase = "complete"
	CompactionPhaseFailed    CompactionPhase = "failed"
	CompactionPhaseCancelled CompactionPhase = "cancelled"
	CompactionPhaseNoop      CompactionPhase = "noop"

	CompactionStatusRunning   CompactionStatus = "running"
	CompactionStatusCompacted CompactionStatus = "compacted"
	CompactionStatusFailed    CompactionStatus = "failed"
	CompactionStatusCancelled CompactionStatus = "cancelled"
	CompactionStatusNoop      CompactionStatus = "noop"
)

type CompactionPhase string

func (p CompactionPhase) Valid() bool {
	switch p {
	case CompactionPhaseStart, CompactionPhaseComplete, CompactionPhaseFailed, CompactionPhaseCancelled, CompactionPhaseNoop:
		return true
	default:
		return false
	}
}

type CompactionStatus string

func (s CompactionStatus) Valid() bool {
	switch s {
	case CompactionStatusRunning, CompactionStatusCompacted, CompactionStatusFailed, CompactionStatusCancelled, CompactionStatusNoop:
		return true
	default:
		return false
	}
}

type CompactionEvent struct {
	RunID               string                 `json:"run_id,omitempty"`
	ThreadID            string                 `json:"thread_id,omitempty"`
	TurnID              string                 `json:"turn_id,omitempty"`
	Step                int                    `json:"step,omitempty"`
	OperationID         string                 `json:"operation_id,omitempty"`
	RequestID           string                 `json:"request_id,omitempty"`
	Phase               CompactionPhase        `json:"phase"`
	Status              CompactionStatus       `json:"status"`
	Trigger             string                 `json:"trigger,omitempty"`
	Reason              string                 `json:"reason,omitempty"`
	Source              string                 `json:"source,omitempty"`
	TokensBefore        int64                  `json:"tokens_before,omitempty"`
	TokensAfterEstimate int64                  `json:"tokens_after_estimate,omitempty"`
	BeforePressure      config.ContextPressure `json:"before_pressure,omitempty"`
	ContextBefore       config.ContextUsage    `json:"context_before,omitempty"`
	ContextAfter        config.ContextUsage    `json:"context_after,omitempty"`
	Error               string                 `json:"error,omitempty"`
	ObservedAt          time.Time              `json:"observed_at"`
}

func (e CompactionEvent) Validate() error {
	if !e.Phase.Valid() {
		return fmt.Errorf("unsupported compaction phase %q", e.Phase)
	}
	if !e.Status.Valid() {
		return fmt.Errorf("unsupported compaction status %q", e.Status)
	}
	want := map[CompactionPhase]CompactionStatus{
		CompactionPhaseStart:     CompactionStatusRunning,
		CompactionPhaseComplete:  CompactionStatusCompacted,
		CompactionPhaseFailed:    CompactionStatusFailed,
		CompactionPhaseCancelled: CompactionStatusCancelled,
		CompactionPhaseNoop:      CompactionStatusNoop,
	}[e.Phase]
	if e.Status != want {
		return fmt.Errorf("compaction phase %q requires status %q, got %q", e.Phase, want, e.Status)
	}
	return nil
}

func CompactionEventFromEvent(ev Event) (CompactionEvent, bool) {
	if ev.Compaction != nil {
		return *ev.Compaction, ev.Compaction.Validate() == nil
	}
	if ev.Type != EventTypeContextCompact {
		return CompactionEvent{}, false
	}
	meta := ev.Metadata
	phase := CompactionPhase(stringFromAny(meta["phase"]))
	if !phase.Valid() {
		return CompactionEvent{}, false
	}
	if ev.Error != "" && phase != CompactionPhaseFailed && phase != CompactionPhaseCancelled {
		return CompactionEvent{}, false
	}
	out := CompactionEvent{
		RunID:       ev.RunID,
		ThreadID:    ev.ThreadID,
		TurnID:      ev.TurnID,
		Step:        ev.Step,
		OperationID: stringFromAny(meta["operation_id"]),
		RequestID:   stringFromAny(meta["request_id"]),
		Phase:       phase,
		Status:      CompactionStatusRunning,
		Trigger:     stringFromAny(meta["trigger"]),
		Reason:      stringFromAny(meta["reason"]),
		Source:      stringFromAny(meta["source"]),
		ObservedAt:  ev.ObservedAt,
		Error:       ev.Error,
	}
	if usage, ok := contextUsageFromAny(meta["message_context_before"]); ok {
		out.ContextBefore = usage
		out.TokensBefore = usage.InputTokens
	}
	if pressure, ok := contextPressureFromAny(meta["before_pressure"]); ok {
		out.BeforePressure = pressure
	}
	if usage, ok := contextUsageFromAny(meta["context_before"]); ok {
		out.ContextBefore = usage
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := contextUsageFromAny(meta["context_after"]); ok {
		out.ContextAfter = usage
	}
	out.TokensBefore = int64FromAny(meta["tokens_before"], out.TokensBefore)
	out.TokensAfterEstimate = int64FromAny(meta["tokens_after_estimate"], out.TokensAfterEstimate)
	switch out.Phase {
	case CompactionPhaseStart:
		out.Status = CompactionStatusRunning
	case CompactionPhaseComplete:
		out.Status = CompactionStatusCompacted
	case CompactionPhaseFailed:
		out.Status = CompactionStatusFailed
	case CompactionPhaseCancelled:
		out.Status = CompactionStatusCancelled
	case CompactionPhaseNoop:
		out.Status = CompactionStatusNoop
	default:
		return CompactionEvent{}, false
	}
	return out, true
}

func CompactionEventsFromEvents(events []Event) []CompactionEvent {
	out := []CompactionEvent{}
	for _, ev := range events {
		if compact, ok := CompactionEventFromEvent(ev); ok {
			out = append(out, compact)
		}
	}
	return out
}

func stringFromAny(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		if value == nil {
			return ""
		}
		return fmt.Sprintf("%v", value)
	}
}

func int64FromAny(value any, defaultValue int64) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return defaultValue
	}
}

func intFromAny(value any, defaultValue int) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	default:
		return defaultValue
	}
}
