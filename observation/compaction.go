package observation

import (
	"fmt"
	"time"

	"github.com/floegence/floret/config"
)

const (
	EventTypeProviderUsage    = "provider_usage"
	EventTypeContextCompact   = "context_compact"
	CompactionPhaseStart      = "start"
	CompactionPhaseComplete   = "complete"
	CompactionPhaseFailed     = "failed"
	CompactionStatusRunning   = "running"
	CompactionStatusCompacted = "compacted"
	CompactionStatusFailed    = "failed"
)

type CompactionEvent struct {
	RunID                   string                 `json:"run_id,omitempty"`
	ThreadID                string                 `json:"thread_id,omitempty"`
	TurnID                  string                 `json:"turn_id,omitempty"`
	Step                    int                    `json:"step,omitempty"`
	OperationID             string                 `json:"operation_id,omitempty"`
	Phase                   string                 `json:"phase"`
	Status                  string                 `json:"status"`
	Trigger                 string                 `json:"trigger,omitempty"`
	Reason                  string                 `json:"reason,omitempty"`
	CompactionID            string                 `json:"compaction_id,omitempty"`
	CompactionGeneration    int                    `json:"compaction_generation,omitempty"`
	CompactionWindowID      string                 `json:"compaction_window_id,omitempty"`
	CompactedThroughEntryID string                 `json:"compacted_through_entry_id,omitempty"`
	TokensBefore            int64                  `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64                  `json:"tokens_after_estimate,omitempty"`
	BeforePressure          config.ContextPressure `json:"before_pressure,omitempty"`
	ContextBefore           config.ContextUsage    `json:"context_before,omitempty"`
	ContextAfter            config.ContextUsage    `json:"context_after,omitempty"`
	Error                   string                 `json:"error,omitempty"`
	ObservedAt              time.Time              `json:"observed_at"`
}

func CompactionEventFromEvent(ev Event) (CompactionEvent, bool) {
	if ev.Type != EventTypeContextCompact {
		return CompactionEvent{}, false
	}
	meta := ev.Metadata
	phase := stringFromAny(meta["phase"])
	if phase == "" {
		return CompactionEvent{}, false
	}
	if ev.Error != "" && phase != CompactionPhaseFailed {
		return CompactionEvent{}, false
	}
	out := CompactionEvent{
		RunID:       ev.RunID,
		ThreadID:    ev.ThreadID,
		TurnID:      ev.TurnID,
		Step:        ev.Step,
		OperationID: stringFromAny(meta["operation_id"]),
		Phase:       phase,
		Status:      CompactionStatusRunning,
		Trigger:     stringFromAny(meta["trigger"]),
		Reason:      stringFromAny(meta["reason"]),
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
	out.CompactionID = stringFromAny(meta["compaction_id"])
	out.CompactionGeneration = intFromAny(meta["compaction_generation"], out.CompactionGeneration)
	out.CompactionWindowID = stringFromAny(meta["compaction_window_id"])
	out.CompactedThroughEntryID = stringFromAny(meta["compacted_through_entry_id"])
	out.TokensBefore = int64FromAny(meta["tokens_before"], out.TokensBefore)
	out.TokensAfterEstimate = int64FromAny(meta["tokens_after_estimate"], out.TokensAfterEstimate)
	switch out.Phase {
	case CompactionPhaseStart:
		out.Status = CompactionStatusRunning
	case CompactionPhaseComplete:
		out.Status = CompactionStatusCompacted
	case CompactionPhaseFailed:
		out.Status = CompactionStatusFailed
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
