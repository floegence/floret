package observation

import (
	"fmt"
	"time"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/session/contextpolicy"
)

const (
	CompactionStatusRunning   = "running"
	CompactionStatusCompacted = "compacted"
	CompactionStatusFailed    = "failed"
)

type CompactionEvent struct {
	RunID                   string                        `json:"run_id,omitempty"`
	SessionID               string                        `json:"session_id,omitempty"`
	TurnID                  string                        `json:"turn_id,omitempty"`
	Step                    int                           `json:"step,omitempty"`
	Phase                   string                        `json:"phase"`
	Status                  string                        `json:"status"`
	Trigger                 string                        `json:"trigger,omitempty"`
	Reason                  string                        `json:"reason,omitempty"`
	CompactionID            string                        `json:"compaction_id,omitempty"`
	CompactionGeneration    int                           `json:"compaction_generation,omitempty"`
	CompactionWindowID      string                        `json:"compaction_window_id,omitempty"`
	CompactedThroughEntryID string                        `json:"compacted_through_entry_id,omitempty"`
	TokensBefore            int64                         `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64                         `json:"tokens_after_estimate,omitempty"`
	BeforePressure          contextpolicy.ContextPressure `json:"before_pressure,omitempty"`
	ContextBefore           contextpolicy.Usage           `json:"context_before,omitempty"`
	ContextAfter            contextpolicy.Usage           `json:"context_after,omitempty"`
	Error                   string                        `json:"error,omitempty"`
	ObservedAt              time.Time                     `json:"observed_at"`
}

func CompactionEventFromEngineEvent(ev event.Event) (CompactionEvent, bool) {
	if ev.Type != event.ContextCompact {
		return CompactionEvent{}, false
	}
	meta, _ := ev.Metadata.(map[string]any)
	phase := stringFromAny(meta["phase"])
	if phase == "" {
		return CompactionEvent{}, false
	}
	if ev.Err != "" && phase != engine.ContextCompactPhaseFailed {
		return CompactionEvent{}, false
	}
	out := CompactionEvent{
		RunID:      ev.RunID,
		SessionID:  ev.SessionID,
		TurnID:     ev.RunID,
		Step:       ev.Step,
		Phase:      phase,
		Status:     CompactionStatusRunning,
		Trigger:    stringFromAny(meta["trigger"]),
		Reason:     stringFromAny(meta["reason"]),
		ObservedAt: ev.Timestamp,
		Error:      ev.Err,
	}
	if usage, ok := meta["message_context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = usage
		out.TokensBefore = usage.InputTokens
	}
	if pressure, ok := meta["before_pressure"].(contextpolicy.ContextPressure); ok {
		out.BeforePressure = pressure
	}
	if usage, ok := meta["context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = usage
		if out.TokensBefore == 0 {
			out.TokensBefore = usage.InputTokens
		}
	}
	if usage, ok := meta["context_after"].(contextpolicy.Usage); ok {
		out.ContextAfter = usage
	}
	out.CompactionID = stringFromAny(meta["compaction_id"])
	out.CompactionGeneration = intFromAny(meta["compaction_generation"], out.CompactionGeneration)
	out.CompactionWindowID = stringFromAny(meta["compaction_window_id"])
	out.CompactedThroughEntryID = stringFromAny(meta["compacted_through_entry_id"])
	out.TokensBefore = int64FromAny(meta["tokens_before"], out.TokensBefore)
	out.TokensAfterEstimate = int64FromAny(meta["tokens_after_estimate"], out.TokensAfterEstimate)
	switch out.Phase {
	case engine.ContextCompactPhaseStart:
		out.Status = CompactionStatusRunning
	case engine.ContextCompactPhaseComplete:
		out.Status = CompactionStatusCompacted
	case engine.ContextCompactPhaseFailed:
		out.Status = CompactionStatusFailed
	default:
		return CompactionEvent{}, false
	}
	return out, true
}

func CompactionEventsFromEngineEvents(events []event.Event) []CompactionEvent {
	out := []CompactionEvent{}
	for _, ev := range events {
		if compact, ok := CompactionEventFromEngineEvent(ev); ok {
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

func int64FromAny(value any, fallback int64) int64 {
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
		return fallback
	}
}

func intFromAny(value any, fallback int) int {
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
		return fallback
	}
}
