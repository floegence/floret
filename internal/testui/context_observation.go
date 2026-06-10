package testui

import (
	"fmt"
	"slices"
	"strings"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/sessiontree"
)

const (
	contextStatusPhaseProjectedRequest = "projected_request"
	contextStatusPhaseProviderUsage    = "provider_usage"

	compactionStatusRunning   = "running"
	compactionStatusCompacted = "compacted"
	compactionStatusFailed    = "failed"
)

func contextStatusFromProviderRequest(req ObservedProviderRequest) ObservedContextStatus {
	return ObservedContextStatus{
		RunID:                req.RunID,
		SessionID:            req.SessionID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		RequestID:            requestID(req.RunID, req.Step),
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Phase:                contextStatusPhaseProjectedRequest,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.ObservedAt,
		RequestEstimate:      req.RequestEstimate,
		ContextPressure:      req.ProjectedPressure,
		UsedRatio:            engine.ContextPressureUsedRatio(req.ProjectedPressure),
		ThresholdRatio:       engine.ContextPressureThresholdRatio(req.ProjectedPressure),
		Status:               engine.ContextPressureDisplayStatus(req.ProjectedPressure),
		CompactionGeneration: req.CacheSummary.CompactionGeneration,
		CompactionWindowID:   req.CacheSummary.CompactionWindowID,
	}
}

func contextStatusFromEngineEvent(ev event.Event) (ObservedContextStatus, bool) {
	if ev.Type != event.ProviderUsage {
		return ObservedContextStatus{}, false
	}
	status, ok := providerUsageContextStatusFromMetadata(ev.Metadata)
	if !ok || status.Phase != engine.ProviderUsagePhaseFinalContextStatus {
		return ObservedContextStatus{}, false
	}
	return ObservedContextStatus{
		RunID:                ev.RunID,
		SessionID:            ev.SessionID,
		TurnID:               ev.RunID,
		Step:                 ev.Step,
		RequestID:            status.RequestID,
		LogicalRequestID:     status.LogicalRequestID,
		Attempt:              status.Attempt,
		Phase:                contextStatusPhaseProviderUsage,
		Provider:             ev.Provider,
		Model:                ev.Model,
		ObservedAt:           ev.Timestamp,
		Usage:                status.Usage,
		RequestEstimate:      status.RequestEstimate,
		ContextPressure:      status.ContextPressure,
		UsedRatio:            status.UsedRatio,
		ThresholdRatio:       status.ThresholdRatio,
		Status:               status.Status,
		CompactionGeneration: status.CompactionGeneration,
		CompactionWindowID:   status.CompactionWindowID,
	}, true
}

func providerUsageContextStatusFromMetadata(value any) (engine.ProviderUsageContextStatus, bool) {
	switch v := value.(type) {
	case engine.ProviderUsageContextStatus:
		return v, true
	case *engine.ProviderUsageContextStatus:
		if v == nil {
			return engine.ProviderUsageContextStatus{}, false
		}
		return *v, true
	case map[string]any:
		return providerUsageContextStatusFromMetadata(v["details"])
	default:
		return engine.ProviderUsageContextStatus{}, false
	}
}

func compactionEventFromEngineEvent(ev event.Event) (ObservedCompactionEvent, bool) {
	if ev.Type != event.ContextCompact {
		return ObservedCompactionEvent{}, false
	}
	meta, _ := ev.Metadata.(map[string]any)
	phase := stringFromAny(meta["phase"])
	if phase == "" {
		return ObservedCompactionEvent{}, false
	}
	out := ObservedCompactionEvent{
		RunID:      ev.RunID,
		SessionID:  ev.SessionID,
		TurnID:     ev.RunID,
		Step:       ev.Step,
		Phase:      phase,
		Status:     compactionStatusRunning,
		Trigger:    stringFromAny(meta["trigger"]),
		Reason:     stringFromAny(meta["reason"]),
		ObservedAt: ev.Timestamp,
		Error:      ev.Err,
	}
	if usage, ok := meta["message_context_before"].(contextpolicy.Usage); ok {
		out.ContextBefore = usage
		out.TokensBefore = usage.InputTokens
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
	if out.Phase == engine.ContextCompactPhaseComplete {
		out.Status = compactionStatusCompacted
		if out.CompactionID == "" {
			out.CompactionID = ev.Message
		}
		out.Summary = ev.Result
		out.SummaryPreview = previewOneLine(ev.Result, 160)
	}
	if out.Phase == engine.ContextCompactPhaseFailed {
		out.Status = compactionStatusFailed
	}
	if out.Error != "" {
		out.Status = compactionStatusFailed
	}
	return out, true
}

func compactionEventFromEntry(entry ObservedSessionEntry) (ObservedCompactionEvent, bool) {
	if entry.Type != sessiontree.EntryCompaction {
		return ObservedCompactionEvent{}, false
	}
	return ObservedCompactionEvent{
		SessionID:               entry.ThreadID,
		TurnID:                  entry.TurnID,
		Phase:                   engine.ContextCompactPhaseComplete,
		Status:                  compactionStatusCompacted,
		Trigger:                 entry.CompactionTrigger,
		Reason:                  entry.CompactionReason,
		CompactionID:            entry.CompactionID,
		CompactionGeneration:    entry.CompactionGeneration,
		CompactionWindowID:      entry.CompactionWindowID,
		CompactedThroughEntryID: entry.CompactedThroughEntryID,
		TokensBefore:            entry.TokensBefore,
		TokensAfterEstimate:     entry.TokensAfterEstimate,
		ContextBefore:           entry.ContextUsageBefore,
		ContextAfter:            entry.ContextUsageAfter,
		SummaryPreview:          previewOneLine(entry.Summary, 160),
		Summary:                 entry.Summary,
		Error:                   entry.Error,
		ObservedAt:              entry.CreatedAt,
	}, true
}

func contextStatusesForObservation(requests []ObservedProviderRequest, events []event.Event) []ObservedContextStatus {
	out := make([]ObservedContextStatus, 0, len(requests)+len(events))
	for _, req := range requests {
		out = append(out, contextStatusFromProviderRequest(req))
	}
	for _, ev := range events {
		if status, ok := contextStatusFromEngineEvent(ev); ok {
			out = append(out, status)
		}
	}
	sortContextStatuses(out)
	return out
}

func contextStatusesFromPromptRecords(requests []cache.ProviderRequestRecord, responses []cache.ProviderResponseRecord) []ObservedContextStatus {
	out := make([]ObservedContextStatus, 0, len(requests)+len(responses))
	requestByID := map[string]cache.ProviderRequestRecord{}
	requestByStepID := map[string]cache.ProviderRequestRecord{}
	for _, req := range requests {
		requestByID[req.ID] = req
		if stepID := requestID(req.RunID, req.Step); stepID != "" {
			requestByStepID[stepID] = req
		}
		out = append(out, contextStatusFromPromptRequest(req))
	}
	for _, resp := range responses {
		req, ok := requestByID[resp.RequestID]
		if !ok {
			req, ok = requestByStepID[resp.RequestID]
		}
		if status, ok := contextStatusFromPromptResponse(resp, req, ok); ok {
			out = append(out, status)
		}
	}
	sortContextStatuses(out)
	return out
}

func contextStatusFromPromptRequest(req cache.ProviderRequestRecord) ObservedContextStatus {
	return ObservedContextStatus{
		RunID:                req.RunID,
		SessionID:            req.SessionID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		RequestID:            req.ID,
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Phase:                contextStatusPhaseProjectedRequest,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.CreatedAt,
		RequestEstimate:      req.RequestEstimate,
		ContextPressure:      req.ProjectedPressure,
		UsedRatio:            engine.ContextPressureUsedRatio(req.ProjectedPressure),
		ThresholdRatio:       engine.ContextPressureThresholdRatio(req.ProjectedPressure),
		Status:               engine.ContextPressureDisplayStatus(req.ProjectedPressure),
		CompactionGeneration: req.CompactionGeneration,
		CompactionWindowID:   req.CompactionWindowID,
	}
}

func contextStatusFromPromptResponse(resp cache.ProviderResponseRecord, req cache.ProviderRequestRecord, hasRequest bool) (ObservedContextStatus, bool) {
	if !hasContextPressure(resp.NativePressure) {
		return ObservedContextStatus{}, false
	}
	usage := provider.Usage{
		InputTokens:       resp.InputTokens,
		OutputTokens:      resp.OutputTokens,
		ReasoningTokens:   resp.ReasoningTokens,
		CacheReadTokens:   resp.CacheReadTokens,
		CacheWriteTokens:  resp.CacheWriteTokens,
		TotalTokens:       resp.TotalTokens,
		Source:            provider.UsageSource(resp.UsageSource),
		Available:         resp.UsageAvailable,
		WindowInputTokens: resp.WindowInputTokens,
	}.Normalized()
	status := ObservedContextStatus{
		RunID:           resp.RunID,
		SessionID:       resp.ThreadID,
		TurnID:          resp.TurnID,
		RequestID:       resp.RequestID,
		Phase:           contextStatusPhaseProviderUsage,
		ObservedAt:      resp.CreatedAt,
		Usage:           usage,
		ContextPressure: resp.NativePressure,
		UsedRatio:       engine.ContextPressureUsedRatio(resp.NativePressure),
		ThresholdRatio:  engine.ContextPressureThresholdRatio(resp.NativePressure),
		Status:          engine.ContextPressureDisplayStatus(resp.NativePressure),
	}
	if hasRequest {
		status.SessionID = req.SessionID
		status.TurnID = req.TurnID
		status.Step = req.Step
		status.LogicalRequestID = req.LogicalRequestID
		status.Attempt = req.Attempt
		status.Provider = req.Provider
		status.Model = req.Model
		status.RequestEstimate = req.RequestEstimate
		status.CompactionGeneration = req.CompactionGeneration
		status.CompactionWindowID = req.CompactionWindowID
	}
	if status.SessionID == "" {
		status.SessionID = resp.ThreadID
	}
	if status.TurnID == "" {
		status.TurnID = resp.TurnID
	}
	return status, true
}

func hasContextPressure(pressure contextpolicy.ContextPressure) bool {
	return pressure.ContextWindowTokens > 0 ||
		pressure.ThresholdTokens > 0 ||
		pressure.ProjectedInputTokens > 0 ||
		pressure.WindowInputTokens > 0 ||
		pressure.RequestSafeLimit > 0 ||
		pressure.OutputHeadroomTokens > 0 ||
		pressure.Source != "" ||
		pressure.Signal != ""
}

func sortContextStatuses(out []ObservedContextStatus) {
	slices.SortStableFunc(out, func(a, b ObservedContextStatus) int {
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

func compactionEventsForObservation(entries []ObservedSessionEntry, events []event.Event) []ObservedCompactionEvent {
	out := []ObservedCompactionEvent{}
	for _, ev := range events {
		if compact, ok := compactionEventFromEngineEvent(ev); ok {
			out = append(out, compact)
		}
	}
	seenComplete := map[string]struct{}{}
	for _, existing := range out {
		if existing.CompactionID != "" && existing.Phase == engine.ContextCompactPhaseComplete {
			seenComplete[existing.CompactionID] = struct{}{}
		}
	}
	for _, entry := range entries {
		compact, ok := compactionEventFromEntry(entry)
		if !ok {
			continue
		}
		if compact.CompactionID != "" {
			if _, ok := seenComplete[compact.CompactionID]; ok {
				continue
			}
			seenComplete[compact.CompactionID] = struct{}{}
		}
		out = append(out, compact)
	}
	return out
}

func (s ObservedContextStatus) PhaseOrder() int {
	switch s.Phase {
	case contextStatusPhaseProjectedRequest:
		return 1
	case contextStatusPhaseProviderUsage:
		return 2
	default:
		return 9
	}
}

func requestID(runID string, step int) string {
	if step <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:req:%d", runID, step)
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

func previewOneLine(value string, limit int) string {
	text := strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit-3] + "..."
}
