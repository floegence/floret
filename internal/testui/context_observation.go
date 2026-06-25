package testui

import (
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/observation"
)

func contextStatusFromProviderRequest(req ObservedProviderRequest) ObservedContextStatus {
	return observation.ContextStatusFromRequest(requestObservation(req))
}

func contextStatusFromEngineEvent(ev event.Event) (ObservedContextStatus, bool) {
	return observation.ContextStatusFromProviderUsageEvent(observationEvent(ev))
}

func compactionEventFromEngineEvent(ev event.Event) (ObservedCompactionEvent, bool) {
	compact, ok := observation.CompactionEventFromEvent(observationEvent(ev))
	if !ok {
		return ObservedCompactionEvent{}, false
	}
	out := ObservedCompactionEvent{CompactionEvent: compact}
	if compact.Phase == engine.ContextCompactPhaseComplete {
		out.SummaryPreview = previewOneLine(ev.Result, 160)
		out.Summary = ev.Result
	}
	return out, true
}

func compactionDebugEventFromEngineEvent(ev event.Event) (ObservedCompactionDebugEvent, bool) {
	return observation.CompactionDebugEventFromEvent(observationEvent(ev))
}

func compactionEventFromEntry(entry ObservedSessionEntry) (ObservedCompactionEvent, bool) {
	if entry.Type != sessiontree.EntryCompaction {
		return ObservedCompactionEvent{}, false
	}
	return ObservedCompactionEvent{
		CompactionEvent: observation.CompactionEvent{
			ThreadID:                entry.ThreadID,
			TurnID:                  entry.TurnID,
			Phase:                   observation.CompactionPhaseComplete,
			Status:                  observation.CompactionStatusCompacted,
			Trigger:                 entry.CompactionTrigger,
			Reason:                  entry.CompactionReason,
			CompactionID:            entry.CompactionID,
			CompactionGeneration:    entry.CompactionGeneration,
			CompactionWindowID:      entry.CompactionWindowID,
			CompactedThroughEntryID: entry.CompactedThroughEntryID,
			TokensBefore:            entry.TokensBefore,
			TokensAfterEstimate:     entry.TokensAfterEstimate,
			ContextBefore:           configbridge.PublicContextUsage(entry.ContextUsageBefore),
			ContextAfter:            configbridge.PublicContextUsage(entry.ContextUsageAfter),
			Error:                   entry.Error,
			ObservedAt:              entry.CreatedAt,
		},
		SummaryPreview: previewOneLine(entry.Summary, 160),
		Summary:        entry.Summary,
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
	observation.SortContextStatuses(out)
	return out
}

func mergeContextStatuses(primary []ObservedContextStatus, secondary []ObservedContextStatus) []ObservedContextStatus {
	out := append([]ObservedContextStatus(nil), primary...)
	seen := make(map[string]struct{}, len(out)+len(secondary))
	for _, status := range out {
		seen[contextStatusKey(status)] = struct{}{}
	}
	for _, status := range secondary {
		key := contextStatusKey(status)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, status)
	}
	observation.SortContextStatuses(out)
	return out
}

func contextStatusKey(status ObservedContextStatus) string {
	if status.RequestID != "" {
		return status.Phase + "\x00" + status.RequestID + "\x00" + fmt.Sprintf("%d", status.Attempt)
	}
	return status.Phase + "\x00" + status.LogicalRequestID + "\x00" + fmt.Sprintf("%d\x00%d\x00%d", status.Attempt, status.Step, status.ObservedAt.UTC().UnixNano())
}

func contextStatusesFromPromptRecords(requests []cache.ProviderRequestRecord, responses []cache.ProviderResponseRecord) []ObservedContextStatus {
	requestObservations := make([]observation.RequestObservation, 0, len(requests))
	requestByID := map[string]observation.RequestObservation{}
	requestByStepID := map[string]observation.RequestObservation{}
	for _, req := range requests {
		observed := requestObservationFromPromptRecord(req)
		requestObservations = append(requestObservations, observed)
		if req.ID != "" {
			requestByID[req.ID] = observed
		}
		if stepID := requestID(req.RunID, req.Step); stepID != "" {
			requestByStepID[stepID] = observed
		}
	}

	usageObservations := make([]observation.ProviderUsageObservation, 0, len(responses))
	for _, resp := range responses {
		req, ok := requestByID[resp.RequestID]
		if !ok {
			req, ok = requestByStepID[resp.RequestID]
		}
		usageObservations = append(usageObservations, providerUsageObservationFromPromptResponse(resp, req, ok))
	}
	return observation.ContextStatusesFromObservations(requestObservations, usageObservations, nil)
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
		if existing.CompactionID != "" && existing.Phase == observation.CompactionPhaseComplete {
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

func compactionDebugEventsForObservation(events []event.Event) []ObservedCompactionDebugEvent {
	out := []ObservedCompactionDebugEvent{}
	seen := map[string]struct{}{}
	for _, ev := range events {
		debug, ok := compactionDebugEventFromEngineEvent(ev)
		if !ok {
			continue
		}
		key := compactionDebugEventKey(debug)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, debug)
	}
	return out
}

func compactionDebugEventKey(debug ObservedCompactionDebugEvent) string {
	return strings.Join([]string{
		debug.OperationID,
		debug.RequestID,
		debug.Stage,
		debug.Status,
		fmt.Sprintf("%d", debug.CompactionConvergenceAttempt),
		fmt.Sprintf("%d", debug.Step),
		fmt.Sprintf("%d", debug.ObservedAt.UTC().UnixNano()),
	}, "\x00")
}

func requestID(runID string, step int) string {
	return observation.RequestID(runID, step)
}

func requestObservation(req ObservedProviderRequest) observation.RequestObservation {
	return observation.RequestObservation{
		RunID:                req.RunID,
		ThreadID:             req.ThreadID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.ObservedAt,
		RequestEstimate:      configbridge.RequestEstimate(req.RequestEstimate),
		ProjectedPressure:    configbridge.PublicContextPressure(req.ProjectedPressure),
		CompactionGeneration: req.CacheSummary.CompactionGeneration,
		CompactionWindowID:   req.CacheSummary.CompactionWindowID,
	}
}

func requestObservationFromPromptRecord(req cache.ProviderRequestRecord) observation.RequestObservation {
	return observation.RequestObservation{
		RunID:                req.RunID,
		ThreadID:             req.ThreadID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		RequestID:            req.ID,
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.CreatedAt,
		RequestEstimate:      configbridge.RequestEstimate(req.RequestEstimate),
		ProjectedPressure:    configbridge.PublicContextPressure(req.ProjectedPressure),
		CompactionGeneration: req.CompactionGeneration,
		CompactionWindowID:   req.CompactionWindowID,
	}
}

func providerUsageObservationFromPromptResponse(resp cache.ProviderResponseRecord, req observation.RequestObservation, hasRequest bool) observation.ProviderUsageObservation {
	observed := observation.ProviderUsageObservation{
		RunID:           resp.RunID,
		ThreadID:        resp.ThreadID,
		TurnID:          resp.TurnID,
		RequestID:       resp.RequestID,
		ObservedAt:      resp.CreatedAt,
		Usage:           providerUsageFromPromptResponse(resp),
		ContextPressure: configbridge.PublicContextPressure(resp.NativePressure),
	}
	if hasRequest {
		observed.ThreadID = req.ThreadID
		observed.TurnID = req.TurnID
		observed.Step = req.Step
		observed.LogicalRequestID = req.LogicalRequestID
		observed.Attempt = req.Attempt
		observed.Provider = req.Provider
		observed.Model = req.Model
		observed.RequestEstimate = req.RequestEstimate
		observed.CompactionGeneration = req.CompactionGeneration
		observed.CompactionWindowID = req.CompactionWindowID
	}
	if observed.ThreadID == "" {
		observed.ThreadID = resp.ThreadID
	}
	if observed.TurnID == "" {
		observed.TurnID = resp.TurnID
	}
	return observed
}

func providerUsageFromPromptResponse(resp cache.ProviderResponseRecord) observation.ProviderUsage {
	return providerUsage(provider.Usage{
		InputTokens:       resp.InputTokens,
		OutputTokens:      resp.OutputTokens,
		ReasoningTokens:   resp.ReasoningTokens,
		CacheReadTokens:   resp.CacheReadTokens,
		CacheWriteTokens:  resp.CacheWriteTokens,
		TotalTokens:       resp.TotalTokens,
		Source:            provider.UsageSource(resp.UsageSource),
		Available:         resp.UsageAvailable,
		WindowInputTokens: resp.WindowInputTokens,
	})
}

func observationEvent(ev event.Event) observation.Event {
	return observation.Event{
		Type:         string(ev.Type),
		TraceID:      ev.TraceID,
		RunID:        ev.RunID,
		ThreadID:     ev.ThreadID,
		TurnID:       ev.TurnID,
		Step:         ev.Step,
		Provider:     ev.Provider,
		Model:        ev.Model,
		Message:      ev.Message,
		Result:       ev.Result,
		Error:        ev.Err,
		ToolID:       ev.ToolID,
		ToolName:     ev.ToolName,
		ToolKind:     ev.ToolKind,
		ArgsHash:     ev.ArgsHash,
		DurationMS:   ev.Duration,
		FinishReason: ev.FinishReason,
		Activity:     ev.Activity,
		Metadata:     observationMetadata(ev.Metadata),
		ObservedAt:   ev.Timestamp,
	}
}

func activityTimelineForObservation(meta observation.ActivityRunMeta, events []event.Event, now time.Time) observation.ActivityTimeline {
	observed := make([]observation.Event, 0, len(events))
	for _, ev := range events {
		observed = append(observed, observationEvent(event.Sanitize(ev)))
	}
	return observation.BuildActivityTimeline(meta, observed, now.UnixMilli())
}

func observationMetadata(value any) map[string]any {
	switch v := value.(type) {
	case nil:
		return nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = observationMetadataValue(item)
		}
		return out
	case engine.ProviderUsageContextStatus:
		return map[string]any{
			"phase":                 v.Phase,
			"request_id":            v.RequestID,
			"logical_request_id":    v.LogicalRequestID,
			"attempt":               v.Attempt,
			"usage":                 providerUsage(v.Usage),
			"request_estimate":      configbridge.RequestEstimate(v.RequestEstimate),
			"context_pressure":      configbridge.PublicContextPressure(v.ContextPressure),
			"used_ratio":            v.UsedRatio,
			"threshold_ratio":       v.ThresholdRatio,
			"status":                v.Status,
			"compaction_generation": v.CompactionGeneration,
			"compaction_window_id":  v.CompactionWindowID,
		}
	default:
		return nil
	}
}

func observationMetadataValue(value any) any {
	switch v := value.(type) {
	case provider.Usage:
		return providerUsage(v)
	case config.ContextPressure, config.ContextUsage, config.RequestEstimate:
		return v
	case engine.ProviderUsageContextStatus:
		return observationMetadata(v)
	default:
		return v
	}
}

func providerUsage(usage provider.Usage) observation.ProviderUsage {
	usage = usage.Normalized()
	return observation.ProviderUsage{
		InputTokens:       usage.InputTokens,
		OutputTokens:      usage.OutputTokens,
		ReasoningTokens:   usage.ReasoningTokens,
		CacheReadTokens:   usage.CacheReadTokens,
		CacheWriteTokens:  usage.CacheWriteTokens,
		TotalTokens:       usage.TotalTokens,
		WindowInputTokens: usage.WindowInputTokens,
		CostUSD:           usage.CostUSD,
		Source:            string(usage.Source),
		Available:         usage.Available,
	}
}

func previewOneLine(value string, limit int) string {
	text := strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit-3] + "..."
}
