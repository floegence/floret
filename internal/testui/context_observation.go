package testui

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/sessiontree"
)

func contextStatusFromProviderRequest(req ObservedProviderRequest) ObservedContextStatus {
	return observation.ContextStatusFromRequest(requestObservation(req))
}

func contextStatusFromEngineEvent(ev event.Event) (ObservedContextStatus, bool) {
	return observation.ContextStatusFromProviderUsageEvent(ev)
}

func compactionEventFromEngineEvent(ev event.Event) (ObservedCompactionEvent, bool) {
	compact, ok := observation.CompactionEventFromEngineEvent(ev)
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

func compactionEventFromEntry(entry ObservedSessionEntry) (ObservedCompactionEvent, bool) {
	if entry.Type != sessiontree.EntryCompaction {
		return ObservedCompactionEvent{}, false
	}
	return ObservedCompactionEvent{
		CompactionEvent: observation.CompactionEvent{
			SessionID:               entry.ThreadID,
			TurnID:                  entry.TurnID,
			Phase:                   engine.ContextCompactPhaseComplete,
			Status:                  observation.CompactionStatusCompacted,
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

func requestID(runID string, step int) string {
	return observation.RequestID(runID, step)
}

func requestObservation(req ObservedProviderRequest) observation.RequestObservation {
	return observation.RequestObservation{
		RunID:                req.RunID,
		SessionID:            req.SessionID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.ObservedAt,
		RequestEstimate:      req.RequestEstimate,
		ProjectedPressure:    req.ProjectedPressure,
		CompactionGeneration: req.CacheSummary.CompactionGeneration,
		CompactionWindowID:   req.CacheSummary.CompactionWindowID,
	}
}

func requestObservationFromPromptRecord(req cache.ProviderRequestRecord) observation.RequestObservation {
	return observation.RequestObservation{
		RunID:                req.RunID,
		SessionID:            req.SessionID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		RequestID:            req.ID,
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.CreatedAt,
		RequestEstimate:      req.RequestEstimate,
		ProjectedPressure:    req.ProjectedPressure,
		CompactionGeneration: req.CompactionGeneration,
		CompactionWindowID:   req.CompactionWindowID,
	}
}

func providerUsageObservationFromPromptResponse(resp cache.ProviderResponseRecord, req observation.RequestObservation, hasRequest bool) observation.ProviderUsageObservation {
	observed := observation.ProviderUsageObservation{
		RunID:           resp.RunID,
		SessionID:       resp.ThreadID,
		TurnID:          resp.TurnID,
		RequestID:       resp.RequestID,
		ObservedAt:      resp.CreatedAt,
		Usage:           providerUsageFromPromptResponse(resp),
		ContextPressure: resp.NativePressure,
	}
	if hasRequest {
		observed.SessionID = req.SessionID
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
	if observed.SessionID == "" {
		observed.SessionID = resp.ThreadID
	}
	if observed.TurnID == "" {
		observed.TurnID = resp.TurnID
	}
	return observed
}

func providerUsageFromPromptResponse(resp cache.ProviderResponseRecord) provider.Usage {
	return provider.Usage{
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
}

func previewOneLine(value string, limit int) string {
	text := strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit-3] + "..."
}
