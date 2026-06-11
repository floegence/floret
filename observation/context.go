package observation

import (
	"fmt"
	"slices"
	"time"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session/contextpolicy"
)

const (
	ContextPhaseProjectedRequest = "projected_request"
	ContextPhaseProviderUsage    = "provider_usage"
)

type RequestObservation struct {
	RunID                string                        `json:"run_id,omitempty"`
	ThreadID             string                        `json:"thread_id,omitempty"`
	TurnID               string                        `json:"turn_id,omitempty"`
	Step                 int                           `json:"step"`
	RequestID            string                        `json:"request_id,omitempty"`
	LogicalRequestID     string                        `json:"logical_request_id,omitempty"`
	Attempt              int                           `json:"attempt,omitempty"`
	Provider             string                        `json:"provider"`
	Model                string                        `json:"model"`
	ObservedAt           time.Time                     `json:"observed_at"`
	RequestEstimate      contextpolicy.RequestEstimate `json:"request_estimate,omitempty"`
	ProjectedPressure    contextpolicy.ContextPressure `json:"projected_context_pressure,omitempty"`
	CompactionGeneration int                           `json:"compaction_generation,omitempty"`
	CompactionWindowID   string                        `json:"compaction_window_id,omitempty"`
}

type ProviderUsageObservation struct {
	RunID                string                        `json:"run_id,omitempty"`
	ThreadID             string                        `json:"thread_id,omitempty"`
	TurnID               string                        `json:"turn_id,omitempty"`
	Step                 int                           `json:"step,omitempty"`
	RequestID            string                        `json:"request_id,omitempty"`
	LogicalRequestID     string                        `json:"logical_request_id,omitempty"`
	Attempt              int                           `json:"attempt,omitempty"`
	Provider             string                        `json:"provider,omitempty"`
	Model                string                        `json:"model,omitempty"`
	ObservedAt           time.Time                     `json:"observed_at"`
	Usage                provider.Usage                `json:"usage,omitempty"`
	RequestEstimate      contextpolicy.RequestEstimate `json:"request_estimate,omitempty"`
	ContextPressure      contextpolicy.ContextPressure `json:"context_pressure,omitempty"`
	CompactionGeneration int                           `json:"compaction_generation,omitempty"`
	CompactionWindowID   string                        `json:"compaction_window_id,omitempty"`
}

type ContextStatus struct {
	RunID                string                        `json:"run_id,omitempty"`
	ThreadID             string                        `json:"thread_id,omitempty"`
	TurnID               string                        `json:"turn_id,omitempty"`
	Step                 int                           `json:"step,omitempty"`
	RequestID            string                        `json:"request_id,omitempty"`
	LogicalRequestID     string                        `json:"logical_request_id,omitempty"`
	Attempt              int                           `json:"attempt,omitempty"`
	Phase                string                        `json:"phase"`
	Provider             string                        `json:"provider,omitempty"`
	Model                string                        `json:"model,omitempty"`
	ObservedAt           time.Time                     `json:"observed_at"`
	Usage                provider.Usage                `json:"usage,omitempty"`
	RequestEstimate      contextpolicy.RequestEstimate `json:"request_estimate,omitempty"`
	ContextPressure      contextpolicy.ContextPressure `json:"context_pressure,omitempty"`
	UsedRatio            float64                       `json:"used_ratio,omitempty"`
	ThresholdRatio       float64                       `json:"threshold_ratio,omitempty"`
	Status               string                        `json:"status"`
	CompactionGeneration int                           `json:"compaction_generation,omitempty"`
	CompactionWindowID   string                        `json:"compaction_window_id,omitempty"`
}

func ContextStatusFromRequest(req RequestObservation) ContextStatus {
	return ContextStatus{
		RunID:                req.RunID,
		ThreadID:             req.ThreadID,
		TurnID:               req.TurnID,
		Step:                 req.Step,
		RequestID:            requestIDOrDefault(req.RequestID, req.RunID, req.Step),
		LogicalRequestID:     req.LogicalRequestID,
		Attempt:              req.Attempt,
		Phase:                ContextPhaseProjectedRequest,
		Provider:             req.Provider,
		Model:                req.Model,
		ObservedAt:           req.ObservedAt,
		RequestEstimate:      req.RequestEstimate,
		ContextPressure:      req.ProjectedPressure,
		UsedRatio:            engine.ContextPressureUsedRatio(req.ProjectedPressure),
		ThresholdRatio:       engine.ContextPressureThresholdRatio(req.ProjectedPressure),
		Status:               engine.ContextPressureDisplayStatus(req.ProjectedPressure),
		CompactionGeneration: req.CompactionGeneration,
		CompactionWindowID:   req.CompactionWindowID,
	}
}

func ContextStatusFromProviderUsage(usage ProviderUsageObservation) (ContextStatus, bool) {
	if !hasContextPressure(usage.ContextPressure) {
		return ContextStatus{}, false
	}
	return ContextStatus{
		RunID:                usage.RunID,
		ThreadID:             usage.ThreadID,
		TurnID:               usage.TurnID,
		Step:                 usage.Step,
		RequestID:            usage.RequestID,
		LogicalRequestID:     usage.LogicalRequestID,
		Attempt:              usage.Attempt,
		Phase:                ContextPhaseProviderUsage,
		Provider:             usage.Provider,
		Model:                usage.Model,
		ObservedAt:           usage.ObservedAt,
		Usage:                usage.Usage.Normalized(),
		RequestEstimate:      usage.RequestEstimate,
		ContextPressure:      usage.ContextPressure,
		UsedRatio:            engine.ContextPressureUsedRatio(usage.ContextPressure),
		ThresholdRatio:       engine.ContextPressureThresholdRatio(usage.ContextPressure),
		Status:               engine.ContextPressureDisplayStatus(usage.ContextPressure),
		CompactionGeneration: usage.CompactionGeneration,
		CompactionWindowID:   usage.CompactionWindowID,
	}, true
}

func ContextStatusFromProviderUsageEvent(ev event.Event) (ContextStatus, bool) {
	if ev.Type != event.ProviderUsage {
		return ContextStatus{}, false
	}
	status, ok := providerUsageContextStatusFromMetadata(ev.Metadata)
	if !ok || status.Phase != engine.ProviderUsagePhaseFinalContextStatus {
		return ContextStatus{}, false
	}
	return ContextStatusFromProviderUsage(ProviderUsageObservation{
		RunID:                ev.RunID,
		ThreadID:             ev.ThreadID,
		TurnID:               ev.TurnID,
		Step:                 ev.Step,
		RequestID:            status.RequestID,
		LogicalRequestID:     status.LogicalRequestID,
		Attempt:              status.Attempt,
		Provider:             ev.Provider,
		Model:                ev.Model,
		ObservedAt:           ev.Timestamp,
		Usage:                status.Usage,
		RequestEstimate:      status.RequestEstimate,
		ContextPressure:      status.ContextPressure,
		CompactionGeneration: status.CompactionGeneration,
		CompactionWindowID:   status.CompactionWindowID,
	})
}

func ContextStatusesFromRequests(requests []RequestObservation, events []event.Event) []ContextStatus {
	return ContextStatusesFromObservations(requests, nil, events)
}

func ContextStatusesFromObservations(requests []RequestObservation, usages []ProviderUsageObservation, events []event.Event) []ContextStatus {
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

func requestIDOrDefault(requestID, runID string, step int) string {
	if requestID != "" {
		return requestID
	}
	return RequestID(runID, step)
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
	default:
		return engine.ProviderUsageContextStatus{}, false
	}
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
