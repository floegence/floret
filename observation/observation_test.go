package observation

import (
	"testing"
	"time"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
)

func TestContextStatusFromRequestUsesProjectedPressure(t *testing.T) {
	status := ContextStatusFromRequest(RequestObservation{
		RunID:            "turn-1",
		SessionID:        "thread-1",
		TurnID:           "turn-1",
		Step:             2,
		LogicalRequestID: "logical-1",
		Attempt:          2,
		Provider:         "fake",
		Model:            "fake-model",
		ObservedAt:       time.Unix(10, 0),
		RequestEstimate: contextpolicy.RequestEstimate{
			EstimatedInputTokens: 910,
			Source:               "test_estimator",
			Method:               contextpolicy.EstimateMethodProviderRenderedPayload,
			Confidence:           contextpolicy.EstimateApproximate,
		},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 910,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			RequestSafeLimit:     900,
			HardLimitExceeded:    true,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
			Signal:               contextpolicy.PressureSignalProjected,
		},
		CompactionGeneration: 3,
		CompactionWindowID:   "window-3",
	})

	if status.Phase != ContextPhaseProjectedRequest ||
		status.RequestID != "turn-1:req:2" ||
		status.LogicalRequestID != "logical-1" ||
		status.Attempt != 2 ||
		status.Status != engine.ContextStatusHardLimit ||
		status.UsedRatio != 0.91 ||
		status.ThresholdRatio != 0.8 ||
		status.CompactionGeneration != 3 ||
		status.CompactionWindowID != "window-3" {
		t.Fatalf("context status = %#v", status)
	}
}

func TestContextStatusFromRequestKeepsExplicitRequestID(t *testing.T) {
	status := ContextStatusFromRequest(RequestObservation{
		RunID:     "turn-1",
		Step:      2,
		RequestID: "provider-request-42",
	})
	if status.RequestID != "provider-request-42" {
		t.Fatalf("RequestID = %q", status.RequestID)
	}
}

func TestContextStatusFromFinalProviderUsageEvent(t *testing.T) {
	ev := event.Event{
		Type:      event.ProviderUsage,
		RunID:     "turn-1",
		SessionID: "thread-1",
		Step:      1,
		Provider:  "fake",
		Model:     "fake-model",
		Timestamp: time.Unix(20, 0),
		Metadata: engine.ProviderUsageContextStatus{
			Phase:            engine.ProviderUsagePhaseFinalContextStatus,
			RequestID:        "turn-1:req:1",
			LogicalRequestID: "logical-1",
			Attempt:          1,
			Usage: provider.Usage{
				InputTokens:       400,
				WindowInputTokens: 420,
				OutputTokens:      30,
				Source:            provider.UsageNative,
				Available:         true,
			},
			RequestEstimate: contextpolicy.RequestEstimate{EstimatedInputTokens: 390},
			ContextPressure: contextpolicy.ContextPressure{
				WindowInputTokens:   420,
				ContextWindowTokens: 1000,
				ThresholdTokens:     800,
				Source:              contextpolicy.PressureSourceProviderUsage,
				Signal:              contextpolicy.PressureSignalNativeUsage,
			},
			UsedRatio:            0.42,
			ThresholdRatio:       0.8,
			Status:               engine.ContextStatusStable,
			CompactionGeneration: 1,
			CompactionWindowID:   "window-1",
		},
	}

	status, ok := ContextStatusFromProviderUsageEvent(ev)
	if !ok {
		t.Fatalf("final provider usage context status was not converted")
	}
	if status.Phase != ContextPhaseProviderUsage ||
		status.TurnID != "turn-1" ||
		status.RequestID != "turn-1:req:1" ||
		status.Provider != "fake" ||
		status.Usage.WindowInputTokens != 420 ||
		status.ContextPressure.WindowInputTokens != 420 ||
		status.UsedRatio != 0.42 ||
		status.ThresholdRatio != 0.8 ||
		status.Status != engine.ContextStatusStable ||
		status.CompactionGeneration != 1 ||
		status.CompactionWindowID != "window-1" {
		t.Fatalf("status = %#v", status)
	}

	streamUsage := ev
	streamUsage.Metadata = map[string]any{"phase": engine.ProviderUsagePhaseStreamUsage}
	if _, ok := ContextStatusFromProviderUsageEvent(streamUsage); ok {
		t.Fatalf("map-shaped stream usage phase should not become a final context status")
	}
	typedStreamUsage := ev
	typedStreamUsage.Metadata = engine.ProviderUsageContextStatus{Phase: engine.ProviderUsagePhaseStreamUsage}
	if _, ok := ContextStatusFromProviderUsageEvent(typedStreamUsage); ok {
		t.Fatalf("typed stream usage phase should not become a final context status")
	}
	wrappedStatus := ev
	wrappedStatus.Metadata = map[string]any{"details": ev.Metadata}
	if _, ok := ContextStatusFromProviderUsageEvent(wrappedStatus); ok {
		t.Fatalf("wrapped provider usage metadata should not be guessed")
	}
}

func TestContextStatusesFromObservations(t *testing.T) {
	created := time.Unix(25, 0)
	statuses := ContextStatusesFromObservations([]RequestObservation{{
		RequestID:            "turn-1:req:1",
		RunID:                "turn-1",
		SessionID:            "thread-1",
		TurnID:               "turn-1",
		Step:                 1,
		LogicalRequestID:     "logical-1",
		Attempt:              1,
		Provider:             "fake",
		Model:                "fake-model",
		CompactionGeneration: 2,
		CompactionWindowID:   "window-2",
		RequestEstimate:      contextpolicy.RequestEstimate{EstimatedInputTokens: 320},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 320,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
		ObservedAt: created,
	}}, []ProviderUsageObservation{{
		RequestID:            "turn-1:req:1",
		RunID:                "turn-1",
		SessionID:            "thread-1",
		TurnID:               "turn-1",
		Step:                 1,
		LogicalRequestID:     "logical-1",
		Attempt:              1,
		Provider:             "fake",
		Model:                "fake-model",
		Usage:                provider.Usage{WindowInputTokens: 420, InputTokens: 400, OutputTokens: 30, Source: provider.UsageNative, Available: true},
		RequestEstimate:      contextpolicy.RequestEstimate{EstimatedInputTokens: 320},
		ContextPressure:      contextpolicy.ContextPressure{WindowInputTokens: 420, ContextWindowTokens: 1000, ThresholdTokens: 800, Source: contextpolicy.PressureSourceProviderUsage},
		CompactionGeneration: 2,
		CompactionWindowID:   "window-2",
		ObservedAt:           created.Add(time.Second),
	}}, nil)

	if len(statuses) != 2 {
		t.Fatalf("statuses = %#v", statuses)
	}
	if statuses[0].Phase != ContextPhaseProjectedRequest || statuses[0].RequestID != "turn-1:req:1" {
		t.Fatalf("projected status = %#v", statuses[0])
	}
	if statuses[1].Phase != ContextPhaseProviderUsage ||
		statuses[1].Step != 1 ||
		statuses[1].LogicalRequestID != "logical-1" ||
		statuses[1].Attempt != 1 ||
		statuses[1].Provider != "fake" ||
		statuses[1].Model != "fake-model" ||
		statuses[1].Usage.WindowInputTokens != 420 ||
		statuses[1].CompactionGeneration != 2 ||
		statuses[1].CompactionWindowID != "window-2" {
		t.Fatalf("provider usage status = %#v", statuses[1])
	}
}

func TestContextStatusesFromObservationsSkipProviderUsageWithoutPressure(t *testing.T) {
	created := time.Unix(25, 0)
	statuses := ContextStatusesFromObservations([]RequestObservation{{
		RequestID:        "turn-1:req:1",
		RunID:            "turn-1",
		SessionID:        "thread-1",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "logical-1",
		Attempt:          1,
		Provider:         "fake",
		Model:            "fake-model",
		RequestEstimate:  contextpolicy.RequestEstimate{EstimatedInputTokens: 320},
		ProjectedPressure: contextpolicy.ContextPressure{
			ProjectedInputTokens: 320,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               contextpolicy.PressureSourceFullRequestEstimate,
		},
		ObservedAt: created,
	}}, []ProviderUsageObservation{{
		RequestID:  "turn-1:req:1",
		RunID:      "turn-1",
		SessionID:  "thread-1",
		TurnID:     "turn-1",
		ObservedAt: created.Add(time.Second),
	}}, nil)

	if len(statuses) != 1 ||
		statuses[0].Phase != ContextPhaseProjectedRequest ||
		statuses[0].RequestID != "turn-1:req:1" {
		t.Fatalf("provider usage without pressure should only keep projected status: %#v", statuses)
	}
}

func TestCompactionEventFromEngineEvents(t *testing.T) {
	start := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		SessionID: "thread-1",
		Step:      2,
		Timestamp: time.Unix(30, 0),
		Metadata: map[string]any{
			"phase":                  engine.ContextCompactPhaseStart,
			"trigger":                compaction.TriggerPostResponse,
			"reason":                 compaction.ReasonThreshold,
			"before_pressure":        contextpolicy.ContextPressure{WindowInputTokens: 850},
			"message_context_before": contextpolicy.Usage{InputTokens: 850},
			"tokens_before":          int64(850),
		},
	}
	complete := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		SessionID: "thread-1",
		Step:      2,
		Message:   "message-fallback-must-not-be-used",
		Result:    "summary text with /Users/example/private/path",
		Timestamp: time.Unix(31, 0),
		Metadata: map[string]any{
			"phase":                      engine.ContextCompactPhaseComplete,
			"trigger":                    compaction.TriggerPostResponse,
			"reason":                     compaction.ReasonThreshold,
			"compaction_id":              "compact-1",
			"compaction_generation":      3,
			"compaction_window_id":       "window-3",
			"compacted_through_entry_id": "entry-7",
			"tokens_before":              int64(850),
			"tokens_after_estimate":      int64(240),
			"context_before":             contextpolicy.Usage{InputTokens: 850},
			"context_after":              contextpolicy.Usage{InputTokens: 240},
		},
	}
	failed := event.Event{
		Type:      event.ContextCompact,
		RunID:     "turn-1",
		SessionID: "thread-1",
		Step:      3,
		Err:       "summary failed",
		Timestamp: time.Unix(32, 0),
		Metadata: map[string]any{
			"phase":                  engine.ContextCompactPhaseFailed,
			"trigger":                compaction.TriggerOverflow,
			"reason":                 compaction.ReasonProviderOverflow,
			"before_pressure":        contextpolicy.ContextPressure{WindowInputTokens: 990},
			"message_context_before": contextpolicy.Usage{InputTokens: 990},
			"tokens_before":          int64(990),
		},
	}

	started, ok := CompactionEventFromEngineEvent(start)
	if !ok {
		t.Fatalf("start event was not converted")
	}
	if started.Phase != engine.ContextCompactPhaseStart ||
		started.Status != CompactionStatusRunning ||
		started.Trigger != string(compaction.TriggerPostResponse) ||
		started.Reason != string(compaction.ReasonThreshold) ||
		started.TokensBefore != 850 ||
		started.BeforePressure.WindowInputTokens != 850 ||
		started.TurnID != "turn-1" {
		t.Fatalf("start compaction = %#v", started)
	}

	done, ok := CompactionEventFromEngineEvent(complete)
	if !ok {
		t.Fatalf("complete event was not converted")
	}
	if done.Phase != engine.ContextCompactPhaseComplete ||
		done.Status != CompactionStatusCompacted ||
		done.CompactionID != "compact-1" ||
		done.CompactionGeneration != 3 ||
		done.CompactionWindowID != "window-3" ||
		done.CompactedThroughEntryID != "entry-7" ||
		done.TokensAfterEstimate != 240 {
		t.Fatalf("complete compaction = %#v", done)
	}

	failedEvent, ok := CompactionEventFromEngineEvent(failed)
	if !ok {
		t.Fatalf("failed event was not converted")
	}
	if failedEvent.Phase != engine.ContextCompactPhaseFailed ||
		failedEvent.Status != CompactionStatusFailed ||
		failedEvent.Error != "summary failed" ||
		failedEvent.Trigger != string(compaction.TriggerOverflow) ||
		failedEvent.TokensBefore != 990 ||
		failedEvent.BeforePressure.WindowInputTokens != 990 {
		t.Fatalf("failed compaction = %#v", failedEvent)
	}

	malformed := start
	malformed.Metadata = map[string]any{"trigger": compaction.TriggerPostResponse}
	if _, ok := CompactionEventFromEngineEvent(malformed); ok {
		t.Fatalf("ContextCompact event without explicit phase should not become a DTO")
	}
	conflictingError := complete
	conflictingError.Err = "complete cannot also be failed"
	if _, ok := CompactionEventFromEngineEvent(conflictingError); ok {
		t.Fatalf("ContextCompact event with non-failed phase and error should not become a DTO")
	}
	unknownPhase := complete
	unknownPhase.Metadata = map[string]any{"phase": "retrying"}
	if _, ok := CompactionEventFromEngineEvent(unknownPhase); ok {
		t.Fatalf("ContextCompact event with unknown phase should not become a DTO")
	}

	missingID := complete
	missingID.Metadata = map[string]any{
		"phase": engine.ContextCompactPhaseComplete,
	}
	noID, ok := CompactionEventFromEngineEvent(missingID)
	if !ok {
		t.Fatalf("complete compaction without id should still convert")
	}
	if noID.CompactionID != "" {
		t.Fatalf("CompactionID = %q, want empty without explicit metadata", noID.CompactionID)
	}
}
