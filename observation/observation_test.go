package observation

import (
	"testing"
	"time"

	"github.com/floegence/floret/config"
)

func TestContextStatusFromRequestUsesProjectedPressure(t *testing.T) {
	status := ContextStatusFromRequest(RequestObservation{
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		Step:             2,
		LogicalRequestID: "logical-1",
		Attempt:          2,
		Provider:         "fake",
		Model:            "fake-model",
		ObservedAt:       time.Unix(10, 0),
		RequestEstimate: config.RequestEstimate{
			EstimatedInputTokens: 910,
			Source:               "test_estimator",
			Method:               config.EstimateMethodProviderRenderedPayload,
			Confidence:           config.EstimateApproximate,
		},
		ProjectedPressure: config.ContextPressure{
			ProjectedInputTokens: 910,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			RequestSafeLimit:     900,
			HardLimitExceeded:    true,
			Source:               config.PressureSourceFullRequestEstimate,
			Signal:               config.PressureSignalProjected,
		},
	})

	if status.Phase != ContextPhaseProjectedRequest ||
		status.RequestID != "turn-1:req:2" ||
		status.LogicalRequestID != "logical-1" ||
		status.Attempt != 2 ||
		status.Status != ContextStatusHardLimit ||
		status.UsedRatio != 0.91 ||
		status.ThresholdRatio != 0.8 {
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
	ev := Event{
		Type:       EventTypeProviderUsage,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       1,
		Provider:   "fake",
		Model:      "fake-model",
		ObservedAt: time.Unix(20, 0),
		Metadata: map[string]any{
			"phase":              ProviderUsagePhaseFinalContextStatus,
			"request_id":         "turn-1:req:1",
			"logical_request_id": "logical-1",
			"attempt":            1,
			"usage": ProviderUsage{
				InputTokens:       400,
				WindowInputTokens: 420,
				OutputTokens:      30,
				Source:            "native",
				Available:         true,
			},
			"request_estimate": config.RequestEstimate{EstimatedInputTokens: 390},
			"context_pressure": config.ContextPressure{
				WindowInputTokens:   420,
				ContextWindowTokens: 1000,
				ThresholdTokens:     800,
				Source:              config.PressureSourceProviderUsage,
				Signal:              config.PressureSignalNativeUsage,
			},
			"used_ratio":            0.42,
			"threshold_ratio":       0.8,
			"status":                ContextStatusStable,
			"compaction_generation": 1,
			"compaction_window_id":  "window-1",
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
		status.Status != ContextStatusStable {
		t.Fatalf("status = %#v", status)
	}

	streamUsage := ev
	streamUsage.Metadata = map[string]any{"phase": ProviderUsagePhaseStreamUsage}
	if _, ok := ContextStatusFromProviderUsageEvent(streamUsage); ok {
		t.Fatalf("map-shaped stream usage phase should not become a final context status")
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
		RequestID:        "turn-1:req:1",
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "logical-1",
		Attempt:          1,
		Provider:         "fake",
		Model:            "fake-model",
		RequestEstimate:  config.RequestEstimate{EstimatedInputTokens: 320},
		ProjectedPressure: config.ContextPressure{
			ProjectedInputTokens: 320,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               config.PressureSourceFullRequestEstimate,
		},
		ObservedAt: created,
	}}, []ProviderUsageObservation{{
		RequestID:        "turn-1:req:1",
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "logical-1",
		Attempt:          1,
		Provider:         "fake",
		Model:            "fake-model",
		Usage:            ProviderUsage{WindowInputTokens: 420, InputTokens: 400, OutputTokens: 30, Source: "native", Available: true},
		RequestEstimate:  config.RequestEstimate{EstimatedInputTokens: 320},
		ContextPressure:  config.ContextPressure{WindowInputTokens: 420, ContextWindowTokens: 1000, ThresholdTokens: 800, Source: config.PressureSourceProviderUsage},
		ObservedAt:       created.Add(time.Second),
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
		statuses[1].Usage.WindowInputTokens != 420 {
		t.Fatalf("provider usage status = %#v", statuses[1])
	}
}

func TestContextStatusesFromObservationsSkipProviderUsageWithoutPressure(t *testing.T) {
	created := time.Unix(25, 0)
	statuses := ContextStatusesFromObservations([]RequestObservation{{
		RequestID:        "turn-1:req:1",
		RunID:            "turn-1",
		ThreadID:         "thread-1",
		TurnID:           "turn-1",
		Step:             1,
		LogicalRequestID: "logical-1",
		Attempt:          1,
		Provider:         "fake",
		Model:            "fake-model",
		RequestEstimate:  config.RequestEstimate{EstimatedInputTokens: 320},
		ProjectedPressure: config.ContextPressure{
			ProjectedInputTokens: 320,
			ContextWindowTokens:  1000,
			ThresholdTokens:      800,
			Source:               config.PressureSourceFullRequestEstimate,
		},
		ObservedAt: created,
	}}, []ProviderUsageObservation{{
		RequestID:  "turn-1:req:1",
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		ObservedAt: created.Add(time.Second),
	}}, nil)

	if len(statuses) != 1 ||
		statuses[0].Phase != ContextPhaseProjectedRequest ||
		statuses[0].RequestID != "turn-1:req:1" {
		t.Fatalf("provider usage without pressure should only keep projected status: %#v", statuses)
	}
}

func TestCompactionEventFromEvents(t *testing.T) {
	start := Event{
		Type:       EventTypeContextCompact,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       2,
		ObservedAt: time.Unix(30, 0),
		Metadata: map[string]any{
			"phase":                  CompactionPhaseStart,
			"operation_id":           "op-1",
			"trigger":                "post_response",
			"reason":                 "threshold",
			"before_pressure":        config.ContextPressure{WindowInputTokens: 850},
			"message_context_before": config.ContextUsage{InputTokens: 850},
			"tokens_before":          int64(850),
		},
	}
	complete := Event{
		Type:       EventTypeContextCompact,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       2,
		Result:     "summary text with /Users/example/private/path",
		ObservedAt: time.Unix(31, 0),
		Metadata: map[string]any{
			"phase":                      CompactionPhaseComplete,
			"operation_id":               "op-1",
			"trigger":                    "post_response",
			"reason":                     "threshold",
			"compaction_id":              "compact-1",
			"compaction_generation":      3,
			"compaction_window_id":       "window-3",
			"compacted_through_entry_id": "entry-7",
			"tokens_before":              int64(850),
			"tokens_after_estimate":      int64(240),
			"context_before":             config.ContextUsage{InputTokens: 850},
			"context_after":              config.ContextUsage{InputTokens: 240},
		},
	}
	failed := Event{
		Type:       EventTypeContextCompact,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       3,
		Error:      "summary failed",
		ObservedAt: time.Unix(32, 0),
		Metadata: map[string]any{
			"phase":                  CompactionPhaseFailed,
			"operation_id":           "op-2",
			"trigger":                "provider_overflow",
			"reason":                 "provider_overflow",
			"before_pressure":        config.ContextPressure{WindowInputTokens: 990},
			"message_context_before": config.ContextUsage{InputTokens: 990},
			"tokens_before":          int64(990),
		},
	}
	noop := Event{
		Type:       EventTypeContextCompact,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       4,
		ObservedAt: time.Unix(33, 0),
		Metadata: map[string]any{
			"phase":                  CompactionPhaseNoop,
			"operation_id":           "op-3",
			"request_id":             "manual-1",
			"trigger":                "manual",
			"reason":                 "context_too_small",
			"before_pressure":        config.ContextPressure{WindowInputTokens: 1200},
			"message_context_before": config.ContextUsage{InputTokens: 1200},
			"tokens_before":          int64(1200),
		},
	}

	started, ok := CompactionEventFromEvent(start)
	if !ok {
		t.Fatalf("start event was not converted")
	}
	if started.Phase != CompactionPhaseStart ||
		started.Status != CompactionStatusRunning ||
		started.OperationID != "op-1" ||
		started.Trigger != string("post_response") ||
		started.Reason != string("threshold") ||
		started.TokensBefore != 850 ||
		started.BeforePressure.WindowInputTokens != 850 ||
		started.TurnID != "turn-1" {
		t.Fatalf("start compaction = %#v", started)
	}

	done, ok := CompactionEventFromEvent(complete)
	if !ok {
		t.Fatalf("complete event was not converted")
	}
	if done.Phase != CompactionPhaseComplete ||
		done.Status != CompactionStatusCompacted ||
		done.OperationID != "op-1" ||
		done.TokensAfterEstimate != 240 {
		t.Fatalf("complete compaction = %#v", done)
	}

	failedEvent, ok := CompactionEventFromEvent(failed)
	if !ok {
		t.Fatalf("failed event was not converted")
	}
	if failedEvent.Phase != CompactionPhaseFailed ||
		failedEvent.Status != CompactionStatusFailed ||
		failedEvent.OperationID != "op-2" ||
		failedEvent.Error != "summary failed" ||
		failedEvent.Trigger != string("provider_overflow") ||
		failedEvent.TokensBefore != 990 ||
		failedEvent.BeforePressure.WindowInputTokens != 990 {
		t.Fatalf("failed compaction = %#v", failedEvent)
	}

	noopEvent, ok := CompactionEventFromEvent(noop)
	if !ok {
		t.Fatalf("noop event was not converted")
	}
	if noopEvent.Phase != CompactionPhaseNoop ||
		noopEvent.Status != CompactionStatusNoop ||
		noopEvent.OperationID != "op-3" ||
		noopEvent.RequestID != "manual-1" ||
		noopEvent.Reason != "context_too_small" ||
		noopEvent.TokensBefore != 1200 {
		t.Fatalf("noop compaction = %#v", noopEvent)
	}

	malformed := start
	malformed.Metadata = map[string]any{"trigger": "post_response"}
	if _, ok := CompactionEventFromEvent(malformed); ok {
		t.Fatalf("ContextCompact event without explicit phase should not become a DTO")
	}
	conflictingError := complete
	conflictingError.Error = "complete cannot also be failed"
	if _, ok := CompactionEventFromEvent(conflictingError); ok {
		t.Fatalf("ContextCompact event with non-failed phase and error should not become a DTO")
	}
	unknownPhase := complete
	unknownPhase.Metadata = map[string]any{"phase": "retrying"}
	if _, ok := CompactionEventFromEvent(unknownPhase); ok {
		t.Fatalf("ContextCompact event with unknown phase should not become a DTO")
	}

	missingID := complete
	missingID.Metadata = map[string]any{
		"phase": CompactionPhaseComplete,
	}
	if _, ok := CompactionEventFromEvent(missingID); !ok {
		t.Fatalf("complete compaction without id should still convert")
	}
}

func TestCompactionDebugEventFromEvents(t *testing.T) {
	debug := Event{
		Type:       EventTypeContextCompactDebug,
		RunID:      "turn-1",
		ThreadID:   "thread-1",
		TurnID:     "turn-1",
		Step:       2,
		ObservedAt: time.Unix(40, 0),
		Metadata: map[string]any{
			"stage":                          CompactionDebugStagePreflight,
			"status":                         CompactionDebugStatusRetrying,
			"operation_id":                   "op-1",
			"request_id":                     "manual-1",
			"trigger":                        "manual",
			"reason":                         "manual",
			"compaction_convergence_attempt": 2,
			"history_message_count":          5,
			"active_message_count":           3,
			"tokens_before":                  int64(910),
			"tokens_after_estimate":          int64(240),
			"context_before": config.ContextUsage{
				InputTokens:     910,
				ContextWindow:   1000,
				ThresholdTokens: 800,
			},
			"validated_context_pressure": config.ContextPressure{
				ProjectedInputTokens: 910,
				ContextWindowTokens:  1000,
				ThresholdTokens:      800,
				RequestSafeLimit:     900,
				HardLimitExceeded:    true,
			},
			"request_estimate":                     config.RequestEstimate{EstimatedInputTokens: 760},
			"fixed_input_tokens":                   int64(120),
			"reducible_input_tokens":               int64(680),
			"request_safe_limit":                   int64(900),
			"compacted_context_target_tokens":      int64(260),
			"next_compacted_context_target_tokens": int64(220),
			"consecutive_failures":                 1,
			"duration_ms":                          int64(33),
		},
	}

	out, ok := CompactionDebugEventFromEvent(debug)
	if !ok {
		t.Fatalf("debug event was not converted")
	}
	if out.Stage != CompactionDebugStagePreflight ||
		out.Status != CompactionDebugStatusRetrying ||
		out.OperationID != "op-1" ||
		out.RequestID != "manual-1" ||
		out.CompactionConvergenceAttempt != 2 ||
		out.HistoryMessageCount != 5 ||
		out.ActiveMessageCount != 3 ||
		out.TokensBefore != 910 ||
		out.TokensAfterEstimate != 240 ||
		out.RequestEstimate.EstimatedInputTokens != 760 ||
		out.ValidatedContextPressure.RequestSafeLimit != 900 ||
		out.FixedInputTokens != 120 ||
		out.ReducibleInputTokens != 680 ||
		out.RequestSafeLimit != 900 ||
		out.CompactedContextTargetTokens != 260 ||
		out.NextCompactedContextTargetTokens != 220 ||
		out.ConsecutiveFailures != 1 ||
		out.DurationMS != 33 {
		t.Fatalf("debug event = %#v", out)
	}
}
