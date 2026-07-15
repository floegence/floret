package observation

import "testing"

func TestPublicContractEnumsValidate(t *testing.T) {
	t.Parallel()

	eventTypes := []EventType{
		EventTypeStepStart, EventTypeProviderRequest, EventTypeProviderDelta,
		EventTypeProviderReasoning, EventTypeProviderToolCallStart,
		EventTypeProviderToolCallDelta, EventTypeProviderToolCallEnd,
		EventTypeProviderUsage, EventTypeProviderSources, EventTypeProviderFinish,
		EventTypeProviderRetry, EventTypeToolCall, EventTypeToolDispatchStarted,
		EventTypeToolActivityUpdated, EventTypeToolResult,
		EventTypeToolApprovalRequested, EventTypeToolApprovalApproved,
		EventTypeToolApprovalRejected, EventTypeToolApprovalTimedOut,
		EventTypeToolApprovalCanceled, EventTypeHostedToolCall,
		EventTypeHostedToolResult, EventTypeMCPServerConnecting,
		EventTypeMCPServerReady, EventTypeMCPServerFailed, EventTypeMCPToolsListed,
		EventTypeMCPToolCall, EventTypeMCPToolResult, EventTypeSkillDetected,
		EventTypeSkillLoaded, EventTypeSkillBlocked, EventTypeSkillInstallRequired,
		EventTypeSkillDisclosureApplied, EventTypeContextCompact,
		EventTypeContextCompactDebug, EventTypeContextContinue,
		EventTypeThreadEntryCommitted, EventTypeControlSignal,
		EventTypeBudgetExceeded, EventTypeStepEnd, EventTypeRunEnd,
	}
	for _, typ := range eventTypes {
		if !typ.Valid() {
			t.Fatalf("EventType %q is not valid", typ)
		}
	}
	if EventType("future_event").Valid() {
		t.Fatal("unknown event type validated")
	}

	if err := (ContextStatus{Phase: ContextPhaseProjectedRequest, Status: ContextStatusStable}).Validate(); err != nil {
		t.Fatalf("valid context status: %v", err)
	}
	if err := (ContextStatus{Phase: "future_phase", Status: ContextStatusStable}).Validate(); err == nil {
		t.Fatal("unknown context phase validated")
	}
	if err := (ContextStatus{Phase: ContextPhaseProviderUsage, Status: "future_status"}).Validate(); err == nil {
		t.Fatal("unknown context status validated")
	}

	if err := (CompactionEvent{Phase: CompactionPhaseComplete, Status: CompactionStatusCompacted}).Validate(); err != nil {
		t.Fatalf("valid compaction: %v", err)
	}
	if err := (CompactionEvent{Phase: CompactionPhaseComplete, Status: CompactionStatusRunning}).Validate(); err == nil {
		t.Fatal("inconsistent compaction phase/status validated")
	}
	if err := (CompactionDebugEvent{Stage: CompactionDebugStagePreflight, Status: CompactionDebugStatusFailed}).Validate(); err != nil {
		t.Fatalf("valid compaction debug: %v", err)
	}
	if err := (CompactionDebugEvent{Stage: "future_stage", Status: CompactionDebugStatusOK}).Validate(); err == nil {
		t.Fatal("unknown compaction debug stage validated")
	}
}

func TestObservationEventValidateRejectsNestedContractErrors(t *testing.T) {
	t.Parallel()

	if err := (Event{Type: EventTypeToolCall}).Validate(); err != nil {
		t.Fatalf("valid event: %v", err)
	}
	if err := (Event{Type: "future_event"}).Validate(); err == nil {
		t.Fatal("unknown event validated")
	}
	if err := (Event{
		Type: EventTypeContextCompact,
		Compaction: &CompactionEvent{
			Phase:  CompactionPhaseFailed,
			Status: CompactionStatusRunning,
		},
	}).Validate(); err == nil {
		t.Fatal("event with invalid compaction validated")
	}
}
