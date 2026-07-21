package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestCanonicalTurnFailureRequiresStableCodeAndMessage(t *testing.T) {
	now := time.Now().UTC()
	failure := canonicalTurnFailure([]ThreadDetailEvent{
		{Kind: ThreadDetailEventError, Error: "provider failed", CreatedAt: now},
		{Kind: ThreadDetailEventTurnMarker, TurnMarker: &ThreadDetailTurnMarker{
			Status:   string(sessiontree.TurnFailed),
			Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider},
		}, CreatedAt: now},
	})
	if failure == nil || failure.Code != ThreadTurnFailureProvider || failure.Message != "provider failed" {
		t.Fatalf("canonical failure = %#v", failure)
	}
	if err := failure.Validate(); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
}

func TestTurnResultValidationRequiresTypedFailure(t *testing.T) {
	if err := validateThreadTurnFailureForStatus(TurnStatusFailed, nil); err == nil {
		t.Fatal("failed turn without typed failure passed validation")
	}
	failure := &ThreadTurnFailure{Code: ThreadTurnFailureProvider, Message: "provider failed"}
	if err := validateThreadTurnFailureForStatus(TurnStatusFailed, failure); err != nil {
		t.Fatalf("typed failed turn Validate(): %v", err)
	}
	if err := validateThreadTurnFailureForStatus(TurnStatusCancelled, failure); err == nil {
		t.Fatal("cancelled turn with non-cancelled failure code passed validation")
	}
	for _, code := range []ThreadTurnFailureCode{ThreadTurnFailureCancelled, ThreadTurnFailureInterrupted} {
		if err := validateThreadTurnFailureForStatus(TurnStatusFailed, &ThreadTurnFailure{Code: code, Message: "invalid"}); err == nil {
			t.Fatalf("failed turn with %q failure code passed validation", code)
		}
	}
}

func TestLegacyUnclassifiedFailureProjectsAsFailed(t *testing.T) {
	if string(ThreadTurnFailureLegacyUnclassified) != sessiontree.TurnFailureLegacyUnclassified {
		t.Fatalf("runtime/sessiontree legacy failure code mismatch: %q != %q", ThreadTurnFailureLegacyUnclassified, sessiontree.TurnFailureLegacyUnclassified)
	}
	failure := &ThreadTurnFailure{Code: ThreadTurnFailureLegacyUnclassified, Message: "legacy failure without durable origin"}
	if err := validateThreadTurnFailureForStatus(TurnStatusFailed, failure); err != nil {
		t.Fatalf("legacy failed turn validation: %v", err)
	}
	if err := failure.Validate(); err != nil {
		t.Fatalf("legacy failure validation: %v", err)
	}
	projected := canonicalTurnFailure([]ThreadDetailEvent{
		{Kind: ThreadDetailEventError, Error: failure.Message},
		{Kind: ThreadDetailEventTurnMarker, TurnMarker: &ThreadDetailTurnMarker{
			Status: string(sessiontree.TurnFailed), Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureLegacyUnclassified},
		}},
	})
	if projected == nil || *projected != *failure {
		t.Fatalf("legacy failure projection=%#v, want %#v", projected, failure)
	}
}

func TestApplyLatestThreadLifecycleProjectsInterruptedTurn(t *testing.T) {
	turn := ThreadTurnSnapshot{TurnID: "turn", Status: TurnStatusRunning}
	applyLatestThreadLifecycle(&turn, ThreadSnapshot{
		LatestTurnID: "turn", Status: ThreadStatusInterrupted, Recoverable: true, CanRetry: true,
	})
	if turn.Status != TurnStatusInterrupted || !turn.Recoverable || !turn.CanRetry || turn.Failure == nil ||
		turn.Failure.Code != ThreadTurnFailureInterrupted || turn.Failure.Message != sessiontree.InterruptedTurnFailureMessage {
		t.Fatalf("interrupted turn = %#v", turn)
	}
}

func TestInterruptedRecoveryStatusUsesCanonicalFailureCode(t *testing.T) {
	interrupted := &ThreadTurnFailure{Code: ThreadTurnFailureInterrupted, Message: sessiontree.InterruptedTurnFailureMessage}
	if got := interruptedRecoveryTurnStatus(sessiontree.TurnAborted, interrupted); got != TurnStatusInterrupted {
		t.Fatalf("interrupted recovery status = %q", got)
	}
	unknown := &ThreadTurnFailure{Code: ThreadTurnFailureEffectOutcomeUnknown, Message: sessiontree.InterruptedTurnEffectOutcomeUnknownMessage}
	if got := interruptedRecoveryTurnStatus(sessiontree.TurnFailed, unknown); got != TurnStatusFailed {
		t.Fatalf("unknown effect recovery status = %q", got)
	}
	if got := interruptedRecoveryTurnStatus(sessiontree.TurnFailed, interrupted); got != TurnStatusFailed {
		t.Fatalf("failed marker was overridden by interrupted code: %q", got)
	}
	if got := interruptedRecoveryTurnStatus(sessiontree.TurnCompleted, interrupted); got != "" {
		t.Fatalf("non-recovery marker was accepted: %q", got)
	}
}

func TestCanonicalTurnReadersRejectIncompleteFailure(t *testing.T) {
	now := time.Now().UTC()
	events := []ThreadDetailEvent{
		{
			ID: "started", Ordinal: 1, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &ThreadDetailTurnMarker{Status: string(sessiontree.TurnStarted), Metadata: map[string]string{"run_id": "run"}},
		},
		{
			ID: "user", Ordinal: 2, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventUserMessage, CreatedAt: now,
			Message: &ThreadDetailMessage{Role: "user", Content: "work"},
		},
		{
			ID: "terminal", Ordinal: 3, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &ThreadDetailTurnMarker{Status: string(sessiontree.TurnFailed)},
		},
	}
	if _, _, err := projectThreadTurnSnapshots("thread", events); err == nil {
		t.Fatal("latest/overview projection accepted failed turn without typed failure")
	}

	canonicalEvents := []agentharness.SubAgentDetailEvent{
		{
			ID: "started", Ordinal: 1, ThreadID: "thread", TurnID: "turn", Kind: agentharness.SubAgentDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &agentharness.SubAgentDetailTurnMarker{Status: string(sessiontree.TurnStarted), Metadata: map[string]string{"run_id": "run"}},
		},
		{
			ID: "user", Ordinal: 2, ThreadID: "thread", TurnID: "turn", Kind: agentharness.SubAgentDetailEventUserMessage, CreatedAt: now,
			Message: &agentharness.SubAgentDetailMessage{Role: "user", Content: "work"},
		},
		{
			ID: "terminal", Ordinal: 3, ThreadID: "thread", TurnID: "turn", Kind: agentharness.SubAgentDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &agentharness.SubAgentDetailTurnMarker{Status: string(sessiontree.TurnFailed)},
		},
	}
	if _, err := projectCanonicalThreadTurnSnapshot("thread", agentharness.CanonicalTurnDetail{
		TurnID: "turn", RunID: "run", StartedOrdinal: 1, Events: canonicalEvents,
	}); err == nil {
		t.Fatal("turn page projection accepted failed turn without typed failure")
	}

	result := TurnResult{ThreadID: "thread", TurnID: "turn", RunID: "run", Status: TurnStatusFailed}
	host := &providerHost{}
	if err := host.attachThreadTurnProjection(context.Background(), "thread", &result, canonicalEvents); err == nil {
		t.Fatal("turn result attachment accepted failed turn without typed failure")
	}
	if result.ProjectionAvailability != TurnProjectionAvailabilityUnavailable || result.Projection != nil || result.ProjectionError == "" {
		t.Fatalf("invalid projection was published: %#v", result)
	}
}

func TestCanonicalTurnReadersProjectInterruptedTerminal(t *testing.T) {
	now := time.Now().UTC()
	events := []ThreadDetailEvent{
		{
			ID: "started", Ordinal: 1, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &ThreadDetailTurnMarker{Status: string(sessiontree.TurnStarted), Metadata: map[string]string{"run_id": "run"}},
		},
		{
			ID: "user", Ordinal: 2, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventUserMessage, CreatedAt: now,
			Message: &ThreadDetailMessage{Role: "user", Content: "work"},
		},
		{
			ID: "failure", Ordinal: 3, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventError, CreatedAt: now,
			Error: sessiontree.InterruptedTurnFailureMessage,
		},
		{
			ID: "terminal", Ordinal: 4, ThreadID: "thread", TurnID: "turn", Kind: ThreadDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &ThreadDetailTurnMarker{Status: string(sessiontree.TurnAborted), Metadata: map[string]string{
				"run_id": "run", sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureInterrupted,
			}},
		},
	}
	turns, _, err := projectThreadTurnSnapshots("thread", events)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].Status != TurnStatusInterrupted || turns[0].Failure == nil || turns[0].Failure.Code != ThreadTurnFailureInterrupted {
		t.Fatalf("interrupted terminal projection = %#v", turns)
	}
}
