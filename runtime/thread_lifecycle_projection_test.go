package runtime

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessionlifecycle"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestThreadLifecycleProjectionMatchesCompletePrefixes(t *testing.T) {
	entries := []sessiontree.Entry{
		{TurnID: "", RunID: "legacy", Type: sessiontree.EntryCustom},
		{TurnID: "first", RunID: "run-first", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.TurnKindMetadataKey: string(TurnKindContextCompaction)}},
		{TurnID: "second", RunID: "run-second", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{TurnID: "first", RunID: "observation-must-not-advance", Type: sessiontree.EntryCustom, Metadata: map[string]string{"failure_reason": "initial failure"}},
		{TurnID: "first", RunID: "run-retry", Type: sessiontree.EntryRunFailure, Error: " exact failure "},
		{TurnID: "first", Type: sessiontree.EntryCancelRequested},
		{TurnID: "first", Type: sessiontree.EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "must-not-replace-first"}},
		{TurnID: "first", RunID: "run-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnAborted, Metadata: map[string]string{"recoverable": "true", sessiontree.TurnFailureCodeMetadataKey: string(ThreadTurnFailureProvider)}},
		{TurnID: "second", RunID: "run-second", Type: sessiontree.EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)}},
		{TurnID: "second", RunID: "run-second", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnWaiting},
		{TurnID: "second", RunID: "run-second", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
		{TurnID: "first", RunID: "run-retry", Type: sessiontree.EntryRunFailure, Error: "", Metadata: map[string]string{"failure_reason": "replacement failure"}},
		{TurnID: "first", RunID: "run-retry", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnFailed, Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: "unsupported"}},
	}
	for i := range 60 {
		turn := fmt.Sprintf("turn-%d", i%7)
		entries = append(entries, sessiontree.Entry{TurnID: turn, RunID: fmt.Sprint(i), Type: sessiontree.EntryTurnMarker, TurnStatus: []sessiontree.TurnMarkerStatus{sessiontree.TurnStarted, sessiontree.TurnWaiting, sessiontree.TurnSavePoint, sessiontree.TurnFailed, sessiontree.TurnAborted, sessiontree.TurnCompleted}[i%6]})
	}
	incremental := threadLifecycleProjection{}
	for i, entry := range entries {
		incremental.observe(entry)
		prefix := entries[:i+1]
		lifecycle := sessionlifecycle.Derive(prefix, sessionlifecycle.PhaseTurn)
		turn := identity.TurnID(lifecycle.LatestTurnID())
		var run identity.RunID
		message := ""
		code := ThreadTurnFailureCode("")
		for _, source := range prefix {
			if source.TurnID != turn.String() {
				continue
			}
			if value := strings.TrimSpace(source.RunID); source.Type != sessiontree.EntryCustom && value != "" {
				run = identity.RunID(value)
			}
			if source.Type == sessiontree.EntryRunFailure {
				message = strings.TrimSpace(source.Error)
			}
			if message == "" {
				message = strings.TrimSpace(source.Metadata["failure_reason"])
			}
			if value := strings.TrimSpace(source.Metadata[sessiontree.TurnFailureCodeMetadataKey]); value != "" {
				code = ThreadTurnFailureCode(value)
			}
		}
		gotTurn, gotRun, activity, outcome, failure := incremental.snapshot()
		if gotTurn != turn || gotRun != run {
			t.Fatalf("prefix=%d identity=%s/%s want=%s/%s", i, gotTurn, gotRun, turn, run)
		}
		wantActivity := ThreadActivityIdle
		var wantOutcome *TurnOutcome
		outcomes := map[string]TurnOutcome{sessionlifecycle.StatusCompleted: TurnOutcomeCompleted, sessionlifecycle.StatusFailed: TurnOutcomeFailed, sessionlifecycle.StatusCancelled: TurnOutcomeCancelled, sessionlifecycle.StatusInterrupted: TurnOutcomeInterrupted}
		if value, found := outcomes[lifecycle.Status()]; found {
			wantOutcome = &value
		}
		if lifecycle.Status() == sessionlifecycle.StatusRunning || lifecycle.Status() == sessionlifecycle.StatusWaiting {
			wantActivity = ThreadActivityActive
		}
		if activity != wantActivity || !reflect.DeepEqual(outcome, wantOutcome) {
			t.Fatalf("prefix=%d lifecycle differs", i)
		}
		var wantFailure *ThreadTurnFailure
		if message != "" && (lifecycle.Status() == sessionlifecycle.StatusFailed || lifecycle.Status() == sessionlifecycle.StatusInterrupted) {
			if !code.Valid() {
				code = ThreadTurnFailureLegacyUnclassified
			}
			wantFailure = &ThreadTurnFailure{Code: code, Message: message, Reason: classifyThreadTurnFailureReason(code, message)}
		}
		if !reflect.DeepEqual(failure, wantFailure) {
			t.Fatalf("prefix=%d failure=%+v want=%+v", i, failure, wantFailure)
		}
		facts := incremental.turns[turn.String()]
		if facts != nil && (!reflect.DeepEqual(facts.cancellation, threadCancellationForTurn(prefix, turn)) || facts.kind != turnKindFromEntries(prefix, turn)) {
			t.Fatalf("prefix=%d control precedence changed", i)
		}
	}
}
