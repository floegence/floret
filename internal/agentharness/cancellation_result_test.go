package agentharness

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestNormalizeCancelledEngineResultOverridesLateProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := normalizeCancelledEngineResult(ctx, engine.Result{
		Status:        engine.Failed,
		Err:           errors.New("provider stream closed after cancellation"),
		FailureOrigin: engine.FailureOriginProvider,
	})

	if result.Status != engine.Cancelled || !errors.Is(result.Err, context.Canceled) || result.FailureOrigin != engine.FailureOriginCancelled {
		t.Fatalf("normalized result = %#v", result)
	}
}

func TestProviderContextErrorsPreserveFailedTerminal(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			result := normalizeCancelledEngineResult(context.Background(), engine.Result{Status: engine.Failed, FailureOrigin: engine.FailureOriginProvider, Err: cause})
			code, err := turnFailureCode(result.Status, result.Err, result.FailureOrigin)
			if err != nil || result.Status != engine.Failed || code != sessiontree.TurnFailureProvider || !errors.Is(result.Err, cause) {
				t.Fatalf("result=%+v code=%q classification=%v", result, code, err)
			}
			req := sessiontree.FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "outcome", Status: markerForStatus(result.Status), Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: code}, FailureMessage: result.Err.Error()}
			if err := sessiontree.ValidateFinishTurnRequest(req); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCancelledTerminalPrecedesContextCollectionFailure(t *testing.T) {
	code, err := turnFailureCode(engine.Cancelled, errors.Join(engine.ErrModelContextUnavailable, context.Canceled), engine.FailureOriginCancelled)
	if err != nil || code != sessiontree.TurnFailureCancelled {
		t.Fatalf("code=%q err=%v", code, err)
	}
	code, err = turnFailureCode(engine.Cancelled, errors.Join(sessiontree.ErrEffectOutcomeUnknown, context.Canceled), engine.FailureOriginCancelled)
	if err != nil || code != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("code=%q err=%v", code, err)
	}
}

func TestPreparationCancellationRequiresExecutionContext(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := statusForExecutionError(context.Background(), cause); got != engine.Failed {
			t.Fatalf("active execution status=%s", got)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := statusForExecutionError(ctx, cause); got != engine.Cancelled {
			t.Fatalf("cancelled execution status=%s", got)
		}
	}
}

func TestFinalizationFailureRetainsExecutionDiagnostics(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	original := engine.Result{Status: engine.Failed, FailureOrigin: engine.FailureOriginProvider, Err: context.DeadlineExceeded}
	result, err := thread.finalizeFailedTurn(t.Context(), "turn", "run", engine.Failed, context.DeadlineExceeded, "turn_finalization_error", engine.FailureOriginStorage, &original)
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != engine.Failed || result.FailureCode != sessiontree.TurnFailureStorage {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if result.Diagnostics["execution_failure_origin"] != string(engine.FailureOriginProvider) || result.Diagnostics["execution_error"] != original.Err.Error() {
		t.Fatalf("diagnostics=%v", result.Diagnostics)
	}
	meta, err := repo.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := repo.Entry(t.Context(), "thread", meta.LeafID)
	if err != nil || terminal.Metadata["execution_error"] != original.Err.Error() || terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureStorage {
		t.Fatalf("terminal=%+v err=%v", terminal, err)
	}
}

func TestExecutionCancellationIncludesInterruptedWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	original := engine.Result{Status: engine.Failed, FailureOrigin: engine.FailureOriginStorage, Err: context.Canceled}
	result := normalizeCancelledEngineResult(ctx, original)
	code, err := turnFailureCode(result.Status, result.Err, result.FailureOrigin)
	if err != nil || result.Status != engine.Cancelled || code != sessiontree.TurnFailureCancelled {
		t.Fatalf("result=%+v code=%s err=%v", result, code, err)
	}
}
