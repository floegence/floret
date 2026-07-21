package agentharness

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
)

func TestTurnFailureCodeIsDeterministic(t *testing.T) {
	tests := []struct {
		name   string
		status engine.Status
		err    error
		origin engine.FailureOrigin
		want   string
	}{
		{name: "cancelled", status: engine.Cancelled, err: context.Canceled, want: sessiontree.TurnFailureCancelled},
		{name: "effect unknown", status: engine.Failed, err: sessiontree.ErrEffectOutcomeUnknown, want: sessiontree.TurnFailureEffectOutcomeUnknown},
		{name: "authorization unavailable", status: engine.Failed, err: ErrAuthorizationUnavailable, want: sessiontree.TurnFailureAuthorizationUnavailable},
		{name: "authorization contract", status: engine.Failed, err: ErrInvalidAuthorizationProof, want: sessiontree.TurnFailureAuthorizationContract},
		{name: "committed effect", status: engine.Failed, err: &CommittedEffectError{EffectAttemptID: "effect", Err: errors.New("commit failed")}, want: sessiontree.TurnFailureToolDispatch},
		{name: "storage", status: engine.Failed, err: errors.New("read failed"), origin: engine.FailureOriginStorage, want: sessiontree.TurnFailureStorage},
		{name: "provider", status: engine.Failed, err: errors.New("provider failed"), origin: engine.FailureOriginProvider, want: sessiontree.TurnFailureProvider},
		{name: "tool dispatch", status: engine.Failed, err: errors.New("dispatch failed"), origin: engine.FailureOriginToolDispatch, want: sessiontree.TurnFailureToolDispatch},
		{name: "engine contract", status: engine.Failed, err: engine.ErrDuplicateToolCallID, origin: engine.FailureOriginContract, want: sessiontree.TurnFailureEngineContract},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := turnFailureCode(test.status, test.err, test.origin)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("turnFailureCode()=%q, want %q", got, test.want)
			}
		})
	}
}

func TestTurnFailureCodeRejectsMissingOrInvalidOrigin(t *testing.T) {
	for _, origin := range []engine.FailureOrigin{engine.FailureOriginNone, engine.FailureOrigin("future_origin")} {
		if _, err := turnFailureCode(engine.Failed, errors.New("unclassified"), origin); err == nil {
			t.Fatalf("origin %q was accepted", origin)
		}
	}
}

type invalidLeasePolicyRepo struct {
	*sessiontree.MemoryRepo
}

func (*invalidLeasePolicyRepo) AuthorityLeasePolicy() sessiontree.LeasePolicy {
	return sessiontree.LeasePolicy{}
}

type invalidLeasePolicyFinishFailureRepo struct {
	*invalidLeasePolicyRepo
	finishErr error
}

func (r *invalidLeasePolicyFinishFailureRepo) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	if req.Metadata["diagnostic"] == "lease_renewal_start_error" {
		return sessiontree.FinishTurnResult{}, r.finishErr
	}
	return r.MemoryRepo.FinishTurn(ctx, req)
}

func TestRunFinalizesTypedFailureWhenLeaseRenewalCannotStart(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &invalidLeasePolicyRepo{MemoryRepo: base}
	h := newTestHarness(
		scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done())),
		repo,
		cache.NewMemoryStore(),
	)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(ctx, "inspect", RunOptions{TurnID: "turn", RunID: "run"})
	assertStartupFailureTerminal(t, base, result, runErr, "thread", "turn", "run", "lease_renewal_start_error", "lease TTL must be positive")
}

func TestRunReturnsStartupAndFinalizationErrorsWhenTerminalCannotPersist(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &invalidLeasePolicyFinishFailureRepo{
		invalidLeasePolicyRepo: &invalidLeasePolicyRepo{MemoryRepo: base},
		finishErr:              errors.New("injected turn finish failure"),
	}
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := thread.Run(ctx, "inspect", RunOptions{TurnID: "turn", RunID: "run"})
	if runErr == nil || !strings.Contains(runErr.Error(), "lease TTL must be positive") || !strings.Contains(runErr.Error(), repo.finishErr.Error()) {
		t.Fatalf("startup finalization error = %v", runErr)
	}
	if result.Status != "" || result.FailureCode != "" {
		t.Fatalf("uncommitted terminal returned a canonical result: %#v", result)
	}
	if lease, active, err := base.ActiveTurnLease(ctx, "thread"); err != nil || !active || lease.TurnID != "turn" {
		t.Fatalf("uncommitted terminal did not preserve recoverable lease: lease=%#v active=%v err=%v", lease, active, err)
	}
}

func TestRetryFinalizesTypedFailureWhenLeaseRenewalCannotStart(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	initial := newTestHarness(
		scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done())),
		base,
		cache.NewMemoryStore(),
	)
	thread, err := initial.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "inspect", RunOptions{TurnID: "initial-turn", RunID: "initial-run"}); err != nil {
		t.Fatal(err)
	}

	repo := &invalidLeasePolicyRepo{MemoryRepo: base}
	h := newTestHarness(
		scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done())),
		repo,
		cache.NewMemoryStore(),
	)
	resumed, err := h.ResumeThread(ctx, "thread", ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, retryErr := resumed.Retry(ctx, RetryOptions{Reason: "test"})
	if strings.TrimSpace(result.ID) == "" || strings.TrimSpace(result.RunID) == "" {
		t.Fatalf("retry startup failure lost execution identity: %#v", result)
	}
	assertStartupFailureTerminal(t, base, result, retryErr, "thread", result.ID, result.RunID, "lease_renewal_start_error", "lease TTL must be positive")
}

func TestPendingToolCompletionFinalizesTypedFailureWhenLeaseRenewalCannotStart(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &invalidLeasePolicyRepo{MemoryRepo: base}
	h := newTestHarness(
		scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done())),
		repo,
		cache.NewMemoryStore(),
	)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, base, "thread", "turn-1")
	result, completionErr := thread.CompletePendingTool(ctx, PendingToolCompletion{
		CompletionRequestID: "completion",
		Target: sessiontree.PendingToolSettlementTarget{
			ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		},
		ContinuationTurnID: "continuation-turn", ContinuationRunID: "continuation-run",
		Status: PendingToolCompleted, Summary: "done", Input: session.Message{Role: session.User, Content: "background work completed"},
	})
	assertStartupFailureTerminal(t, base, result, completionErr, "thread", "continuation-turn", "continuation-run", "lease_renewal_start_error", "lease TTL must be positive")
}

func TestSubAgentFinalizesTypedFailureWhenLeaseRenewalCannotStart(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &invalidLeasePolicyRepo{MemoryRepo: base}
	provider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done()))
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		PublicationID: "publication", ParentThreadID: "parent", ParentTurnID: "parent-turn",
		ThreadID: "child", TaskName: "worker", Message: "inspect", ForkMode: SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusFailed {
		t.Fatalf("subagent startup outcome = %#v", waited)
	}
	detail, err := h.ReadSubAgentDetail(ctx, ReadSubAgentDetailOptions{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	terminal := firstSubAgentDetailEvent(detail.Events, SubAgentDetailEventTurnMarker)
	for _, event := range detail.Events {
		if event.Kind == SubAgentDetailEventTurnMarker && event.TurnMarker != nil && event.TurnMarker.Status == string(sessiontree.TurnFailed) {
			terminal = event
		}
	}
	if terminal.TurnMarker == nil || terminal.TurnMarker.Status != string(sessiontree.TurnFailed) ||
		terminal.TurnMarker.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureEngineContract ||
		terminal.TurnMarker.Metadata["diagnostic"] != "lease_renewal_start_error" {
		t.Fatalf("subagent startup terminal = %#v", terminal)
	}
	if len(provider.Requests) != 0 {
		t.Fatalf("subagent provider ran after startup failure: %#v", provider.Requests)
	}
}

func TestSubAgentReportsBackgroundErrorWhenStartupFailureCannotFinalize(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	repo := &invalidLeasePolicyFinishFailureRepo{
		invalidLeasePolicyRepo: &invalidLeasePolicyRepo{MemoryRepo: base},
		finishErr:              errors.New("injected turn finish failure"),
	}
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	backgroundErrors := make(chan error, 1)
	h.options.ReportBackgroundError = func(err error) { backgroundErrors <- err }
	if _, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SpawnSubAgent(ctx, SpawnSubAgentOptions{
		PublicationID: "publication", ParentThreadID: "parent", ParentTurnID: "parent-turn",
		ThreadID: "child", TaskName: "worker", Message: "inspect", ForkMode: SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = h.WaitSubAgents(ctx, WaitSubAgentsOptions{ParentThreadID: "parent", ChildThreadIDs: []string{"child"}, Timeout: time.Second})
	}()
	select {
	case err := <-backgroundErrors:
		if !strings.Contains(err.Error(), "lease TTL must be positive") || !strings.Contains(err.Error(), repo.finishErr.Error()) {
			t.Fatalf("background error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subagent startup finalization failure was not reported")
	}
}

func TestRunFinalizesTypedFailureWhenExecutionRegistryRejectsLease(t *testing.T) {
	ctx := context.Background()
	base := sessiontree.NewMemoryRepo()
	h := newTestHarness(
		scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done())),
		base,
		cache.NewMemoryStore(),
	)
	h.options.TurnExecutions = &TurnExecutionRegistry{
		Register:   func(sessiontree.TurnLease) error { return errors.New("injected execution registry bind failure") },
		Renew:      func(sessiontree.TurnLease, sessiontree.TurnLease) error { return nil },
		Unregister: func(sessiontree.TurnLease) {},
		Active:     func(string) (sessiontree.TurnLease, bool) { return sessiontree.TurnLease{}, false },
	}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(ctx, "inspect", RunOptions{TurnID: "turn", RunID: "run"})
	assertStartupFailureTerminal(t, base, result, runErr, "thread", "turn", "run", "local_owner_bind_error", "injected execution registry bind failure")
}

func TestTurnAdmissionReplayRejectsInvalidTerminalFailureMatrix(t *testing.T) {
	ctx := context.Background()
	thread := New(Options{Repo: sessiontree.NewMemoryRepo()}).cacheThread("thread")
	tests := []struct {
		name      string
		status    sessiontree.TurnMarkerStatus
		code      string
		failure   *sessiontree.Entry
		markerRun string
	}{
		{name: "failed without failure", status: sessiontree.TurnFailed, code: sessiontree.TurnFailureProvider, markerRun: "run"},
		{name: "failed with cancelled code", status: sessiontree.TurnFailed, code: sessiontree.TurnFailureCancelled, markerRun: "run", failure: replayFailureEntry("provider failed")},
		{name: "completed with failure", status: sessiontree.TurnCompleted, markerRun: "run", failure: replayFailureEntry("unexpected failure")},
		{name: "terminal run mismatch", status: sessiontree.TurnFailed, code: sessiontree.TurnFailureProvider, markerRun: "other-run", failure: replayFailureEntry("provider failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := thread.turnAdmissionReplayResult(ctx, sessiontree.AdmitTurnResult{Terminal: &sessiontree.TurnTerminalOutcome{
				Failure: test.failure,
				Terminal: sessiontree.Entry{
					ID: "terminal", ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryTurnMarker, TurnStatus: test.status,
					Metadata: map[string]string{"run_id": test.markerRun, sessiontree.TurnFailureCodeMetadataKey: test.code},
				},
			}}, "turn", "run")
			if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("replay error = %v, want authority corruption", err)
			}
		})
	}
}

func replayFailureEntry(message string) *sessiontree.Entry {
	return &sessiontree.Entry{
		ID: "failure", ParentID: "terminal-parent", ThreadID: "thread", TurnID: "turn", Type: sessiontree.EntryRunFailure, Error: message,
	}
}

func assertStartupFailureTerminal(
	t *testing.T,
	repo *sessiontree.MemoryRepo,
	result TurnResult,
	runErr error,
	threadID, turnID, runID, diagnostic, message string,
) {
	t.Helper()
	if runErr == nil || !strings.Contains(runErr.Error(), message) {
		t.Fatalf("startup error = %v, want %q", runErr, message)
	}
	if result.ID != turnID || result.RunID != runID || result.Status != engine.Failed || result.FailureCode != sessiontree.TurnFailureEngineContract {
		t.Fatalf("startup result = %#v", result)
	}
	admission, found, err := repo.ReadTurnAdmission(context.Background(), threadID, turnID, runID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || admission.Terminal == nil || admission.Terminal.Terminal.TurnStatus != sessiontree.TurnFailed {
		t.Fatalf("startup admission terminal = %#v found=%v", admission.Terminal, found)
	}
	if admission.Terminal.Terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureEngineContract ||
		admission.Terminal.Terminal.Metadata["diagnostic"] != diagnostic ||
		admission.Terminal.Failure == nil || admission.Terminal.Failure.Error != message {
		t.Fatalf("startup terminal = %#v", admission.Terminal)
	}
	if _, active, err := repo.ActiveTurnLease(context.Background(), threadID); err != nil || active {
		t.Fatalf("startup failure retained active lease: active=%v err=%v", active, err)
	}
}
