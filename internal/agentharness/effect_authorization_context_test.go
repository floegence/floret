package agentharness

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/tools"
)

type effectExecutionContextKey struct{}

func TestAuthorizedEffectUsesGateSelectedExecutionContext(t *testing.T) {
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		selected := context.WithValue(ctx, effectExecutionContextKey{}, "selected")
		return effect(selected, boundaryAuthorizationProof(req))
	})
	mustRegister(h.options.Tools, stringTool("shell", func(ctx context.Context, _ string) (string, error) {
		if got := ctx.Value(effectExecutionContextKey{}); got != "selected" {
			t.Fatalf("handler context value = %v, want selected", got)
		}
		return "ok", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if runErr != nil || result.Status != engine.Completed {
		t.Fatalf("run result=%#v err=%v, want completed", result, runErr)
	}
}

func TestAuthorizedEffectSelectedContextCancellationReachesHandler(t *testing.T) {
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	handlerStarted := make(chan struct{})
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		selected, cancelSelected := context.WithCancel(ctx)
		go func() {
			<-handlerStarted
			cancelSelected()
		}()
		return effect(selected, boundaryAuthorizationProof(req))
	})
	mustRegister(h.options.Tools, stringTool("shell", func(ctx context.Context, _ string) (string, error) {
		close(handlerStarted)
		<-ctx.Done()
		return "", context.Cause(ctx)
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if runErr != nil || result.Status != engine.Completed {
		t.Fatalf("run result=%#v err=%v, want provider recovery after canceled handler", result, runErr)
	}
}

func TestAuthorizedEffectRemainsBoundedByTurnContext(t *testing.T) {
	cancelCause := errors.New("host run closed")
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(context.Background(), boundaryAuthorizationProof(req))
	})
	handlerStarted := make(chan struct{})
	mustRegister(h.options.Tools, stringTool("shell", func(ctx context.Context, _ string) (string, error) {
		close(handlerStarted)
		<-ctx.Done()
		return "", context.Cause(ctx)
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancelCause(context.Background())
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, runErr := thread.Run(runCtx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: runErr}
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		cancelRun(cancelCause)
		t.Fatal("handler did not start")
	}
	cancelRun(cancelCause)
	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) || !errors.Is(out.err, cancelCause) || out.result.Status != engine.Cancelled {
			t.Fatalf("run result=%#v err=%v, want canceled", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn cancellation did not reach handler")
	}
}

func TestAuthorizedEffectCannotStartAfterTurnCancellation(t *testing.T) {
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(provider, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	gateEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		close(gateEntered)
		<-releaseGate
		return effect(context.Background(), boundaryAuthorizationProof(req))
	})
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		handlerCalls.Add(1)
		return "unexpected", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, runErr := thread.Run(runCtx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: runErr}
	}()
	select {
	case <-gateEntered:
	case <-time.After(time.Second):
		cancelRun()
		t.Fatal("authorization gate did not start")
	}
	cancelRun()
	close(releaseGate)
	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) || out.result.Status != engine.Cancelled {
			t.Fatalf("run result=%#v err=%v, want canceled", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled turn did not finish")
	}
	if handlerCalls.Load() != 0 {
		t.Fatalf("handler calls=%d, want zero", handlerCalls.Load())
	}
}

func TestAuthorizedEffectCannotStartWithCancelledSelectedContext(t *testing.T) {
	cancelCause := errors.New("execution admission closed")
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	probe := &effectBoundaryProbe{}
	repo := &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		selected, cancelSelected := context.WithCancelCause(context.Background())
		cancelSelected(cancelCause)
		return effect(selected, boundaryAuthorizationProof(req))
	})
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		handlerCalls.Add(1)
		return "unexpected", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if !errors.Is(runErr, context.Canceled) || !errors.Is(runErr, cancelCause) || result.Status != engine.Cancelled {
		t.Fatalf("run result=%#v err=%v, want canceled", result, runErr)
	}
	if handlerCalls.Load() != 0 || probe.rejectCalls.Load() != 0 {
		t.Fatalf("handler/reject calls=%d/%d, want zero", handlerCalls.Load(), probe.rejectCalls.Load())
	}
}

type approvalCancellationRepo interface {
	sessiontree.Repo
	Approval(context.Context, string) (sessiontree.ApprovalRecord, error)
	ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
}

type approvalCommitCancellationMemoryRepo struct {
	*sessiontree.MemoryRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *approvalCommitCancellationMemoryRepo) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	if err := contextCancellationError(ctx); err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	return r.MemoryRepo.CommitApprovalDispatch(ctx, req)
}

type approvalCommitCancellationSQLiteRepo struct {
	*sqlite.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *approvalCommitCancellationSQLiteRepo) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	if err := contextCancellationError(ctx); err != nil {
		return sessiontree.CommitApprovalDispatchResult{}, err
	}
	return r.Store.CommitApprovalDispatch(ctx, req)
}

type approvalCommitPanicMemoryRepo struct {
	*sessiontree.MemoryRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *approvalCommitPanicMemoryRepo) CommitApprovalDispatch(context.Context, sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	panic("approval commit before durable transition")
}

type approvalCommitPanicSQLiteRepo struct {
	*sqlite.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *approvalCommitPanicSQLiteRepo) CommitApprovalDispatch(context.Context, sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	panic("approval commit before durable transition")
}

func TestAuthorizedEffectCancelledSelectedContextCancelsApprovedBatch(t *testing.T) {
	stores := []struct {
		name string
		open func(*testing.T) (approvalCancellationRepo, func())
	}{
		{name: "memory", open: func(*testing.T) (approvalCancellationRepo, func()) {
			return sessiontree.NewMemoryRepo(), func() {}
		}},
		{name: "sqlite", open: func(t *testing.T) (approvalCancellationRepo, func()) {
			store, err := sqlite.Open(filepath.Join(t.TempDir(), "approval-context-cancel.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store, func() { _ = store.Close() }
		}},
	}
	for _, storeCase := range stores {
		t.Run(storeCase.name, func(t *testing.T) {
			cancelCause := errors.New("approved execution admission closed")
			repo, closeRepo := storeCase.open(t)
			defer closeRepo()
			provider := harness.NewScriptedProvider(
				harness.Step(harness.Tool("call-1", "write_file", `{"value":"x"}`), harness.DoneReason("tool_calls")),
			)
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				selected, cancelSelected := context.WithCancelCause(context.Background())
				cancelSelected(cancelCause)
				return effect(selected, boundaryAuthorizationProof(req))
			})
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, tools.Define[stringArgs](
				tools.Definition{
					Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
					Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
				}, nil, nil,
				func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
					handlerCalls.Add(1)
					return tools.Result{Text: "unexpected"}, nil
				},
			))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, runErr := thread.Run(context.Background(), "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: runErr}
			}()
			queue := waitForApprovalQueue(t, context.Background(), h, "thread")
			pending := queue.Approvals[0]
			if _, err := h.ResolveApproval(context.Background(), ResolveApprovalOptions{
				DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID,
				ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
				ExpectedCurrent: sessiontree.ApprovalIdentity{
					ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID,
					RunID: pending.RunID, ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
				},
				ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
			}); err != nil {
				t.Fatal(err)
			}
			var outcome struct {
				result TurnResult
				err    error
			}
			select {
			case outcome = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("approved cancellation did not finish")
			}
			if !errors.Is(outcome.err, context.Canceled) || !errors.Is(outcome.err, cancelCause) || outcome.result.Status != engine.Cancelled {
				t.Fatalf("run result=%#v err=%v, want canceled", outcome.result, outcome.err)
			}
			if handlerCalls.Load() != 0 {
				t.Fatalf("handler calls=%d, want zero", handlerCalls.Load())
			}
			approval, err := repo.Approval(context.Background(), pending.ApprovalID)
			if err != nil || approval.State != sessiontree.ApprovalCancelled {
				t.Fatalf("approval=%#v err=%v, want cancelled", approval, err)
			}
			after, err := h.ReadApprovalQueue(context.Background(), ReadApprovalQueueOptions{ThreadID: "thread"})
			if err != nil || len(after.Approvals) != 0 {
				t.Fatalf("approval queue=%#v err=%v, want empty", after, err)
			}
			if _, active, err := repo.ActiveTurnLease(context.Background(), "thread"); err != nil || active {
				t.Fatalf("active lease=%v err=%v, want released", active, err)
			}
			replay, replayErr := thread.Run(context.Background(), "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
			if replayErr == nil || !strings.Contains(replayErr.Error(), context.Canceled.Error()) ||
				!strings.Contains(replayErr.Error(), cancelCause.Error()) || replay.Status != engine.Cancelled || !replay.Replayed {
				t.Fatalf("replay result=%#v err=%v, want stable cancelled replay", replay, replayErr)
			}
			if handlerCalls.Load() != 0 {
				t.Fatalf("replay called handler: %d", handlerCalls.Load())
			}
		})
	}
}

func TestAuthorizedEffectParentCancellationDuringApprovalCommitCancelsBatch(t *testing.T) {
	stores := []struct {
		name string
		open func(*testing.T, int) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func())
	}{
		{name: "memory", open: func(*testing.T, int) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func()) {
			entered := make(chan struct{})
			release := make(chan struct{})
			return &approvalCommitCancellationMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), entered: entered, release: release}, entered, release, func() {}
		}},
		{name: "sqlite", open: func(t *testing.T, iteration int) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func()) {
			store, err := sqlite.Open(filepath.Join(t.TempDir(), fmt.Sprintf("approval-commit-cancel-%d.db", iteration)))
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			return &approvalCommitCancellationSQLiteRepo{Store: store, entered: entered, release: release}, entered, release, func() { _ = store.Close() }
		}},
	}
	for _, storeCase := range stores {
		t.Run(storeCase.name, func(t *testing.T) {
			for iteration := range 10 {
				repo, commitEntered, releaseCommit, closeRepo := storeCase.open(t, iteration)
				func() {
					defer closeRepo()
					cancelCause := errors.New("turn canceled during approval commit")
					provider := harness.NewScriptedProvider(
						harness.Step(harness.Tool("call-1", "write_file", `{"value":"x"}`), harness.DoneReason("tool_calls")),
					)
					h := newTestHarness(provider, repo, cache.NewMemoryStore())
					h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
						return effect(ctx, boundaryAuthorizationProof(req))
					})
					var handlerCalls atomic.Int64
					mustRegister(h.options.Tools, tools.Define[stringArgs](
						tools.Definition{
							Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
							Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
						}, nil, nil,
						func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
							handlerCalls.Add(1)
							return tools.Result{Text: "unexpected"}, nil
						},
					))
					threadID := fmt.Sprintf("thread-%d", iteration)
					thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: threadID})
					if err != nil {
						t.Fatal(err)
					}
					runCtx, cancelRun := context.WithCancelCause(context.Background())
					done := make(chan struct {
						result TurnResult
						err    error
					}, 1)
					go func() {
						result, runErr := thread.Run(runCtx, "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
						done <- struct {
							result TurnResult
							err    error
						}{result: result, err: runErr}
					}()
					queue := waitForApprovalQueue(t, context.Background(), h, threadID)
					pending := queue.Approvals[0]
					if _, err := h.ResolveApproval(context.Background(), ResolveApprovalOptions{
						DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID,
						ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
						ExpectedCurrent: sessiontree.ApprovalIdentity{
							ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID,
							RunID: pending.RunID, ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
						},
						ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
					}); err != nil {
						t.Fatal(err)
					}
					select {
					case <-commitEntered:
					case <-time.After(time.Second):
						t.Fatal("approval commit did not start")
					}
					cancelRun(cancelCause)
					close(releaseCommit)
					select {
					case outcome := <-done:
						if !errors.Is(outcome.err, context.Canceled) || !errors.Is(outcome.err, cancelCause) || outcome.result.Status != engine.Cancelled {
							t.Fatalf("iteration %d result=%#v err=%v, want canceled", iteration, outcome.result, outcome.err)
						}
					case <-time.After(2 * time.Second):
						t.Fatalf("iteration %d did not finish", iteration)
					}
					approval, err := repo.Approval(context.Background(), pending.ApprovalID)
					if err != nil || approval.State != sessiontree.ApprovalCancelled {
						t.Fatalf("iteration %d approval=%#v err=%v, want cancelled", iteration, approval, err)
					}
					after, err := h.ReadApprovalQueue(context.Background(), ReadApprovalQueueOptions{ThreadID: threadID})
					if err != nil || len(after.Approvals) != 0 {
						t.Fatalf("iteration %d queue=%#v err=%v, want empty", iteration, after, err)
					}
					if handlerCalls.Load() != 0 {
						t.Fatalf("iteration %d handler calls=%d, want zero", iteration, handlerCalls.Load())
					}
					if _, active, err := repo.ActiveTurnLease(context.Background(), threadID); err != nil || active {
						t.Fatalf("iteration %d active lease=%v err=%v, want released", iteration, active, err)
					}
				}()
			}
		})
	}
}

func TestAuthorizedEffectApprovalCommitPanicCannotLeaveVisibleApproval(t *testing.T) {
	stores := []struct {
		name string
		open func(*testing.T) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func())
	}{
		{name: "memory", open: func(*testing.T) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func()) {
			entered := make(chan struct{})
			release := make(chan struct{})
			return &approvalCommitPanicMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), entered: entered, release: release}, entered, release, func() {}
		}},
		{name: "sqlite", open: func(t *testing.T) (approvalCancellationRepo, <-chan struct{}, chan struct{}, func()) {
			store, err := sqlite.Open(filepath.Join(t.TempDir(), "approval-commit-panic.db"))
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			return &approvalCommitPanicSQLiteRepo{Store: store, entered: entered, release: release}, entered, release, func() { _ = store.Close() }
		}},
	}
	for _, storeCase := range stores {
		t.Run(storeCase.name, func(t *testing.T) {
			repo, commitEntered, releaseCommit, closeRepo := storeCase.open(t)
			defer closeRepo()
			provider := harness.NewScriptedProvider(
				harness.Step(harness.Tool("call-1", "write_file", `{"value":"x"}`), harness.DoneReason("tool_calls")),
			)
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, boundaryAuthorizationProof(req))
			})
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, tools.Define[stringArgs](
				tools.Definition{
					Name: "write_file", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
					Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}, Effects: []tools.Effect{tools.EffectWrite},
				}, nil, nil,
				func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
					handlerCalls.Add(1)
					return tools.Result{Text: "unexpected"}, nil
				},
			))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancelRun := context.WithCancelCause(context.Background())
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, runErr := thread.Run(runCtx, "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: runErr}
			}()
			queue := waitForApprovalQueue(t, context.Background(), h, "thread")
			pending := queue.Approvals[0]
			if _, err := h.ResolveApproval(context.Background(), ResolveApprovalOptions{
				DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID,
				ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
				ExpectedCurrent: sessiontree.ApprovalIdentity{
					ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID,
					RunID: pending.RunID, ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
				},
				ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-commitEntered:
			case <-time.After(time.Second):
				t.Fatal("approval commit did not start")
			}
			cancelRun(errors.New("turn canceled during approval commit panic"))
			close(releaseCommit)
			var outcome struct {
				result TurnResult
				err    error
			}
			select {
			case outcome = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("approval commit panic did not finish")
			}
			if outcome.err == nil || (outcome.result.Status != engine.Failed && outcome.result.Status != engine.Cancelled) {
				t.Fatalf("run result=%#v err=%v, want terminal failure or cancellation", outcome.result, outcome.err)
			}
			approval, err := repo.Approval(context.Background(), pending.ApprovalID)
			if err != nil || sessiontree.ApprovalQueueVisible(approval.State) {
				t.Fatalf("approval=%#v err=%v, want non-visible terminal state", approval, err)
			}
			after, err := h.ReadApprovalQueue(context.Background(), ReadApprovalQueueOptions{ThreadID: "thread"})
			if err != nil || len(after.Approvals) != 0 {
				t.Fatalf("approval queue=%#v err=%v, want empty", after, err)
			}
			if handlerCalls.Load() != 0 {
				t.Fatalf("handler calls=%d, want zero", handlerCalls.Load())
			}
			if _, active, err := repo.ActiveTurnLease(context.Background(), "thread"); err != nil || active {
				t.Fatalf("active lease=%v err=%v, want released", active, err)
			}
			journal, err := thread.Journal(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			terminalCount := 0
			for _, entry := range journal.Entries {
				if entry.Type == sessiontree.EntryTurnMarker && isTerminalTurnMarker(entry.TurnStatus) {
					terminalCount++
				}
			}
			if terminalCount != 1 {
				t.Fatalf("terminal markers=%d, want one", terminalCount)
			}
			replay, replayErr := thread.Run(context.Background(), "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
			if replayErr == nil || !replay.Replayed || replay.Status != outcome.result.Status || replay.FailureCode != outcome.result.FailureCode {
				t.Fatalf("replay result=%#v err=%v, want canonical %#v", replay, replayErr, outcome.result)
			}
			if handlerCalls.Load() != 0 {
				t.Fatalf("replay called handler: %d", handlerCalls.Load())
			}
			replayedJournal, err := thread.Journal(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			replayedTerminalCount := 0
			for _, entry := range replayedJournal.Entries {
				if entry.Type == sessiontree.EntryTurnMarker && isTerminalTurnMarker(entry.TurnStatus) {
					replayedTerminalCount++
				}
			}
			if replayedTerminalCount != 1 {
				t.Fatalf("terminal markers after replay=%d, want one", replayedTerminalCount)
			}
		})
	}
}
