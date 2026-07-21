package agentharness

import (
	"context"
	"errors"
	"path/filepath"
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

type effectBoundaryProbe struct {
	rejectCalls  atomic.Int64
	markCalls    atomic.Int64
	finishCalls  atomic.Int64
	state        atomic.Value
	beginPanic   string
	markObserved chan struct{}
	markOnce     sync.Once
}

type effectBoundaryMemoryRepo struct {
	*sessiontree.MemoryRepo
	probe *effectBoundaryProbe
}

type effectBoundarySQLiteRepo struct {
	*sqlite.Store
	probe *effectBoundaryProbe
}

type approvalReplayBoundaryMemoryRepo struct {
	*effectBoundaryMemoryRepo
	commitReached chan struct{}
	releaseCommit chan struct{}
	commitOnce    sync.Once
}

type approvalReplayBoundarySQLiteRepo struct {
	*effectBoundarySQLiteRepo
	commitReached chan struct{}
	releaseCommit chan struct{}
	commitOnce    sync.Once
}

func (r *approvalReplayBoundaryMemoryRepo) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	result, err := r.MemoryRepo.CommitApprovalDispatch(ctx, req)
	if err != nil {
		return result, err
	}
	result.Replayed = true
	r.commitOnce.Do(func() { close(r.commitReached) })
	<-r.releaseCommit
	return result, nil
}

func (r *approvalReplayBoundarySQLiteRepo) CommitApprovalDispatch(ctx context.Context, req sessiontree.CommitApprovalDispatchRequest) (sessiontree.CommitApprovalDispatchResult, error) {
	result, err := r.Store.CommitApprovalDispatch(ctx, req)
	if err != nil {
		return result, err
	}
	result.Replayed = true
	r.commitOnce.Do(func() { close(r.commitReached) })
	<-r.releaseCommit
	return result, nil
}

func (r *effectBoundaryMemoryRepo) RejectEffectAttempt(ctx context.Context, req sessiontree.RejectEffectAttemptRequest) (sessiontree.EffectAttempt, error) {
	r.probe.rejectCalls.Add(1)
	attempt, err := r.MemoryRepo.RejectEffectAttempt(ctx, req)
	r.probe.record(attempt, err)
	return attempt, err
}

func (r *effectBoundaryMemoryRepo) BeginEffectDispatch(ctx context.Context, req sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error) {
	if r.probe.beginPanic == "before" {
		panic("begin before commit")
	}
	attempt, err := r.MemoryRepo.BeginEffectDispatch(ctx, req)
	if r.probe.beginPanic == "after" {
		panic("begin after commit")
	}
	return attempt, err
}

func (r *effectBoundaryMemoryRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.probe.finishCalls.Add(1)
	result, err := r.MemoryRepo.FinishEffectDispatch(ctx, req)
	r.probe.record(result.Attempt, err)
	return result, err
}

func (r *effectBoundaryMemoryRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.probe.markCalls.Add(1)
	attempt, err := r.MemoryRepo.MarkEffectUnknown(ctx, req)
	r.probe.recordMark(attempt, err)
	return attempt, err
}

func (r *effectBoundarySQLiteRepo) RejectEffectAttempt(ctx context.Context, req sessiontree.RejectEffectAttemptRequest) (sessiontree.EffectAttempt, error) {
	r.probe.rejectCalls.Add(1)
	attempt, err := r.Store.RejectEffectAttempt(ctx, req)
	r.probe.record(attempt, err)
	return attempt, err
}

func (r *effectBoundarySQLiteRepo) BeginEffectDispatch(ctx context.Context, req sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error) {
	if r.probe.beginPanic == "before" {
		panic("begin before commit")
	}
	attempt, err := r.Store.BeginEffectDispatch(ctx, req)
	if r.probe.beginPanic == "after" {
		panic("begin after commit")
	}
	return attempt, err
}

func (r *effectBoundarySQLiteRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.probe.finishCalls.Add(1)
	result, err := r.Store.FinishEffectDispatch(ctx, req)
	r.probe.record(result.Attempt, err)
	return result, err
}

func (r *effectBoundarySQLiteRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.probe.markCalls.Add(1)
	attempt, err := r.Store.MarkEffectUnknown(ctx, req)
	r.probe.recordMark(attempt, err)
	return attempt, err
}

func (p *effectBoundaryProbe) record(attempt sessiontree.EffectAttempt, err error) {
	if err == nil && attempt.State != "" {
		p.state.Store(attempt.State)
	}
}

func (p *effectBoundaryProbe) recordMark(attempt sessiontree.EffectAttempt, err error) {
	p.record(attempt, err)
	if p.markObserved != nil {
		p.markOnce.Do(func() { close(p.markObserved) })
	}
}

func TestEffectPanicBoundariesConvergeFromDurableAttemptState(t *testing.T) {
	type storeFactory struct {
		name string
		open func(*testing.T, *effectBoundaryProbe) (sessiontree.Repo, func())
	}
	stores := []storeFactory{
		{
			name: "memory",
			open: func(_ *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				return &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "effect-boundary.db"))
				if err != nil {
					t.Fatal(err)
				}
				return &effectBoundarySQLiteRepo{Store: store, probe: probe}, func() { _ = store.Close() }
			},
		},
	}
	tests := []struct {
		name         string
		beginPanic   string
		gate         func(EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error)
		handler      func()
		wantCode     string
		wantState    sessiontree.EffectAttemptState
		wantHandlers int64
		wantRejects  int64
		wantMarks    int64
		wantFinishes int64
	}{
		{
			name: "gate panic before dispatch", gate: func(EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error) {
				panic("gate before dispatch")
			},
			wantCode: sessiontree.TurnFailureAuthorizationContract, wantState: sessiontree.EffectAttemptRejected, wantRejects: 1,
		},
		{
			name: "callback panic before dispatch", beginPanic: "before", gate: invokeBoundaryEffect,
			wantCode: sessiontree.TurnFailureAuthorizationContract, wantState: sessiontree.EffectAttemptRejected, wantRejects: 1,
		},
		{
			name: "callback panic after dispatch", beginPanic: "after", gate: invokeBoundaryEffect,
			wantCode: sessiontree.TurnFailureEffectOutcomeUnknown, wantState: sessiontree.EffectAttemptUnknown, wantMarks: 1,
		},
		{
			name: "handler panic after dispatch", gate: invokeBoundaryEffect, handler: func() { panic("handler after dispatch") },
			wantCode: sessiontree.TurnFailureEffectOutcomeUnknown, wantState: sessiontree.EffectAttemptUnknown, wantHandlers: 1, wantMarks: 1,
		},
		{
			name: "gate panic after known completion", gate: func(req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				result, err := effect(boundaryAuthorizationProof(req))
				if err != nil {
					return EffectDispatchResult{}, err
				}
				_ = result
				panic("gate after completion")
			},
			wantState: sessiontree.EffectAttemptCompleted, wantHandlers: 1, wantFinishes: 1,
		},
	}
	for _, store := range stores {
		for _, test := range tests {
			t.Run(store.name+"/"+test.name, func(t *testing.T) {
				probe := &effectBoundaryProbe{beginPanic: test.beginPanic}
				repo, closeRepo := store.open(t, probe)
				defer closeRepo()
				provider := harness.NewScriptedProvider(
					harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
					harness.Step(harness.Text("done"), harness.Done()),
				)
				h := newTestHarness(provider, repo, cache.NewMemoryStore())
				h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
					return test.gate(req, effect)
				})
				var handlerCalls atomic.Int64
				mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
					handlerCalls.Add(1)
					if test.handler != nil {
						test.handler()
					}
					return "known effect", nil
				}))
				thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
				if err != nil {
					t.Fatal(err)
				}

				result, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				if test.wantCode == "" {
					if runErr != nil || result.Status != engine.Completed {
						t.Fatalf("run result=%#v err=%v, want completed known effect", result, runErr)
					}
				} else if runErr == nil || result.Status != engine.Failed || result.FailureCode != test.wantCode {
					t.Fatalf("run result=%#v err=%v, want failed %s", result, runErr, test.wantCode)
				}
				if handlerCalls.Load() != test.wantHandlers || probe.rejectCalls.Load() != test.wantRejects || probe.markCalls.Load() != test.wantMarks || probe.finishCalls.Load() != test.wantFinishes {
					t.Fatalf("calls handler/reject/mark/finish=%d/%d/%d/%d, want %d/%d/%d/%d", handlerCalls.Load(), probe.rejectCalls.Load(), probe.markCalls.Load(), probe.finishCalls.Load(), test.wantHandlers, test.wantRejects, test.wantMarks, test.wantFinishes)
				}
				if state, _ := probe.state.Load().(sessiontree.EffectAttemptState); state != test.wantState {
					t.Fatalf("effect state=%q, want %q", state, test.wantState)
				}
				assertEffectBoundaryTerminalAndReplay(t, thread, repo, "run", result.FailureCode, &handlerCalls, test.wantHandlers)
			})
		}
	}
}

func TestAsyncEffectCallbackEarlyReturnConvergesAndExits(t *testing.T) {
	tests := []struct {
		name         string
		dispatch     bool
		wantCode     string
		wantHandlers int64
		wantRejects  int64
		wantMarks    int64
	}{
		{name: "before dispatch", wantCode: sessiontree.TurnFailureAuthorizationContract, wantRejects: 1},
		{name: "after dispatch", dispatch: true, wantCode: sessiontree.TurnFailureEffectOutcomeUnknown, wantHandlers: 1, wantMarks: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &effectBoundaryProbe{}
			if test.dispatch {
				probe.markObserved = make(chan struct{})
			}
			repo := &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}
			provider := harness.NewScriptedProvider(harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")))
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			callbackRelease := make(chan struct{})
			callbackDone := make(chan struct{})
			handlerStarted := make(chan struct{})
			handlerRelease := make(chan struct{})
			var handlerCalls atomic.Int64
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				go func() {
					defer close(callbackDone)
					<-callbackRelease
					_, _ = effect(boundaryAuthorizationProof(req))
				}()
				if test.dispatch {
					close(callbackRelease)
					<-handlerStarted
				}
				return EffectDispatchResult{}, nil
			})
			mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
				handlerCalls.Add(1)
				close(handlerStarted)
				<-handlerRelease
				return "side effect", nil
			}))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, err := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: err}
			}()
			if test.dispatch {
				select {
				case <-probe.markObserved:
				case <-time.After(time.Second):
					t.Fatal("early-return gate did not converge the dispatched effect")
				}
				close(handlerRelease)
			} else {
				close(callbackRelease)
				close(handlerRelease)
			}
			select {
			case out := <-done:
				if out.err == nil || out.result.Status != engine.Failed || out.result.FailureCode != test.wantCode {
					t.Fatalf("run result=%#v err=%v, want failed %s", out.result, out.err, test.wantCode)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("run did not finish after asynchronous gate contract failure")
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("asynchronous effect callback did not exit")
			}
			if handlerCalls.Load() != test.wantHandlers || probe.rejectCalls.Load() != test.wantRejects || probe.markCalls.Load() != test.wantMarks {
				t.Fatalf("calls handler/reject/mark=%d/%d/%d, want %d/%d/%d", handlerCalls.Load(), probe.rejectCalls.Load(), probe.markCalls.Load(), test.wantHandlers, test.wantRejects, test.wantMarks)
			}
			thread.effectFinalizeMu.Lock()
			finalizers := len(thread.effectFinalizers)
			thread.effectFinalizeMu.Unlock()
			if finalizers != 0 {
				t.Fatalf("effect finalizers=%d, want 0", finalizers)
			}
		})
	}
}

func TestGateEarlyReturnAfterFinalizerRegistrationSettlesBeforeOuterReturn(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T, *effectBoundaryProbe) (sessiontree.Repo, func())
	}{
		{
			name: "memory",
			open: func(_ *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				return &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "registered-finalizer.db"))
				if err != nil {
					t.Fatal(err)
				}
				return &effectBoundarySQLiteRepo{Store: store, probe: probe}, func() { _ = store.Close() }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &effectBoundaryProbe{markObserved: make(chan struct{})}
			repo, closeRepo := test.open(t, probe)
			defer closeRepo()
			provider := harness.NewScriptedProvider(harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")))
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			registered := make(chan struct{})
			releaseRegistered := make(chan struct{})
			var registeredOnce sync.Once
			h.effectFinalizerRegistration = func(err error) {
				if err != nil {
					t.Errorf("effect finalizer registration failed: %v", err)
					return
				}
				registeredOnce.Do(func() { close(registered) })
				<-releaseRegistered
			}
			callbackDone := make(chan struct{})
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				go func() {
					defer close(callbackDone)
					_, _ = effect(boundaryAuthorizationProof(req))
				}()
				<-registered
				return EffectDispatchResult{}, nil
			})
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
				handlerCalls.Add(1)
				return "side effect", nil
			}))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, err := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: err}
			}()
			select {
			case <-probe.markObserved:
			case <-time.After(time.Second):
				t.Fatal("gate early-return did not converge the registered effect")
			}
			select {
			case out := <-done:
				t.Fatalf("outer returned before registered callback settled: result=%#v err=%v", out.result, out.err)
			default:
			}
			close(releaseRegistered)
			var out struct {
				result TurnResult
				err    error
			}
			select {
			case out = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("run did not finish after registered callback settled")
			}
			if out.err == nil || out.result.Status != engine.Failed || out.result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
				t.Fatalf("run result=%#v err=%v, want failed effect_outcome_unknown", out.result, out.err)
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("registered callback did not exit")
			}
			thread.effectFinalizeMu.Lock()
			finalizers := len(thread.effectFinalizers)
			thread.effectFinalizeMu.Unlock()
			if finalizers != 0 {
				t.Fatalf("effect finalizers=%d, want 0", finalizers)
			}
			if probe.markCalls.Load() != 1 || handlerCalls.Load() != 1 {
				t.Fatalf("calls mark/handler=%d/%d, want 1/1", probe.markCalls.Load(), handlerCalls.Load())
			}
			assertEffectBoundaryTerminalAndReplay(t, thread, repo, "run", sessiontree.TurnFailureEffectOutcomeUnknown, &handlerCalls, 1)
		})
	}
}

func TestRegisterFinalizerFailureAndGateEarlyReturnShareUnknownOwner(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T, *effectBoundaryProbe) (sessiontree.Repo, func())
	}{
		{
			name: "memory",
			open: func(_ *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				return &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "registration-failure.db"))
				if err != nil {
					t.Fatal(err)
				}
				return &effectBoundarySQLiteRepo{Store: store, probe: probe}, func() { _ = store.Close() }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &effectBoundaryProbe{markObserved: make(chan struct{})}
			repo, closeRepo := test.open(t, probe)
			defer closeRepo()
			provider := harness.NewScriptedProvider(harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")))
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			registrationFailed := make(chan struct{})
			releaseFailure := make(chan struct{})
			var failureOnce sync.Once
			h.effectFinalizerRegistration = func(err error) {
				if err == nil {
					return
				}
				failureOnce.Do(func() { close(registrationFailed) })
				<-releaseFailure
			}
			callbackDone := make(chan struct{})
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				go func() {
					defer close(callbackDone)
					_, _ = effect(boundaryAuthorizationProof(req))
				}()
				<-registrationFailed
				return EffectDispatchResult{}, nil
			})
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
				handlerCalls.Add(1)
				return "side effect", nil
			}))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			key := effectFinalizerKey("run-1", "turn-1", "call-1")
			if err := thread.registerEffectFinalizer(key, func(context.Context, engine.EffectResultFinalizationRequest) (engine.EffectResultFinalizationResult, error) {
				return engine.EffectResultFinalizationResult{}, errors.New("test reservation must not run")
			}); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, err := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: err}
			}()
			select {
			case <-probe.markObserved:
			case <-time.After(time.Second):
				t.Fatal("registration failure did not converge unknown")
			}
			thread.removeEffectFinalizer(key)
			close(releaseFailure)
			var out struct {
				result TurnResult
				err    error
			}
			select {
			case out = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("run did not finish after registration failure settled")
			}
			if out.err == nil || out.result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
				t.Fatalf("run result=%#v err=%v, want effect_outcome_unknown", out.result, out.err)
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("registration-failure callback did not exit")
			}
			if probe.markCalls.Load() != 1 || handlerCalls.Load() != 1 {
				t.Fatalf("calls mark/handler=%d/%d, want 1/1", probe.markCalls.Load(), handlerCalls.Load())
			}
			thread.effectFinalizeMu.Lock()
			finalizers := len(thread.effectFinalizers)
			thread.effectFinalizeMu.Unlock()
			if finalizers != 0 {
				t.Fatalf("effect finalizers=%d, want 0", finalizers)
			}
			assertEffectBoundaryTerminalAndReplay(t, thread, repo, "run", sessiontree.TurnFailureEffectOutcomeUnknown, &handlerCalls, 1)
		})
	}
}

func TestApprovalReplayAndGateEarlyReturnShareUnknownOwner(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T, *effectBoundaryProbe, chan struct{}, chan struct{}) (sessiontree.Repo, func())
	}{
		{
			name: "memory",
			open: func(_ *testing.T, probe *effectBoundaryProbe, reached, release chan struct{}) (sessiontree.Repo, func()) {
				base := &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}
				return &approvalReplayBoundaryMemoryRepo{effectBoundaryMemoryRepo: base, commitReached: reached, releaseCommit: release}, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T, probe *effectBoundaryProbe, reached, release chan struct{}) (sessiontree.Repo, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "approval-replay.db"))
				if err != nil {
					t.Fatal(err)
				}
				base := &effectBoundarySQLiteRepo{Store: store, probe: probe}
				return &approvalReplayBoundarySQLiteRepo{effectBoundarySQLiteRepo: base, commitReached: reached, releaseCommit: release}, func() { _ = store.Close() }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &effectBoundaryProbe{markObserved: make(chan struct{})}
			commitReached := make(chan struct{})
			releaseCommit := make(chan struct{})
			repo, closeRepo := test.open(t, probe, commitReached, releaseCommit)
			defer closeRepo()
			provider := harness.NewScriptedProvider(harness.Step(harness.Tool("call-1", "write_file", `{"value":"notes.md"}`), harness.DoneReason("tool_calls")))
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			callbackDone := make(chan struct{})
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				go func() {
					defer close(callbackDone)
					_, _ = effect(boundaryAuthorizationProof(req))
				}()
				<-commitReached
				return EffectDispatchResult{}, nil
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
				result, err := thread.Run(context.Background(), "write", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: err}
			}()
			queue := waitForApprovalQueue(t, context.Background(), h, "thread")
			pending := queue.Approvals[0]
			if _, err := h.ResolveApproval(context.Background(), ResolveApprovalOptions{
				DecisionID: "decision", ExpectedRootThreadID: queue.RootThreadID, ExpectedGeneration: queue.Generation,
				ExpectedRevision: queue.Revision, ExpectedCurrent: sessiontree.ApprovalIdentity{
					ApprovalID: pending.ApprovalID, ThreadID: pending.ThreadID, TurnID: pending.TurnID, RunID: pending.RunID,
					ToolCallID: pending.ToolCallID, EffectAttemptID: pending.EffectAttemptID,
				}, ExpectedApprovalRevision: pending.Revision, Decision: sessiontree.ApprovalDecisionApprove,
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-probe.markObserved:
			case <-time.After(time.Second):
				t.Fatal("approval replay did not converge unknown")
			}
			close(releaseCommit)
			var out struct {
				result TurnResult
				err    error
			}
			select {
			case out = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("approval replay run did not finish")
			}
			if out.err == nil || out.result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
				t.Fatalf("run result=%#v err=%v, want effect_outcome_unknown", out.result, out.err)
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("approval replay callback did not exit")
			}
			if probe.markCalls.Load() != 1 || handlerCalls.Load() != 0 {
				t.Fatalf("calls mark/handler=%d/%d, want 1/0", probe.markCalls.Load(), handlerCalls.Load())
			}
			thread.effectFinalizeMu.Lock()
			finalizers := len(thread.effectFinalizers)
			thread.effectFinalizeMu.Unlock()
			if finalizers != 0 {
				t.Fatalf("effect finalizers=%d, want 0", finalizers)
			}
			assertEffectBoundaryTerminalAndReplay(t, thread, repo, "write", sessiontree.TurnFailureEffectOutcomeUnknown, &handlerCalls, 0)
		})
	}
}

func TestHandlerPanicAndGateEarlyReturnShareUnknownOwner(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T, *effectBoundaryProbe) (sessiontree.Repo, func())
	}{
		{
			name: "memory",
			open: func(_ *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				return &effectBoundaryMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), probe: probe}, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T, probe *effectBoundaryProbe) (sessiontree.Repo, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "handler-panic.db"))
				if err != nil {
					t.Fatal(err)
				}
				return &effectBoundarySQLiteRepo{Store: store, probe: probe}, func() { _ = store.Close() }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &effectBoundaryProbe{markObserved: make(chan struct{})}
			repo, closeRepo := test.open(t, probe)
			defer closeRepo()
			provider := harness.NewScriptedProvider(harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")))
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			handlerStarted := make(chan struct{})
			releaseHandler := make(chan struct{})
			callbackDone := make(chan struct{})
			h.options.EffectAuthorizationGate = EffectAuthorizationGateFunc(func(_ context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				go func() {
					defer close(callbackDone)
					_, _ = effect(boundaryAuthorizationProof(req))
				}()
				<-handlerStarted
				return EffectDispatchResult{}, nil
			})
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
				handlerCalls.Add(1)
				close(handlerStarted)
				<-releaseHandler
				panic("handler panic after gate return")
			}))
			thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct {
				result TurnResult
				err    error
			}, 1)
			go func() {
				result, err := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
				done <- struct {
					result TurnResult
					err    error
				}{result: result, err: err}
			}()
			select {
			case <-probe.markObserved:
			case <-time.After(time.Second):
				t.Fatal("gate early-return did not converge before handler panic")
			}
			close(releaseHandler)
			var out struct {
				result TurnResult
				err    error
			}
			select {
			case out = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handler panic run did not finish")
			}
			if out.err == nil || out.result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
				t.Fatalf("run result=%#v err=%v, want effect_outcome_unknown", out.result, out.err)
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("handler-panic callback did not exit")
			}
			if probe.markCalls.Load() != 1 || handlerCalls.Load() != 1 {
				t.Fatalf("calls mark/handler=%d/%d, want 1/1", probe.markCalls.Load(), handlerCalls.Load())
			}
			thread.effectFinalizeMu.Lock()
			finalizers := len(thread.effectFinalizers)
			thread.effectFinalizeMu.Unlock()
			if finalizers != 0 {
				t.Fatalf("effect finalizers=%d, want 0", finalizers)
			}
			assertEffectBoundaryTerminalAndReplay(t, thread, repo, "run", sessiontree.TurnFailureEffectOutcomeUnknown, &handlerCalls, 1)
		})
	}
}

func invokeBoundaryEffect(req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
	return effect(boundaryAuthorizationProof(req))
}

func boundaryAuthorizationProof(req EffectAuthorizationRequest) EffectAuthorizationProof {
	return EffectAuthorizationProof{
		EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
		ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
		LeaseOwnerID: req.LeaseOwnerID, LeaseGeneration: req.LeaseGeneration,
		PolicyRevision: "boundary-test-policy", AuditReference: "boundary-test-audit", AuditHash: "boundary-test-hash", AuthorizedAt: time.Now(),
	}
}

func assertEffectBoundaryTerminalAndReplay(t *testing.T, thread *Thread, repo sessiontree.Repo, input, failureCode string, handlerCalls *atomic.Int64, wantHandlers int64) {
	t.Helper()
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	terminalCount := 0
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-1" && isTerminalTurnMarker(entry.TurnStatus) {
			terminalCount++
			if failureCode == "" {
				if entry.TurnStatus != sessiontree.TurnCompleted || entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] != "" {
					t.Fatalf("terminal=%#v, want completed", entry)
				}
			} else if entry.TurnStatus != sessiontree.TurnFailed || entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] != failureCode {
				t.Fatalf("terminal=%#v, want failed %s", entry, failureCode)
			}
		}
	}
	if terminalCount != 1 {
		t.Fatalf("terminal markers=%d, want 1: %#v", terminalCount, journal.Path)
	}
	leaseRepo := repo.(interface {
		ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
	})
	if _, active, err := leaseRepo.ActiveTurnLease(context.Background(), "thread"); err != nil || active {
		t.Fatalf("active lease=%v err=%v, want released", active, err)
	}
	replayed, replayErr := thread.Run(context.Background(), input, RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if failureCode == "" {
		if replayErr != nil || replayed.Status != engine.Completed || replayed.FailureCode != "" {
			t.Fatalf("replay result=%#v err=%v, want canonical completed turn", replayed, replayErr)
		}
	} else if replayErr == nil || replayed.FailureCode != failureCode {
		t.Fatalf("replay result=%#v err=%v, want canonical %s failure", replayed, replayErr, failureCode)
	}
	if handlerCalls.Load() != wantHandlers {
		t.Fatalf("replay called handler: got %d, want %d", handlerCalls.Load(), wantHandlers)
	}
}

func TestEffectOutcomeUnknownPrecedesJoinedDeadline(t *testing.T) {
	err := errors.Join(context.DeadlineExceeded, sessiontree.ErrEffectOutcomeUnknown)
	code, classifyErr := turnFailureCode(engine.Failed, err, engine.FailureOriginToolDispatch)
	if classifyErr != nil || code != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("turnFailureCode()=%q err=%v, want effect_outcome_unknown", code, classifyErr)
	}
}

var _ sessiontree.EffectAttemptAuthorityRepo = (*effectBoundaryMemoryRepo)(nil)
var _ sessiontree.EffectAttemptAuthorityRepo = (*effectBoundarySQLiteRepo)(nil)
