package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestShutdownDuringScopedDependencyObservation(t *testing.T) {
	for _, scenario := range []string{"observation", "returned-observation", "parallel-effect", "external-observer"} {
		t.Run(scenario, func(t *testing.T) { testShutdownObservation(t, scenario) })
	}
}

func testShutdownObservation(t *testing.T, scenario string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "wait.db")
	childGateway := newBlockingThreadGateway()
	unknown := scenario == "parallel-effect" || scenario == "external-observer"
	returned := make(chan struct{})
	release := make(chan struct{})
	toolCalls := []provider.ToolCall{{ID: "observe-child", Name: "observe", Args: `{}`}}
	if scenario == "external-observer" {
		toolCalls = nil
	}
	if unknown {
		toolCalls = append(toolCalls, provider.ToolCall{ID: "change-state", Name: "work", Args: `{}`})
	}
	parentGateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "parent", StateCompatibilityKey: "test:parent"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: toolCalls}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Continued explicitly."}, {Type: provider.EventDone, Reason: "stop"}}})
	var parent, child identity.ThreadID
	var calls atomic.Int32
	var workCalls atomic.Int32
	workStarted := make(chan struct{})
	work := tools.Define[map[string]any](tools.Definition{Name: "work", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		if workCalls.Add(1) == 1 {
			close(workStarted)
		}
		<-ctx.Done()
		return tools.Result{}, ctx.Err()
	})
	const ended = "The observation returned without a child report."
	var observedRun identity.RunID
	open := func() (*Host, *threadRuntimeService) {
		t.Helper()
		h, err := Open(ctx, Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		var waiter ThreadWaiter
		observe := tools.Define[map[string]any](tools.Definition{Name: "observe", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
			calls.Add(1)
			result, err := waiter.WaitThreads(ctx, WaitThreadsInput{Scope: ThreadScope{ParentID: &parent}, ThreadIDs: []identity.ThreadID{child}, Until: ThreadWaitAll})
			if scenario == "returned-observation" && err == nil && result.Reason == ThreadWaitCompleted && len(result.Threads) == 1 && result.Threads[0].ID == child {
				observedRun = result.Threads[0].RunID
				close(returned)
				<-release
				return tools.Result{Text: "Observed completed child execution " + observedRun.String()}, nil
			}
			if errors.Is(err, ErrHostClosed) || errors.Is(err, ErrExecutionDeferred) || errors.Is(err, context.Canceled) {
				return tools.CanceledResult(inv.CallID, inv.Name, ended), nil
			}
			return tools.Result{Text: "The wait returned normally."}, err
		})
		s, err := h.ThreadService(AgentFactoryFunc(func(_ context.Context, req AgentRequest) (*Agent, error) {
			if req.ThreadID != parent {
				return testAgent(childGateway)
			}
			return testAgent(parentGateway, WithAgentTools(observe, work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
		}))
		if err != nil {
			t.Fatal(err)
		}
		svc := s.(*threadRuntimeService)
		waiter = svc
		return h, svc
	}
	host, svc := open()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	p, err := svc.Create(ctx, CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	parent = p.ThreadID
	c, err := svc.Create(ctx, CreateThreadInput{ParentThreadID: parent, TaskName: "child", HostProfileRef: "test", RequestKey: "child"})
	if err != nil {
		t.Fatal(err)
	}
	child = c.ThreadID
	c, err = svc.Send(ctx, SendInput{ThreadID: child, RequestKey: "child-start", Input: UserInput{Text: "Research."}})
	if err != nil {
		t.Fatal(err)
	}
	waitClosed(t, childGateway.started, "child provider did not start")
	p, err = svc.Send(ctx, SendInput{ThreadID: parent, RequestKey: "parent-start", Input: UserInput{Text: "Observe the child."}})
	if err != nil {
		t.Fatal(err)
	}
	if unknown {
		waitClosed(t, workStarted, "parallel or independently observed work did not start")
	}
	var external chan error
	if scenario == "external-observer" {
		external = make(chan error, 1)
		go func(service ThreadWaiter) {
			_, err := service.WaitThreads(ctx, WaitThreadsInput{Scope: ThreadScope{ParentID: &parent}, ThreadIDs: []identity.ThreadID{child}, Until: ThreadWaitAll})
			external <- err
		}(svc)
	}
	for {
		svc.mu.Lock()
		registered := len(svc.waiters) == 1
		for interest := range svc.waiters {
			if (interest.observer == nil) != (scenario == "external-observer") {
				svc.mu.Unlock()
				t.Fatal("observation registration has the wrong execution owner")
			}
		}
		svc.mu.Unlock()
		if registered {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("await never registered")
		case <-time.After(time.Millisecond):
		}
	}
	wantText := ended
	if scenario == "returned-observation" {
		close(childGateway.release)
		waitClosed(t, returned, "parent did not read the completed child")
		if observedRun != c.RunID {
			t.Fatalf("observed run=%s, want %s", observedRun, c.RunID)
		}
		wantText = "Observed completed child execution " + c.RunID.String()
	}
	if err := host.SuspendExecution(ctx); err != nil {
		t.Fatal(err)
	}
	if scenario == "returned-observation" {
		short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		err := host.Shutdown(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown did not join the returning handler: %v", err)
		}
		close(release)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if external != nil {
		if err := <-external; !errors.Is(err, ErrHostClosed) {
			t.Fatalf("external observer: %v", err)
		}
	}
	_, svc = open()
	for _, old := range []ThreadView{p, c} {
		v, err := svc.View(ctx, old.ThreadID)
		if err != nil || v.TurnID != old.TurnID || v.RunID != old.RunID || v.Activity != ThreadActivityIdle {
			t.Fatalf("reopened %s: turn=%s run=%s activity=%s err=%v", old.ThreadID, v.TurnID, v.RunID, v.Activity, err)
		}
	}
	wantCalls := int32(1)
	if external != nil {
		wantCalls = 0
	}
	if len(parentGateway.Requests()) != 1 || childGateway.requests.Load() != 1 || calls.Load() != wantCalls {
		t.Fatal("reopen dispatched work")
	}
	_, err = svc.Retry(ctx, RetryInput{ThreadID: parent, SourceTurnID: p.TurnID, RequestKey: "explicit-retry"})
	if unknown {
		if !errors.Is(err, ErrEffectOutcomeUnknown) || workCalls.Load() != 1 || calls.Load() != wantCalls || len(parentGateway.Requests()) != 1 || childGateway.requests.Load() != 1 {
			t.Fatalf("unknown work: retry=%v, work=%d, await=%d, parent=%d, child=%d", err, workCalls.Load(), calls.Load(), len(parentGateway.Requests()), childGateway.requests.Load())
		}
		return
	}
	if err != nil {
		t.Fatalf("explicit retry of returned observation: %v", err)
	}
	v := waitThreadView(t, svc, parent, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.TurnID != p.TurnID })
	reqs := parentGateway.Requests()
	if v.LastOutcome == nil || *v.LastOutcome != TurnOutcomeCompleted || len(reqs) != 2 || childGateway.requests.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("retry: outcome=%v parent requests=%d child requests=%d observations=%d", v.LastOutcome, len(reqs), childGateway.requests.Load(), calls.Load())
	}
	results := 0
	for _, msg := range reqs[1].Messages {
		if msg.ToolResult != nil && msg.ToolResult.CallID == "observe-child" && strings.Contains(msg.ToolResult.Text, wantText) {
			results++
		}
	}
	if results != 1 {
		t.Fatalf("retry received %d copies of the committed observation", results)
	}
}
