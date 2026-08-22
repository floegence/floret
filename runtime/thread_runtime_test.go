package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/florettest"
	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/internal/sessiontree"
	"github.com/floegence/floret/v4/observation"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/storage"
	"github.com/floegence/floret/v4/tools"
)

type oneShotThreadContextCompaction struct {
	consumed atomic.Bool
}

func (source *oneShotThreadContextCompaction) PollManualCompaction(context.Context, ManualCompactionPollRequest) (ManualCompactionRequest, bool, error) {
	if !source.consumed.CompareAndSwap(false, true) {
		return ManualCompactionRequest{}, false, nil
	}
	return ManualCompactionRequest{RequestID: "manual-context-read", Source: "runtime_test"}, true, nil
}

type blockingThreadGateway struct {
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
	requests atomic.Int32
}

type automaticTitleGateway struct {
	failTitle bool
	requests  atomic.Int32
	titles    atomic.Int32
}

type rejectingRuntimeTurnRepo struct {
	sessiontree.Repo
	turns sessiontree.RuntimeTurnRepo
	err   error
}

type blockingCanonicalPathRepo struct {
	sessiontree.Repo
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (repo *blockingCanonicalPathRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	repo.once.Do(func() { close(repo.started) })
	select {
	case <-repo.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return repo.Repo.Path(ctx, threadID, leafID)
}

func (repo rejectingRuntimeTurnRepo) AcceptTurn(context.Context, sessiontree.AcceptTurnRequest) (sessiontree.AcceptTurnResult, error) {
	return sessiontree.AcceptTurnResult{}, repo.err
}

func (repo rejectingRuntimeTurnRepo) ReadAcceptedTurn(ctx context.Context, threadID, turnID, runID string) (sessiontree.AcceptTurnResult, bool, error) {
	return repo.turns.ReadAcceptedTurn(ctx, threadID, turnID, runID)
}

func (repo rejectingRuntimeTurnRepo) FinishTurn(ctx context.Context, request sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	return repo.turns.FinishTurn(ctx, request)
}

type orderedPresentationEventGate struct {
	mu                     sync.Mutex
	reasoningEvents        int
	toolStartEvents        int
	modelDoneEvents        int
	firstToolStart         chan struct{}
	releaseFirstToolStart  chan struct{}
	secondReasoning        chan struct{}
	releaseSecondReasoning chan struct{}
	secondToolStart        chan struct{}
	releaseSecondToolStart chan struct{}
	finalDelta             chan struct{}
	releaseFinalDelta      chan struct{}
	finalDone              chan struct{}
	releaseFinalDone       chan struct{}
}

func newOrderedPresentationEventGate() *orderedPresentationEventGate {
	return &orderedPresentationEventGate{
		firstToolStart: make(chan struct{}, 1), releaseFirstToolStart: make(chan struct{}, 1),
		secondReasoning: make(chan struct{}, 1), releaseSecondReasoning: make(chan struct{}, 1),
		secondToolStart: make(chan struct{}, 1), releaseSecondToolStart: make(chan struct{}, 1),
		finalDelta: make(chan struct{}, 1), releaseFinalDelta: make(chan struct{}, 1),
		finalDone: make(chan struct{}, 1), releaseFinalDone: make(chan struct{}, 1),
	}
}

func (gate *orderedPresentationEventGate) EmitEvent(event Event) {
	if gate == nil || event.Stream == nil {
		return
	}
	var arrived, release chan struct{}
	gate.mu.Lock()
	switch event.Stream.Type {
	case StreamObservationReasoningDelta:
		gate.reasoningEvents++
		if gate.reasoningEvents == 2 {
			arrived, release = gate.secondReasoning, gate.releaseSecondReasoning
		}
	case StreamObservationToolCallEnd:
		gate.toolStartEvents++
		if gate.toolStartEvents == 1 {
			arrived, release = gate.firstToolStart, gate.releaseFirstToolStart
		} else if gate.toolStartEvents == 2 {
			arrived, release = gate.secondToolStart, gate.releaseSecondToolStart
		}
	case StreamObservationAssistantDelta:
		arrived, release = gate.finalDelta, gate.releaseFinalDelta
	case StreamObservationModelStreamDone:
		gate.modelDoneEvents++
		if gate.modelDoneEvents == 3 {
			arrived, release = gate.finalDone, gate.releaseFinalDone
		}
	}
	gate.mu.Unlock()
	if arrived != nil {
		arrived <- struct{}{}
		<-release
	}
}

func (gate *orderedPresentationEventGate) wait(t *testing.T, checkpoint <-chan struct{}) {
	t.Helper()
	select {
	case <-checkpoint:
	case <-time.After(3 * time.Second):
		t.Fatal("ordered presentation event checkpoint timed out")
	}
}

func (gate *orderedPresentationEventGate) release(checkpoint chan<- struct{}) {
	select {
	case checkpoint <- struct{}{}:
	default:
	}
}

func (gate *orderedPresentationEventGate) releaseAll() {
	gate.release(gate.releaseFirstToolStart)
	gate.release(gate.releaseSecondReasoning)
	gate.release(gate.releaseSecondToolStart)
	gate.release(gate.releaseFinalDelta)
	gate.release(gate.releaseFinalDone)
}

func (*automaticTitleGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "automatic-title", StateCompatibilityKey: "test:automatic-title:v1"}
}

func (*automaticTitleGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (gateway *automaticTitleGateway) Stream(_ context.Context, request provider.Request) (<-chan provider.Event, error) {
	gateway.requests.Add(1)
	events := make(chan provider.Event, 2)
	if request.LogicalRequestID == "thread_title" {
		gateway.titles.Add(1)
		if gateway.failTitle {
			events <- provider.Event{Type: provider.EventError, Err: errors.New("title unavailable")}
		} else {
			events <- provider.Event{Type: provider.EventDelta, Text: "Provider title"}
			events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
		}
	} else {
		events <- provider.Event{Type: provider.EventDelta, Text: "assistant response"}
		events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	}
	close(events)
	return events, nil
}

func newBlockingThreadGateway() *blockingThreadGateway {
	return &blockingThreadGateway{started: make(chan struct{}), release: make(chan struct{})}
}

func (*blockingThreadGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "blocking", StateCompatibilityKey: "test:blocking:v1"}
}

func (*blockingThreadGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (gateway *blockingThreadGateway) Stream(ctx context.Context, _ provider.Request) (<-chan provider.Event, error) {
	gateway.requests.Add(1)
	gateway.once.Do(func() { close(gateway.started) })
	events := make(chan provider.Event, 2)
	go func() {
		defer close(events)
		select {
		case <-ctx.Done():
			return
		case <-gateway.release:
			events <- provider.Event{Type: provider.EventDelta, Text: "done"}
			events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
		}
	}()
	return events, nil
}

func testThreadService(t *testing.T, gateway provider.Gateway) (*Host, ThreadService) {
	t.Helper()
	agent, err := NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	return testThreadServiceWithAgent(t, agent)
}

func testThreadServiceWithAgent(t *testing.T, agent *Agent) (*Host, ThreadService) {
	t.Helper()
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	return host, service
}

func TestThreadServiceSendIsImmediateDeduplicatedAndCancelable(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-fast-send"})
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "slow"}, RequestKey: "same-send"})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("Send latency=%s, want <500ms", elapsed)
	}
	if first.Activity != ThreadActivityActive {
		t.Fatalf("first view=%#v", first)
	}
	second, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "slow"}, RequestKey: "same-send"})
	if err != nil || second.TurnID != first.TurnID {
		t.Fatalf("duplicate send=%#v err=%v", second, err)
	}
	userItems := 0
	for _, item := range second.Items {
		if item.Kind == ThreadItemUser {
			userItems++
		}
	}
	if userItems != 1 {
		t.Fatalf("canonical user items=%d, want 1", userItems)
	}
	select {
	case <-gateway.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	cancelStarted := time.Now()
	_, err = service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-fast-send"})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(cancelStarted); elapsed >= 500*time.Millisecond {
		t.Fatalf("Cancel latency=%s, want <500ms", elapsed)
	}
	cancelled := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCancelled
	})
	replayed, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-again"})
	if err != nil || replayed.Activity != ThreadActivityIdle {
		t.Fatalf("idempotent cancel=%#v prior=%#v err=%v", replayed, cancelled, err)
	}
}

func TestThreadServiceSendFailsBeforePublishingWhenCanonicalAcceptanceFails(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-rejected-send"})
	if err != nil {
		t.Fatal(err)
	}
	original := host.store.repo
	turns := original.(sessiontree.RuntimeTurnRepo)
	host.store.repo = rejectingRuntimeTurnRepo{Repo: original, turns: turns, err: errors.New("injected canonical acceptance failure")}
	t.Cleanup(func() { host.store.repo = original })

	if _, err := service.Send(t.Context(), SendInput{
		ThreadID: created.ThreadID, Input: UserInput{Text: "must stay canonical"}, RequestKey: "rejected-send",
	}); err == nil || !strings.Contains(err.Error(), "injected canonical acceptance failure") {
		t.Fatalf("Send error=%v, want synchronous canonical acceptance failure", err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Activity != ThreadActivityIdle || len(view.Items) != 0 {
		t.Fatalf("failed send view=%#v, want unchanged idle thread", view)
	}
}

func TestThreadServicePromoteFailsBeforePublishingWhenCanonicalAcceptanceFails(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-rejected-promote"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "send-rejected-promote-active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "queued"}, RequestKey: "send-rejected-promote-queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-rejected-promote-active"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && len(view.Queue) == 1
	})
	original := host.store.repo
	turns := original.(sessiontree.RuntimeTurnRepo)
	host.store.repo = rejectingRuntimeTurnRepo{Repo: original, turns: turns, err: errors.New("injected promote acceptance failure")}
	t.Cleanup(func() { host.store.repo = original })
	if _, err := service.PromoteQueued(t.Context(), PromoteQueuedInput{
		ThreadID: created.ThreadID, QueueItemID: "queue:send-rejected-promote-queued", RequestKey: "promote-rejected",
	}); err == nil || !strings.Contains(err.Error(), "injected promote acceptance failure") {
		t.Fatalf("PromoteQueued error=%v, want synchronous canonical acceptance failure", err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Activity != ThreadActivityIdle || len(view.Queue) != 1 || len(view.Items) != 1 {
		t.Fatalf("failed promote view=%#v, want unchanged idle queue", view)
	}
}

func TestThreadContextReaderRestoresOneTerminalCompactionPerOperation(t *testing.T) {
	path := t.TempDir() + "/thread-context.db"
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "after compact"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	agent, err := testAgent(gateway, WithAgentManualCompactions(&oneShotThreadContextCompaction{}))
	if err != nil {
		t.Fatal(err)
	}
	firstHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	firstService, err := firstHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := firstService.Create(t.Context(), CreateThreadInput{RequestKey: "create-context-read"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstService.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "compact context"}, RequestKey: "send-context-read"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, firstService, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	firstReader, ok := firstService.(ThreadContextReader)
	if !ok {
		t.Fatal("Host.ThreadService result does not expose ThreadContextReader")
	}
	assertContext := func(label string, reader ThreadContextReader) {
		t.Helper()
		snapshot, readErr := reader.Context(t.Context(), created.ThreadID)
		if readErr != nil {
			t.Fatalf("%s context: %v", label, readErr)
		}
		if len(snapshot.Compactions) != 1 {
			t.Fatalf("%s compactions=%#v, want one merged operation", label, snapshot.Compactions)
		}
		compaction := snapshot.Compactions[0]
		if compaction.RequestID != "manual-context-read" || compaction.Source != "runtime_test" || compaction.Status != string(observation.CompactionStatusNoop) || compaction.Phase != string(observation.CompactionPhaseNoop) {
			t.Fatalf("%s compaction=%#v", label, compaction)
		}
	}
	assertContext("live", firstReader)
	if err := firstHost.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondHost.Shutdown(context.Background()) })
	secondService, err := secondHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	secondReader, ok := secondService.(ThreadContextReader)
	if !ok {
		t.Fatal("reopened Host.ThreadService result does not expose ThreadContextReader")
	}
	assertContext("reopened", secondReader)
}

func TestThreadServiceAutomaticTitleReplacesFallbackOnlyAfterSuccess(t *testing.T) {
	for _, test := range []struct {
		name      string
		failTitle bool
		wantTitle string
		wantState ThreadTitleStatus
	}{
		{name: "success", wantTitle: "Provider title", wantState: ThreadTitleStatusReady},
		{name: "failure", failTitle: true, wantTitle: "fallback title", wantState: ThreadTitleStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway := &automaticTitleGateway{failTitle: test.failTitle}
			agent, err := testAgent(gateway, WithAgentThreadTitleMode(ThreadTitleModeProvider))
			if err != nil {
				t.Fatal(err)
			}
			host, err := Open(t.Context(), Options{Storage: storage.Memory()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
			if err != nil {
				t.Fatal(err)
			}
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: RequestKey("create-title-" + test.name)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "fallback title"}, RequestKey: RequestKey("send-title-" + test.name)}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				summaries, listErr := service.List(t.Context(), ThreadScope{})
				if listErr != nil {
					t.Fatal(listErr)
				}
				if len(summaries) == 1 && summaries[0].Title == test.wantTitle && summaries[0].TitleStatus == test.wantState {
					if gateway.titles.Load() != 1 {
						t.Fatalf("automatic title requests = %d, want 1", gateway.titles.Load())
					}
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			summaries, _ := service.List(t.Context(), ThreadScope{})
			t.Fatalf("automatic title did not settle: summaries=%#v requests=%d titles=%d", summaries, gateway.requests.Load(), gateway.titles.Load())
		})
	}
}

func TestThreadServiceParentChildInventoryIsIsolated(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-parent"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: parent.ThreadID, TaskName: "child", HostProfileRef: "default", RequestKey: "create-child"})
	if err != nil {
		t.Fatal(err)
	}
	children, err := service.List(t.Context(), ThreadScope{ParentID: &parent.ThreadID})
	if err != nil || len(children) != 1 || children[0].ID != child.ThreadID {
		t.Fatalf("children=%#v err=%v", children, err)
	}
	roots, err := service.List(t.Context(), ThreadScope{})
	if err != nil || len(roots) != 1 || roots[0].ID != parent.ThreadID {
		t.Fatalf("roots=%#v err=%v", roots, err)
	}
}

func TestThreadServiceApprovalRejectAndAcceptStayOnInteraction(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, typed := testThreadService(t, gateway)
	service := typed.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-approval"})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "approve"}, RequestKey: "send-approval"})
	if err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	var runID identity.RunID
	_ = actor.apply(t.Context(), func() error { runID = actor.state.runID; return nil })
	downstreamCalls := atomic.Int32{}
	gate := threadRuntimeEffectGate{service: service, downstream: EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		downstreamCalls.Add(1)
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID})
	})}
	dispatch := func(effectID string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, dispatchErr := gate.Dispatch(context.Background(), EffectAuthorizationRequest{
				EffectAttemptID: effectID, ThreadID: created.ThreadID, TurnID: started.TurnID, RunID: runID,
				ToolCallID: "call-" + effectID, ToolName: "write", Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
			}, func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error) {
				return EffectDispatchResult{}, nil
			})
			done <- dispatchErr
		}()
		return done
	}
	firstDone := dispatch("effect-reject")
	first := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 1 && !view.Interactions[0].Resolved
	})
	rejected := false
	_, err = service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: first.Interactions[0].ID,
		Answers: []InteractionAnswer{{Approved: &rejected}}, RequestKey: "reject-effect",
	})
	if err != nil {
		t.Fatal(err)
	}
	if dispatchErr := <-firstDone; !errors.Is(dispatchErr, ErrEffectUnauthorized) {
		t.Fatalf("reject dispatch error=%v", dispatchErr)
	}
	if downstreamCalls.Load() != 0 {
		t.Fatal("rejected effect reached downstream")
	}
	secondDone := dispatch("effect-accept")
	second := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 2 && !view.Interactions[1].Resolved
	})
	approved := true
	_, err = service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: second.Interactions[1].ID,
		Answers: []InteractionAnswer{{Approved: &approved}}, RequestKey: "accept-effect",
	})
	if err != nil {
		t.Fatal(err)
	}
	if dispatchErr := <-secondDone; dispatchErr != nil {
		t.Fatalf("accept dispatch error=%v", dispatchErr)
	}
	if downstreamCalls.Load() != 1 {
		t.Fatalf("downstream calls=%d, want 1", downstreamCalls.Load())
	}
	_, _ = service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-approval"})
}

func TestThreadServiceApprovalPresentsTerminalCommand(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, typed := testThreadService(t, gateway)
	service := typed.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-terminal-approval"})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "run curl"}, RequestKey: "send-terminal-approval"})
	if err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	var runID identity.RunID
	_ = actor.apply(t.Context(), func() error { runID = actor.state.runID; return nil })

	gate := threadRuntimeEffectGate{service: service}
	done := make(chan error, 1)
	go func() {
		_, dispatchErr := gate.Dispatch(context.Background(), EffectAuthorizationRequest{
			EffectAttemptID: "effect-terminal-command", ThreadID: created.ThreadID, TurnID: started.TurnID, RunID: runID,
			ToolCallID: "call-terminal-command", ToolName: "terminal.exec",
			Arguments: `{"command":"curl -s https://example.test","yield_ms":10000}`,
			Activity: &tools.ActivityPresentation{
				Label: "  Fetch example  ", Description: "  Download the example response.  ",
				Renderer: tools.ActivityRendererTerminal,
				Payload:  tools.TerminalActivityPayload{Command: "curl -s https://example.test"},
			},
			Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		}, func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error) {
			return EffectDispatchResult{}, nil
		})
		done <- dispatchErr
	}()

	waiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 1 && view.Interactions[0].Kind == ThreadInteractionApproval && !view.Interactions[0].Resolved
	})
	approval := waiting.Interactions[0].Approval
	if approval == nil {
		t.Fatal("pending interaction has no approval presentation")
	}
	if approval.Label != "Fetch example" || approval.Description != "Download the example response." {
		t.Fatalf("approval activity copy=(%q, %q)", approval.Label, approval.Description)
	}
	if approval.Command != "curl -s https://example.test" || strings.Contains(approval.Command, `{"command"`) {
		t.Fatalf("approval command=%q", approval.Command)
	}
	rejected := false
	if _, err := service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID,
		Answers: []InteractionAnswer{{Approved: &rejected}}, RequestKey: "reject-terminal-approval",
	}); err != nil {
		t.Fatal(err)
	}
	if dispatchErr := <-done; !errors.Is(dispatchErr, ErrEffectUnauthorized) {
		t.Fatalf("dispatch error=%v", dispatchErr)
	}
}

func TestThreadServiceRespondResumesWaitingInput(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-1", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	agent, err := NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-input-resume"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "begin"}, RequestKey: "send-input-resume"})
	if err != nil || result.Activity != ThreadActivityActive {
		t.Fatalf("send=%#v err=%v", result, err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 1 && view.Interactions[0].Kind == ThreadInteractionInput && !view.Interactions[0].Resolved
	})
	assertOrderedThreadItems(t, waiting.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "begin"},
		{kind: ThreadItemInteraction},
	})
	waitingPrefix := orderedThreadItemIdentity(waiting.Items)
	if input := waiting.Interactions[0].Input; input == nil || len(input.Questions) != 1 || input.Questions[0].Prompt != "Continue?" || input.Questions[0].Kind != "write" {
		t.Fatalf("waiting input presentation = %#v", input)
	}
	responded, err := service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID,
		Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}, RequestKey: "respond-input-resume",
	})
	if err != nil || len(responded.Interactions) != 1 || !responded.Interactions[0].Resolved {
		t.Fatalf("respond=%#v err=%v", responded, err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted
	})
	if completed.Error != "" || completed.LastOutcome == nil || *completed.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("completed=%#v provider_requests=%d", completed, len(gateway.Requests()))
	}
	assertStableThreadItemPrefix(t, waitingPrefix, completed.Items)
	assertOrderedThreadItems(t, completed.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "begin"},
		{kind: ThreadItemInteraction},
		{kind: ThreadItemAssistant, text: "continued"},
	})
	if completed.Items[1].Interaction == nil || !completed.Items[1].Interaction.Resolved {
		t.Fatalf("completed input interaction=%#v, want resolved in place", completed.Items[1].Interaction)
	}
	if requests := gateway.Requests(); len(requests) != 2 {
		t.Fatalf("provider requests=%d, want 2", len(requests))
	} else {
		messages := requests[1].Messages
		if len(messages) < 2 || messages[len(messages)-1].Role != provider.RoleUser || !strings.Contains(messages[len(messages)-1].Text, `{"q":"yes"}`) {
			t.Fatalf("continuation answer is not the final provider message: %#v", messages)
		}
	}
	reader, ok := service.(ThreadContextReader)
	if !ok {
		t.Fatal("Host.ThreadService result does not expose ThreadContextReader")
	}
	contextSnapshot, err := reader.Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatalf("read context after input resume: %v", err)
	}
	if contextSnapshot.Usage == nil || contextSnapshot.Usage.TurnID != completed.TurnID {
		t.Fatalf("context after input resume=%#v, want latest canonical usage for turn %q", contextSnapshot, completed.TurnID)
	}
}

func TestThreadServiceApprovedEffectCommitsCanonicalResult(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "shell-1", Name: "shell", Args: `{"command":"printf accepted"}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	exitCode := 0
	effectTool := tools.Define[map[string]string](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"command": tools.String("command")}, []string{"command"}),
			Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true, PreserveFullSet: true},
			Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
				args, _ := inv.Args.(map[string]string)
				return &tools.ActivityPresentation{Label: "Shell command", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: args["command"]}}, nil
			},
		},
		nil, nil,
		func(context.Context, tools.Invocation[map[string]string]) (tools.Result, error) {
			return tools.Result{
				Text: "FLOWER_APPROVAL_ACCEPTED",
				Activity: &tools.ActivityPresentation{Label: "Shell command", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{
					Status: string(observation.ActivityStatusSuccess), Stdout: "FLOWER_APPROVAL_ACCEPTED", ExitCode: &exitCode,
				}},
			}, nil
		},
	)
	agent, err := testAgent(gateway,
		WithAgentTools(effectTool),
		WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
			return effect(ctx, EffectAuthorizationProof{
				EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
				ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
				PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-audit-hash", AuthorizedAt: time.Now().UTC(),
			})
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-effect-finalization"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "run shell"}, RequestKey: "send-effect-finalization"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return len(view.Interactions) == 1 && view.Interactions[0].Kind == ThreadInteractionApproval && !view.Interactions[0].Resolved
	})
	approved := true
	if _, err := service.Respond(t.Context(), RespondInput{
		ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID,
		Answers: []InteractionAnswer{{Approved: &approved}}, RequestKey: "approve-effect-finalization",
	}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if completed.Error != "" || *completed.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("completed=%#v", completed)
	}
	for _, item := range completed.Items {
		if item.Kind == ThreadItemTool && item.Activity != nil && item.Activity.ToolID == "shell-1" {
			if item.Activity.Status != observation.ActivityStatusSuccess || item.Activity.Presentation == nil {
				t.Fatalf("tool activity=%#v", item.Activity)
			}
			return
		}
	}
	t.Fatalf("canonical tool result missing: %#v", completed.Items)
}

func TestThreadServiceRemovesSchemaCorrectionFromTerminalPresentation(t *testing.T) {
	path := t.TempDir() + "/schema-correction-presentation.db"
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "invalid-read-1", Name: "terminal.read", Args: `{"process_id":"proc-1","after_seq":0}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var called atomic.Bool
	invalidTool := tools.Define[map[string]any](
		tools.Definition{
			Name: "terminal.read",
			InputSchema: tools.StrictObject(map[string]any{
				"process_id":  tools.String("process id"),
				"description": tools.String("description"),
				"after_seq":   map[string]any{"type": "integer", "minimum": 0},
			}, []string{"process_id", "description", "after_seq"}),
			ReadOnly:   true,
			Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
			InvalidActivity: func(tools.Invocation[map[string]any]) (*tools.ActivityPresentation, error) {
				return &tools.ActivityPresentation{Label: "Read terminal output", Renderer: tools.ActivityRendererStructured}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			called.Store(true)
			return tools.Result{Text: "unexpected"}, nil
		},
	)
	agent, err := testAgent(gateway, WithAgentTools(invalidTool))
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-schema-correction"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "read terminal"}, RequestKey: "send-schema-correction"}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if called.Load() {
		t.Fatal("schema-invalid tool handler ran")
	}
	assertOrderedThreadItems(t, completed.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "read terminal"},
		{kind: ThreadItemAssistant, text: "recovered"},
	})
	for _, item := range completed.Items {
		if item.Live || item.Kind == ThreadItemTool {
			t.Fatalf("terminal presentation retained schema correction: %#v", completed.Items)
		}
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopenedHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedHost.Shutdown(context.Background()) })
	reopenedService, err := reopenedHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := reopenedService.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertOrderedThreadItems(t, reopened.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "read terminal"},
		{kind: ThreadItemAssistant, text: "recovered"},
	})
}

func TestThreadServiceSettlesTerminalOutputFromCanonicalSegments(t *testing.T) {
	t.Run("single step", func(t *testing.T) {
		gateway := florettest.NewScriptedGateway(
			provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
			provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
			florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "one reply"}, {Type: provider.EventDone, Reason: "stop"}}},
		)
		agent, err := testAgent(gateway)
		if err != nil {
			t.Fatal(err)
		}
		_, service := testThreadServiceWithAgent(t, agent)
		created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-terminal-single"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "reply"}, RequestKey: "send-terminal-single"}); err != nil {
			t.Fatal(err)
		}
		completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
		})
		assertOrderedThreadItems(t, completed.Items, []orderedThreadItemExpectation{
			{kind: ThreadItemUser, text: "reply"},
			{kind: ThreadItemAssistant, text: "one reply"},
		})
		assertNoLiveThreadItems(t, completed.Items)
	})

	t.Run("multiple steps preserve assistant segments", func(t *testing.T) {
		gateway := florettest.NewScriptedGateway(
			provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
			provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
			florettest.Step{Events: []provider.Event{
				{Type: provider.EventDelta, Text: "progress"},
				{Type: provider.EventTruncated, Reason: "length"},
			}},
			florettest.Step{Events: []provider.Event{{Type: provider.EventReasoning, Text: "thinking-2"}, {Type: provider.EventDelta, Text: "final"}, {Type: provider.EventDone, Reason: "stop"}}},
		)
		agent, err := testAgent(gateway)
		if err != nil {
			t.Fatal(err)
		}
		_, service := testThreadServiceWithAgent(t, agent)
		created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-terminal-multiple"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "run"}, RequestKey: "send-terminal-multiple"}); err != nil {
			t.Fatal(err)
		}
		completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && len(view.Items) == 4
		})
		assertOrderedThreadItems(t, completed.Items, []orderedThreadItemExpectation{
			{kind: ThreadItemUser, text: "run"},
			{kind: ThreadItemAssistant, text: "progress"},
			{kind: ThreadItemThinking, text: "thinking-2"},
			{kind: ThreadItemAssistant, text: "final"},
		})
		assertNoLiveThreadItems(t, completed.Items)
		for _, item := range completed.Items {
			if item.Kind == ThreadItemAssistant && strings.Contains(item.Text, "progressfinal") {
				t.Fatalf("run aggregate was appended as a display item: %#v", completed.Items)
			}
		}
	})
}

func TestReconcileCanonicalThreadItemsUsesTerminalJournalAuthority(t *testing.T) {
	current := []ThreadItem{
		{ID: "assistant:turn-a:1", TurnID: "turn-a", Ordinal: 1, Kind: ThreadItemAssistant, Text: "stream draft", Live: true},
		{ID: "assistant:turn-a:2", TurnID: "turn-a", Ordinal: 2, Kind: ThreadItemAssistant, Text: "stream draft", Live: true},
		{ID: "assistant:turn-a:3", TurnID: "turn-a", Ordinal: 3, Kind: ThreadItemAssistant, Text: "aggregate copy", Live: false},
	}
	canonical := []ThreadItem{
		{ID: "assistant:turn-a:1", TurnID: "turn-a", Ordinal: 1, Kind: ThreadItemAssistant, Text: "canonical first"},
		{ID: "assistant:turn-a:2", TurnID: "turn-a", Ordinal: 2, Kind: ThreadItemAssistant, Text: "canonical first"},
	}

	terminal, ok := reconcileCanonicalThreadItems(current, canonical, true)
	if !ok {
		t.Fatal("terminal reconciliation rejected canonical items")
	}
	assertOrderedThreadItems(t, terminal, []orderedThreadItemExpectation{
		{kind: ThreadItemAssistant, text: "canonical first"},
		{kind: ThreadItemAssistant, text: "canonical first"},
	})
	if terminal[0].ID == terminal[1].ID {
		t.Fatalf("distinct canonical IDs were merged: %#v", terminal)
	}

	active, ok := reconcileCanonicalThreadItems(current[:1], canonical[:1], false)
	if !ok || len(active) != 1 || active[0].Text != "stream draft" || !active[0].Live {
		t.Fatalf("active reconciliation lost live text: %#v, ok=%v", active, ok)
	}
}

func TestThreadRuntimeCanonicalRefreshRejectsStaleSnapshot(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-canonical-race"})
	if err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(created.ThreadID)
	started := make(chan struct{})
	release := make(chan struct{})
	host.store.repo = &blockingCanonicalPathRepo{Repo: host.store.repo, started: started, release: release}

	_ = actor.apply(t.Context(), func() error {
		actor.state.view.Items = []ThreadItem{{ID: "assistant:turn-race:1", TurnID: "turn-race", Ordinal: 1, Kind: ThreadItemAssistant, Text: "live", Live: true}}
		actor.state.view.ViewVersion++
		return nil
	})
	refreshDone := make(chan struct{})
	go func() {
		typed.refreshCanonical(created.ThreadID, "turn-race")
		close(refreshDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("canonical refresh did not start reading")
	}
	_ = actor.apply(t.Context(), func() error {
		outcome := TurnOutcomeCompleted
		actor.state.view.Activity = ThreadActivityIdle
		actor.state.view.LastOutcome = &outcome
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "assistant:turn-race:2", TurnID: "turn-race", Ordinal: 2, Kind: ThreadItemAssistant, Text: "terminal"})
		actor.state.view.ViewVersion++
		return nil
	})
	close(release)
	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("canonical refresh did not finish")
	}
	view := typed.currentView(actor)
	if len(view.Items) != 2 || view.Items[1].ID != "assistant:turn-race:2" || view.Items[1].Text != "terminal" {
		t.Fatalf("stale canonical snapshot replaced terminal view: %#v", view.Items)
	}
}

func TestThreadServicePreservesOrderedReasoningAndToolsAcrossApprovalAndReopen(t *testing.T) {
	path := t.TempDir() + "/ordered-presentation.db"
	eventGate := newOrderedPresentationEventGate()
	t.Cleanup(eventGate.releaseAll)
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{Kind: config.ReasoningKindNone}},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventReasoning, Text: "reasoning-1"},
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ordered-tool-1", Name: "ordered_shell", Args: `{"command":"first"}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventReasoning, Text: "reasoning-2"},
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ordered-tool-2", Name: "ordered_shell", Args: `{"command":"second"}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "final-text"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	exitCode := 0
	toolStarted := make(chan string, 2)
	toolRelease := make(chan struct{}, 2)
	t.Cleanup(func() {
		select {
		case toolRelease <- struct{}{}:
		default:
		}
		select {
		case toolRelease <- struct{}{}:
		default:
		}
	})
	effectTool := tools.Define[map[string]string](
		tools.Definition{
			Name: "ordered_shell", InputSchema: tools.StrictObject(map[string]any{"command": tools.String("command")}, []string{"command"}),
			Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
			Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
				args, _ := inv.Args.(map[string]string)
				return &tools.ActivityPresentation{Label: "Ordered shell", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: args["command"]}}, nil
			},
		},
		nil, nil,
		func(_ context.Context, inv tools.Invocation[map[string]string]) (tools.Result, error) {
			toolStarted <- inv.Args["command"]
			<-toolRelease
			return tools.Result{Text: "result-" + inv.Args["command"], Activity: &tools.ActivityPresentation{
				Label: "Ordered shell", Renderer: tools.ActivityRendererTerminal,
				Payload: tools.TerminalActivityPayload{Command: inv.Args["command"], Status: string(observation.ActivityStatusSuccess), Stdout: "result-" + inv.Args["command"], ExitCode: &exitCode},
			}}, nil
		},
	)
	agent, err := testAgent(gateway,
		WithAgentTools(effectTool),
		WithAgentEventSink(eventGate),
		WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
			return effect(ctx, EffectAuthorizationProof{
				EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
				ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
				PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-audit-hash", AuthorizedAt: time.Now().UTC(),
			})
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-ordered-presentation"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "run two tools"}, RequestKey: "send-ordered-presentation"}); err != nil {
		t.Fatal(err)
	}

	eventGate.wait(t, eventGate.firstToolStart)
	firstThinking := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking)
	})
	assertOrderedThreadItems(t, firstThinking.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "run two tools"},
		{kind: ThreadItemThinking, text: "reasoning-1"},
	})
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, firstThinking.Items)
	eventGate.release(eventGate.releaseFirstToolStart)

	firstWaiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return unresolvedApprovalCount(view) == 1 && threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool)
	})
	assertOrderedThreadItems(t, firstWaiting.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "run two tools"},
		{kind: ThreadItemThinking, text: "reasoning-1"},
		{kind: ThreadItemTool, toolID: "ordered-tool-1"},
	})
	assertToolInteractionState(t, firstWaiting.Items, "ordered-tool-1", false)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, firstWaiting.Items)
	firstPrefix := orderedThreadItemIdentity(firstWaiting.Items)
	approved := true
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: unresolvedApprovalID(firstWaiting), Answers: []InteractionAnswer{{Approved: &approved}}, RequestKey: "approve-ordered-tool-1"}); err != nil {
		t.Fatal(err)
	}
	if command := waitString(t, toolStarted); command != "first" {
		t.Fatalf("first running command=%q", command)
	}
	firstRunning := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return unresolvedApprovalCount(view) == 0 && threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool)
	})
	assertStableThreadItemPrefix(t, firstPrefix, firstRunning.Items)
	if status := threadToolStatus(firstRunning.Items, "ordered-tool-1"); status != observation.ActivityStatusRunning {
		t.Fatalf("first running tool status=%q", status)
	}
	assertToolInteractionState(t, firstRunning.Items, "ordered-tool-1", true)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, firstRunning.Items)
	toolRelease <- struct{}{}
	eventGate.wait(t, eventGate.secondReasoning)
	firstCompleted := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return threadToolStatus(view.Items, "ordered-tool-1") == observation.ActivityStatusSuccess
	})
	assertStableThreadItemPrefix(t, firstPrefix, firstCompleted.Items)
	assertThreadToolTerminal(t, firstCompleted.Items, "ordered-tool-1")
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, firstCompleted.Items)
	eventGate.release(eventGate.releaseSecondReasoning)
	eventGate.wait(t, eventGate.secondToolStart)
	secondThinking := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool, ThreadItemThinking)
	})
	assertStableThreadItemPrefix(t, firstPrefix, secondThinking.Items)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, secondThinking.Items)
	eventGate.release(eventGate.releaseSecondToolStart)

	secondWaiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return unresolvedApprovalCount(view) == 1 && threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool, ThreadItemThinking, ThreadItemTool)
	})
	assertStableThreadItemPrefix(t, firstPrefix, secondWaiting.Items)
	assertOrderedThreadItems(t, secondWaiting.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "run two tools"},
		{kind: ThreadItemThinking, text: "reasoning-1"},
		{kind: ThreadItemTool, toolID: "ordered-tool-1"},
		{kind: ThreadItemThinking, text: "reasoning-2"},
		{kind: ThreadItemTool, toolID: "ordered-tool-2"},
	})
	assertToolInteractionState(t, secondWaiting.Items, "ordered-tool-2", false)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, secondWaiting.Items)
	secondPrefix := orderedThreadItemIdentity(secondWaiting.Items)
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: unresolvedApprovalID(secondWaiting), Answers: []InteractionAnswer{{Approved: &approved}}, RequestKey: "approve-ordered-tool-2"}); err != nil {
		t.Fatal(err)
	}
	if command := waitString(t, toolStarted); command != "second" {
		t.Fatalf("second running command=%q", command)
	}
	secondRunning := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return unresolvedApprovalCount(view) == 0 && threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool, ThreadItemThinking, ThreadItemTool)
	})
	assertStableThreadItemPrefix(t, secondPrefix, secondRunning.Items)
	if status := threadToolStatus(secondRunning.Items, "ordered-tool-2"); status != observation.ActivityStatusRunning {
		t.Fatalf("second running tool status=%q", status)
	}
	assertToolInteractionState(t, secondRunning.Items, "ordered-tool-2", true)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, secondRunning.Items)
	toolRelease <- struct{}{}
	eventGate.wait(t, eventGate.finalDelta)
	secondCompleted := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return threadToolStatus(view.Items, "ordered-tool-2") == observation.ActivityStatusSuccess
	})
	assertStableThreadItemPrefix(t, secondPrefix, secondCompleted.Items)
	assertThreadToolTerminal(t, secondCompleted.Items, "ordered-tool-2")
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, secondCompleted.Items)
	eventGate.release(eventGate.releaseFinalDelta)
	eventGate.wait(t, eventGate.finalDone)
	finalLive := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool, ThreadItemThinking, ThreadItemTool, ThreadItemAssistant)
	})
	assertStableThreadItemPrefix(t, secondPrefix, finalLive.Items)
	assertSubscriptionThreadItems(t, subscription, created.ThreadID, finalLive.Items)
	eventGate.release(eventGate.releaseFinalDone)

	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted && threadItemKinds(view.Items, ThreadItemUser, ThreadItemThinking, ThreadItemTool, ThreadItemThinking, ThreadItemTool, ThreadItemAssistant)
	})
	assertStableThreadItemPrefix(t, secondPrefix, completed.Items)
	assertOrderedThreadItems(t, completed.Items, []orderedThreadItemExpectation{
		{kind: ThreadItemUser, text: "run two tools"},
		{kind: ThreadItemThinking, text: "reasoning-1"},
		{kind: ThreadItemTool, toolID: "ordered-tool-1"},
		{kind: ThreadItemThinking, text: "reasoning-2"},
		{kind: ThreadItemTool, toolID: "ordered-tool-2"},
		{kind: ThreadItemAssistant, text: "final-text"},
	})
	assertNoLiveThreadItems(t, completed.Items)

	assertSubscriptionThreadItems(t, subscription, created.ThreadID, completed.Items)
	subscription.Close()
	history, err := service.History(t.Context(), created.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	assertStableThreadItemPrefix(t, orderedThreadItemIdentity(completed.Items), history.Items)
	latestPage, err := service.History(t.Context(), created.ThreadID, "", 2)
	if err != nil || len(latestPage.Items) != 2 || latestPage.Items[0].Ordinal != 5 || latestPage.Items[1].Ordinal != 6 || !latestPage.HasMore {
		t.Fatalf("latest ordered history page=%#v err=%v", latestPage, err)
	}
	middlePage, err := service.History(t.Context(), created.ThreadID, latestPage.Before, 2)
	if err != nil || len(middlePage.Items) != 2 || middlePage.Items[0].Ordinal != 3 || middlePage.Items[1].Ordinal != 4 || !middlePage.HasMore {
		t.Fatalf("middle ordered history page=%#v err=%v", middlePage, err)
	}
	oldestPage, err := service.History(t.Context(), created.ThreadID, middlePage.Before, 2)
	if err != nil || len(oldestPage.Items) != 2 || oldestPage.Items[0].Ordinal != 1 || oldestPage.Items[1].Ordinal != 2 || oldestPage.HasMore {
		t.Fatalf("oldest ordered history page=%#v err=%v", oldestPage, err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopenedHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedHost.Shutdown(context.Background()) })
	reopenedService, err := reopenedHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := reopenedService.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertStableThreadItemPrefix(t, orderedThreadItemIdentity(completed.Items), reopened.Items)
	assertNoLiveThreadItems(t, reopened.Items)
}

func TestThreadServiceRetryReusesCanonicalUserInput(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "first"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retry"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-retry"})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "hello"}, RequestKey: "send-retry"})
	if err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted
	})
	retried, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: started.TurnID, RequestKey: "retry-once"})
	if err != nil || retried.Activity != ThreadActivityActive {
		t.Fatalf("retry=%#v err=%v", retried, err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.TurnID != started.TurnID && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted
	})
	userItems := 0
	for _, item := range completed.Items {
		if item.Kind == ThreadItemUser {
			userItems++
		}
	}
	if userItems != 1 {
		t.Fatalf("retry produced %d canonical user items, want 1: %#v", userItems, completed.Items)
	}
	replayed, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: started.TurnID, RequestKey: "retry-once"})
	if err != nil || replayed.TurnID != completed.TurnID || len(gateway.Requests()) != 2 ||
		countThreadItems(replayed.Items, ThreadItemUser) != 1 || countThreadItems(replayed.Items, ThreadItemAssistant) != 2 {
		t.Fatalf("retry replay=%#v requests=%d err=%v", replayed, len(gateway.Requests()), err)
	}
}

func TestThreadServiceQueueOperationsRemainCanonical(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-queue"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "active"}, RequestKey: "send-active"}); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ key, text string }{{"queue-a", "A"}, {"queue-b", "B"}, {"queue-c", "C"}} {
		if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: item.text}, RequestKey: RequestKey(item.key)}); err != nil {
			t.Fatal(err)
		}
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return len(view.Queue) == 3 })
	reordered, err := service.ReorderQueue(t.Context(), ReorderQueueInput{
		ThreadID: created.ThreadID, OrderedItemIDs: []string{"queue:queue-c", "queue:queue-a", "queue:queue-b"}, RequestKey: "reorder-queue",
	})
	if err != nil || reordered.Queue[0].Input.Text != "C" {
		t.Fatalf("reorder=%#v err=%v", reordered.Queue, err)
	}
	deleted, err := service.DeleteQueued(t.Context(), DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queue-a", RequestKey: "delete-queue-a"})
	if err != nil || len(deleted.Queue) != 2 || deleted.Queue[0].Input.Text != "C" || deleted.Queue[1].Input.Text != "B" {
		t.Fatalf("delete=%#v err=%v", deleted.Queue, err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-active-for-queue"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle })
	promoted, err := service.PromoteQueued(t.Context(), PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queue-c", RequestKey: "promote-queue-c"})
	if err != nil || promoted.Activity != ThreadActivityActive || len(promoted.Queue) != 1 || promoted.Queue[0].Input.Text != "B" {
		t.Fatalf("promote=%#v err=%v", promoted, err)
	}
}

func TestThreadServiceSubscriptionPublishesReplaceableCurrentViews(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(subscription.Close)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-subscription"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "watch"}, RequestKey: "send-subscription"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		current, nextErr := subscription.Next(ctx)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if current.ThreadID == created.ThreadID && current.Activity == ThreadActivityActive {
			if len(current.Items) != 1 || current.Items[0].Text != "watch" {
				t.Fatalf("current view=%#v", current)
			}
			break
		}
	}
}

func TestThreadRuntimeFencesLateAndDuplicateProviderAttempts(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{turnID: "turn-current", runID: "run-current", view: ThreadView{ThreadID: "thread-current", TurnID: "turn-current"}}}
	current := Event{ThreadID: "thread-current", TurnID: "turn-current", RunID: "run-current", Stream: &StreamObservation{
		Type: StreamObservationAssistantDelta, Text: "new", LogicalRequestID: "request", AttemptID: "attempt-2", AttemptEpoch: 2,
	}}
	if !actor.acceptLiveEvent(current) || len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "new" {
		t.Fatalf("current attempt was not accepted: %#v", actor.state)
	}
	late := current
	late.Stream = &StreamObservation{Type: StreamObservationAssistantDelta, Text: "stale", LogicalRequestID: "request", AttemptID: "attempt-1", AttemptEpoch: 1}
	if actor.acceptLiveEvent(late) || len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "new" {
		t.Fatalf("late attempt changed draft: %#v", actor.state)
	}
	duplicateIdentity := current
	duplicateIdentity.Stream = &StreamObservation{Type: StreamObservationAssistantDelta, Text: "other", LogicalRequestID: "request", AttemptID: "attempt-other", AttemptEpoch: 2}
	if actor.acceptLiveEvent(duplicateIdentity) || len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "new" {
		t.Fatalf("conflicting attempt changed draft: %#v", actor.state)
	}
}

func TestThreadRuntimeDuplicateStreamEventKeepsOneAssistantIdentity(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{turnID: "turn-duplicate", runID: "run-duplicate", view: ThreadView{ThreadID: "thread-duplicate", TurnID: "turn-duplicate"}}}
	event := Event{ThreadID: "thread-duplicate", TurnID: "turn-duplicate", RunID: "run-duplicate", Stream: &StreamObservation{
		Type: StreamObservationAssistantDelta, Text: "same", LogicalRequestID: "request-duplicate", AttemptID: "attempt-duplicate", AttemptEpoch: 1,
	}}
	if !actor.acceptLiveEvent(event) || !actor.acceptLiveEvent(event) {
		t.Fatal("duplicate stream event was unexpectedly rejected")
	}
	if len(actor.state.view.Items) != 1 || actor.state.view.Items[0].ID != "assistant:turn-duplicate:1" {
		t.Fatalf("duplicate stream event created another assistant identity: %#v", actor.state.view.Items)
	}
}

func TestThreadServiceCancelIsIdempotentAcrossIdlePreparingWaitingAndTerminal(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		_, service := testThreadService(t, newBlockingThreadGateway())
		created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-idle-cancel"})
		if err != nil {
			t.Fatal(err)
		}
		view, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-idle"})
		if err != nil || view.Activity != ThreadActivityIdle {
			t.Fatalf("idle cancel=%#v err=%v", view, err)
		}
	})

	t.Run("preparing", func(t *testing.T) {
		releaseFactory := make(chan struct{})
		gateway := newBlockingThreadGateway()
		agent, err := testAgent(gateway)
		if err != nil {
			t.Fatal(err)
		}
		host, err := Open(t.Context(), Options{Storage: storage.Memory()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(AgentFactoryFunc(func(ctx context.Context, _ AgentRequest) (*Agent, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseFactory:
				return agent, nil
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		created, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-preparing-cancel"})
		if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "prepare"}, RequestKey: "send-preparing"}); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-preparing"}); err != nil {
			t.Fatal(err)
		}
		close(releaseFactory)
		cancelled := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCancelled
		})
		if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-preparing-again"}); err != nil || cancelled.Error != "" {
			t.Fatalf("preparing cancel=%#v err=%v", cancelled, err)
		}
	})

	t.Run("waiting input", func(t *testing.T) {
		gateway := florettest.NewScriptedGateway(
			provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
			provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
			florettest.Step{Events: []provider.Event{
				{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-cancel", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}},
				{Type: provider.EventDone, Reason: "tool_calls"},
			}},
		)
		_, service := testThreadService(t, gateway)
		created, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-waiting-cancel"})
		_, _ = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "wait"}, RequestKey: "send-waiting"})
		waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Attention.InputCount == 1 })
		if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-waiting"}); err != nil {
			t.Fatal(err)
		}
		view := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.Attention.InputCount == 0
		})
		if len(view.Interactions) != 1 || !view.Interactions[0].Resolved || view.Interactions[0].Resolution == nil || view.Interactions[0].Resolution.Outcome != "cancelled" {
			t.Fatalf("waiting cancel=%#v", view)
		}
	})

	t.Run("terminal", func(t *testing.T) {
		gateway := florettest.NewScriptedGateway(
			provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
			provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
			florettest.Step{Events: []provider.Event{{Type: provider.EventDone, Reason: "stop"}}},
		)
		_, service := testThreadService(t, gateway)
		created, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-terminal-cancel"})
		_, _ = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "finish"}, RequestKey: "send-terminal"})
		before := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.LastOutcome != nil
		})
		after, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel-terminal"})
		if err != nil || after.LastOutcome == nil || *after.LastOutcome != *before.LastOutcome {
			t.Fatalf("terminal cancel before=%#v after=%#v err=%v", before, after, err)
		}
	})
}

func TestThreadServiceRestartHydratesAcceptedTurnAndDurableQueue(t *testing.T) {
	path := t.TempDir() + "/thread-runtime.db"
	firstAgent, err := testAgent(newBlockingThreadGateway())
	if err != nil {
		t.Fatal(err)
	}
	firstHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	typedFirstService, err := firstHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return firstAgent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	firstService := typedFirstService.(*threadRuntimeService)
	created, err := firstService.Create(t.Context(), CreateThreadInput{RequestKey: "create-restart"})
	if err != nil {
		t.Fatal(err)
	}
	turnID, runID, err := firstHost.nextTurnRunIDs()
	if err != nil {
		t.Fatal(err)
	}
	input := UserInput{Text: "resume me"}
	inputFingerprint, _ := stableFingerprint(input)
	acceptRequest := sessiontree.AcceptTurnRequest{
		ThreadID: created.ThreadID.String(), TurnID: turnID.String(), RunID: runID.String(), LogicalRequestID: "send-restart",
		Input: session.Message{Role: session.User, Content: input.Text}, InputRequestFingerprint: inputFingerprint, Now: time.Now().UTC(),
	}
	acceptRequest.RequestFingerprint, err = sessiontree.TurnAcceptanceRequestFingerprint(acceptRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstHost.store.repo.(sessiontree.RuntimeTurnRepo).AcceptTurn(t.Context(), acceptRequest); err != nil {
		t.Fatal(err)
	}
	firstSummaries, err := firstService.List(t.Context(), ThreadScope{})
	if err != nil {
		t.Fatal(err)
	}
	if len(firstSummaries) != 1 || firstSummaries[0].Title != "resume me" || firstSummaries[0].TitleStatus != ThreadTitleStatusReady {
		t.Fatalf("accepted thread summary = %#v", firstSummaries)
	}
	queued := QueuedInput{ID: "queue:queue-restart", RequestKey: "queue-restart", Input: UserInput{Text: "queued"}, CreatedAt: time.Now().UTC()}
	queueFingerprint, _ := stableFingerprint(queued.Input)
	if err := firstService.appendQueueFact(t.Context(), created.ThreadID, sessiontree.EntryQueueAdded, queued.ID, queued.RequestKey, queueFingerprint, queued); err != nil {
		t.Fatal(err)
	}
	if err := firstHost.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "resumed"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "queued done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	secondAgent, err := testAgent(secondGateway)
	if err != nil {
		t.Fatal(err)
	}
	secondHost, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondHost.Shutdown(context.Background()) })
	secondService, err := secondHost.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return secondAgent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := secondService.View(t.Context(), created.ThreadID)
	if err != nil || hydrated.Activity != ThreadActivityActive || len(hydrated.Queue) != 1 {
		t.Fatalf("hydrated=%#v err=%v", hydrated, err)
	}
	restartedSummaries, err := secondService.List(t.Context(), ThreadScope{})
	if err != nil {
		t.Fatal(err)
	}
	if len(restartedSummaries) != 1 || restartedSummaries[0].Title != "resume me" || restartedSummaries[0].TitleStatus != ThreadTitleStatusReady {
		t.Fatalf("restarted thread summary = %#v", restartedSummaries)
	}
	completed := waitThreadView(t, secondService, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && len(view.Queue) == 0 && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeCompleted
	})
	if len(secondGateway.Requests()) != 2 || countThreadItems(completed.Items, ThreadItemUser) != 2 {
		t.Fatalf("restart requests=%d view=%#v", len(secondGateway.Requests()), completed)
	}
}

func TestThreadServiceUnknownEffectRequiresExplicitOneShotRetry(t *testing.T) {
	gateway := newBlockingThreadGateway()
	var toolRuns atomic.Int32
	effectTool := tools.Define[map[string]string](
		tools.Definition{
			Name: "write", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil, nil,
		func(context.Context, tools.Invocation[map[string]string]) (tools.Result, error) {
			toolRuns.Add(1)
			return tools.Result{Text: "written"}, nil
		},
	)
	agent, err := testAgent(gateway, WithAgentTools(effectTool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{
			EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
			PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-audit-hash", AuthorizedAt: time.Now().UTC(),
		})
	})))
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	typed, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	service := typed.(*threadRuntimeService)
	created, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-unknown-effect"})
	started, _ := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "effect"}, RequestKey: "send-unknown-effect"})
	select {
	case <-gateway.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	actor := service.runtime(created.ThreadID)
	var runID identity.RunID
	_ = actor.apply(t.Context(), func() error { runID = actor.state.runID; return nil })
	args := `{"value":"x"}`
	toolEntry := sessiontree.Entry{
		ID: "tool-call:unknown", ThreadID: created.ThreadID.String(), TurnID: started.TurnID.String(), RunID: runID.String(), Type: sessiontree.EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: "call-unknown", ToolName: "write", ToolArgs: args},
	}
	writer := host.store.repo.(sessiontree.RuntimeJournalRepo)
	if _, err := writer.AppendRuntimeFacts(t.Context(), created.ThreadID.String(), []sessiontree.Entry{toolEntry}); err != nil {
		t.Fatal(err)
	}
	authority := host.store.repo.(sessiontree.EffectAttemptRepo)
	invocation := sessiontree.EffectInvocationIdentity{
		ThreadID: created.ThreadID.String(), TurnID: started.TurnID.String(), RunID: runID.String(),
		ToolCallID: "call-unknown", ToolName: "write", ArgumentHash: sessiontree.StableHash(args),
	}
	prepared, err := authority.PrepareEffectAttempt(t.Context(), sessiontree.PrepareEffectAttemptRequest{Invocation: invocation, RequestFingerprint: "effect-fingerprint", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.BeginEffectDispatch(t.Context(), sessiontree.BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-fingerprint", AuthorizationProofHash: "proof", Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.MarkEffectUnknown(t.Context(), sessiontree.MarkEffectUnknownRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-fingerprint", OutcomeFingerprint: "unknown", Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	// Unknown work is stable and does not run until the user acknowledges it.
	time.Sleep(20 * time.Millisecond)
	if toolRuns.Load() != 0 {
		t.Fatalf("unknown effect replayed automatically: runs=%d", toolRuns.Load())
	}
	first, err := service.RetryEffect(t.Context(), RetryEffectInput{
		ThreadID: created.ThreadID, EffectAttemptID: prepared.Attempt.EffectAttemptID, ToolCallID: "call-unknown",
		AcknowledgeUnknownRisk: true, RequestKey: "retry-effect-once",
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && toolRuns.Load() != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if toolRuns.Load() != 1 {
		entries, _ := host.store.repo.Entries(t.Context(), created.ThreadID.String())
		t.Fatalf("effect retry did not run: entries=%#v", entries)
	}
	_, err = service.RetryEffect(t.Context(), RetryEffectInput{
		ThreadID: created.ThreadID, EffectAttemptID: prepared.Attempt.EffectAttemptID, ToolCallID: "call-unknown",
		AcknowledgeUnknownRisk: true, RequestKey: "retry-effect-once",
	})
	if err != nil {
		t.Fatalf("effect retry replay first=%#v err=%v", first, err)
	}
	time.Sleep(20 * time.Millisecond)
	if toolRuns.Load() != 1 {
		t.Fatalf("effect retry crossed one-shot fence %d times", toolRuns.Load())
	}
}

func testAgent(gateway provider.Gateway, options ...AgentOption) (*Agent, error) {
	return NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, options...)
}

func waitForCanonicalEntry(t *testing.T, host *Host, threadID identity.ThreadID, entryID string) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := host.store.repo.Entry(t.Context(), threadID.String(), entryID); err == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("canonical entry did not appear: " + entryID)
}

func waitForAtomic(t *testing.T, value *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if value.Load() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("atomic value=%d, want %d", value.Load(), want)
}

func countThreadItems(items []ThreadItem, kind ThreadItemKind) int {
	count := 0
	for _, item := range items {
		if item.Kind == kind {
			count++
		}
	}
	return count
}

type orderedThreadItemExpectation struct {
	kind   ThreadItemKind
	text   string
	toolID string
}

type orderedThreadItemIdentitySnapshot struct {
	ID      string
	Ordinal uint64
}

func threadItemKinds(items []ThreadItem, kinds ...ThreadItemKind) bool {
	if len(items) != len(kinds) {
		return false
	}
	for index := range kinds {
		if items[index].Kind != kinds[index] {
			return false
		}
	}
	return true
}

func assertOrderedThreadItems(t *testing.T, items []ThreadItem, expected []orderedThreadItemExpectation) {
	t.Helper()
	if len(items) != len(expected) {
		t.Fatalf("ordered items=%#v, want %d items", items, len(expected))
	}
	seenIDs := make(map[string]struct{}, len(items))
	for index, want := range expected {
		item := items[index]
		if item.ID == "" || item.Ordinal != uint64(index+1) || item.Kind != want.kind {
			t.Fatalf("ordered item[%d]=%#v, want kind=%q ordinal=%d", index, item, want.kind, index+1)
		}
		if _, duplicate := seenIDs[item.ID]; duplicate {
			t.Fatalf("ordered item ID %q is duplicated: %#v", item.ID, items)
		}
		seenIDs[item.ID] = struct{}{}
		if want.text != "" && item.Text != want.text {
			t.Fatalf("ordered item[%d] text=%q, want %q", index, item.Text, want.text)
		}
		if want.toolID != "" && (item.Activity == nil || item.Activity.ToolID != want.toolID) {
			t.Fatalf("ordered item[%d] activity=%#v, want tool %q", index, item.Activity, want.toolID)
		}
	}
}

func orderedThreadItemIdentity(items []ThreadItem) []orderedThreadItemIdentitySnapshot {
	out := make([]orderedThreadItemIdentitySnapshot, len(items))
	for index, item := range items {
		out[index] = orderedThreadItemIdentitySnapshot{ID: item.ID, Ordinal: item.Ordinal}
	}
	return out
}

func assertStableThreadItemPrefix(t *testing.T, prefix []orderedThreadItemIdentitySnapshot, items []ThreadItem) {
	t.Helper()
	if len(items) < len(prefix) {
		t.Fatalf("ordered sequence shrank from %d to %d: %#v", len(prefix), len(items), items)
	}
	for index, want := range prefix {
		if items[index].ID != want.ID || items[index].Ordinal != want.Ordinal {
			t.Fatalf("ordered prefix changed at %d: got=(%q,%d) want=(%q,%d)", index, items[index].ID, items[index].Ordinal, want.ID, want.Ordinal)
		}
	}
}

func unresolvedApprovalCount(view ThreadView) int {
	count := 0
	for _, interaction := range view.Interactions {
		if interaction.Kind == ThreadInteractionApproval && !interaction.Resolved {
			count++
		}
	}
	return count
}

func unresolvedApprovalID(view ThreadView) string {
	for _, interaction := range view.Interactions {
		if interaction.Kind == ThreadInteractionApproval && !interaction.Resolved {
			return interaction.ID
		}
	}
	return ""
}

func threadToolStatus(items []ThreadItem, toolID string) observation.ActivityStatus {
	for _, item := range items {
		if item.Kind == ThreadItemTool && item.Activity != nil && item.Activity.ToolID == toolID {
			return item.Activity.Status
		}
	}
	return ""
}

func assertToolInteractionState(t *testing.T, items []ThreadItem, toolID string, resolved bool) {
	t.Helper()
	for _, item := range items {
		if item.Kind == ThreadItemTool && item.Activity != nil && item.Activity.ToolID == toolID {
			if item.Interaction == nil || item.Interaction.Kind != ThreadInteractionApproval || item.Interaction.Resolved != resolved {
				t.Fatalf("tool %q interaction=%#v, want resolved=%v", toolID, item.Interaction, resolved)
			}
			return
		}
	}
	t.Fatalf("tool %q is missing from %#v", toolID, items)
}

func assertThreadToolTerminal(t *testing.T, items []ThreadItem, toolID string) {
	t.Helper()
	for _, item := range items {
		if item.Kind == ThreadItemTool && item.Activity != nil && item.Activity.ToolID == toolID {
			if item.Live || item.Activity.Status != observation.ActivityStatusSuccess {
				t.Fatalf("tool %q terminal item=%#v", toolID, item)
			}
			return
		}
	}
	t.Fatalf("tool %q is missing from %#v", toolID, items)
}

func assertNoLiveThreadItems(t *testing.T, items []ThreadItem) {
	t.Helper()
	for _, item := range items {
		if item.Live {
			t.Fatalf("terminal sequence retains live item %#v", item)
		}
	}
}

func waitString(t *testing.T, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("ordered presentation tool checkpoint timed out")
		return ""
	}
}

func assertSubscriptionThreadItems(t *testing.T, subscription *WorkspaceSubscription, threadID identity.ThreadID, expected []ThreadItem) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		view, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.ThreadID == threadID && sameThreadItemCheckpoint(view.Items, expected) {
			return
		}
	}
}

func sameThreadItemCheckpoint(items, expected []ThreadItem) bool {
	if len(items) != len(expected) {
		return false
	}
	for index := range expected {
		left, right := items[index], expected[index]
		if left.ID != right.ID || left.Ordinal != right.Ordinal || left.Kind != right.Kind || left.Text != right.Text || left.Live != right.Live {
			return false
		}
		if (left.Activity == nil) != (right.Activity == nil) || (left.Interaction == nil) != (right.Interaction == nil) {
			return false
		}
		if left.Activity != nil && (left.Activity.ToolID != right.Activity.ToolID || left.Activity.Status != right.Activity.Status) {
			return false
		}
		if left.Interaction != nil && (left.Interaction.ID != right.Interaction.ID || left.Interaction.Resolved != right.Interaction.Resolved) {
			return false
		}
	}
	return true
}

func TestThreadViewJSONRemainsDetachedFromSubscriptionMutation(t *testing.T) {
	// Keep the replaceable-current contract honest for host transports.
	view := ThreadView{ThreadID: "thread-json", Items: []ThreadItem{{ID: "item", Kind: ThreadItemAssistant, Text: "ok"}}}
	encoded, err := json.Marshal(cloneThreadRuntimeView(view))
	if err != nil || len(encoded) == 0 || !json.Valid(encoded) {
		t.Fatalf("view json=%q err=%v", encoded, err)
	}
}

func waitThreadView(t *testing.T, service ThreadService, threadID identity.ThreadID, ready func(ThreadView) bool) ThreadView {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		view, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if ready(view) {
			return view
		}
		time.Sleep(5 * time.Millisecond)
	}
	view, err := service.View(t.Context(), threadID)
	t.Fatalf("thread view did not converge: view=%#v err=%v", view, err)
	return ThreadView{}
}
