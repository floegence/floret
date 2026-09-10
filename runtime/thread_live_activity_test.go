package runtime

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestThreadServiceShowsCommandWhileToolAndProviderAreBlocked(t *testing.T) {
	toolStarted, releaseTool, releaseProvider := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishTool, finishProvider := sync.OnceFunc(func() { close(releaseTool) }), sync.OnceFunc(func() { close(releaseProvider) })
	t.Cleanup(finishTool)
	t.Cleanup(finishProvider)
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningSupported, ReasoningCapability: config.ReasoningCapability{Kind: config.ReasoningKindNone}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "read-live", Name: "read_status", Args: `{"command":"printf ready"}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{BlockUntil: releaseProvider, Events: []provider.Event{{Type: provider.EventDelta, Text: "Finished."}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	tool := tools.Define[map[string]string](tools.Definition{
		Name: "read_status", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{"command": tools.String("command")}, []string{"command"}),
		Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
			return &tools.ActivityPresentation{Label: "Inspect environment", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: "exec", Command: inv.Args.(map[string]string)["command"]}}, nil
		},
	}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]string]) (tools.Result, error) {
		close(toolStarted)
		select {
		case <-releaseTool:
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		}
		return tools.Result{Text: "ready", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "ready", Status: "success"}}}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{
			EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
			PolicyRevision: "test-policy", AuditReference: "test-audit", AuditHash: "test-audit-hash", AuthorizedAt: time.Now().UTC(),
		})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(subscription.Close)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-live-command"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-live-command", Input: UserInput{Text: "Inspect environment"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-toolStarted:
	case <-time.After(3 * time.Second):
		view, _ := service.View(t.Context(), created.ThreadID)
		t.Fatalf("tool did not start: failure=%+v items=%+v", view.Failure, view.Items)
	}
	assert := func(status observation.ActivityStatus, output string) ThreadView {
		t.Helper()
		view, err := service.View(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range view.Items {
			if item.Activity == nil || item.Activity.ToolID != "read-live" {
				continue
			}
			activity := item.Activity
			if activity.Status != status || activity.Presentation == nil || activity.Presentation.Label != "Inspect environment" {
				t.Fatalf("missing immediate command presentation: %#v", activity)
			}
			payload := activity.Presentation.Payload.(tools.TerminalActivityPayload)
			if payload.Command != "printf ready" || payload.Output != output {
				t.Fatalf("command facts: %#v", payload)
			}
			assertLiveCommandSubscription(t, subscription, view)
			return view
		}
		t.Fatalf("missing executing tool: %#v", view.Items)
		return ThreadView{}
	}
	assert(observation.ActivityStatusRunning, "")
	finishTool()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	assert(observation.ActivityStatusSuccess, "ready")
	finishProvider()
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle })
	assert(observation.ActivityStatusSuccess, "ready")
}

func liveActivityFixture(t *testing.T) (*threadRuntimeService, *threadRuntimeState, threadRuntimeEventSink, *WorkspaceSubscription) {
	t.Helper()
	actor := &threadRuntimeState{threadID: "thread-live", state: threadRuntimeData{
		turnID: "turn-live", runID: "run-live", logicalRequestID: "logical-live",
		view: ThreadView{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Activity: ThreadActivityActive, ViewVersion: 1},
	}}
	service := &threadRuntimeService{runtimes: map[string]*threadRuntimeState{"thread-live": actor}, subscribers: make(map[*WorkspaceSubscription]struct{}), published: make(map[string]uint64)}
	subscription := &WorkspaceSubscription{views: make(chan ThreadView, 64), done: make(chan struct{})}
	service.subscribers[subscription] = struct{}{}
	t.Cleanup(subscription.Close)
	return service, actor, threadRuntimeEventSink{service: service}, subscription
}

func TestThreadRuntimePublishesToolActivityBeforeOutput(t *testing.T) {
	service, actor, sink, subscription := liveActivityFixture(t)
	sink.EmitEvent(Event{Type: observation.EventTypeProviderToolCallEnd, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Stream: &StreamObservation{
		Type: StreamObservationToolCallEnd, ToolCallStream: &ToolCallStream{ID: "call-live", Name: "terminal.exec"},
	}})
	if items := service.currentView(actor).Items; len(items) != 0 {
		t.Fatalf("unvalidated provider arguments created a running card: %#v", items)
	}
	events := []observation.Event{}
	emit := func(kind observation.EventType, presentation *tools.ActivityPresentation) {
		t.Helper()
		events = append(events, observation.Event{Type: kind, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", ToolID: "call-live", ToolName: "terminal.exec", ToolKind: "local", Activity: presentation, ObservedAt: time.Now()})
		timeline := observation.BuildActivityTimeline(observation.ActivityRunMeta{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live"}, events, time.Now().UnixMilli())
		sink.EmitEvent(Event{Type: kind, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", ToolID: "call-live", ActivityTimeline: &timeline})
	}
	assert := func(status observation.ActivityStatus, output string) {
		t.Helper()
		view := service.currentView(actor)
		if len(view.Items) != 1 || view.Items[0].Activity == nil {
			t.Fatalf("missing tool item: %#v", view.Items)
		}
		item := view.Items[0]
		activity := item.Activity
		if item.ID != "tool:turn-live:call-live" || item.Ordinal != 1 || activity.Status != status || activity.Presentation == nil || activity.Presentation.Label != "Inspect environment" {
			t.Fatalf("tool presentation/state mismatch: %#v", activity)
		}
		payload := activity.Presentation.Payload.(tools.TerminalActivityPayload)
		if payload.Command != "printf ready" || payload.Output != output {
			t.Fatalf("command/output mismatch: %#v", payload)
		}
		assertLiveCommandSubscription(t, subscription, view)
	}
	emit(observation.EventTypeToolCall, &tools.ActivityPresentation{Label: "Inspect environment", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "printf ready", Operation: "exec"}})
	assert(observation.ActivityStatusPending, "")
	emit(observation.EventTypeToolDispatchStarted, nil)
	assert(observation.ActivityStatusRunning, "")
	emit(observation.EventTypeToolActivityUpdated, &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "ready"}})
	assert(observation.ActivityStatusRunning, "ready")
	emit(observation.EventTypeToolResult, &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Status: "success"}})
	assert(observation.ActivityStatusSuccess, "ready")
	before := service.currentView(actor).Items[0]
	for range 30 {
		sink.EmitEvent(Event{Type: observation.EventTypeProviderReasoning, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Stream: &StreamObservation{Type: StreamObservationReasoningDelta, Text: "more "}})
	}
	if after := service.currentView(actor).Items[0]; !reflect.DeepEqual(before, after) {
		t.Fatalf("subsequent reasoning changed completed command: before=%#v after=%#v", before, after)
	}
}

func TestThreadRuntimeToolProjectionRejectsWrongExecution(t *testing.T) {
	for _, mismatch := range []string{"thread", "turn", "run", "attempt", "timeline"} {
		t.Run(mismatch, func(t *testing.T) {
			service, actor, sink, _ := liveActivityFixture(t)
			actor.state.attemptID, actor.state.attemptEpoch = "attempt-current", 2
			timeline := observation.ActivityTimeline{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Items: []observation.ActivityItem{{ToolID: "call-live", ToolName: "terminal.exec", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusRunning}}}
			ev := Event{Type: observation.EventTypeToolDispatchStarted, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", ActivityTimeline: &timeline}
			switch mismatch {
			case "thread":
				actor.state.view.ThreadID = "thread-other"
			case "turn":
				ev.TurnID = "turn-old"
			case "run":
				ev.RunID = "run-old"
			case "attempt":
				ev.attemptLogicalID, ev.attemptID, ev.attemptEpoch = "logical-live", "attempt-old", 1
			case "timeline":
				timeline.RunID = identity.RunID("run-old")
			}
			sink.EmitEvent(ev)
			if view := service.currentView(actor); len(view.Items) != 0 || view.ViewVersion != 1 {
				t.Fatalf("mismatched execution advanced view: %#v", view)
			}
		})
	}
}

func TestCanonicalRefreshPreservesObservedToolProgress(t *testing.T) {
	for _, status := range []observation.ActivityStatus{observation.ActivityStatusPending, observation.ActivityStatusWaiting, observation.ActivityStatusRunning, observation.ActivityStatusSuccess, observation.ActivityStatusError, observation.ActivityStatusCanceled, observation.ActivityStatusDeclined} {
		t.Run(string(status), func(t *testing.T) {
			current := []ThreadItem{{ID: "tool:turn:call", TurnID: "turn", RunID: "run", Ordinal: 1, Kind: ThreadItemTool, Live: true, Activity: &observation.ActivityItem{ToolID: "call", Status: status, Presentation: &tools.ActivityPresentation{Label: "Inspect environment", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "printf ready", Output: "ready"}}}}}
			canonical := cloneThreadItems(current)
			canonical[0].Live = false
			canonical[0].Activity.Status = observation.ActivityStatusRunning
			canonical[0].Activity.Presentation.Payload = tools.TerminalActivityPayload{Command: "printf ready"}
			got, ok := reconcileCanonicalThreadItems(current, canonical, false)
			if !ok || !reflect.DeepEqual(got, current) {
				t.Fatalf("canonical call regressed observed tool: %#v", got)
			}
			got, ok = reconcileCanonicalThreadItems(current, canonical, true)
			if !ok || !reflect.DeepEqual(got, canonical) {
				t.Fatal("terminal settlement did not restore journal authority")
			}
		})
	}
}

func assertLiveCommandSubscription(t *testing.T, subscription *WorkspaceSubscription, expected ThreadView) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		view, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.ThreadID != expected.ThreadID || view.ViewVersion < expected.ViewVersion {
			continue
		}
		for index, item := range expected.Items {
			if item.Activity == nil {
				continue
			}
			if len(view.Items) <= index || view.Items[index].Activity == nil || view.Items[index].Activity.Status != item.Activity.Status || !reflect.DeepEqual(view.Items[index].Activity.Presentation, item.Activity.Presentation) {
				t.Fatalf("published command differs from View: got=%#v want=%#v", view.Items, expected.Items)
			}
		}
		return
	}
}

func TestThreadRuntimeToolResultSurvivesBlockedCanonicalRefresh(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-tool-refresh-race"})
	if err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(created.ThreadID)
	_ = actor.apply(t.Context(), func() error {
		actor.state.turnID, actor.state.runID = "turn-live", "run-live"
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.TurnID, actor.state.view.RunID = "turn-live", "run-live"
		return nil
	})
	started, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	host.store.repo = &blockingCanonicalPathRepo{Repo: host.store.repo, started: started, release: release}
	refreshed := make(chan error, 1)
	go func() { refreshed <- typed.refreshCanonical(created.ThreadID, "turn-live") }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("canonical refresh did not start")
	}
	sink := threadRuntimeEventSink{service: typed}
	for _, status := range []observation.ActivityStatus{observation.ActivityStatusRunning, observation.ActivityStatusSuccess} {
		timeline := observation.ActivityTimeline{ThreadID: created.ThreadID, TurnID: "turn-live", RunID: "run-live", Items: []observation.ActivityItem{{
			ToolID: "call-live", ToolName: "terminal.exec", Kind: observation.ActivityKindTool, Status: status,
			Presentation: &tools.ActivityPresentation{Label: "Inspect environment", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "printf ready", Output: "ready"}},
		}}}
		sink.EmitEvent(Event{Type: observation.EventTypeToolActivityUpdated, ThreadID: created.ThreadID, TurnID: "turn-live", RunID: "run-live", ActivityTimeline: &timeline})
	}
	sink.EmitEvent(Event{Type: observation.EventTypeProviderDelta, ThreadID: created.ThreadID, TurnID: "turn-live", RunID: "run-live", Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: "Continuing analysis"}})
	before := typed.currentView(actor)
	releaseOnce()
	select {
	case err := <-refreshed:
		if err == nil {
			t.Fatal("stale canonical refresh unexpectedly replaced the live view")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canonical refresh did not finish")
	}
	after := typed.currentView(actor)
	if !reflect.DeepEqual(before, after) || len(after.Items) != 2 || after.Items[0].Activity.Status != observation.ActivityStatusSuccess {
		t.Fatalf("tool result was lost during refresh: %#v", after)
	}
}

func TestThreadRuntimeTerminalViewWaitsForCanonicalSnapshot(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-terminal-snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(created.ThreadID)
	_ = actor.apply(t.Context(), func() error {
		actor.state.turnID, actor.state.runID = "turn-stop", "run-stop"
		actor.state.view.TurnID, actor.state.view.RunID = "turn-stop", "run-stop"
		actor.state.view.Activity = ThreadActivityActive
		actor.state.view.Cancellation = &ThreadCancellation{ThreadID: created.ThreadID, TurnID: "turn-stop", RunID: "run-stop", Source: "user_stop", Mode: CancelModeGraceful, RequestedAt: time.Now()}
		return nil
	})
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	host.store.repo = &blockingCanonicalPathRepo{Repo: host.store.repo, started: started, release: release}
	finished := make(chan struct{})
	go func() {
		typed.finishSend(actor, "turn-stop", "run-stop", TurnResult{Status: TurnStatusCancelled}, context.Canceled)
		close(finished)
	}()
	waitClosed(t, started, "terminal snapshot read did not start")
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil || view.Activity != ThreadActivityActive || view.LastOutcome != nil || view.Cancellation == nil {
		t.Fatalf("partial terminal snapshot became visible: %#v, %v", view, err)
	}
	unblock()
	waitClosed(t, finished, "terminal snapshot did not settle")
	view, err = service.View(t.Context(), created.ThreadID)
	if err != nil || view.Activity != ThreadActivityIdle || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("complete terminal snapshot missing: %#v, %v", view, err)
	}
}
