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

type blockingThreadGateway struct {
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
	requests atomic.Int32
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
	if requests := gateway.Requests(); len(requests) != 2 {
		t.Fatalf("provider requests=%d, want 2", len(requests))
	} else {
		messages := requests[1].Messages
		if len(messages) < 2 || messages[len(messages)-1].Role != provider.RoleUser || !strings.Contains(messages[len(messages)-1].Text, `{"q":"yes"}`) {
			t.Fatalf("continuation answer is not the final provider message: %#v", messages)
		}
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
	if !actor.acceptLiveEvent(current) || actor.state.assistantDraft != "new" {
		t.Fatalf("current attempt was not accepted: %#v", actor.state)
	}
	late := current
	late.Stream = &StreamObservation{Type: StreamObservationAssistantDelta, Text: "stale", LogicalRequestID: "request", AttemptID: "attempt-1", AttemptEpoch: 1}
	if actor.acceptLiveEvent(late) || actor.state.assistantDraft != "new" {
		t.Fatalf("late attempt changed draft: %#v", actor.state)
	}
	duplicateIdentity := current
	duplicateIdentity.Stream = &StreamObservation{Type: StreamObservationAssistantDelta, Text: "other", LogicalRequestID: "request", AttemptID: "attempt-other", AttemptEpoch: 2}
	if actor.acceptLiveEvent(duplicateIdentity) || actor.state.assistantDraft != "new" {
		t.Fatalf("conflicting attempt changed draft: %#v", actor.state)
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
