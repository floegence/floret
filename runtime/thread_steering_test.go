package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/provider"
)

type steeringGateway struct {
	mu          sync.Mutex
	requests    []provider.Request
	started     chan struct{}
	interrupted chan struct{}
}

func (g *steeringGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}
}
func (g *steeringGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}
func (g *steeringGateway) Stream(ctx context.Context, request provider.Request) (<-chan provider.Event, error) {
	g.mu.Lock()
	g.requests = append(g.requests, request)
	n := len(g.requests)
	g.mu.Unlock()
	events := make(chan provider.Event, 2)
	if n == 1 {
		close(g.started)
		go func() { defer close(events); <-ctx.Done(); close(g.interrupted) }()
	} else {
		events <- provider.Event{Type: provider.EventDelta, Text: "guided"}
		events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
		close(events)
	}
	return events, nil
}
func TestSteerInterruptsProviderAndContinuesSameExecution(t *testing.T) {
	gateway := &steeringGateway{started: make(chan struct{}), interrupted: make(chan struct{})}
	_, service := testThreadService(t, gateway)
	child, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "original"}, RequestKey: "first"})
	if err != nil {
		t.Fatal(err)
	}
	waitClosed(t, gateway.started, "provider did not start")
	controller, ok := service.(ThreadSteeringController)
	if !ok {
		t.Fatal("thread steering capability is missing")
	}
	command := SteerInput{ThreadID: child.ThreadID, Input: UserInput{Text: "new direction"}, RequestKey: "guide"}
	accepted, err := controller.Steer(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted.Queue) != 0 || accepted.TurnID != first.TurnID || accepted.RunID != first.RunID {
		t.Fatalf("steer changed execution or queued input: %#v", accepted)
	}
	waitClosed(t, gateway.interrupted, "old provider was not interrupted")
	final := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if final.LastOutcome == nil || *final.LastOutcome != TurnOutcomeCompleted || final.TurnID != first.TurnID || final.RunID != first.RunID {
		t.Fatalf("final=%#v", final)
	}
	gateway.mu.Lock()
	requests := append([]provider.Request(nil), gateway.requests...)
	gateway.mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("provider requests=%d", len(requests))
	}
	found := false
	for _, message := range requests[1].Messages {
		if message.Text == "new direction" {
			found = true
		}
	}
	if !found {
		t.Fatalf("steering was not in provider context: %#v", requests[1].Messages)
	}
	if _, err := controller.Steer(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	command.Input.Text = "conflicting"
	if _, err := controller.Steer(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestSteerSettlesToolsBeforeApplyingInput(t *testing.T) {
	for _, scenario := range []string{"cancelled", "completed", "unconfirmed", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			work := tools.Define[map[string]any](tools.Definition{Name: "work", Effects: []tools.Effect{tools.EffectWrite}, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
				close(started)
				<-ctx.Done()
				switch scenario {
				case "timeout":
					<-release
					return tools.CanceledResult(inv.CallID, inv.Name, "late"), nil
				case "unconfirmed":
					return tools.Result{}, context.Canceled
				case "completed":
					return tools.Result{Text: "confirmed"}, nil
				default:
					return tools.CanceledResult(inv.CallID, inv.Name, "confirmed cancelled"), nil
				}
			})
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work-call", Name: "work", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "guided"}, {Type: provider.EventDone, Reason: "stop"}}})
			agent, err := testAgent(gateway, WithAgentTools(work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			host, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "work"})
			if err != nil {
				t.Fatal(err)
			}
			waitClosed(t, started, "tool did not start")
			accepted, err := service.(ThreadSteeringController).Steer(t.Context(), SteerInput{ThreadID: created.ThreadID, Input: UserInput{Text: "new instruction"}, RequestKey: "guide"})
			if err != nil {
				t.Fatal(err)
			}
			if accepted.RunID != first.RunID || len(accepted.Queue) != 0 {
				t.Fatal("steering changed execution")
			}
			waitDeadline := time.Now().Add(8 * time.Second)
			var final ThreadView
			for time.Now().Before(waitDeadline) {
				final, err = service.View(t.Context(), created.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				if final.Activity == ThreadActivityIdle {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if final.Activity != ThreadActivityIdle {
				t.Fatal("steering did not settle")
			}
			unknown := scenario == "unconfirmed" || scenario == "timeout"
			if unknown {
				if final.Failure == nil || final.Failure.Code != ThreadTurnFailureEffectOutcomeUnknown {
					t.Fatalf("unknown effect resumed: %#v", final)
				}
				if len(gateway.Requests()) != 1 {
					t.Fatalf("unknown effect was replayed: %#v", gateway.Requests())
				}
			} else {
				if final.LastOutcome == nil || *final.LastOutcome != TurnOutcomeCompleted {
					t.Fatalf("steering failed: %#v", final)
				}
				requests := gateway.Requests()
				if len(requests) != 2 {
					t.Fatalf("requests=%d", len(requests))
				}
				msgs := requests[1].Messages
				if msgs[len(msgs)-1].Text != "new instruction" || msgs[len(msgs)-2].ToolResult == nil {
					t.Fatalf("unpaired steering history: %#v", msgs)
				}
			}
			entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
			if err != nil {
				t.Fatal(err)
			}
			users, results := 0, 0
			for _, entry := range entries {
				if entry.Type == sessiontree.EntryUserMessage {
					users++
				}
				if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "work-call" {
					results++
				}
			}
			if users != 1 || results != 1 {
				t.Fatalf("duplicate facts: users=%d results=%d", users, results)
			}
		})
	}
}

func TestSteerWithdrawsWaitingInput(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "guided"}, {Type: provider.EventDone, Reason: "stop"}}})
	_, service := testThreadService(t, gateway)
	child, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "work"})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return len(v.Interactions) > 0 })
	// Wait for the waiting Engine to release its Run ownership.
	actor := service.(*threadRuntimeService).runtime(child.ThreadID)
	var done <-chan struct{}
	_ = actor.apply(t.Context(), func() error { done = actor.state.executionDone; return nil })
	waitClosed(t, done, "waiting execution did not stop")
	_, err = service.(ThreadSteeringController).Steer(t.Context(), SteerInput{ThreadID: child.ThreadID, Input: UserInput{Text: "proceed without an answer"}, RequestKey: "guide"})
	if err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if final.TurnID != first.TurnID || final.RunID == first.RunID || final.LastOutcome == nil || *final.LastOutcome != TurnOutcomeCompleted {
		t.Fatalf("continuation=%#v", final)
	}
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: child.ThreadID, RequestKey: "late", Answers: []InteractionAnswer{{InteractionID: waiting.Interactions[0].ID, Input: map[string]string{"q": "yes"}}}}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("late answer=%v", err)
	}
}

func TestSteerQueuedConsumesExactInputAndRejectsStaleContent(t *testing.T) {
	gateway := &steeringGateway{started: make(chan struct{}), interrupted: make(chan struct{})}
	_, service := testThreadService(t, gateway)
	child, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	first, _ := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "work"})
	waitClosed(t, gateway.started, "provider did not start")
	queued, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "queued instruction"}, RequestKey: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	controller := service.(ThreadSteeringController)
	command := SteerQueuedInput{ThreadID: child.ThreadID, QueueItemID: queued.Queue[0].ID, ExpectedInput: UserInput{Text: "stale"}, RequestKey: "guide"}
	if _, err := controller.SteerQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("stale=%v", err)
	}
	command.ExpectedInput = queued.Queue[0].Input
	accepted, err := controller.SteerQueued(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted.Queue) != 0 || accepted.RunID != first.RunID {
		t.Fatal("queue was not atomically consumed")
	}
	waitClosed(t, gateway.interrupted, "provider did not stop")
	waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if _, err := controller.SteerQueued(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	command.RequestKey = "other"
	if _, err := controller.SteerQueued(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("consumed=%v", err)
	}
}

func TestSteerStopCancelsPendingWithoutRestart(t *testing.T) {
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	work := tools.Define[map[string]any](tools.Definition{Name: "work", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return tools.CanceledResult(inv.CallID, inv.Name, "stopped"), nil
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work-call", Name: "work", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway, WithAgentTools(work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	child, _ := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "work"})
	waitClosed(t, started, "tool did not start")
	controller := service.(ThreadSteeringController)
	command := SteerInput{ThreadID: child.ThreadID, Input: UserInput{Text: "pending instruction"}, RequestKey: "guide"}
	if _, err := controller.Steer(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, cancelled, "tool was not cancelled")
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: child.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if len(gateway.Requests()) != 1 {
		t.Fatal("stop revived execution")
	}
	for _, item := range final.Items {
		if item.ID == "steer:guide" && (item.Steering == nil || item.Steering.Status != ThreadSteeringCancelled) {
			t.Fatalf("pending instruction stuck: %#v", item)
		}
	}
	if _, err := controller.Steer(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("idempotent replay restarted stopped task")
	}
}

// The durable admission boundary may survive a process exit before an Engine
// sees it. Seed exactly that boundary without running the old provider.
func TestSteerRecoversPendingAdmissionFromSQLite(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})
	path := t.TempDir() + "/steering.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
		if err != nil {
			t.Fatal(err)
		}
		return h, s
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	turnID, runID, err := host.nextTurnRunIDs()
	if err != nil {
		t.Fatal(err)
	}
	input := UserInput{Text: "original"}
	fingerprint, _ := stableFingerprint(input)
	_, err = service.(*threadRuntimeService).acceptCanonicalTurn(t.Context(), created.ThreadID, turnExecutionRequest{TurnID: turnID, RunID: runID, LogicalRequestID: "start", Input: input, InputFingerprint: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	guide := UserInput{Text: "recover this instruction"}
	guideFingerprint, _ := stableFingerprint(guide)
	_, err = host.store.repo.(sessiontree.RuntimeJournalRepo).AppendRuntimeFacts(t.Context(), created.ThreadID.String(), []sessiontree.Entry{{ID: "steer:guide", Type: sessiontree.EntrySteeringRequested, ThreadID: created.ThreadID.String(), TurnID: turnID.String(), RunID: runID.String(), RequestKey: "guide", RequestFingerprint: guideFingerprint, Message: session.Message{Role: session.User, Content: guide.Text}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	if _, err := service.View(t.Context(), created.ThreadID); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted || final.TurnID != turnID {
		t.Fatalf("recovery failed: %#v", final)
	}
	requests := gateway.Requests()
	if len(requests) != 1 || requests[0].Messages[len(requests[0].Messages)-1].Text != guide.Text {
		t.Fatalf("recovered history=%#v", requests)
	}
	if _, err := service.(ThreadSteeringController).Steer(t.Context(), SteerInput{ThreadID: created.ThreadID, Input: guide, RequestKey: "guide"}); err != nil {
		t.Fatal(err)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("replayed admission ran again")
	}
}

func TestSteerAcceptsOrderedInstructionsDuringToolSettlement(t *testing.T) {
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	work := tools.Define[map[string]any](tools.Definition{Name: "work", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return tools.CanceledResult(inv.CallID, inv.Name, "cancelled"), nil
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work", Name: "work", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway, WithAgentTools(work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "original"}, RequestKey: "start"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, started, "tool did not start")
	controller := service.(ThreadSteeringController)
	first := SteerInput{ThreadID: created.ThreadID, Input: UserInput{Text: "first correction"}, RequestKey: "first"}
	if _, err := controller.Steer(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, cancelled, "tool did not cancel")
	if _, err := controller.Steer(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Steer(t.Context(), SteerInput{ThreadID: created.ThreadID, Input: UserInput{Text: "second correction"}, RequestKey: "second"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	final := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if final.Failure != nil {
		t.Fatalf("failure=%#v", final.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	messages := requests[1].Messages
	if messages[len(messages)-2].Text != "first correction" || messages[len(messages)-1].Text != "second correction" {
		t.Fatalf("instructions reordered: %#v", messages)
	}
}

func TestSteerWithdrawsApprovalWithoutDispatch(t *testing.T) {
	work := tools.Define[map[string]any](tools.Definition{Name: "write", Effects: []tools.Effect{tools.EffectWrite}, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		t.Error("withdrawn approval dispatched a tool")
		return tools.Result{}, nil
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "steering", StateCompatibilityKey: "test:steering"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write", Name: "write", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway, WithAgentTools(work))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "start"})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Attention.ApprovalCount == 1 })
	if _, err := service.(ThreadSteeringController).Steer(t.Context(), SteerInput{ThreadID: created.ThreadID, Input: UserInput{Text: "do not write"}, RequestKey: "guide"}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if final.Failure != nil || final.TurnID != first.TurnID || final.RunID != first.RunID || final.Attention.ApprovalCount != 0 {
		t.Fatalf("approval steering=%#v", final)
	}
	approved := true
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID, RequestKey: "late", Answers: []InteractionAnswer{{Approved: &approved}}}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("late approval=%v", err)
	}
	if len(gateway.Requests()) != 2 {
		t.Fatalf("requests=%d", len(gateway.Requests()))
	}
}
