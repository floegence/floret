package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
)

func TestAcceptedExecutionCancellationBeforeHarnessOwnershipSettlesJournal(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	service := full.(*threadRuntimeService)
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	request := turnExecutionRequest{
		TurnID: "turn-before-harness", RunID: "run-before-harness",
		LogicalRequestID: "before-harness", Input: TurnInput{Text: "accepted"},
	}
	accepted, err := service.acceptCanonicalTurn(t.Context(), created.ThreadID, request)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := service.factory.Agent(t.Context(), AgentRequest{ThreadID: created.ThreadID})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := host.turnRunner(t.Context(), created.ThreadID, service.executionAgent(actor, agent))
	if err != nil {
		t.Fatal(err)
	}
	applyAgentExecutionPolicy(&request, agent)
	executionCtx, cancel := context.WithCancelCause(t.Context())
	defer cancel(context.Canceled)
	done := make(chan struct{})
	if err := actor.apply(t.Context(), func() error {
		_, err := actor.beginRun(beginThreadRunInput{
			turnID: request.TurnID, runID: request.RunID, logicalRequestID: request.LogicalRequestID,
			cancel: cancel, executionDone: done, clearOutcome: true,
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Cancellation here deterministically prevents ResumeThread from transferring
	// execution to the harness. No provider or terminal marker has run yet.
	cancel(context.Canceled)
	service.executeAcceptedSend(executionCtx, actor, runner, accepted, request, nil, done)
	canonical, found, err := host.store.repo.(sessiontree.RuntimeTurnRepo).ReadAcceptedTurn(t.Context(), created.ThreadID.String(), request.TurnID.String(), request.RunID.String())
	if err != nil || !found || canonical.Terminal == nil || canonical.Terminal.Terminal.TurnStatus != sessiontree.TurnAborted {
		t.Fatalf("accepted turn was not canonically cancelled: found=%t terminal=%v err=%v", found, canonical.Terminal != nil, err)
	}
	view, err := full.View(t.Context(), created.ThreadID)
	if err != nil || view.Activity != ThreadActivityIdle || view.Failure != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("cancelled actor=%+v err=%v", view, err)
	}
	// The next admission must succeed immediately, without retrying a falsely
	// idle actor whose canonical predecessor is still running.
	next := turnExecutionRequest{TurnID: "next-turn", RunID: "next-run", LogicalRequestID: "next", Input: TurnInput{Text: "next"}}
	if _, err := service.acceptCanonicalTurn(t.Context(), created.ThreadID, next); err != nil {
		t.Fatalf("next canonical admission failed: %v", err)
	}
}

func TestInputResumeCancellationBeforeHarnessOwnershipSettlesJournal(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "cancel", StateCompatibilityKey: "test:cancel"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: cancelHistoryAskArgs}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, full := testThreadServiceWithAgent(t, agent)
	service := full.(*threadRuntimeService)
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := full.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "ask"}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, full, created.ThreadID, func(view ThreadView) bool { return view.Attention.InputCount == 1 })
	actor := service.runtime(created.ThreadID)
	var previous <-chan struct{}
	if err := actor.apply(t.Context(), func() error { previous = actor.state.executionDone; return nil }); err != nil {
		t.Fatal(err)
	}
	if previous != nil {
		<-previous
	}
	request := resumeInputRequest{TurnID: waiting.TurnID, WaitingRunID: waiting.RunID, RunID: "resume-before-harness", Answer: "yes"}
	if err := service.appendRuntimeFactsError(t.Context(), created.ThreadID, []sessiontree.Entry{{
		ID: "continue-run:" + request.RunID.String(), Type: sessiontree.EntryTurnMarker,
		ThreadID: created.ThreadID.String(), TurnID: request.TurnID.String(), RunID: request.RunID.String(),
		TurnStatus: sessiontree.TurnSavePoint, Metadata: map[string]string{"reason": "input_resume"},
	}}); err != nil {
		t.Fatal(err)
	}
	runner, err := host.turnRunner(t.Context(), created.ThreadID, service.executionAgent(actor, agent))
	if err != nil {
		t.Fatal(err)
	}
	executionCtx, cancel := context.WithCancelCause(t.Context())
	defer cancel(context.Canceled)
	done := make(chan struct{})
	if err := actor.apply(t.Context(), func() error {
		_, err := actor.beginRun(beginThreadRunInput{turnID: request.TurnID, runID: request.RunID, logicalRequestID: "resume", cancel: cancel, executionDone: done})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cancel(context.Canceled)
	service.executeInputResume(executionCtx, actor, runner, created.ThreadID, request)
	close(done)
	terminal, err := host.store.repo.Entry(t.Context(), created.ThreadID.String(), stableCancellationEntryID(created.ThreadID, request.TurnID, request.RunID))
	if err != nil || terminal.TurnStatus != sessiontree.TurnAborted || terminal.RunID != request.RunID.String() {
		t.Fatalf("resume terminal status=%s run=%s err=%v", terminal.TurnStatus, terminal.RunID, err)
	}
	view, err := full.View(t.Context(), created.ThreadID)
	if err != nil || view.Activity != ThreadActivityIdle || view.Failure != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("cancelled continuation=%+v err=%v", view, err)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("cancelled resume reached provider")
	}
	if _, err := service.acceptCanonicalTurn(t.Context(), created.ThreadID, turnExecutionRequest{TurnID: "next-turn", RunID: "next-run", LogicalRequestID: "next", Input: TurnInput{Text: "next"}}); err != nil {
		t.Fatalf("next admission: %v", err)
	}
}
