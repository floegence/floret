package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

type unavailableFinishKernel struct {
	*internalstorage.BackendKernel
	reject *atomic.Bool
}

func (kernel unavailableFinishKernel) FinishTurn(ctx context.Context, request sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	if request.Status == sessiontree.TurnCompleted && kernel.reject.Load() {
		return sessiontree.FinishTurnResult{}, context.DeadlineExceeded
	}
	return kernel.BackendKernel.FinishTurn(ctx, request)
}

func TestFinishConfirmationAfterRestartDoesNotRedispatch(t *testing.T) {
	for _, scenario := range []string{"restart", "confirm", "cancel"} {
		t.Run(scenario, func(t *testing.T) { exerciseFinishConfirmation(t, scenario) })
	}
}

func exerciseFinishConfirmation(t *testing.T, scenario string) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "turn.db")
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:finish"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "confirmed response"}, {Type: provider.EventDone, Reason: "stop"}}})
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(ctx, Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	created, err := service.Create(ctx, CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	var reject atomic.Bool
	reject.Store(true)
	host.store.repo = unavailableFinishKernel{host.store.repo.(*internalstorage.BackendKernel), &reject}
	if _, err := service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	pending := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.RunProgress != nil && view.RunProgress.Phase == ThreadRunPhaseAwaitingConfirmation
	})
	if pending.Failure != nil || pending.LastOutcome != nil || len(gateway.Requests()) != 1 {
		t.Fatalf("premature settlement: %+v", pending)
	}
	switch scenario {
	case "restart":
		if err := host.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		host, service = open()
	case "confirm":
		reject.Store(false)
		if err := service.(ThreadFinishConfirmer).ConfirmTurnFinish(ctx, pending.ThreadID, pending.TurnID, pending.RunID); err != nil {
			t.Fatal(err)
		}
	case "cancel":
		if _, err := service.Cancel(ctx, CancelInput{ThreadID: pending.ThreadID, RequestKey: "cancel"}); err != nil {
			t.Fatal(err)
		}
		reject.Store(false)
		if err := service.(ThreadFinishConfirmer).ConfirmTurnFinish(ctx, pending.ThreadID, pending.TurnID, pending.RunID); err == nil {
			t.Fatal("confirmation replaced a cancellation")
		}
		cancelled, err := service.View(ctx, pending.ThreadID)
		if err != nil || cancelled.LastOutcome == nil || *cancelled.LastOutcome != TurnOutcomeCancelled || len(gateway.Requests()) != 1 {
			t.Fatalf("cancellation changed: %+v err=%v", cancelled, err)
		}
		return
	}
	final := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if final.TurnID != pending.TurnID || final.RunID != pending.RunID || *final.LastOutcome != TurnOutcomeCompleted || final.Failure != nil || len(gateway.Requests()) != 1 {
		t.Fatalf("wrong recovered completion: %+v; provider calls=%d", final, len(gateway.Requests()))
	}
	confirmer := service.(ThreadFinishConfirmer)
	if err := confirmer.ConfirmTurnFinish(ctx, final.ThreadID, final.TurnID, final.RunID); err != nil {
		t.Fatalf("repeated confirmation: %v", err)
	}
	if err := confirmer.ConfirmTurnFinish(ctx, final.ThreadID, final.TurnID, "unrelated"); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("unrelated execution: %v", err)
	}
}
