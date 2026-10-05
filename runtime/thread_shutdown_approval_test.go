package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestCancellationSettlementReusesPendingGracefulRequest(t *testing.T) {
	for _, boundary := range []string{"shutdown", "immediate_stop", "execution_context"} {
		t.Run(boundary, func(t *testing.T) {
			gateway := newBlockingThreadGateway()
			host, threadService := testThreadService(t, gateway)
			service := threadService.(*threadRuntimeService)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "wait"}}); err != nil {
				t.Fatal(err)
			}
			waitClosed(t, gateway.started, "provider did not start")
			actor := service.runtime(created.ThreadID)
			key := "stop"
			request := cancellationRequest{EntryID: "cancel:" + key, RequestKey: key}
			request.RequestFingerprint, err = stableFingerprint(struct {
				ThreadID string
				TurnID   string
				Mode     CancelMode
			}{created.ThreadID.String(), service.currentView(actor).TurnID.String(), CancelModeGraceful})
			if err != nil {
				t.Fatal(err)
			}
			err = actor.apply(t.Context(), func() error {
				turnID, runID := actor.state.turnID, actor.state.runID
				result, err := host.store.repo.(sessiontree.RuntimeTurnRepo).CancelTurn(t.Context(), sessiontree.CancelTurnRequest{
					RequestOnly: true, ThreadID: created.ThreadID.String(), TurnID: turnID.String(), RunID: runID.String(),
					CancelEntryID: request.EntryID, RequestKey: request.RequestKey, RequestFingerprint: request.RequestFingerprint,
					TerminalEntryID: stableCancellationEntryID(created.ThreadID, turnID, runID), OutcomeFingerprint: request.RequestFingerprint,
					InteractionResolutionPayload: []byte(`{"accepted":false,"outcome":"cancelled"}`),
					Metadata:                     map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureCancelled},
					CancellationMetadata:         map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)},
					Now:                          time.Now().UTC(),
				})
				if err == nil {
					actor.state.view.Cancellation = threadCancellationForTurn([]sessiontree.Entry{result.CancelRequest}, turnID)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			switch boundary {
			case "immediate_stop":
				if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop-again", Mode: CancelModeImmediate}); err != nil {
					t.Fatal(err)
				}
			case "execution_context":
				view := service.currentView(actor)
				service.finishUnloadedCancellation(actor, created.ThreadID, view.TurnID, view.RunID)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
			if err != nil {
				t.Fatal(err)
			}
			cancellations := 0
			for _, entry := range entries {
				if entry.Type == sessiontree.EntryCancelRequested {
					cancellations++
					if entry.Metadata["cancellation_source"] != "user_stop" || entry.Metadata["cancellation_mode"] != string(CancelModeGraceful) {
						t.Fatalf("settlement replaced original cancellation metadata: %+v", entry.Metadata)
					}
					if entry.ID != request.EntryID {
						t.Fatalf("settlement used a different cancel request: %s", entry.ID)
					}
				}
			}
			if cancellations != 1 {
				t.Fatalf("cancel requests=%d, want one", cancellations)
			}
		})
	}
}
func TestShutdownSettlesActiveApprovalBeforeReopen(t *testing.T) {
	var effects atomic.Int32
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "approval", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write", Name: "write", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	tool := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		effects.Add(1)
		return tools.Result{Text: "written"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/approval.db"
	open := func() (*Host, ThreadService) {
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return h, s
	}
	h, s := open()
	created, err := s.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write once"}})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, s, created.ThreadID, func(v ThreadView) bool { return unresolvedApprovalCount(v) == 1 })
	_, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "queued", Input: UserInput{Text: "later"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, s = open()
	v, err := s.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Activity != ThreadActivityIdle || v.LastOutcome == nil || *v.LastOutcome != TurnOutcomeCancelled || unresolvedApprovalCount(v) != 0 || len(v.Queue) != 1 {
		t.Fatalf("shutdown did not settle approval and retain queue: %+v", v)
	}
	approved := true
	if _, err := s.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: unresolvedApprovalID(waiting), RequestKey: "stale-approve", Answers: []InteractionAnswer{{Approved: &approved}}}); err == nil {
		t.Fatal("late approval accepted after shutdown")
	}
	if effects.Load() != 0 {
		t.Fatal("shutdown or stale approval executed effect")
	}
}

func TestShutdownJoinsEffectAndPreservesUnknownOutcome(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var executions atomic.Int32
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "effect", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write", Name: "write", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	tool := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectShell}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		executions.Add(1)
		close(entered)
		<-release
		return tools.Result{Text: "late result"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/effect.db"
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "write"}})
	if err != nil {
		t.Fatal(err)
	}
	waitClosed(t, entered, "effect did not start")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := host.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not wait for effect: %v", err)
	}
	close(release)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	service, err = reopened.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Activity != ThreadActivityIdle || view.Failure == nil || view.Failure.Code != ThreadTurnFailureEffectOutcomeUnknown {
		t.Fatalf("unknown effect lost across shutdown: %+v", view)
	}
	if executions.Load() != 1 {
		t.Fatalf("effect replayed: %d", executions.Load())
	}
}
