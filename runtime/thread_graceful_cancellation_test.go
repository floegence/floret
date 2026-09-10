package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestGracefulStopPreservesConfirmedCancellation(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	slow := tools.Define[map[string]any](tools.Definition{Name: "slow", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return tools.CanceledResult(inv.CallID, inv.Name, "partial output; process exit confirmed"), nil
		})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "stop", StateCompatibilityKey: "test:stop"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "slow-call", Name: "slow", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway, WithAgentTools(slow), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(context.WithoutCancel(ctx), EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
			ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
			PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, started, "tool did not start")
	view, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: CancelModeGraceful})
	if err != nil {
		t.Fatal(err)
	}
	if view.Activity != ThreadActivityActive || view.Cancellation == nil || view.Failure != nil {
		t.Fatalf("stop admission = %#v", view)
	}
	waitClosed(t, canceled, "tool did not receive cancellation")
	for _, key := range []RequestKey{"stop", "stop-again"} {
		repeated, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: key, Mode: CancelModeGraceful})
		if err != nil || repeated.Cancellation == nil || !repeated.Cancellation.RequestedAt.Equal(view.Cancellation.RequestedAt) {
			t.Fatalf("repeated stop = %#v, %v", repeated.Cancellation, err)
		}
	}
	queued, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "queued while stopping"}, RequestKey: "queued"})
	if err != nil || len(queued.Queue) != 1 {
		t.Fatalf("stopping rejected queued input: %#v, %v", queued.Queue, err)
	}
	close(release)
	view = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCancelled || view.Failure != nil || view.Cancellation == nil {
		t.Fatalf("settled view = %#v", view)
	}
	if len(view.Queue) != 1 || len(gateway.Requests()) != 1 {
		t.Fatal("stop consumed or automatically started queued input")
	}
	var found bool
	for _, item := range view.Items {
		if item.Activity != nil && item.Activity.ToolID == "slow-call" {
			found = true
			if item.Activity.Status != observation.ActivityStatusCanceled {
				t.Fatalf("tool status = %s", item.Activity.Status)
			}
		}
	}
	if !found {
		t.Fatal("confirmed tool result missing")
	}
	list, err := service.List(t.Context(), ThreadScope{})
	if err != nil || len(list) != 1 || list[0].Cancellation == nil {
		t.Fatalf("summary lost cancellation: %#v %v", list, err)
	}
}

func TestGracefulStopSettlesEachToolWithoutReplay(t *testing.T) {
	for _, scenario := range []string{"canceled", "completed", "failed", "unconfirmed", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			quickStarted := make(chan struct{})
			started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			tool := tools.Define[map[string]any](tools.Definition{Name: "work", OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true, PreserveFullSet: true}, Effects: []tools.Effect{tools.EffectWrite}, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
				func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
					defer close(exited)
					close(started)
					<-ctx.Done()
					if scenario == "timeout" {
						<-release
					}
					switch scenario {
					case "completed":
						return tools.Result{Text: "finished before exit"}, nil
					case "failed":
						return tools.ErrorResult(inv.CallID, inv.Name, "real command error"), nil
					case "unconfirmed":
						return tools.Result{}, context.Canceled
					default:
						return tools.CanceledResult(inv.CallID, inv.Name, "partial output retained"), nil
					}
				})
			quick := tools.Define[map[string]any](tools.Definition{Name: "quick", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
				func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
					close(quickStarted)
					<-ctx.Done()
					return tools.ErrorResult(inv.CallID, inv.Name, "independent command error"), nil
				})
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "stop", StateCompatibilityKey: "test:stop"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work-call", Name: "work", Args: `{}`}, {ID: "quick-call", Name: "quick", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "next response"}, {Type: provider.EventDone, Reason: "stop"}}})
			agent, err := testAgent(gateway, WithAgentTools(tool, quick), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
					ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
					PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/stop.sqlite"
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			host, service := open()
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "send"}); err != nil {
				t.Fatal(err)
			}
			waitClosed(t, started, "tool did not start")
			waitClosed(t, quickStarted, "parallel tool did not start")
			sub, err := service.Subscribe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			began := time.Now()
			accepted, err := service.Cancel(requestCtx, CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: CancelModeGraceful})
			cancelRequest()
			if err != nil {
				t.Fatal(err)
			}
			if time.Since(began) > time.Second || accepted.Cancellation == nil {
				t.Fatal("stop waited for execution instead of admitting")
			}
			waitCtx, finishWait := context.WithTimeout(t.Context(), 8*time.Second)
			defer finishWait()
			var terminal ThreadView
			for {
				terminal, err = sub.Next(waitCtx)
				if err != nil {
					t.Fatal(err)
				}
				if terminal.TurnID == accepted.TurnID && terminal.Activity == ThreadActivityIdle {
					break
				}
			}
			unknown := scenario == "timeout" || scenario == "unconfirmed"
			if unknown {
				if terminal.Failure == nil || terminal.Failure.Code != ThreadTurnFailureEffectOutcomeUnknown {
					t.Fatalf("missing unknown failure: %#v", terminal.Failure)
				}
				if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: accepted.TurnID, RequestKey: "retry"}); !errors.Is(err, ErrEffectOutcomeUnknown) {
					t.Fatalf("unsafe retry: %v", err)
				}
			} else if terminal.Failure != nil || terminal.LastOutcome == nil || *terminal.LastOutcome != TurnOutcomeCancelled {
				t.Fatalf("normal stop failed: %#v", terminal)
			}
			if threadToolStatus(terminal.Items, "quick-call") != observation.ActivityStatusError {
				t.Fatal("parallel confirmed error was lost")
			}
			entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
			if err != nil {
				t.Fatal(err)
			}
			confirmedError := false
			for _, entry := range entries {
				if entry.Message.ToolCallID == "quick-call" && entry.Message.ToolResult != nil {
					confirmedError = entry.Message.Content == "ERROR: independent command error"
				}
			}
			if !confirmedError {
				t.Fatal("parallel real error was replaced with an unknown result")
			}
			wantStatus := observation.ActivityStatusCanceled
			if scenario == "completed" {
				wantStatus = observation.ActivityStatusSuccess
			}
			if scenario == "failed" || unknown {
				wantStatus = observation.ActivityStatusError
			}
			if got := threadToolStatus(terminal.Items, "work-call"); got != wantStatus {
				t.Fatalf("status=%s want %s", got, wantStatus)
			}
			if scenario == "timeout" && time.Since(began) < 4500*time.Millisecond {
				t.Fatal("unconfirmed effect settled before its grace period")
			}
			if scenario == "timeout" {
				if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "continue before late callback"}, RequestKey: "next"}); err != nil {
					t.Fatal(err)
				}
				next := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.TurnID != accepted.TurnID && v.Activity == ThreadActivityIdle })
				if next.Cancellation != nil || next.LastOutcome == nil || *next.LastOutcome != TurnOutcomeCompleted {
					t.Fatalf("next turn was blocked by the old tool: %#v", next)
				}
			}
			close(release)
			waitClosed(t, exited, "tool did not exit")
			if scenario == "timeout" {
				after, err := service.View(t.Context(), created.ThreadID)
				if err != nil || after.TurnID == accepted.TurnID || after.Cancellation != nil || after.Failure != nil || threadToolStatus(after.Items, "work-call") != observation.ActivityStatusError {
					t.Fatalf("late callback changed the next turn: %#v, %v", after, err)
				}
				return
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
			reopened, err := service.View(t.Context(), created.ThreadID)
			if err != nil || reopened.Failure != nil && !unknown || reopened.Cancellation == nil || !reopened.Cancellation.RequestedAt.Equal(accepted.Cancellation.RequestedAt) || threadToolStatus(reopened.Items, "work-call") != wantStatus {
				t.Fatalf("restart drift: %#v, %v", reopened, err)
			}
			if !unknown {
				found := false
				entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if entry.Message.ToolCallID == "work-call" && entry.Message.ToolResult != nil {
						found = entry.Message.ToolResult.FullOutput != nil && (strings.Contains(entry.Message.Content, "retained") || scenario != "canceled")
					}
				}
				if !found {
					t.Fatal("partial output was lost")
				}
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "continue"}, RequestKey: "next"}); err != nil {
				t.Fatal(err)
			}
			next := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.TurnID != accepted.TurnID && v.Activity == ThreadActivityIdle })
			if next.Cancellation != nil || next.LastOutcome == nil || *next.LastOutcome != TurnOutcomeCompleted {
				t.Fatalf("next turn inherited stop: %#v", next)
			}
		})
	}
}

func TestGracefulStopCancelsPendingApprovalWithoutDispatch(t *testing.T) {
	tool := tools.Define[map[string]any](tools.Definition{Name: "approval", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			t.Error("tool ran after stop of its pending approval")
			return tools.Result{}, nil
		})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "stop", StateCompatibilityKey: "test:stop"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "approval-call", Name: "approval", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway, WithAgentTools(tool))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "work"}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Attention.ApprovalCount == 1 })
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: CancelModeGraceful}); err != nil {
		t.Fatal(err)
	}
	approved := true
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, RequestKey: "approve-after-stop", InteractionID: waiting.Interactions[0].ID, Answers: []InteractionAnswer{{Approved: &approved}}}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("approval after stop was accepted: %v", err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if view.Failure != nil || view.LastOutcome == nil || *view.LastOutcome != TurnOutcomeCancelled || view.Attention.ApprovalCount != 0 || threadToolStatus(view.Items, "approval-call") != observation.ActivityStatusCanceled {
		t.Fatalf("approval stop: failure=%+v outcome=%v attention=%+v", view.Failure, view.LastOutcome, view.Attention)
	}
}
