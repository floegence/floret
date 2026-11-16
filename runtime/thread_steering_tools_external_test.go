package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestSteeringYieldsObservationAndPreservesHostCancellation(t *testing.T) {
	for _, hostCancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "yield", true: "host cancellation"}[hostCancel], func(t *testing.T) {
			started := make(chan context.Context, 1)
			cancelHost := make(chan context.CancelFunc, 1)
			work := tools.Define[map[string]any](tools.Definition{Name: "observe", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
				started <- ctx
				select {
				case <-tools.YieldSignal(ctx):
				case <-ctx.Done():
					return tools.CanceledResult(inv.CallID, inv.Name, "cancelled before yield"), nil
				}
				if hostCancel {
					<-ctx.Done()
					return tools.CanceledResult(inv.CallID, inv.Name, "host cancelled"), nil
				}
				return tools.Result{Text: "work still running; current progress"}, nil
			})
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "yield", StateCompatibilityKey: "yield"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "observe", Name: "observe", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "guided"}, {Type: provider.EventDone, Reason: "stop"}}})
			_, service := inputPolicyService(t, storage.Memory(), gateway, runtime.WithAgentTools(work), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
				bounded, cancel := context.WithCancel(ctx)
				defer cancel()
				cancelHost <- cancel
				return effect(bounded, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
			})))
			created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			first, err := service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "start", Input: runtime.UserInput{Text: "observe work"}})
			if err != nil {
				t.Fatal(err)
			}
			var toolCtx context.Context
			select {
			case toolCtx = <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("tool did not start")
			}
			cancel := <-cancelHost
			t.Cleanup(cancel)
			ack, err := service.(runtime.ThreadSteeringCommandService).SteerAck(t.Context(), runtime.SteerInput{ThreadID: created.ThreadID, RequestKey: "guide", Input: runtime.UserInput{Text: "new instruction"}})
			if err != nil {
				t.Fatal(err)
			}
			if ack.RunID != first.RunID || ack.TurnID != first.TurnID {
				t.Fatal("steering replaced execution")
			}
			if hostCancel {
				select {
				case <-tools.YieldSignal(toolCtx):
				case <-time.After(5 * time.Second):
					t.Fatal("yield was not signalled")
				}
				if toolCtx.Err() != nil {
					t.Fatal("steering cancelled dispatched work")
				}
				cancel()
			}
			final := inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool { return v.Activity == runtime.ThreadActivityIdle })
			if final.Failure != nil {
				t.Fatalf("final=%+v", final)
			}
			requests := gateway.Requests()
			if len(requests) != 2 {
				t.Fatalf("requests=%d", len(requests))
			}
			messages := requests[1].Messages
			if messages[len(messages)-1].Text != "new instruction" || messages[len(messages)-2].ToolResult == nil {
				t.Fatalf("unpaired history: %+v", messages)
			}
		})
	}
	if tools.YieldSignal(context.Background()) != nil {
		t.Fatal("unmanaged context has a yield signal")
	}
}

func TestSteeringWithdrawsHostAuthorizationBeforeDispatch(t *testing.T) {
	waiting := make(chan struct{})
	work := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		t.Error("withdrawn authorization executed")
		return tools.Result{}, nil
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "withdraw", StateCompatibilityKey: "withdraw"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write", Name: "write", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "guided"}, {Type: provider.EventDone, Reason: "stop"}}})
	_, service := inputPolicyService(t, storage.Memory(), gateway, runtime.WithAgentTools(work), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, _ runtime.EffectAuthorizationRequest, _ runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		close(waiting)
		<-ctx.Done()
		return runtime.EffectDispatchResult{}, ctx.Err()
	})))
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "start", Input: runtime.UserInput{Text: "write"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("authorization did not start")
	}
	if _, err = service.(runtime.ThreadSteeringCommandService).SteerAck(t.Context(), runtime.SteerInput{ThreadID: created.ThreadID, RequestKey: "guide", Input: runtime.UserInput{Text: "do not write"}}); err != nil {
		t.Fatal(err)
	}
	final := inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool { return v.Activity == runtime.ThreadActivityIdle })
	if final.Failure != nil || len(gateway.Requests()) != 2 {
		t.Fatalf("final=%+v requests=%d", final, len(gateway.Requests()))
	}
}
