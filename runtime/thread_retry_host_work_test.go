package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestRetryAdmitsCompletedHostWorkWithoutReplayingEffect(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			failedRequest := make(chan struct{})
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "host-retry", StateCompatibilityKey: "host-retry"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "shell-once", Name: "shell", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{BlockUntil: failedRequest, ReturnError: context.DeadlineExceeded},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "observed completion"}, {Type: provider.EventDone, Reason: "stop"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained completion"}, {Type: provider.EventDone, Reason: "stop"}}})
			var service ThreadService
			var binding BindHostWorkInput
			var executions atomic.Int32
			tool := tools.Define[map[string]any](tools.Definition{Name: "shell", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
				executions.Add(1)
				binding.WorkID = "process-once"
				if err := service.(ThreadHostWorkService).BindHostWork(ctx, binding); err != nil {
					return tools.Result{}, err
				}
				return tools.Result{Text: "PROCESS_STARTED"}, nil
			})
			agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				binding = BindHostWorkInput{EffectAttemptID: req.EffectAttemptID, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID}
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/host-retry.db"
			open := func() *Host {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err = host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host
			}
			host := open()
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "start", Input: UserInput{Text: "run once then report"}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := gateway.WaitForRequests(ctx, 2); err != nil {
				t.Fatal(err)
			}
			output := "COMPLETION_7319"
			if err := service.(ThreadHostWorkService).CompleteHostWork(ctx, CompleteHostWorkInput{ThreadID: created.ThreadID, WorkID: "process-once", Notice: output, Output: &output}); err != nil {
				t.Fatal(err)
			}
			close(failedRequest)
			failed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if failed.Failure == nil || failed.Failure.Code != ThreadTurnFailureProvider {
				t.Fatalf("failure=%+v", failed.Failure)
			}
			if restart {
				if err := host.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				host = open()
			}
			if _, err := service.Retry(ctx, RetryInput{ThreadID: created.ThreadID, SourceTurnID: failed.TurnID, RequestKey: "retry"}); err != nil {
				t.Fatal(err)
			}
			settled := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if settled.Failure != nil {
				t.Fatalf("retry failed: %+v", settled.Failure)
			}
			if err := host.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			host = open()
			if _, err := service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "next", Input: UserInput{Text: "remember the result"}}); err != nil {
				t.Fatal(err)
			}
			settled = waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if settled.Failure != nil || executions.Load() != 1 {
				t.Fatalf("failure=%+v executions=%d", settled.Failure, executions.Load())
			}
			requests := gateway.Requests()
			if len(requests) != 4 {
				t.Fatalf("requests=%d", len(requests))
			}
			for _, request := range requests[2:] {
				notices, results := 0, 0
				for _, message := range request.Messages {
					if strings.Contains(message.Text, output) {
						notices++
					}
					if message.ToolResult != nil && strings.Contains(message.ToolResult.Text, "PROCESS_STARTED") {
						results++
					}
				}
				if notices != 1 || results != 1 {
					t.Fatalf("notices=%d confirmed results=%d", notices, results)
				}
			}
		})
	}
}
