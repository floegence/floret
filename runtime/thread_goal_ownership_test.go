package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestGoalContinuationInstallsGoalWithoutAgentOptIn(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goal-owner", StateCompatibilityKey: "test:goal-owner"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"The requested result is verified."}`), parentEventDone())
	_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Deliver the verified result"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: thread.ThreadID, RequestKey: "continue", ContinueGoal: true})
	if err != nil {
		t.Fatal(err)
	}
	final := waitGoalCompactionView(t, service, thread.ThreadID, func(v ThreadView) bool {
		return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	if final.Failure != nil || final.Goal == nil || final.Goal.Status != "completed" {
		t.Fatalf("explicit continuation did not complete: goal=%+v failure=%+v", final.Goal, final.Failure)
	}
}

func TestGoalInstallationKeepsOrdinaryToolAuthorization(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprint("allow=", allow), func(t *testing.T) {
			var invoked, authorized atomic.Int32
			work := tools.Define[map[string]any](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Annotations: map[string]any{"runtime_owned": true}}, nil, nil,
				func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
					invoked.Add(1)
					return tools.Result{Text: "Inspection is verified."}, nil
				})
			steps := []florettest.Step{
				{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{
					{ID: "set", Name: "goal", Args: `{"operation":"set","expected_version":0,"text":"Deliver the verified result"}`},
					{ID: "work", Name: "inspect", Args: "{}"},
				}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Inspection is verified."}`), parentEventDone(),
			}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goal-owner", StateCompatibilityKey: "test:goal-owner"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			agent, err := testAgent(gateway, WithAgentGoals(), WithAgentTools(work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				authorized.Add(1)
				if req.ToolName != "inspect" {
					return EffectDispatchResult{}, fmt.Errorf("internal tool reached host authorization: %s", req.ToolName)
				}
				if !allow {
					return EffectDispatchResult{}, errors.New("host denied inspection")
				}
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			host, service := testThreadServiceWithAgent(t, agent)
			thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Inspect and verify."}}); err != nil {
				t.Fatal(err)
			}
			final := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if authorized.Load() != 1 || allow && invoked.Load() != 1 || !allow && invoked.Load() != 0 {
				t.Fatalf("authorization=%d handler=%d allow=%v", authorized.Load(), invoked.Load(), allow)
			}
			if !allow {
				if final.Failure == nil || final.Goal == nil || final.Goal.Status != "active" {
					t.Fatalf("denial lost failure or Goal: %+v", final)
				}
				return
			}
			if final.Failure != nil || final.Goal == nil || final.Goal.Status != "completed" {
				t.Fatalf("goal=%+v failure=%+v", final.Goal, final.Failure)
			}
			path, err := host.store.repo.Path(t.Context(), thread.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			results := map[string]int{}
			for _, entry := range path {
				if entry.Type != sessiontree.EntryToolResult {
					continue
				}
				results[entry.Message.ToolCallID]++
				hasEffect := entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey] != ""
				if hasEffect != (entry.Message.ToolName == "inspect") {
					t.Fatalf("incorrect result owner: %+v", entry)
				}
			}
			for _, call := range []string{"set", "work", "review"} {
				if results[call] != 1 {
					t.Fatalf("paired results=%v", results)
				}
			}
			for _, request := range gateway.Requests() {
				for _, definition := range request.Tools {
					if definition.Name == "goal" {
						effects, err := json.Marshal(definition.Annotations["effects"])
						if err != nil || definition.Annotations["read_only"] != false || string(effects) != "[\"write\"]" {
							t.Fatalf("incorrect Goal effect declaration: %+v", definition.Annotations)
						}
					}
				}
			}
		})
	}
}

func TestGoalNamedHostToolRetainsAuthorizationWhenGoalsDisabled(t *testing.T) {
	var invoked, authorized atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "goal", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Annotations: map[string]any{"runtime_owned": true}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			invoked.Add(1)
			return tools.Result{Text: "host result"}, nil
		})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goal-owner", StateCompatibilityKey: "test:goal-owner"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, goalCall("host-goal", "{}"), parentEventDone())
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		authorized.Add(1)
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	host, service := testThreadServiceWithAgent(t, agent)
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Use the host tool."}}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil || invoked.Load() != 1 || authorized.Load() != 1 {
		t.Fatalf("failure=%+v handler=%d authorization=%d", final.Failure, invoked.Load(), authorized.Load())
	}
	path, err := host.store.repo.Path(t.Context(), thread.ThreadID.String(), "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range path {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "host-goal" {
			found = entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey] != ""
		}
	}
	if !found {
		t.Fatal("host Goal-named tool lost its canonical effect result")
	}
}
