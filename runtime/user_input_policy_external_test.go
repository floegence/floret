package runtime_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func inputPolicyService(t *testing.T, source storage.Source, gateway provider.Gateway, options ...runtime.AgentOption) (*runtime.Host, runtime.ThreadService) {
	t.Helper()
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: source})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "policy", Name: "Policy"}, SystemPrompt: "Complete the task.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, options...)
	}))
	if err != nil {
		t.Fatal(err)
	}
	return host, service
}

func inputPolicyWait(t *testing.T, service runtime.ThreadService, threadID identity.ThreadID, ready func(runtime.ThreadView) bool) runtime.ThreadView {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		view, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if ready(view) {
			return view
		}
		time.Sleep(time.Millisecond)
	}
	view, err := service.View(t.Context(), threadID)
	t.Fatalf("input policy did not reach expected boundary: %+v error=%v", view, err)
	return runtime.ThreadView{}
}

func inputPolicySend(t *testing.T, service runtime.ThreadService) identity.ThreadID {
	t.Helper()
	view, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), runtime.SendInput{ThreadID: view.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "Complete the task"}}); err != nil {
		t.Fatal(err)
	}
	return view.ThreadID
}

func TestAgentUserInputPolicyControlsFinalProviderSurface(t *testing.T) {
	for _, tc := range []struct {
		name      string
		options   []runtime.AgentOption
		ask, goal string
	}{
		{name: "default", ask: "ask_user"},
		{name: "explicit_enabled", options: []runtime.AgentOption{runtime.WithAgentUserInput(true)}, ask: "ask_user"},
		{name: "disabled", options: []runtime.AgentOption{runtime.WithAgentUserInput(false)}},
		{name: "named_enabled", options: []runtime.AgentOption{runtime.WithAgentBuiltinToolNames(runtime.BuiltinToolNames{AskUser: "user_ask", Goal: "goal_manage"})}, ask: "user_ask"},
		{name: "named_disabled_with_independent_goals", options: []runtime.AgentOption{runtime.WithAgentUserInput(false), runtime.WithAgentGoals(), runtime.WithAgentBuiltinToolNames(runtime.BuiltinToolNames{AskUser: "user_ask", Goal: "goal_manage"})}, goal: "goal_manage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "policy", StateCompatibilityKey: "policy:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Done"}, {Type: provider.EventDone, Reason: "stop"}}})
			_, service := inputPolicyService(t, storage.Memory(), gateway, tc.options...)
			id := inputPolicySend(t, service)
			view := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			if *view.LastOutcome != runtime.TurnOutcomeCompleted {
				t.Fatalf("outcome=%+v failure=%+v", view.LastOutcome, view.Failure)
			}
			requests := gateway.Requests()
			if len(requests) != 1 {
				t.Fatalf("requests=%d", len(requests))
			}
			var got []string
			for _, tool := range requests[0].Tools {
				got = append(got, tool.Name)
			}
			var want []string
			if tc.ask != "" {
				want = append(want, tc.ask)
			}
			if tc.goal != "" {
				want = append(want, tc.goal)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tools=%v want=%v", got, want)
			}
		})
	}
}

func TestAgentUserInputPolicyRejectsUndeclaredInputCalls(t *testing.T) {
	for _, name := range []string{"ask_user", "user_ask"} {
		t.Run(name, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "policy", StateCompatibilityKey: "policy:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "undeclared", Name: name, Args: `{"evidence_refs":[],"questions":[{"id":"q","header":"Input","question":"Choose?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Completed without asking."}, {Type: provider.EventDone, Reason: "stop"}}})
			_, service := inputPolicyService(t, storage.Memory(), gateway, runtime.WithAgentUserInput(false), runtime.WithAgentBuiltinToolNames(runtime.BuiltinToolNames{AskUser: name}))
			id := inputPolicySend(t, service)
			view := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool {
				return len(v.Interactions) > 0 || v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			if len(view.Interactions) != 0 {
				t.Fatalf("disabled call created interaction: %+v", view.Interactions)
			}
			// The engine owns ordinary undeclared-call handling. A rejected call must
			// never suspend autonomous execution for user input.
			if view.Activity != runtime.ThreadActivityIdle || view.LastOutcome == nil {
				t.Fatalf("execution did not settle: %+v", view)
			}
		})
	}
}

func TestAgentUserInputPolicyPreservesPendingHistoryAcrossRestart(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "frozen_surface", true: "refreshed_surface"}[refresh], func(t *testing.T) {

			const args = `{"evidence_refs":[],"questions":[{"id":"route","header":"Route","question":"Which route?","response_mode":"write","is_secret":false}]}`
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "policy", StateCompatibilityKey: "policy:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "retained-ask", Name: "user_ask", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Continued"}, {Type: provider.EventDone, Reason: "stop"}}})
			source := storage.SQLite(filepath.Join(t.TempDir(), "history.sqlite"))
			names := runtime.WithAgentBuiltinToolNames(runtime.BuiltinToolNames{AskUser: "user_ask"})
			host, service := inputPolicyService(t, source, gateway, names)
			id := inputPolicySend(t, service)
			waiting := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool { return len(v.Interactions) == 1 })
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			options := []runtime.AgentOption{names, runtime.WithAgentUserInput(false)}
			if refresh {
				options = append(options, runtime.WithAgentDynamicToolSurface(func(context.Context, runtime.ToolSurfaceRequest) (runtime.ToolSurface, error) {
					return runtime.ToolSurface{RefreshProviderSurface: true}, nil
				}))
			}
			_, service = inputPolicyService(t, source, gateway, options...)
			restored, err := service.View(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if len(restored.Interactions) != 1 || !reflect.DeepEqual(restored.Interactions[0], waiting.Interactions[0]) {
				t.Fatal("policy change rewrote pending interaction")
			}
			if _, err = service.Respond(t.Context(), runtime.RespondInput{ThreadID: id, InteractionID: restored.Interactions[0].ID, RequestKey: "answer", Answers: []runtime.InteractionAnswer{{Input: map[string]string{"route": "staged"}}}}); err != nil {
				t.Fatal(err)
			}
			final := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool {
				return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
			})
			if *final.LastOutcome != runtime.TurnOutcomeCompleted {
				t.Fatalf("continuation failed: %+v", final.Failure)
			}
			requests := gateway.Requests()
			if len(requests) != 2 {
				t.Fatalf("requests=%d", len(requests))
			}
			found := false
			for _, tool := range requests[1].Tools {
				if tool.Name == "user_ask" {
					found = true
				}
			}
			if found == refresh {
				t.Fatalf("input advertised=%v refresh=%v; retained surfaces must preserve explicit refresh policy", found, refresh)
			}
			calls, results := 0, 0
			for _, message := range requests[1].Messages {
				for _, call := range message.ToolCalls {
					if call.ID == "retained-ask" {
						calls++
						if call.Name != "user_ask" || call.Args != args {
							t.Fatal("original call rewritten")
						}
					}
				}
				if message.ToolResult != nil && message.ToolResult.CallID == "retained-ask" {
					results++
					if !strings.Contains(message.ToolResult.Text, "staged") {
						t.Fatal("answer missing")
					}
				}
			}
			if calls != 1 || results != 1 {
				t.Fatalf("history unpaired: calls=%d results=%d", calls, results)
			}
		})
	}
}
