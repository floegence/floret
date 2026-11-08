package runtime_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func TestGoalArgumentCorrectionRetainsStateAndRestart(t *testing.T) {
	for _, goalName := range []string{"goal", "goal_manage"} {
		for _, tc := range []struct{ name, args, field string }{
			{"set", `{"operation":"set","expected_version":1}`, "text"},
			{"delete", `{"operation":"delete","expected_version":1,"reason":" \t"}`, "reason"},
			{"review", `{"operation":"review","expected_version":1,"outcome":"completed"}`, "evidence"},
		} {
			t.Run(goalName+"/"+tc.name, func(t *testing.T) {
				call := func(id, args string) florettest.Step {
					return florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: goalName, Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
				}
				gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "goal-input", StateCompatibilityKey: "goal-input"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
					call("invalid", tc.args),
					call("valid", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Requested result verified"}`),
					florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Verified and finished."}, {Type: provider.EventDone, Reason: "stop"}}},
				)
				factory := runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
					return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "fixture", Name: "Fixture"}, SystemPrompt: "Maintain the requested Goal.", Context: config.ContextPolicy{ContextWindowTokens: 128000}}, gateway, runtime.WithAgentGoals(), runtime.WithAgentBuiltinToolNames(runtime.BuiltinToolNames{Goal: goalName}))
				})
				path := filepath.Join(t.TempDir(), "goals.sqlite")
				host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = host.Shutdown(context.Background()) }()
				service, err := host.ThreadService(factory)
				if err != nil {
					t.Fatal(err)
				}
				thread, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
				if err != nil {
					t.Fatal(err)
				}
				_, err = service.(runtime.ThreadGoalController).SetGoal(t.Context(), runtime.SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "goal", Text: "Preserve this goal until verified completion"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "Complete this goal."}}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var view runtime.ThreadView
				for {
					view, err = service.View(ctx, thread.ThreadID)
					if err != nil {
						t.Fatal(err)
					}
					if view.Activity == runtime.ThreadActivityIdle && view.LastOutcome != nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				if view.Failure != nil || view.Goal == nil || view.Goal.Version != 1 || view.Goal.Status != "completed" {
					t.Fatalf("failure=%+v goal=%+v", view.Failure, view.Goal)
				}
				requests := gateway.Requests()
				if len(requests) != 3 {
					t.Fatalf("requests=%d", len(requests))
				}
				found := false
				for _, message := range requests[1].Messages {
					if message.ToolResult != nil && message.ToolResult.CallID == "invalid" {
						found = strings.Contains(message.ToolResult.Text, "not executed") && strings.Contains(message.ToolResult.Text, tc.field)
					}
				}
				if !found {
					t.Fatal("next model request lacks actionable validation feedback")
				}
				checkView := func(view runtime.ThreadView) {
					t.Helper()
					final := false
					for _, item := range view.Items {
						if item.Activity != nil && item.Activity.ToolID == "invalid" {
							t.Fatal("rejected input became public tool activity")
						}
						final = final || item.Text == "Verified and finished."
					}
					if !final || view.Goal == nil || view.Goal.Version != 1 || view.Goal.Status != "completed" {
						t.Fatalf("visible result lost: %+v", view)
					}
				}
				checkView(view)
				if err := host.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
				host, err = runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err = host.ThreadService(factory)
				if err != nil {
					t.Fatal(err)
				}
				view, err = service.View(ctx, thread.ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				checkView(view)
				if len(gateway.Requests()) != 3 {
					t.Fatal("restart executed the provider")
				}
			})
		}
	}
}
