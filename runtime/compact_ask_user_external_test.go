package runtime_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

// This public-boundary test also runs in blank candidate/published consumers.
func TestCompactAskUserRestartAndAnswers(t *testing.T) {
	const args = `{"evidence_refs":["observed-blocker"],"questions":[{"id":"route","header":"Route","question":"Which route?","response_mode":"select_or_write","is_secret":false,"write_label":"Another route","write_placeholder":"Describe your route","choices":[{"choice_id":"staged","label":"Staged rollout","description":"Start with a small group."},{"choice_id":"all","label":"Full rollout"}]},{"id":"note","header":"Note","question":"What should the release note say?","response_mode":"write","is_secret":false}]}`
	for _, answer := range []string{"Staged rollout", "Custom route"} {
		t.Run(answer, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-compact", Name: "ask_user", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Continued."}, {Type: provider.EventDone, Reason: "stop"}}})
			path := filepath.Join(t.TempDir(), "questions.sqlite")
			open := func() (*runtime.Host, runtime.ThreadService) {
				t.Helper()
				host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
					return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Ask once.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
				}))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			host, service := open()
			thread, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Send(t.Context(), runtime.SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "Choose a deployment route."}}); err != nil {
				t.Fatal(err)
			}
			wait := func(service runtime.ThreadService, complete bool) runtime.ThreadView {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					view, err := service.View(t.Context(), thread.ThreadID)
					if err != nil {
						t.Fatal(err)
					}
					if view.Failure != nil {
						t.Fatalf("turn failed: %+v", view.Failure)
					}
					if !complete && len(view.Interactions) == 1 && !view.Interactions[0].Resolved {
						return view
					}
					if complete && view.Activity == runtime.ThreadActivityIdle && view.LastOutcome != nil {
						return view
					}
					time.Sleep(time.Millisecond)
				}
				t.Fatal("question did not reach expected boundary")
				return runtime.ThreadView{}
			}
			waiting := wait(service, false)
			input := waiting.Interactions[0].Input
			if input == nil || input.ReasonCode != "" || len(input.RequiredFromUser) != 0 || !reflect.DeepEqual(input.EvidenceRefs, []string{"observed-blocker"}) || len(input.Questions) != 2 {
				t.Fatalf("input=%+v", input)
			}
			q := input.Questions[0]
			if q.Kind != "select_or_write" || q.ChoicesExhaustive != nil || q.WriteLabel != "Another route" || q.WritePlaceholder != "Describe your route" || len(q.Choices) != 2 || q.Choices[0].ChoiceID != "staged" || q.Choices[0].Value != "Staged rollout" || q.Choices[0].Description != "Start with a small group." {
				t.Fatalf("question=%+v", q)
			}
			for _, def := range gateway.Requests()[0].Tools {
				if def.Name != "ask_user" {
					continue
				}
				raw, _ := json.Marshal(def.InputSchema)
				for _, retired := range []string{"reason_code", "required_from_user", "choices_exhaustive", `"kind"`} {
					if strings.Contains(string(raw), retired) {
						t.Fatalf("model still sees %s", retired)
					}
				}
			}
			if err = host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = open()
			restored := wait(service, false)
			if restored.Interactions[0].ID != waiting.Interactions[0].ID || !reflect.DeepEqual(restored.Interactions[0].Input, input) {
				t.Fatal("restart changed input identity or presentation")
			}
			answers := map[string]string{"route": answer, "note": "Ship after review."}
			if _, err = service.Respond(t.Context(), runtime.RespondInput{ThreadID: thread.ThreadID, InteractionID: restored.Interactions[0].ID, RequestKey: "answer", Answers: []runtime.InteractionAnswer{{Input: answers}}}); err != nil {
				t.Fatal(err)
			}
			final := wait(service, true)
			if *final.LastOutcome != runtime.TurnOutcomeCompleted || !reflect.DeepEqual(final.Interactions[0].Resolution.Input, answers) {
				t.Fatalf("final=%+v", final)
			}
			requests := gateway.Requests()
			if len(requests) != 2 {
				t.Fatalf("requests=%d", len(requests))
			}
			calls, results := 0, 0
			for _, message := range requests[1].Messages {
				for _, call := range message.ToolCalls {
					if call.ID == "ask-compact" {
						calls++
						if call.Args != args {
							t.Fatal("original call was rewritten")
						}
					}
				}
				if message.ToolResult != nil && message.ToolResult.CallID == "ask-compact" {
					results++
					if !strings.Contains(message.ToolResult.Text, answer) {
						t.Fatalf("answer missing: %s", message.ToolResult.Text)
					}
				}
			}
			if calls != 1 || results != 1 {
				t.Fatalf("unpaired history calls=%d results=%d", calls, results)
			}
		})
	}
}
