package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

// Exercise the public view, workspace summary, subscription, and durable recovery
// with provider tool arguments rather than hand-constructed presentation objects.
func TestThreadServicePreservesAskUserPresentationAndAnswerValues(t *testing.T) {
	const arguments = `{"reason_code":"missing_external_input","required_from_user":["route","note"],"evidence_refs":[],"questions":[{"id":"route","header":"Choose a route","question":"Which route should the deployment use?","response_mode":"select_or_write","is_secret":false,"choices_exhaustive":false,"write_label":"Another route","write_placeholder":"Describe your route","choices":[{"choice_id":"safe","label":"Staged rollout","description":"Deploy to a small group first.","kind":"select"},{"choice_id":"all","label":"Full rollout","kind":"select"}]},{"id":"note","header":"Deployment note","question":"What should the release note say?","response_mode":"write","is_secret":false}]}`
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "presentation", StateCompatibilityKey: "test:presentation:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-presentation", Name: "ask_user", Args: arguments}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/presentation.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-presentation"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "deploy"}, RequestKey: "send-presentation"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var waiting ThreadView
	for len(waiting.Interactions) == 0 {
		waiting, err = subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	expected := assertRichInputPresentation(t, waiting.Interactions[0].Input)
	corrupt := func(input *InputPresentation) {
		input.Questions[0].Options[0] = "caller mutation"
		input.Questions[0].Choices[0].Description = "caller mutation"
		*input.Questions[0].ChoicesExhaustive = true
	}
	corrupt(waiting.Interactions[0].Input)
	assertCurrent := func(service ThreadService) ThreadView {
		t.Helper()
		view, err := service.View(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if got := assertRichInputPresentation(t, view.Interactions[0].Input); !reflect.DeepEqual(got, expected) {
			t.Fatalf("view presentation changed: %#v", got)
		}
		summaries, err := service.List(t.Context(), ThreadScope{})
		if err != nil || len(summaries) != 1 {
			t.Fatalf("summaries=%#v err=%v", summaries, err)
		}
		if got := assertRichInputPresentation(t, summaries[0].PendingInput); !reflect.DeepEqual(got, expected) {
			t.Fatalf("summary presentation changed: %#v", got)
		}
		corrupt(summaries[0].PendingInput)
		return view
	}
	detached := assertCurrent(service)
	corrupt(detached.Interactions[0].Input)
	for _, item := range detached.Items {
		if item.Interaction != nil && item.Interaction.Input != nil {
			corrupt(item.Interaction.Input)
		}
	}
	assertCurrent(service)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	restored := assertCurrent(service)
	if restored.Interactions[0].ID != waiting.Interactions[0].ID {
		t.Fatal("restart changed interaction identity")
	}
	// Existing v7 clients submit option labels, not original choice identifiers.
	answers := map[string]string{"route": "Staged rollout", "note": "Custom release note"}
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: restored.Interactions[0].ID, Answers: []InteractionAnswer{{Input: answers}}, RequestKey: "answer-presentation"}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if *completed.LastOutcome != TurnOutcomeCompleted || !reflect.DeepEqual(completed.Interactions[0].Resolution.Input, answers) {
		t.Fatalf("completed=%#v", completed)
	}
	if got := assertRichInputPresentation(t, completed.Interactions[0].Input); !reflect.DeepEqual(got, expected) {
		t.Fatal("history lost presentation")
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("provider requests=%d", len(requests))
	}
	encoded, _ := json.Marshal(requests[1].Messages)
	if !strings.Contains(string(encoded), "Staged rollout") || !strings.Contains(string(encoded), "Custom release note") {
		t.Fatalf("continuation lost original answer values: %s", encoded)
	}
}

func assertRichInputPresentation(t *testing.T, input *InputPresentation) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Questions []map[string]any `json:"questions"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Questions) != 2 {
		t.Fatalf("questions=%s", encoded)
	}
	q := wire.Questions[0]
	if q["header"] != "Choose a route" || q["write_placeholder"] != "Describe your route" || q["choices_exhaustive"] != false {
		t.Fatalf("presentation fields lost through runtime: %s", encoded)
	}
	want := []any{map[string]any{"choice_id": "safe", "value": "Staged rollout", "label": "Staged rollout", "description": "Deploy to a small group first."}, map[string]any{"choice_id": "all", "value": "Full rollout", "label": "Full rollout"}}
	if !reflect.DeepEqual(q["choices"], want) || !reflect.DeepEqual(q["options"], []any{"Staged rollout", "Full rollout"}) {
		t.Fatalf("choice metadata or legacy options lost: %s", encoded)
	}
	if _, present := wire.Questions[1]["choices_exhaustive"]; present {
		t.Fatal("missing metadata must stay absent")
	}
	return q
}

func TestAskUserPresentationLeavesAbsentHistoricalMetadataUnset(t *testing.T) {
	input := inputPresentationFromControlSignal("Continue?", map[string]any{"questions": []any{map[string]any{
		"id": "q", "question": "Continue?", "response_mode": "write", "is_secret": false,
	}}})
	if len(input.Questions) != 1 {
		t.Fatal("historical question lost")
	}
	question := input.Questions[0]
	if question.Header != "" || question.WritePlaceholder != "" || question.ChoicesExhaustive != nil || question.Choices != nil {
		t.Fatalf("invented historical metadata: %#v", question)
	}
}
