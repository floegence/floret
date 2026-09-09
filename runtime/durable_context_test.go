package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestDurableContextSurvivesCorrectionResponseRestartForkAndRetry(t *testing.T) {
	const validAsk = `{"reason_code":"missing_external_input","required_from_user":["scope"],"evidence_refs":[],"questions":[{"id":"scope","header":"Scope","question":"Which checks?","is_secret":false,"response_mode":"write"}]}`
	invalidAsk := strings.Replace(validAsk, `["scope"]`, `"[\"scope\"]"`, 1)
	ask := func(id, args string) florettest.Step {
		return florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: "ask_user", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}
	}
	done := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, ask("invalid", invalidAsk), ask("valid", validAsk), done, done, done, done, done)
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/context.sqlite"
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
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	input := UserInput{
		References: []MessageReference{{ReferenceID: "device", Kind: MessageReferenceText, Label: "udesk26", Text: "User-selected device: ssh:host:udesk26"}, {ReferenceID: "file", Kind: MessageReferenceFile, Label: "README.md", Text: "Relevant file", ResourceRef: "opaque-locator-never-send", Truncated: true}},
		Context:    []MessageContextItem{{Kind: "runtime", Title: "Tool execution environment", Text: "Actual tool runtime: local. Working directory: /project"}},
	}
	started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: input, RequestKey: "send"})
	if err != nil {
		t.Fatal(err)
	}
	input.Context[0].Text = "mutated caller slice"
	input.References[0].Text = "mutated caller reference"
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: input, RequestKey: "send"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed context reused an admitted request key: %v", err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return len(view.Interactions) == 1 })
	if waiting.Failure != nil || waiting.Interactions[0].ID != "valid" || countThreadItems(waiting.Items, ThreadItemTool) != 0 {
		t.Fatalf("waiting=%#v", waiting)
	}
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: "valid", Answers: []InteractionAnswer{{Input: map[string]string{"scope": "memory"}}}, RequestKey: "answer"}); err != nil {
		t.Fatal(err)
	}
	waitDone := func(threadID identity.ThreadID, prior identity.TurnID) ThreadView {
		return waitThreadView(t, service, threadID, func(view ThreadView) bool {
			return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && (prior == "" || view.TurnID != prior)
		})
	}
	if view := waitDone(created.ThreadID, ""); view.Failure != nil {
		t.Fatal(view.Failure)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "And storage?", Context: []MessageContextItem{{Kind: "runtime", Title: "Working directory", Text: "Now /other"}}}, RequestKey: "followup"}); err != nil {
		t.Fatal(err)
	}
	followup := waitDone(created.ThreadID, started.TurnID)
	if followup.Failure != nil {
		t.Fatal(followup.Failure)
	}
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: fork.ThreadID, Input: UserInput{Text: "Continue in fork"}, RequestKey: "fork-send"}); err != nil {
		t.Fatal(err)
	}
	if view := waitDone(fork.ThreadID, ""); view.Failure != nil {
		t.Fatal(view.Failure)
	}
	if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: started.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	retried := waitDone(created.ThreadID, followup.TurnID)
	if retried.Failure != nil {
		t.Fatal(retried.Failure)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "Continue after retry"}, RequestKey: "after-retry"}); err != nil {
		t.Fatal(err)
	}
	if view := waitDone(created.ThreadID, retried.TurnID); view.Failure != nil {
		t.Fatal(view.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 7 {
		t.Fatalf("requests=%d", len(requests))
	}
	var original string
	for index, request := range requests {
		found := 0
		for _, message := range request.Messages {
			if strings.Contains(message.Text, "opaque-locator-never-send") || strings.Contains(message.Text, "mutated caller") {
				t.Fatalf("request %d leaked locator or aliased input", index)
			}
			if strings.Contains(message.Text, "User-selected device: ssh:host:udesk26") {
				found++
				if original == "" {
					original = message.Text
				}
				if message.Text != original || !strings.Contains(message.Text, "Actual tool runtime: local") {
					t.Fatalf("request %d changed admitted input", index)
				}
			}
		}
		if found != 1 {
			t.Fatalf("request %d contains %d submitted device messages", index, found)
		}
	}
}

func TestCancelDuringControlCorrectionPreservesPairedHistory(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "invalid", Name: "ask_user", Args: `{"required_from_user":"city"}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{WaitForCancellation: true},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Context: []MessageContextItem{{Kind: "runtime", Title: "Device", Text: "udesk26"}}}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel"}); err != nil {
		t.Fatal(err)
	}
	cancelled := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	if len(cancelled.Interactions) != 0 || countThreadItems(cancelled.Items, ThreadItemTool) != 0 {
		t.Fatalf("invalid call became interactive: %#v", cancelled)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "continue"}, RequestKey: "continue"}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.TurnID != cancelled.TurnID && view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
	if completed.Failure != nil {
		t.Fatal(completed.Failure)
	}
	pairs := 0
	for _, message := range gateway.Requests()[2].Messages {
		for _, call := range message.ToolCalls {
			if call.ID == "invalid" {
				pairs++
			}
		}
		if message.ToolResult != nil && message.ToolResult.CallID == "invalid" && strings.Contains(message.ToolResult.Text, "invalid arguments") {
			pairs++
		}
	}
	if pairs != 2 {
		t.Fatalf("cancelled correction lost its pair: %d", pairs)
	}
}

func TestMessageContextValidation(t *testing.T) {
	valid := MessageContextItem{Kind: "runtime", Title: "Device", Text: "udesk26"}
	for _, invalid := range []MessageContextItem{{}, {Kind: "runtime\n", Title: "Device", Text: "value"}, {Kind: "runtime", Title: "Device", Text: string([]byte{0xff})}, {Kind: "runtime", Title: "Device", Text: strings.Repeat("x", 64_001)}} {
		if err := (UserInput{Context: []MessageContextItem{invalid}}).Validate(); err == nil {
			t.Fatalf("invalid context admitted: %#v", invalid)
		}
	}
	if err := (UserInput{Context: []MessageContextItem{valid}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (UserInput{Context: make([]MessageContextItem, 129)}).Validate(); err == nil {
		t.Fatal("unbounded context admitted")
	}
}
