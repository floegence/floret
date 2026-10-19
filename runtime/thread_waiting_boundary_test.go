package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
)

type waitingBoundaryGate struct {
	once      sync.Once
	arrived   chan struct{}
	release   chan struct{}
	eventType observation.EventType
}

func (g *waitingBoundaryGate) EmitEvent(e Event) {
	if e.Type == g.eventType {
		g.once.Do(func() { close(g.arrived); <-g.release })
	}
}

func TestThreadServiceWaitingBoundaryAcceptsResponseAndStop(t *testing.T) {
	for _, action := range []string{"respond", "stop", "stop_after_seal"} {
		t.Run(action, func(t *testing.T) {
			gate := &waitingBoundaryGate{arrived: make(chan struct{}), release: make(chan struct{}), eventType: observation.EventTypeStepEnd}
			if action == "stop_after_seal" {
				gate.eventType = observation.EventTypeRunEnd
			}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			gateway := florettest.NewScriptedGateway(
				provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"},
				provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{
					{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}},
					{Type: provider.EventDone, Reason: "tool_calls"},
				}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone, Reason: "stop"}}},
			)
			agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, WithAgentEventSink(gate))
			if err != nil {
				t.Fatal(err)
			}
			_, service := testThreadServiceWithAgent(t, agent)
			child, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "begin"}, RequestKey: "send"}); err != nil {
				t.Fatal(err)
			}
			waitClosed(t, gate.arrived, "waiting boundary not reached")
			waiting := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
			expected := TurnOutcomeCancelled
			if action == "respond" {
				expected = TurnOutcomeCompleted
				if _, err := service.Respond(t.Context(), RespondInput{ThreadID: child.ThreadID, InteractionID: waiting.Interactions[0].ID, Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}, RequestKey: "answer"}); err != nil {
					t.Fatal(err)
				}
				accepted, err := service.View(t.Context(), child.ThreadID)
				if err != nil || accepted.RunID != waiting.RunID || len(accepted.Interactions) != 1 || !accepted.Interactions[0].Resolved {
					t.Fatalf("answer must commit while the waiting execution retains ownership: %+v, %v", accepted, err)
				}
			} else if action == "stop" {
				if _, err := service.Cancel(context.Background(), CancelInput{ThreadID: child.ThreadID, Mode: CancelModeGraceful, RequestKey: "stop"}); err != nil {
					t.Fatal(err)
				}
			} else {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
				defer cancel()
				if _, err := service.Cancel(ctx, CancelInput{ThreadID: child.ThreadID, Mode: CancelModeGraceful, RequestKey: "stop"}); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("sealed execution stop did not wait for settlement: %v", err)
				}
			}
			release.Do(func() { close(gate.release) })
			if action == "stop_after_seal" {
				if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: child.ThreadID, Mode: CancelModeGraceful, RequestKey: "stop"}); err != nil {
					t.Fatal(err)
				}
			}
			final := waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.Attention.InputCount == 0 })
			if final.LastOutcome == nil || *final.LastOutcome != expected || final.Failure != nil {
				t.Fatalf("outcome=%v failure=%+v, want %s", final.LastOutcome, final.Failure, expected)
			}
		})
	}
}
