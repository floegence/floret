package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
)

// Use the public JSON boundary so this regression runs before the additive API exists.
func exactStopInput(t *testing.T, view ThreadView, key string, mode CancelMode) CancelInput {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"thread_id": view.ThreadID, "expected_turn_id": view.TurnID, "expected_run_id": view.RunID, "request_key": key, "mode": mode})
	if err != nil {
		t.Fatal(err)
	}
	var in CancelInput
	if err := json.Unmarshal(encoded, &in); err != nil {
		t.Fatal(err)
	}
	return in
}

func TestPreciseStopCannotCancelReplacementExecution(t *testing.T) {
	for _, mode := range []CancelMode{CancelModeImmediate, CancelModeGraceful} {
		t.Run(string(mode), func(t *testing.T) {
			gateway := &goalStreamingGateway{events: make(chan provider.Event, 8)}
			agent, err := testAgent(gateway)
			if err != nil {
				t.Fatal(err)
			}
			_, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "first"}, RequestKey: "first"})
			if err != nil {
				t.Fatal(err)
			}
			gateway.events <- provider.Event{Type: provider.EventDelta, Text: "first done"}
			gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
			waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			second, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "second"}, RequestKey: "second"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Cancel(t.Context(), exactStopInput(t, first, "late-stop", mode))
			if !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("stale stop error=%v, want request conflict", err)
			}
			current, err := service.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if current.TurnID != second.TurnID || current.RunID != second.RunID || current.Activity != ThreadActivityActive || current.Cancellation != nil {
				t.Fatalf("late stop changed replacement: %+v", current)
			}
			if _, err := service.Cancel(context.Background(), exactStopInput(t, second, "cleanup", mode)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreciseStopReplayAndRequestConflict(t *testing.T) {
	for _, mode := range []CancelMode{CancelModeImmediate, CancelModeGraceful} {
		t.Run(string(mode), func(t *testing.T) {
			_, service := testThreadService(t, newBlockingThreadGateway())
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "first"}, RequestKey: "first"})
			if err != nil {
				t.Fatal(err)
			}
			stop := exactStopInput(t, first, "stop", mode)
			wrong := stop
			wrong.ExpectedRunID = "another-run"
			if _, err := service.Cancel(t.Context(), wrong); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("wrong run=%v", err)
			}
			if _, err := service.Cancel(t.Context(), stop); err != nil {
				t.Fatal(err)
			}
			waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			second, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "second"}, RequestKey: "second"})
			if err != nil {
				t.Fatal(err)
			}
			replay, err := service.Cancel(t.Context(), stop)
			if err != nil {
				t.Fatal(err)
			}
			if replay.TurnID != second.TurnID || replay.Activity != ThreadActivityActive || replay.Cancellation != nil {
				t.Fatalf("replay affected next execution: %+v", replay)
			}
			if _, err := service.Cancel(t.Context(), exactStopInput(t, second, "stop", mode)); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("reused key=%v", err)
			}
			wrong = stop
			wrong.IncludeDescendants = true
			if _, err := service.Cancel(t.Context(), wrong); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("changed scope=%v", err)
			}
			invalid := stop
			invalid.ExpectedRunID = ""
			if _, err := service.Cancel(t.Context(), invalid); err == nil {
				t.Fatal("accepted incomplete target")
			}
			if _, err := service.Cancel(t.Context(), exactStopInput(t, second, "cleanup", mode)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreciseStopRejectsPreviousRunOfSameTurn(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "cancel", StateCompatibilityKey: "test:cancel"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: cancelHistoryAskArgs}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	preparing := make(chan struct{})
	service.(*threadRuntimeService).factory = AgentFactoryFunc(func(ctx context.Context, request AgentRequest) (*Agent, error) {
		if request.InteractionID != "" {
			close(preparing)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return agent, nil
	})
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "ask"}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Attention.InputCount == 1 })
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: waiting.Interactions[0].ID, Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}, RequestKey: "answer"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, preparing, "continuation preparation did not start")
	current, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if current.TurnID != waiting.TurnID || current.RunID == waiting.RunID {
		t.Fatalf("expected same turn, new run: %+v / %+v", waiting, current)
	}
	if _, err := service.Cancel(t.Context(), exactStopInput(t, waiting, "old-run", CancelModeGraceful)); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("previous run=%v", err)
	}
	if _, err := service.Cancel(t.Context(), exactStopInput(t, current, "current-run", CancelModeGraceful)); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
}

func TestPreciseSubtreeStopReplayDoesNotStopNewChildExecution(t *testing.T) {
	_, service := testThreadService(t, newBlockingThreadGateway())
	root, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "child", ParentThreadID: root.ThreadID, TaskName: "child", HostProfileRef: "test"})
	if err != nil {
		t.Fatal(err)
	}
	root, err = service.Send(t.Context(), SendInput{ThreadID: root.ThreadID, Input: UserInput{Text: "root"}, RequestKey: "root-send"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "child"}, RequestKey: "child-send"}); err != nil {
		t.Fatal(err)
	}
	stop := exactStopInput(t, root, "subtree-stop", CancelModeGraceful)
	stop.IncludeDescendants = true
	if _, err := service.Cancel(t.Context(), stop); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, root.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	waitThreadView(t, service, child.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	next, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "new"}, RequestKey: "child-new"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), stop); err != nil {
		t.Fatal(err)
	}
	current, err := service.View(t.Context(), child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if current.RunID != next.RunID || current.Cancellation != nil || current.Activity != ThreadActivityActive {
		t.Fatalf("replay stopped new child: %+v", current)
	}
}
