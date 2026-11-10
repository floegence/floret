package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

type stopBoundaryGateway struct {
	mu       sync.Mutex
	requests []provider.Request
	events   chan provider.Event
}

func (*stopBoundaryGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "stop-boundary", StateCompatibilityKey: "stop-boundary:v1"}
}
func (*stopBoundaryGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}
func (g *stopBoundaryGateway) Stream(ctx context.Context, req provider.Request) (<-chan provider.Event, error) {
	g.mu.Lock()
	g.requests = append(g.requests, req)
	g.mu.Unlock()
	out := make(chan provider.Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-g.events:
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
				if e.Type == provider.EventDone {
					return
				}
			}
		}
	}()
	return out, nil
}
func (g *stopBoundaryGateway) last() provider.Request {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.requests[len(g.requests)-1]
}

func TestStreamStopSurvivesRestartForkAndRetryAtProviderBoundary(t *testing.T) {
	gateway := &stopBoundaryGateway{events: make(chan provider.Event, 16)}
	source := storage.SQLite(filepath.Join(t.TempDir(), "stop.sqlite"))
	host, service := inputPolicyService(t, source, gateway)
	id := inputPolicySend(t, service)
	gateway.events <- provider.Event{Type: provider.EventReasoning, Text: "partial reasoning"}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "## Answer\n```go\npartial 🌸"}
	gateway.events <- provider.Event{Type: provider.EventToolCallStart, ToolCallStream: &provider.ToolCallStream{ID: "unfinished", Name: "ask_user"}}
	gateway.events <- provider.Event{Type: provider.EventToolCallDelta, ToolCallStream: &provider.ToolCallStream{ID: "unfinished", Name: "ask_user"}, Text: `{"questions":[`}
	live := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool {
		for _, item := range v.Items {
			if item.Kind == runtime.ThreadItemAssistant && strings.Contains(item.Text, "partial 🌸") {
				return true
			}
		}
		return false
	})
	stop := runtime.CancelInput{ThreadID: id, ExpectedTurnID: live.TurnID, ExpectedRunID: live.RunID, RequestKey: "precise-stop", Mode: runtime.CancelModeGraceful}
	if _, err := service.Cancel(t.Context(), stop); err != nil {
		t.Fatal(err)
	}
	stopped := inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool { return v.Activity == runtime.ThreadActivityIdle })
	if stopped.LastOutcome == nil || *stopped.LastOutcome != runtime.TurnOutcomeCancelled || len(stopped.Interactions) != 0 {
		t.Fatalf("stop=%+v", stopped)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = inputPolicyService(t, source, gateway)
	fork, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: id, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: id, RequestKey: "retry-fork"})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []identity.ThreadID{id, fork.ThreadID} {
		gateway.events <- provider.Event{Type: provider.EventDelta, Text: "continued"}
		gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
		if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: target, RequestKey: "continue", Input: runtime.UserInput{Text: "continue"}}); err != nil {
			t.Fatal(err)
		}
		inputPolicyWait(t, service, target, func(v runtime.ThreadView) bool {
			return v.Activity == runtime.ThreadActivityIdle && v.TurnID != stopped.TurnID
		})
		req := gateway.last()
		partial, notices := false, 0
		for _, message := range req.Messages {
			if message.Role == provider.RoleAssistant && strings.Contains(message.Text, "partial 🌸") {
				partial = true
			}
			if len(message.ToolCalls) > 0 {
				t.Fatalf("partial arguments became executable history: %+v", message)
			}
			var fact map[string]string
			if json.Unmarshal([]byte(message.Text), &fact) == nil && fact["type"] == "turn_interruption" {
				notices++
				if fact["turn_id"] != live.TurnID.String() || fact["run_id"] != live.RunID.String() || fact["outcome"] != "cancelled" {
					t.Fatalf("fact=%v", fact)
				}
			}
		}
		if !partial || notices != 1 {
			t.Fatalf("provider lost interruption context: %+v", req.Messages)
		}
	}
	// Replaying the old admitted stop after restart must not target the next run.
	active, err := service.Send(t.Context(), runtime.SendInput{ThreadID: id, RequestKey: "active", Input: runtime.UserInput{Text: "more"}})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Cancel(t.Context(), stop)
	if err != nil || replay.RunID != active.RunID || replay.Cancellation != nil {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	conflict := stop
	conflict.ExpectedTurnID = active.TurnID
	conflict.ExpectedRunID = active.RunID
	if _, err := service.Cancel(t.Context(), conflict); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("key reuse=%v", err)
	}
	conflict.RequestKey = "cleanup"
	if _, err := service.Cancel(t.Context(), conflict); err != nil {
		t.Fatal(err)
	}
	inputPolicyWait(t, service, id, func(v runtime.ThreadView) bool { return v.Activity == runtime.ThreadActivityIdle })
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "retried"}
	gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	if _, err := service.Retry(t.Context(), runtime.RetryInput{ThreadID: retry.ThreadID, SourceTurnID: live.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	inputPolicyWait(t, service, retry.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.TurnID != live.TurnID
	})
	for _, message := range gateway.last().Messages {
		if strings.Contains(message.Text, "partial 🌸") || strings.Contains(message.Text, `"type":"turn_interruption"`) {
			t.Fatalf("retry leaked discarded attempt: %+v", message)
		}
	}
}
