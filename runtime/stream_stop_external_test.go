package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
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

func TestExplicitResumeKeepsConfirmedWorkAfterRestart(t *testing.T) {
	gateway := &stopBoundaryGateway{events: make(chan provider.Event, 16)}
	var calls atomic.Int32
	write := tools.Define[struct{}](tools.Definition{Name: "write_files", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
		calls.Add(1)
		return tools.Result{Text: "file A and file B updated"}, nil
	})
	source := storage.SQLite(filepath.Join(t.TempDir(), "resume.sqlite"))
	authorize := runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	}))
	host, service := inputPolicyService(t, source, gateway, runtime.WithAgentTools(write), authorize)
	id := inputPolicySend(t, service)
	gateway.events <- provider.Event{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-once", Name: "write_files", Args: `{}`}}}
	gateway.events <- provider.Event{Type: provider.EventDone, Reason: "tool_calls"}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "Next: verify tests"}
	live := inputPolicyWait(t, service, id, func(view runtime.ThreadView) bool {
		for _, item := range view.Items {
			if item.Kind == runtime.ThreadItemAssistant && strings.Contains(item.Text, "verify tests") {
				return true
			}
		}
		return false
	})
	target := runtime.ResumeThreadInput{ThreadID: id, ExpectedTurnID: live.TurnID, ExpectedRunID: live.RunID}
	if _, err := service.Cancel(t.Context(), runtime.CancelInput{ThreadID: id, ExpectedTurnID: live.TurnID, ExpectedRunID: live.RunID, RequestKey: "stop", Mode: runtime.CancelModeGraceful}); err != nil {
		t.Fatal(err)
	}
	inputPolicyWait(t, service, id, func(view runtime.ThreadView) bool { return view.Activity == runtime.ThreadActivityIdle })
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, service = inputPolicyService(t, source, gateway, runtime.WithAgentTools(write))
	resumer := service.(runtime.ThreadResumer)
	stale := target
	stale.ExpectedRunID = "other"
	if _, err := resumer.Resume(t.Context(), stale); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("stale target: %v", err)
	}
	for range 2 {
		if _, err := resumer.Resume(t.Context(), target); err != nil {
			t.Fatal(err)
		}
	}
	gateway.events <- provider.Event{Type: provider.EventDelta, Text: "Verified"}
	gateway.events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	final := inputPolicyWait(t, service, id, func(view runtime.ThreadView) bool {
		return view.Activity == runtime.ThreadActivityIdle && view.TurnID != target.ExpectedTurnID
	})
	if final.LastOutcome == nil || *final.LastOutcome != runtime.TurnOutcomeCompleted || final.TurnKind != runtime.TurnKindResume || calls.Load() != 1 {
		t.Fatalf("outcome=%v kind=%v failure=%+v calls=%d", final.LastOutcome, final.TurnKind, final.Failure, calls.Load())
	}
	users := 0
	for _, item := range final.Items {
		if item.Kind == runtime.ThreadItemUser {
			users++
		}
	}
	if users != 1 {
		t.Fatalf("user items=%d", users)
	}
	wire, _ := json.Marshal(gateway.last().Messages)
	for _, fact := range []string{"file A and file B updated", "verify tests", "turn_resume", "turn_interruption"} {
		if !strings.Contains(string(wire), fact) {
			t.Fatalf("provider missing %q: %s", fact, wire)
		}
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = inputPolicyService(t, source, gateway, runtime.WithAgentTools(write))
	if _, err := service.(runtime.ThreadResumer).Resume(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), id)
	if err != nil || view.TurnID != final.TurnID || view.Activity != runtime.ThreadActivityIdle {
		t.Fatalf("durable replay %+v %v", view, err)
	}
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
