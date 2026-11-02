package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestStrictRepetitionFeedbackSurvivesRestartAndRetry(t *testing.T) {
	steps := []florettest.Step{}
	for i := 0; i < 5; i++ {
		steps = append(steps, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: fmt.Sprint("inspect-", i), Name: "inspect", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	}
	steps = append(steps, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Summary from preserved results"}, {Type: provider.EventDone, Reason: "stop"}}})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "script", StateCompatibilityKey: "script:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
	var dispatched, authorized atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(nil, nil), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Activity: func(tools.Invocation[any]) (*tools.ActivityPresentation, error) {
		return &tools.ActivityPresentation{Label: "Inspect task", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{}}, nil
	}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		dispatched.Add(1)
		return tools.Result{Text: "Saved child result"}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, r EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		authorized.Add(1)
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: r.EffectAttemptID, RequestFingerprint: r.RequestFingerprint, ThreadID: r.ThreadID, TurnID: r.TurnID, RunID: r.RunID, ToolCallID: r.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/repetition.sqlite"
	open := func() (*Host, ThreadService) {
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		s, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return h, s
	}
	h, s := open()
	created, err := s.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Inspect child"}}); err != nil {
		t.Fatal(err)
	}
	awaitIdle := func(service ThreadService, differentTurn string) ThreadView {
		t.Helper()
		// SQLite plus race instrumentation can take longer than the generic
		// three-second fixture deadline. Wait for the persisted outcome itself.
		deadline := time.NewTimer(15 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			view, err := service.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if view.Activity == ThreadActivityIdle && view.LastOutcome != nil && string(view.TurnID) != differentTurn {
				return view
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatalf("persisted repetition outcome did not settle: %+v", view.Failure)
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
		}
	}
	failed := awaitIdle(s, "")
	assert := func(v ThreadView) {
		t.Helper()
		if v.Failure == nil || v.Failure.Reason != ThreadTurnFailureRepeatedToolCalls || v.Failure.ToolRepetition == nil {
			t.Fatalf("missing typed failure: %+v", v.Failure)
		}
		d := v.Failure.ToolRepetition
		if d.ToolName != "inspect" || d.CallID != "inspect-4" || d.ConsecutiveCalls != 5 || d.RepeatLimit != 3 || !d.FeedbackGiven || !d.NotExecuted {
			t.Fatalf("diagnostic=%+v", d)
		}
		if err := v.Failure.Validate(); err != nil {
			t.Fatal(err)
		}
		rejected := 0
		for _, item := range v.Items {
			if item.Activity != nil && (item.Activity.ToolID == "inspect-3" || item.Activity.ToolID == "inspect-4") {
				if item.Activity.Status != "error" {
					t.Fatalf("rejected status=%s", item.Activity.Status)
				}
				rejected++
			}
		}
		if rejected != 2 {
			t.Fatalf("visible rejected activities=%d", rejected)
		}
	}
	assert(failed)
	if dispatched.Load() != 3 || authorized.Load() != 3 {
		t.Fatalf("blocked calls acquired authority: dispatch=%d authorization=%d", dispatched.Load(), authorized.Load())
	}
	waitThreadExecutionDone(t, s.(*threadRuntimeService), created.ThreadID)
	if err := h.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, s = open()
	restored, err := s.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assert(restored)
	if _, err = s.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: restored.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	done := awaitIdle(s, string(restored.TurnID))
	if done.Failure != nil || *done.LastOutcome != TurnOutcomeCompleted || dispatched.Load() != 3 {
		t.Fatalf("retry=%+v executions=%d", done.Failure, dispatched.Load())
	}
	requests := gateway.Requests()
	last := requests[len(requests)-1]
	calls, results := map[string]int{}, map[string]int{}
	saved, feedback := false, false
	for _, m := range last.Messages {
		for _, c := range m.ToolCalls {
			calls[c.ID]++
		}
		if m.ToolResult != nil {
			results[m.ToolResult.CallID]++
			saved = saved || strings.Contains(m.ToolResult.Text, "Saved child result")
			feedback = feedback || strings.Contains(m.ToolResult.Text, "not executed")
		}
	}
	for id, n := range calls {
		if n != 1 || results[id] != 1 {
			t.Fatalf("unpaired history %s: calls=%d results=%d", id, n, results[id])
		}
	}
	if !saved || !feedback {
		t.Fatal("retry lost saved results or explicit rejection")
	}
}
