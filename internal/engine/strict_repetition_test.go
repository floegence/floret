package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestStrictRepetitionTransportRetryDoesNotCount(t *testing.T) {
	args := "{\"target\":\"same\",\"description\":\"same\"}"
	steps := [][]provider.StreamEvent{
		repeatCall("one", "inspect", args),
		repeatCall("two", "inspect", args),
		harness.Step(harness.Tool("discarded", "inspect", args), provider.StreamEvent{Type: provider.Error, Err: io.ErrUnexpectedEOF}),
		repeatCall("three", "inspect", args),
		harness.Step(harness.Text("done"), harness.Done()),
	}
	e, p, executions := repeatProbe(t, steps)
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || executions.Load() != 3 || len(p.Requests) != 5 {
		t.Fatalf("status=%s err=%v dispatched=%d requests=%d", got.Status, got.Err, executions.Load(), len(p.Requests))
	}
	if p.Requests[2].LogicalRequestID != p.Requests[3].LogicalRequestID {
		t.Fatal("fixture did not exercise a transport retry")
	}
}

func TestStrictRepetitionExhaustionPreservesSiblingResult(t *testing.T) {
	args := "{\"target\":\"same\",\"description\":\"same\"}"
	var steps [][]provider.StreamEvent
	for i := 0; i < 4; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "inspect", args))
	}
	steps = append(steps, harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "rejected", Name: "inspect", Args: args}, {ID: "sibling", Name: "other", Args: args}}}, harness.DoneReason("tool_calls")))
	e, _, executions := repeatProbe(t, steps)
	got := e.Run(t.Context(), "inspect")
	if !errors.Is(got.Err, engine.ErrDuplicateTools) || executions.Load() != 4 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, executions.Load())
	}
	results := map[string]string{}
	for _, message := range got.Messages {
		if message.Role == session.Tool {
			results[message.ToolCallID] = message.Content
		}
	}
	if results["sibling"] != "running" || !strings.Contains(results["rejected"], "not executed") {
		t.Fatalf("unpaired or lost sibling results: %+v", results)
	}
}

func TestStrictRepetitionControlFeedbackReplacesSuccess(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 4; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "host_continue", "{}"))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	e, p, _ := repeatProbe(t, steps)
	e.Options.ControlSpec = engine.ControlSpec{
		Definitions: []tools.ToolDefinition{{Name: "host_continue", InputSchema: tools.StrictObject(nil, nil), Annotations: map[string]any{"kind": "control"}}},
		Project: func(call provider.ToolCall) (engine.ControlSignal, bool, error) {
			return engine.ControlSignal{Disposition: engine.ControlContinue, Name: call.Name, CallID: call.ID, OutputText: "accepted"}, true, nil
		},
	}
	got := e.Run(t.Context(), "continue")
	if got.Status != engine.Completed || len(p.Requests) != 5 {
		t.Fatalf("status=%s err=%v requests=%d", got.Status, got.Err, len(p.Requests))
	}
	count := 0
	for _, message := range p.Requests[4].Messages {
		if message.Role == session.Tool && message.ToolCallID == "3" {
			count++
			if !strings.Contains(message.Content, "not executed") {
				t.Fatalf("rejected control projected success: %+v", message)
			}
		}
	}
	if count != 1 {
		t.Fatalf("feedback count=%d", count)
	}
}

func repeatCall(id, name, args string) []provider.StreamEvent {
	return harness.Step(harness.Tool(id, name, args), harness.DoneReason("tool_calls"))
}
func repeatProbe(t *testing.T, steps [][]provider.StreamEvent) (*testEngine, *harness.ScriptedProvider, *atomic.Int32) {
	t.Helper()
	p := harness.NewScriptedProvider(steps...)
	e := newTestEngine(p, &event.Recorder{})
	executions := new(atomic.Int32)
	for _, name := range []string{"inspect", "other"} {
		mustRegister(t, e.Tools, tools.Define[map[string]any](tools.Definition{Name: name, ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{"target": tools.String("Target"), "description": tools.String("Description")}, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			executions.Add(1)
			return tools.Result{Text: "running"}, nil
		}))
	}
	return e, p, executions
}
func TestStrictRepetitionInterleavedHostedSearch(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 6; i++ {
		search := provider.ToolCall{ID: fmt.Sprint("search-", i), Name: "web_search", Args: "{\"target\":\"same\",\"description\":\"same\"}"}
		steps = append(steps, harness.Step(provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: search}, provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: search}, harness.Tool(fmt.Sprint("inspect-", i), "inspect", "{\"target\":\"same\",\"description\":\"same\"}"), harness.DoneReason("tool_calls")))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	e, _, n := repeatProbe(t, steps)
	e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}
	got := e.Run(t.Context(), "research")
	if got.Status != engine.Completed || n.Load() != 6 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n.Load())
	}
}
func TestStrictRepetitionAlternatingBatchIsNotConsecutive(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 6; i++ {
		steps = append(steps, harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: fmt.Sprint("a", i), Name: "inspect", Args: "{\"target\":\"same\",\"description\":\"same\"}"}, {ID: fmt.Sprint("b", i), Name: "other", Args: "{\"target\":\"same\",\"description\":\"same\"}"}}}, harness.DoneReason("tool_calls")))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	e, _, n := repeatProbe(t, steps)
	got := e.Run(t.Context(), "alternate")
	if got.Status != engine.Completed || n.Load() != 12 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n.Load())
	}
}
func TestStrictRepetitionDescriptionChangeBreaksSequence(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 6; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "inspect", fmt.Sprintf("{\"target\":\"same\",\"description\":\"check %d\"}", i)))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	e, _, n := repeatProbe(t, steps)
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n.Load() != 6 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n.Load())
	}
}
func TestStrictRepetitionFeedbackAllowsDecision(t *testing.T) {
	steps := [][]provider.StreamEvent{}
	for i := 0; i < 4; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "inspect", "{\"target\":\"same\",\"description\":\"same\"}"))
	}
	steps = append(steps, harness.Step(harness.Text("saved result summary"), harness.Done()))
	e, p, n := repeatProbe(t, steps)
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n.Load() != 3 || len(p.Requests) != 5 {
		t.Fatalf("status=%s err=%v dispatched=%d requests=%d", got.Status, got.Err, n.Load(), len(p.Requests))
	}
	found := false
	for _, m := range p.Requests[4].Messages {
		if m.ToolCallID == "3" && m.Role == "tool" {
			found = strings.Contains(m.Content, "not executed") && strings.Contains(m.Content, "4")
		}
	}
	if !found {
		t.Fatalf("missing paired non-execution feedback before final decision: %+v", p.Requests[4].Messages)
	}
}
func TestStrictRepetitionCanonicalArgumentsAndExhaustion(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 5; i++ {
		args := "{\"target\":\"same\",\"description\":\"same\"}"
		if i%2 == 1 {
			args = "{ \"description\": \"same\", \"target\": \"same\" }"
		}
		steps = append(steps, repeatCall(fmt.Sprint(i), "inspect", args))
	}
	e, _, n := repeatProbe(t, steps)
	got := e.Run(t.Context(), "inspect")
	if !errors.Is(got.Err, engine.ErrDuplicateTools) || n.Load() != 3 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n.Load())
	}
}

func TestStrictRepetitionIdenticalCallsWithinBatch(t *testing.T) {
	calls := []provider.ToolCall{}
	for i := 0; i < 5; i++ {
		calls = append(calls, provider.ToolCall{ID: fmt.Sprint(i), Name: "inspect", Args: "{\"target\":\"same\",\"description\":\"same\"}"})
	}
	steps := [][]provider.StreamEvent{harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}, harness.DoneReason("tool_calls")), harness.Step(harness.Text("done"), harness.Done())}
	e, p, n := repeatProbe(t, steps)
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n.Load() != 3 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n.Load())
	}
	rejected := 0
	for _, m := range p.Requests[1].Messages {
		if m.Role == session.Tool && strings.Contains(m.Content, "not executed") {
			rejected++
		}
	}
	if rejected != 2 {
		t.Fatalf("rejected=%d", rejected)
	}
}
func TestStrictRepetitionDescriptionCannotBeIgnored(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 6; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "poll", fmt.Sprintf("{\"value\":\"same\",\"description\":\"check %d\"}", i)))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	p := harness.NewScriptedProvider(steps...)
	e := newTestEngine(p, &event.Recorder{})
	n := 0
	mustRegister(t, e.Tools, tools.Define[stringArgs](tools.Definition{Name: "poll", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("Value"), "description": tools.String("Description")}, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Annotations: map[string]any{tools.AnnotationRepeatPolicy: tools.RepeatPolicyPolling, tools.AnnotationRepeatIdentityIgnoredArguments: []string{"description"}}}, nil, nil, func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
		n++
		return tools.Result{Text: "same", Metadata: map[string]any{tools.ResultMetadataProgressToken: "unchanged"}}, nil
	}))
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n != 6 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n)
	}
}
func TestStrictRepetitionChangedPollingToken(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 8; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "poll", "{\"value\":\"same\"}"))
	}
	steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
	p := harness.NewScriptedProvider(steps...)
	e := newTestEngine(p, &event.Recorder{})
	n := 0
	mustRegister(t, e.Tools, tools.Define[stringArgs](tools.Definition{Name: "poll", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("Value")}, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}, Annotations: map[string]any{tools.AnnotationRepeatPolicy: tools.RepeatPolicyPolling}}, nil, nil, func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
		n++
		return tools.Result{Text: "new output", Metadata: map[string]any{tools.ResultMetadataProgressToken: fmt.Sprint(n)}}, nil
	}))
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n != 8 {
		t.Fatalf("status=%s err=%v dispatched=%d", got.Status, got.Err, n)
	}
}
func TestStrictRepetitionFeedbackAdmitsPendingChildEvent(t *testing.T) {
	var steps [][]provider.StreamEvent
	for i := 0; i < 4; i++ {
		steps = append(steps, repeatCall(fmt.Sprint(i), "inspect", "{\"target\":\"same\",\"description\":\"same\"}"))
	}
	steps = append(steps, repeatCall("read-report", "inspect", "{\"target\":\"same\",\"description\":\"same\"}"), harness.Step(harness.Text("child report received"), harness.Done()))
	e, p, n := repeatProbe(t, steps)
	boundaries := 0
	e.Options.AdmitParentEvents = func(context.Context) (session.Message, error) {
		boundaries++
		if boundaries == 5 {
			return session.Message{EntryID: "child-completion", Role: session.User, Content: "child-a completed; saved report ready"}, nil
		}
		return session.Message{}, nil
	}
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Completed || n.Load() != 4 || len(p.Requests) != 6 {
		t.Fatalf("status=%s err=%v", got.Status, got.Err)
	}
	found := false
	for _, m := range p.Requests[4].Messages {
		found = found || strings.Contains(m.Content, "child-a completed")
	}
	if !found {
		t.Fatal("pending completion was not admitted at recovery request boundary")
	}
}
