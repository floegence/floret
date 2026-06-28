package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

func TestHostRunsFakeProviderThread(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	started, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if started.ID != "thread" || !started.CanAppendMessage {
		t.Fatalf("started thread = %#v", started)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != ThreadStatusCompleted ||
		len(snapshot.Messages) != 2 ||
		snapshot.Messages[0].Role != string(session.User) ||
		snapshot.Messages[0].Content != "hello" ||
		snapshot.Messages[1].Content != "configured" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Type == "provider_delta" && ev.ThreadID == "thread" && ev.RunID == "turn-1"
	}) {
		t.Fatalf("runtime events missing provider delta: %#v", rec.events)
	}
}

func TestHostEnsureThreadReturnsSummaryWithoutMessages(t *testing.T) {
	ctx := context.Background()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	started, err := host.EnsureThread(ctx, EnsureThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if started.ID != "thread" || !started.CanAppendMessage {
		t.Fatalf("started summary = %#v", started)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"}); err != nil {
		t.Fatal(err)
	}
	ensured, err := host.EnsureThread(ctx, EnsureThreadRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if ensured.ID != "thread" || ensured.Status != ThreadStatusCompleted || ensured.LatestTurnID != "turn-1" {
		t.Fatalf("ensured summary = %#v", ensured)
	}
	data, err := json.Marshal(ensured)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "messages") || strings.Contains(string(data), "content") {
		t.Fatalf("thread summary leaked transcript data: %s", string(data))
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Messages) == 0 {
		t.Fatalf("test setup did not create transcript messages: %#v", snapshot)
	}
}

func TestHostRunsThreadThroughModelGateway(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("gateway hosted thread"), nil
	})
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
		},
		ModelGateway: gateway,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "gateway hosted thread" {
		t.Fatalf("result = %#v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("gateway requests = %#v", requests)
	}
	if req.ThreadID != "thread" || req.TurnID != "turn-1" || req.PromptScopeID != "thread" {
		t.Fatalf("gateway request identity = %#v", req)
	}
	if req.Provider != string(config.ProviderFake) || req.Model != "fake-model" {
		t.Fatalf("gateway request provider/model = %#v", req)
	}
}

func TestHostStreamsProjectedContextStatus(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("gateway hosted thread"), nil
	})
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
			ContextPolicy: config.ContextPolicy{
				ContextWindowTokens: config.DefaultContextWindowTokens,
				MaxOutputTokens:     1024,
			},
		},
		ModelGateway: gateway,
		Sink:         rec,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted {
		t.Fatalf("result = %#v", result)
	}

	var status *observation.ContextStatus
	for _, ev := range rec.events {
		if ev.Type == "provider_request" && ev.ContextStatus != nil {
			status = ev.ContextStatus
			break
		}
	}
	if status == nil {
		t.Fatalf("runtime events missing projected context status: %#v", rec.events)
	}
	if status.Phase != observation.ContextPhaseProjectedRequest ||
		status.ThreadID != "thread" ||
		status.TurnID != "turn-1" ||
		status.Step != 1 ||
		status.ContextPressure.ContextWindowTokens != config.DefaultContextWindowTokens ||
		status.ContextPressure.ProjectedInputTokens <= 0 ||
		status.UsedRatio <= 0 ||
		strings.TrimSpace(status.Status) == "" {
		t.Fatalf("context status = %#v", status)
	}
}

func TestHostModelGatewayPreservesTextAroundToolCalls(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 6)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventDelta, Text: "I will inspect first. "}
			events <- ModelEvent{Type: ModelEventToolCallStart, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCallDelta, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCallEnd, ToolCallStream: &ModelToolCallStream{ID: "read-1", Name: "read"}}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"text":"alpha"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Read returned alpha."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	reg := tools.NewRegistry()
	if err := reg.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "alpha"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
		},
		ModelGateway: gateway,
		Tools:        reg,
		Sink:         rec,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "I will inspect first. Read returned alpha." {
		t.Fatalf("result = %#v", result)
	}
	var streamOrder []StreamObservationType
	var texts []string
	for _, ev := range rec.events {
		if ev.Stream == nil {
			continue
		}
		streamOrder = append(streamOrder, ev.Stream.Type)
		if ev.Stream.Text != "" {
			texts = append(texts, ev.Stream.Text)
		}
	}
	wantOrder := []StreamObservationType{
		StreamObservationAssistantDelta,
		StreamObservationToolCallStart,
		StreamObservationToolCallDelta,
		StreamObservationToolCallEnd,
		StreamObservationModelStreamDone,
		StreamObservationAssistantDelta,
		StreamObservationModelStreamDone,
	}
	if !slices.Equal(streamOrder, wantOrder) {
		t.Fatalf("stream order = %#v, want %#v", streamOrder, wantOrder)
	}
	if !slices.Equal(texts, []string{"I will inspect first. ", "Read returned alpha."}) {
		t.Fatalf("stream texts = %#v", texts)
	}
}

func TestHostToolSurfaceProviderRefreshesGatewayRequests(t *testing.T) {
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		requests = append(requests, req)
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	readOnly := tools.NewRegistry()
	if err := readOnly.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "read"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	full := tools.NewRegistry()
	if err := full.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "read", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "read"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	if err := full.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{Name: "write", InputSchema: runtimeEchoSchema(), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "write"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "base",
		},
		ModelGateway: gateway,
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			if req.Step >= 2 && req.Phase == "provider_request" {
				return ToolSurface{
					Tools:        full,
					SystemPrompt: "full surface",
					Epoch:        "full",
					HostedToolDefinitions: []HostedToolDefinition{{
						Name:    "hosted_search",
						Type:    "web_search",
						Options: map[string]any{"limit": float64(5)},
					}},
				}, nil
			}
			return ToolSurface{Tools: readOnly, SystemPrompt: "read surface", Epoch: "read"}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if _, err := host.StartThread(context.Background(), StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatalf("StartThread: %v", err)
	}
	result, err := host.RunTurn(context.Background(), RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	first, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing first turn request: %#v", requests)
	}
	second, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 2)
	if !ok {
		t.Fatalf("missing second turn request: %#v", requests)
	}
	if names := runtimeToolNames(first.Tools); !slices.Contains(names, "read") || slices.Contains(names, "write") {
		t.Fatalf("first request tools = %v, want read without write", names)
	}
	if names := runtimeToolNames(second.Tools); !slices.Contains(names, "read") || !slices.Contains(names, "write") {
		t.Fatalf("second request tools = %v, want read/write", names)
	}
	if first.Messages[0].Content != "read surface" || second.Messages[0].Content != "full surface" {
		t.Fatalf("dynamic prompts were not forwarded: %#v", requests)
	}
	if len(first.HostedTools) != 0 {
		t.Fatalf("first request hosted tools = %#v, want none", first.HostedTools)
	}
	if len(second.HostedTools) != 1 || second.HostedTools[0].Name != "hosted_search" || second.HostedTools[0].Type != "web_search" || second.HostedTools[0].Options["limit"] != float64(5) {
		t.Fatalf("second request hosted tools = %#v", second.HostedTools)
	}
}

func TestSubAgentActivityTimelineProjectsStatusSummary(t *testing.T) {
	base := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	snapshots := []agentharness.SubAgentSnapshot{
		{ThreadID: "completed", TaskName: "completed task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusCompleted, LastMessage: "done", CreatedAt: base.Add(-8 * time.Minute), UpdatedAt: base.Add(-7 * time.Minute)},
		{ThreadID: "running", TaskName: "running task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusRunning, LastMessage: "working", CreatedAt: base.Add(-6 * time.Minute), UpdatedAt: base.Add(-1 * time.Minute)},
		{ThreadID: "waiting", TaskName: "waiting task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusWaiting, WaitingPrompt: "need input", CreatedAt: base.Add(-5 * time.Minute), UpdatedAt: base.Add(-2 * time.Minute)},
		{ThreadID: "failed", TaskName: "failed task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusFailed, LastMessage: "failed", CreatedAt: base.Add(-4 * time.Minute), UpdatedAt: base.Add(-3 * time.Minute)},
		{ThreadID: "cancelled", TaskName: "cancelled task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusCancelled, CreatedAt: base.Add(-3 * time.Minute), UpdatedAt: base.Add(-4 * time.Minute)},
		{ThreadID: "idle", TaskName: "idle task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusIdle, CreatedAt: base.Add(-2 * time.Minute), UpdatedAt: base.Add(-5 * time.Minute)},
		{ThreadID: "interrupted", TaskName: "interrupted task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusInterrupted, CreatedAt: base.Add(-90 * time.Second), UpdatedAt: base},
		{ThreadID: "closed", TaskName: "closed task", ParentThreadID: "parent", Status: agentharness.SubAgentStatusClosed, CreatedAt: base.Add(-30 * time.Second), UpdatedAt: base.Add(-6 * time.Minute), Closed: true},
	}
	timeline := subAgentActivityTimeline(observation.ActivityRunMeta{
		RunID:    "parent-run",
		ThreadID: "parent",
		TurnID:   "parent-turn",
		TraceID:  "parent-trace",
	}, snapshots, base)
	if err := observation.ValidateActivityTimeline(timeline); err != nil {
		t.Fatalf("ValidateActivityTimeline: %v", err)
	}
	if len(timeline.Items) != len(snapshots) {
		t.Fatalf("items=%d, want %d", len(timeline.Items), len(snapshots))
	}
	if timeline.Summary.Status != observation.ActivityStatusError || timeline.Summary.Severity != observation.ActivitySeverityError || !timeline.Summary.NeedsAttention {
		t.Fatalf("summary=%#v, want error with attention", timeline.Summary)
	}
	counts := timeline.Summary.Counts
	if counts.Pending != 1 || counts.Running != 1 || counts.Waiting != 2 || counts.Success != 1 || counts.Error != 1 || counts.Canceled != 2 {
		t.Fatalf("counts=%#v", counts)
	}
	if timeline.Items[0].ToolName != "subagents" || timeline.Items[0].Payload["subagent_id"] != "interrupted" {
		t.Fatalf("first item=%#v, want newest active subagent", timeline.Items[0])
	}
	if timeline.Items[0].Status != observation.ActivityStatusWaiting {
		t.Fatalf("interrupted status=%q, want waiting", timeline.Items[0].Status)
	}
	for _, item := range timeline.Items {
		if _, ok := item.Payload["operation"]; ok {
			t.Fatalf("floret subagent activity payload must not include product operation: %#v", item.Payload)
		}
		if _, ok := item.Payload["action"]; ok {
			t.Fatalf("floret subagent activity payload must not include product action: %#v", item.Payload)
		}
		if _, ok := item.Payload["delegation_runtime"]; ok {
			t.Fatalf("floret subagent activity payload must not include product runtime label: %#v", item.Payload)
		}
	}
}

func TestHostSubAgentsInheritModelGatewayWithChildPromptScope(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("gateway child done"), nil
	})
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
		},
		ModelGateway: gateway,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID: "parent",
		ParentTurnID:   "parent-turn",
		ThreadID:       "child",
		TaskName:       "Review API",
		Message:        "review the runtime API",
		HostProfileRef: "reviewer",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].LastMessage != "gateway child done" {
		t.Fatalf("waited = %#v", waited)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "child", "", 1)
	if !ok {
		t.Fatalf("gateway requests = %#v", requests)
	}
	if req.ThreadID != "child" || req.PromptScopeID != "child" {
		t.Fatalf("child gateway request should use child identity and prompt scope: %#v", req)
	}
}

func TestHostManagesSubAgentLifecycle(t *testing.T) {
	ctx := context.Background()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	spawned, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID: "parent",
		ParentTurnID:   "parent-turn",
		ThreadID:       "child",
		TaskName:       "Review API",
		Message:        "review the runtime API",
		HostProfileRef: "reviewer",
		ForkMode:       SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spawned.ThreadID != "child" || spawned.ParentThreadID != "parent" || spawned.Path != "/root/review_api" {
		t.Fatalf("spawned = %#v", spawned)
	}
	if spawned.ForkMode != SubAgentForkNone {
		t.Fatalf("spawned fork mode = %q, want %q", spawned.ForkMode, SubAgentForkNone)
	}

	waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != SubAgentStatusCompleted {
		t.Fatalf("waited = %#v", waited)
	}
	listed, err := host.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].HostProfileRef != "reviewer" || listed[0].LastMessage != "child done" {
		t.Fatalf("listed = %#v", listed)
	}
	if listed[0].ForkMode != SubAgentForkNone {
		t.Fatalf("listed fork mode = %q, want %q", listed[0].ForkMode, SubAgentForkNone)
	}
	timeline, err := host.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Timeline.Items) != 1 || timeline.Timeline.Items[0].Payload["fork_mode"] != string(SubAgentForkNone) {
		t.Fatalf("activity timeline fork mode missing: %#v", timeline.Timeline.Items)
	}

	sent, err := host.SendSubAgentInput(ctx, SendSubAgentInputRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		Message:        "one more check",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sent.ThreadID != "child" || !sent.CanSendInput {
		t.Fatalf("sent = %#v", sent)
	}
	closed, err := host.CloseSubAgent(ctx, CloseSubAgentRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != SubAgentStatusClosed || closed.CanSendInput || closed.CanClose {
		t.Fatalf("closed = %#v", closed)
	}
}

func TestHostReadsSubAgentDetailThroughPublicAPI(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	requests := 0
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests++
		request := requests
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		if request == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "child summary"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[stringArgs](
		tools.Definition{
			Name:        "read",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "file content"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Tools:        registry,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "Read",
		Message:        "read file",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	defaultDetail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolCall); got.ToolCall == nil || got.ToolCall.ArgsJSON != "" || got.ToolCall.ArgsPreview == "" || got.ToolCall.ArgsHash == "" {
		t.Fatalf("default detail should expose only safe args preview and keep hash: %#v", got)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "" || got.ToolResult.Preview != "file content" || got.ToolResult.ContentSHA256 == "" {
		t.Fatalf("default detail should expose only safe tool result preview and keep hash: %#v", got)
	}
	detail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot.ThreadID != "child" || len(detail.Events) == 0 || detail.RetainedFrom == 0 {
		t.Fatalf("detail = %#v", detail)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolCall); got.ToolCall == nil || got.ToolCall.Name != "read" || got.ToolCall.ArgsHash == "" {
		t.Fatalf("tool call detail = %#v", got)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "file content" {
		t.Fatalf("tool result detail = %#v", got)
	}
	next, err := host.ListSubAgentDetailEvents(ctx, ListSubAgentDetailEventsRequest{ParentThreadID: "parent", ChildThreadID: "child", AfterOrdinal: detail.Events[0].Ordinal, Limit: 1, IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Events) != 1 || next.Events[0].Ordinal <= detail.Events[0].Ordinal || !next.HasMore {
		t.Fatalf("next detail events = %#v", next)
	}
}

func TestSubAgentDetailCompactionSanitizesInternalMetadata(t *testing.T) {
	out := subAgentDetailCompaction(&agentharness.SubAgentDetailCompaction{
		Trigger: "manual",
		Reason:  "manual",
		Phase:   "complete",
		Metadata: map[string]string{
			"compaction_id":              "compact-1",
			"compaction_generation":      "3",
			"compaction_window_id":       "window-3",
			"compacted_through_entry_id": "entry-7",
			"summary_schema_version":     "v1",
			"safe_fact":                  "kept",
		},
	})
	if out == nil {
		t.Fatal("compaction detail was nil")
	}
	for _, key := range []string{"compaction_id", "compaction_generation", "compaction_window_id", "compacted_through_entry_id", "summary_schema_version"} {
		if _, ok := out.Metadata[key]; ok {
			t.Fatalf("metadata leaked %s: %#v", key, out.Metadata)
		}
	}
	if out.Metadata["safe_fact"] != "kept" {
		t.Fatalf("safe metadata not preserved: %#v", out.Metadata)
	}
}

func TestHostSQLiteStorePersistsSubAgentDetail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "persisted child",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{ParentThreadID: "parent", ThreadID: "child", TaskName: "Persist", Message: "work", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "unused",
			SystemPrompt: "test",
		},
		Store: reopenedStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	detail, err := reopened.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot.ForkMode != SubAgentForkNone {
		t.Fatalf("reopened fork mode = %q, want %q", detail.Snapshot.ForkMode, SubAgentForkNone)
	}
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventAssistantMessage); got.Message == nil || got.Message.Content != "persisted child" {
		t.Fatalf("reopened detail = %#v", detail.Events)
	}
}

func TestHostCloseSubAgentsStopsUnfinishedChildren(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if req.ThreadID == "completed" {
			return runtimeGatewayEvents("completed child"), nil
		}
		events := make(chan ModelEvent)
		go func() {
			defer close(events)
			<-ctx.Done()
		}()
		return events, nil
	})
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{ParentThreadID: "parent", ThreadID: "completed", TaskName: "completed", Message: "finish", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"completed"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("completed wait=%#v err=%v", waited, err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{ParentThreadID: "parent", ThreadID: "running", TaskName: "running", Message: "hang", ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}

	result, err := host.CloseSubAgents(ctx, CloseSubAgentsRequest{ParentThreadID: "parent", Reason: "parent_stop"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Closed != 1 || len(result.Snapshots) != 2 {
		t.Fatalf("close result = %#v", result)
	}
	byID := map[ThreadID]SubAgentSnapshot{}
	for _, snapshot := range result.Snapshots {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["completed"].Status != SubAgentStatusCompleted || byID["completed"].Closed {
		t.Fatalf("completed snapshot = %#v", byID["completed"])
	}
	if byID["running"].Status != SubAgentStatusClosed || !byID["running"].Closed || byID["running"].CanClose {
		t.Fatalf("running snapshot = %#v", byID["running"])
	}
}

func TestHostSQLiteStorePersistsThreadBehindOpaqueStore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "persisted",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	host, err = NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       reopened,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Messages) != 2 || snapshot.Messages[1].Content != "persisted" {
		t.Fatalf("reopened snapshot = %#v", snapshot)
	}
}

func TestHostRejectsZeroValueStore(t *testing.T) {
	_, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store: &Store{},
	})
	if err == nil || !strings.Contains(err.Error(), "runtime store must be created") {
		t.Fatalf("err = %v, want zero store rejection", err)
	}
}

func TestHostDeleteMissingThreadUsesConsistentStoreBoundary(t *testing.T) {
	ctx := context.Background()
	sqliteStore, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	for _, tc := range []struct {
		name  string
		store *Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "sqlite", store: sqliteStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, err := NewHost(HostOptions{
				Config: config.Config{
					Provider:     config.ProviderFake,
					Model:        "fake-model",
					FakeResponse: "ok",
					SystemPrompt: "test",
				},
				Store:       tc.store,
				IDGenerator: deterministicIDs(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := host.DeleteThread(ctx, "missing"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
				t.Fatalf("DeleteThread err = %v, want ErrThreadNotFound", err)
			}
		})
	}
}

func TestHostCompletePendingToolRunsFollowUpTurnThroughPublicFacade(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "completion handled",
			SystemPrompt: "test",
		},
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		ThreadID:   "thread",
		TurnID:     "turn-complete",
		RunID:      "turn-start",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolCompletionCompleted,
		Summary:    "background job finished",
		Output:     "exit 0",
		Labels: RunLabels{
			Correlation: map[string]string{"message_id": "msg-1"},
			Host:        map[string]string{"workspace_id": "ws-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "completion handled" {
		t.Fatalf("result = %#v", result)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LatestTurnID != "turn-complete" || len(snapshot.Messages) != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot.Messages[0].Role != string(session.User) || !strings.Contains(snapshot.Messages[0].Content, "<pending_tool_completion>") {
		t.Fatalf("completion message missing: %#v", snapshot.Messages)
	}
	if len(rec.events) == 0 {
		t.Fatalf("expected runtime events")
	}
}

func TestHostCompletePendingToolRejectsInvalidRequest(t *testing.T) {
	ctx := context.Background()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{}); err == nil || !strings.Contains(err.Error(), "thread id is required") {
		t.Fatalf("err = %v", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "missing", Status: PendingToolCompletionCompleted, Summary: "done", Handle: "terminal:job:123"}); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "thread", Status: PendingToolCompletionStatus("bogus"), Summary: "done", Handle: "terminal:job:123"}); err == nil || !strings.Contains(err.Error(), "invalid status") {
		t.Fatalf("err = %v", err)
	}
}

func TestHarnessHelperRunsCustomToolWithoutPublicProviderAPI(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "echo",
			Title:       "Echo",
			Description: "Return the supplied text.",
			InputSchema: tools.StrictObject(map[string]any{
				"text": tools.String("Text to echo."),
			}, []string{"text"}),
			ReadOnly:     true,
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text + "-0123456789abcdef"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	scripted := harness.NewScriptedProvider(
		harness.Step(harness.Tool("echo-1", "echo", `{"text":"from tool"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	h, err := newHarnessWithProvider(config.Config{
		Provider:     config.ProviderFake,
		Model:        "fake-model",
		SystemPrompt: "test",
	}, scripted, harnessOptions{
		Store:    NewMemoryStore(),
		Tools:    registry,
		Title:    fixedTitleGenerator{},
		NewID:    deterministicIDs(),
		Approver: allowRuntimeTools,
	})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "use the echo tool", agentharness.RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "done" {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("requests = %#v", scripted.Requests)
	}
	if !slices.ContainsFunc(scripted.Requests[0].Tools, func(def provider.ToolDefinition) bool { return def.Name == "echo" }) {
		t.Fatalf("custom tool not exposed internally: %#v", scripted.Requests[0].Tools)
	}
	if !slices.ContainsFunc(scripted.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolName == "echo" && msg.Content == "89abcdef"
	}) {
		t.Fatalf("follow-up request should contain projected tool output: %#v", scripted.Requests[1].Messages)
	}
}

func TestHostThreadDetailEventsPreserveTextAroundToolCalls(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:         "echo",
			Title:        "Echo",
			Description:  "Return the supplied text.",
			InputSchema:  runtimeEchoSchema(),
			ReadOnly:     true,
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 1024, Strategy: tools.OutputHead, PreserveFull: true},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text + " result"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		step := len(requests)
		mu.Unlock()
		events := make(chan ModelEvent, 3)
		switch step {
		case 1:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Before first tool."}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "echo", Args: `{"text":"first"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: "After first tool, before second tool."}
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-2", Name: "echo", Args: `{"text":"second"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Final answer."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        NewMemoryStore(),
		Tools:        registry,
		Approver:     allowRuntimeTools,
		Sink:         rec,
		IDGenerator:  deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "run tools"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "Before first tool.After first tool, before second tool.Final answer." {
		t.Fatalf("result = %#v", result)
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range detail.Events {
		switch ev.Kind {
		case ThreadDetailEventAssistantMessage:
			got = append(got, "assistant:"+ev.Message.Content)
		case ThreadDetailEventToolCall:
			got = append(got, "tool_call:"+ev.ToolCall.ID)
		case ThreadDetailEventToolResult:
			got = append(got, "tool_result:"+ev.ToolResult.CallID)
		}
	}
	want := []string{
		"assistant:Before first tool.",
		"tool_call:call-1",
		"tool_result:call-1",
		"assistant:After first tool, before second tool.",
		"tool_call:call-2",
		"tool_result:call-2",
		"assistant:Final answer.",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("detail order = %#v, want %#v", got, want)
	}

	var committed []string
	for _, ev := range rec.events {
		if ev.Committed == nil {
			continue
		}
		switch ev.Committed.Kind {
		case ThreadDetailEventAssistantMessage:
			committed = append(committed, "assistant:"+ev.Committed.Message.Content)
		case ThreadDetailEventToolCall:
			committed = append(committed, "tool_call:"+ev.Committed.ToolCall.ID)
		case ThreadDetailEventToolResult:
			committed = append(committed, "tool_result:"+ev.Committed.ToolResult.CallID)
		}
	}
	if !slices.Equal(committed, want) {
		t.Fatalf("committed order = %#v, want %#v", committed, want)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Committed != nil &&
			ev.Committed.Kind == ThreadDetailEventToolCall &&
			ev.Committed.ToolCall != nil &&
			ev.Committed.ToolCall.ID == "call-1" &&
			ev.Committed.ToolCall.ArgsJSON == "" &&
			ev.Committed.ToolCall.ArgsHash != ""
	}) {
		t.Fatalf("committed tool call should expose preview/hash without raw args: %#v", rec.events)
	}
	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		return ev.Committed != nil &&
			ev.Committed.Kind == ThreadDetailEventToolResult &&
			ev.Committed.ToolResult != nil &&
			ev.Committed.ToolResult.CallID == "call-1" &&
			ev.Committed.ToolResult.Content == "" &&
			ev.Committed.ToolResult.ContentSHA256 != ""
	}) {
		t.Fatalf("committed tool result should expose preview/hash without raw result: %#v", rec.events)
	}
}

func TestHostListPendingApprovalsDuringActiveRun(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			Title:       "Write note",
			InputSchema: runtimeEchoSchema(),
			Effects:     []tools.Effect{tools.EffectWrite, tools.EffectShell},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			Destructive: true,
			OpenWorld:   true,
		},
		nil,
		func(inv tools.Invocation[runtimeEchoArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Text}}, nil
		},
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		if req.Step == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	requested := make(chan struct{})
	release := make(chan struct{})
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        NewMemoryStore(),
		Tools:        registry,
		Approver: func(ctx context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
			if req.ApprovalID != "call-1" || req.HostContext["target"] != "runtime-test" || req.Labels["host.target"] != "runtime-test" {
				t.Errorf("approval request = %#v", req)
			}
			close(requested)
			select {
			case <-release:
				return tools.PermissionDecisionAllow, nil
			case <-ctx.Done():
				return tools.PermissionDecision{}, ctx.Err()
			}
		},
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{
			ThreadID: "thread",
			TurnID:   "turn-1",
			Input:    "write",
			Labels: RunLabels{
				Host: map[string]string{"target": "runtime-test"},
			},
		})
		runErr <- err
	}()
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for approval request")
	}
	pending, err := host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if pending.ThreadID != "thread" || len(pending.Approvals) != 1 {
		t.Fatalf("pending approvals = %#v", pending)
	}
	approval := pending.Approvals[0]
	if approval.ApprovalID != "call-1" ||
		approval.ToolCallID != "call-1" ||
		approval.ToolName != "write_note" ||
		approval.RunID != "turn-1" ||
		approval.TurnID != "turn-1" ||
		approval.State != "requested" ||
		approval.ArgsHash == "" ||
		approval.Labels["host.target"] != "runtime-test" ||
		approval.HostContext["target"] != "runtime-test" ||
		!approval.Destructive ||
		!approval.OpenWorld {
		t.Fatalf("approval snapshot = %#v", approval)
	}
	if got := approval.Effects; !slices.Equal(got, []string{"write", "shell"}) {
		t.Fatalf("effects = %#v", got)
	}
	if len(approval.Resources) != 1 || approval.Resources[0].Kind != "file" || approval.Resources[0].Value != "notes.md" {
		t.Fatalf("resources = %#v", approval.Resources)
	}
	close(release)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run")
	}
	pending, err = host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Approvals) != 0 {
		t.Fatalf("resolved approval should not remain pending: %#v", pending)
	}
}

func TestHostThreadDetailEventsOmitRawUnlessRequested(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "private answer",
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		Sink:        rec,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "private input"}); err != nil {
		t.Fatal(err)
	}
	preview, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	var assistantPreview ThreadDetailEvent
	for _, ev := range preview.Events {
		if ev.Kind == ThreadDetailEventAssistantMessage {
			assistantPreview = ev
			break
		}
	}
	if assistantPreview.Message == nil || assistantPreview.Message.Preview != "private answer" || assistantPreview.Message.Content != "" {
		t.Fatalf("preview assistant event = %#v", assistantPreview)
	}
	if assistantPreview.Metadata["raw_omitted"] != "true" {
		t.Fatalf("preview metadata = %#v", assistantPreview.Metadata)
	}

	raw, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var assistantRaw ThreadDetailEvent
	for _, ev := range raw.Events {
		if ev.Kind == ThreadDetailEventAssistantMessage {
			assistantRaw = ev
			break
		}
	}
	if assistantRaw.Message == nil || assistantRaw.Message.Content != "private answer" {
		t.Fatalf("raw assistant event = %#v", assistantRaw)
	}
	if _, ok := assistantRaw.Metadata["raw_omitted"]; ok {
		t.Fatalf("raw metadata marked omitted: %#v", assistantRaw.Metadata)
	}

	if !slices.ContainsFunc(rec.events, func(ev Event) bool {
		if ev.Committed == nil || ev.Committed.Kind != ThreadDetailEventAssistantMessage || ev.Committed.Message == nil {
			return false
		}
		if ev.Committed.Message.Content != "private answer" {
			return false
		}
		if ev.Metadata == nil {
			return true
		}
		_, hasDetail := ev.Metadata["detail"]
		return !hasDetail && !strings.Contains(fmt.Sprint(ev.Metadata), "private answer")
	}) {
		t.Fatalf("committed event did not expose raw only through Committed: %#v", rec.events)
	}
}

func TestHostTurnDisablesInternalControlToolsByDefault(t *testing.T) {
	spec, err := engineTurnSignalSpec(TurnSignalSpec{}, engine.CompletionNaturalStop)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Definitions) != 0 {
		t.Fatalf("definitions = %#v, want no default control tools", spec.Definitions)
	}
	if spec.Project == nil {
		t.Fatal("projector should disable engine defaults")
	}
	if signal, ok, err := spec.Project(provider.ToolCall{Name: "ask_user", Args: `{"question":"x"}`}); err != nil || ok || signal.Name != "" {
		t.Fatalf("project = %#v, %v, %v", signal, ok, err)
	}
}

func TestHostTurnExplicitSignalRequiresPublicControlSpec(t *testing.T) {
	_, err := engineTurnSignalSpec(TurnSignalSpec{}, engine.CompletionExplicitSignal)
	if err == nil || !strings.Contains(err.Error(), "signal spec is required") {
		t.Fatalf("err = %v, want required signal spec", err)
	}
}

func TestHostControlSpecUsesPublicToolContracts(t *testing.T) {
	spec, err := engineTurnSignalSpec(TurnSignalSpec{
		Definitions: []tools.ToolDefinition{{
			Name:        "host_wait",
			Title:       "Host wait",
			Description: "Wait for host input.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
			},
			Strict:      true,
			Annotations: map[string]any{"kind": "control"},
		}},
		Project: func(call tools.ToolCall) (TurnSignal, bool, error) {
			if call.Name != "host_wait" || call.Args != "{}" {
				t.Fatalf("call = %#v", call)
			}
			return TurnSignal{
				Disposition: SignalWaiting,
				Name:        call.Name,
				CallID:      call.ID,
				Payload:     map[string]any{"nested": map[string]any{"value": "original"}},
				OutputText:  "Need input",
				Labels:      map[string]string{"surface": "test"},
			}, true, nil
		},
	}, engine.CompletionNaturalStop)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Definitions) != 1 || spec.Definitions[0].Name != "host_wait" || !spec.Definitions[0].Strict {
		t.Fatalf("definitions = %#v", spec.Definitions)
	}
	signal, ok, err := spec.Project(provider.ToolCall{ID: "call-1", Name: "host_wait", Args: "{}"})
	if err != nil || !ok {
		t.Fatalf("project = %#v, %v", signal, err)
	}
	if signal.Disposition != engine.ControlWaiting || signal.OutputText != "Need input" || signal.Labels["surface"] != "test" {
		t.Fatalf("signal = %#v", signal)
	}
	signal.Payload["nested"].(map[string]any)["value"] = "mutated"
	signal.Labels["surface"] = "mutated"
	again, ok, err := spec.Project(provider.ToolCall{ID: "call-2", Name: "host_wait", Args: "{}"})
	if err != nil || !ok {
		t.Fatalf("project again = %#v, %v", again, err)
	}
	if again.Payload["nested"].(map[string]any)["value"] != "original" || again.Labels["surface"] != "test" {
		t.Fatalf("projected signal was aliased: %#v", again)
	}
}

func TestEngineHelperPreservesExplicitZeroMaxOutputTokens(t *testing.T) {
	scripted := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	e, err := newEngineWithProvider(config.Config{
		Provider:     "openai",
		Model:        "gpt-5.4",
		SystemPrompt: "test",
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens: config.DefaultContextWindowTokens,
			MaxOutputTokens:     0,
		},
		MaxOutputTokensSet:      true,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
	}, scripted, nil, nil, engineHelperOptions{RunID: "run", PromptScopeID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	req := scripted.Requests[0]
	if req.MaxOutputTokens != 0 || req.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output should remain unset: max=%d policy=%#v", req.MaxOutputTokens, req.ContextPolicy)
	}
	if req.ContextPolicy.ReservedOutputTokens != contextpolicy.DefaultReservedOutputTokens {
		t.Fatalf("reserved output = %d, want default budget", req.ContextPolicy.ReservedOutputTokens)
	}
}

func TestHostDeleteThreadUsesStoreBoundary(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "ok",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	thread, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if thread.Status != TurnStatusCompleted {
		t.Fatalf("turn result = %#v", thread)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "thread"); err != nil || len(requests) == 0 {
		t.Fatalf("prompt ledger before delete = %#v, %v", requests, err)
	}
	ref, err := store.artifacts.PutToolOutput(ctx, artifact.ToolOutputArtifact{
		ThreadID:      "thread",
		TurnID:        "turn-1",
		RunID:         "turn-1",
		PromptScopeID: "thread",
		Step:          1,
		CallID:        "call-1",
		ToolName:      "echo",
		Text:          "full output",
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts := store.artifacts.(*artifact.MemoryStore)
	if _, exists := artifacts.Ref(ref.ID); !exists {
		t.Fatalf("artifact should exist before delete")
	}
	if err := host.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThread(ctx, "thread"); err == nil {
		t.Fatalf("deleted thread should not be readable")
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "thread"); err != nil || len(requests) != 0 {
		t.Fatalf("prompt ledger after delete = %#v, %v", requests, err)
	}
	if _, exists := artifacts.Ref(ref.ID); exists {
		t.Fatalf("thread artifact should be deleted")
	}
}

func TestHostDeleteThreadCascadesEngineThreadTree(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "child done",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID: "parent",
		ThreadID:       "child",
		TaskName:       "worker",
		Message:        "work",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "child"); err != nil || len(requests) == 0 {
		t.Fatalf("child prompt ledger before delete = %#v, %v", requests, err)
	}
	ref, err := store.artifacts.PutToolOutput(ctx, artifact.ToolOutputArtifact{
		ThreadID:      "child",
		TurnID:        "child-turn-1",
		RunID:         "child-turn-1",
		PromptScopeID: "child",
		Step:          1,
		CallID:        "call-child",
		ToolName:      "echo",
		Text:          "child artifact",
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts := store.artifacts.(*artifact.MemoryStore)
	if _, exists := artifacts.Ref(ref.ID); !exists {
		t.Fatalf("child artifact should exist before delete")
	}

	if err := host.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThread(ctx, "parent"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("parent read err=%v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadThread(ctx, "child"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("child read err=%v, want ErrThreadNotFound", err)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "child"); err != nil || len(requests) != 0 {
		t.Fatalf("child prompt ledger after delete = %#v, %v", requests, err)
	}
	if _, exists := artifacts.Ref(ref.ID); exists {
		t.Fatalf("child artifact should be deleted")
	}
}

type runtimeEchoArgs struct {
	Text string `json:"text"`
}

func runtimeEchoSchema() map[string]any {
	return tools.StrictObject(map[string]any{"text": tools.String("text")}, []string{"text"})
}

func runtimeToolNames(defs []tools.ToolDefinition) []string {
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		if name := strings.TrimSpace(def.Name); name != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

type fixedTitleGenerator struct{}

func (fixedTitleGenerator) GenerateTitle(context.Context, agentharness.TitleRequest) (agentharness.TitleResult, error) {
	return agentharness.TitleResult{Title: "Runtime test title", Source: sessiontree.ThreadTitleSourceProvider}, nil
}

type runtimeEventRecorder struct {
	events []Event
}

func (r *runtimeEventRecorder) EmitEvent(ev Event) {
	r.events = append(r.events, ev)
}

func deterministicIDs() func(string) string {
	var seq int
	return func(prefix string) string {
		seq++
		return fmt.Sprintf("%s-deterministic-%d", prefix, seq)
	}
}

type stringArgs struct {
	Value string `json:"value"`
}

func firstRuntimeSubAgentDetailEvent(events []SubAgentDetailEvent, kind SubAgentDetailEventKind) SubAgentDetailEvent {
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	return SubAgentDetailEvent{}
}

func allowRuntimeTools(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
	return tools.PermissionDecisionAllow, nil
}

type runtimeModelGateway func(context.Context, ModelRequest) (<-chan ModelEvent, error)

func (f runtimeModelGateway) StreamModel(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
	return f(ctx, req)
}

func runtimeGatewayEvents(text string) <-chan ModelEvent {
	events := make(chan ModelEvent, 2)
	events <- ModelEvent{Type: ModelEventDelta, Text: text}
	events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
	close(events)
	return events
}

func findRuntimeModelRequest(requests []ModelRequest, threadID, turnID string, step int) (ModelRequest, bool) {
	for _, req := range requests {
		if string(req.ThreadID) != threadID || req.Step != step {
			continue
		}
		if turnID != "" && string(req.TurnID) != turnID {
			continue
		}
		return req, true
	}
	return ModelRequest{}, false
}
