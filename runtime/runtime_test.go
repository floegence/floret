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
	"github.com/floegence/floret/internal/provider/cache"
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
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"}); err != nil {
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
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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
	if req.Provider != "runtime-test-gateway" || req.Model != "fake-model" {
		t.Fatalf("gateway request provider/model = %#v", req)
	}
}

func TestHostModelGatewayRequiresExplicitIdentity(t *testing.T) {
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		return runtimeGatewayEvents("ok"), nil
	})
	cases := []struct {
		name     string
		config   config.Config
		identity ModelGatewayIdentity
		want     string
	}{
		{
			name:     "missing provider identity",
			config:   runtimeGatewayConfig("gateway system"),
			identity: ModelGatewayIdentity{Model: "fake-model"},
			want:     "model gateway identity provider is required",
		},
		{
			name:     "missing model identity",
			config:   runtimeGatewayConfig("gateway system"),
			identity: ModelGatewayIdentity{Provider: "runtime-test-gateway"},
			want:     "model gateway identity model is required",
		},
		{
			name: "provider transport field",
			config: config.Config{
				Provider:      config.ProviderFake,
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "model transport field",
			config: config.Config{
				Model:         "fake-model",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "base url transport field",
			config: config.Config{
				BaseURL:       "https://example.invalid",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "api key transport field",
			config: config.Config{
				APIKey:        "token",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
		{
			name: "fake response transport field",
			config: config.Config{
				FakeResponse:  "ok",
				SystemPrompt:  "gateway system",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			identity: runtimeGatewayIdentity("fake-model"),
			want:     "must not set provider transport fields",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewHost(HostOptions{
				Config:               tc.config,
				ModelGateway:         gateway,
				ModelGatewayIdentity: tc.identity,
			}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewHost err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHostForwardsTurnModelReasoningAndOpaqueProviderState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventDelta, Text: "ok " + string(req.TurnID)}
		events <- ModelEvent{
			Type:          ModelEventDone,
			Reason:        "stop",
			ResponseState: &ModelState{Kind: "responses", ID: "state-" + string(req.TurnID), Attributes: map[string]string{"cursor": string(req.TurnID), "model": req.Model}},
		}
		close(events)
		return events, nil
	})
	newHost := func(model string) Host {
		t.Helper()
		host, err := NewHost(HostOptions{
			Config:               runtimeGatewayConfig("gateway system"),
			ModelGateway:         gateway,
			ModelGatewayIdentity: runtimeGatewayIdentity(model),
			Store:                store,
			IDGenerator:          deterministicIDs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	firstHost := newHost("model-a")
	defer firstHost.Close()
	if _, err := firstHost.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := firstHost.RunTurn(ctx, RunTurnRequest{
		RunID:     "run-1",
		ThreadID:  "thread",
		TurnID:    "turn-1",
		Input:     "first",
		Reasoning: ReasoningSelection{Level: ReasoningLevelHigh},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != TurnStatusCompleted || first.ProviderState == nil || first.ProviderState.ID != "state-turn-1" {
		t.Fatalf("first turn result = %#v", first)
	}

	secondHost := newHost("model-b")
	defer secondHost.Close()
	previous := &ModelState{Kind: "responses", ID: "state-turn-1", Attributes: map[string]string{"cursor": "turn-1", "custom": "keep"}}
	second, err := secondHost.RunTurn(ctx, RunTurnRequest{
		RunID:                 "run-2",
		ThreadID:              "thread",
		TurnID:                "turn-2",
		Input:                 "second",
		Reasoning:             ReasoningSelection{Level: ReasoningLevelLow},
		PreviousProviderState: previous,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != TurnStatusCompleted || second.ProviderState == nil || second.ProviderState.ID != "state-turn-2" || second.ProviderState.Attributes["model"] != "model-b" {
		t.Fatalf("second turn result = %#v", second)
	}
	previous.Attributes["cursor"] = "mutated"
	second.ProviderState.Attributes["model"] = "mutated"

	if _, err := secondHost.RunTurn(ctx, RunTurnRequest{RunID: "run-3", ThreadID: "thread", TurnID: "turn-3", Input: "third"}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	firstReq, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing first gateway request: %#v", requests)
	}
	secondReq, ok := findRuntimeModelRequest(requests, "thread", "turn-2", 1)
	if !ok {
		t.Fatalf("missing second gateway request: %#v", requests)
	}
	thirdReq, ok := findRuntimeModelRequest(requests, "thread", "turn-3", 1)
	if !ok {
		t.Fatalf("missing third gateway request: %#v", requests)
	}
	if firstReq.Model != "model-a" || firstReq.Reasoning.Level != ReasoningLevelHigh || firstReq.PreviousState != nil {
		t.Fatalf("first gateway request = %#v", firstReq)
	}
	if secondReq.Model != "model-b" || secondReq.Reasoning.Level != ReasoningLevelLow {
		t.Fatalf("second gateway request model/reasoning = %#v", secondReq)
	}
	if secondReq.PreviousState == nil || secondReq.PreviousState.Kind != "responses" || secondReq.PreviousState.ID != "state-turn-1" || secondReq.PreviousState.Attributes["cursor"] != "turn-1" || secondReq.PreviousState.Attributes["custom"] != "keep" {
		t.Fatalf("second gateway request previous state = %#v", secondReq.PreviousState)
	}
	if thirdReq.PreviousState != nil {
		t.Fatalf("runtime should not persist provider state across turns: %#v", thirdReq.PreviousState)
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
			SystemPrompt: "gateway system",
			ContextPolicy: config.ContextPolicy{
				ContextWindowTokens: config.DefaultContextWindowTokens,
				MaxOutputTokens:     1024,
			},
		},
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Tools:                reg,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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

func TestHostEmitsActivityTimelineForToolLifecycle(t *testing.T) {
	ctx := context.Background()
	rec := &runtimeEventRecorder{}
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
				ID:   "exec-1",
				Name: "terminal.exec",
				Args: `{"text":"sleep 10s"}`,
			}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	reg := tools.NewRegistry()
	if err := reg.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal.exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(runtimeEchoArgs)
				if !ok {
					return nil, fmt.Errorf("unexpected args type %T", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    args.Text,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": args.Text},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			time.Sleep(25 * time.Millisecond)
			return tools.Result{
				Text: "ok",
				Activity: &observation.ActivityPresentation{
					Description: "Command completed",
					Payload: map[string]any{
						"exit_code":   0,
						"duration_ms": int64(25),
					},
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Tools:                reg,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: "run"})
	if err != nil {
		t.Fatal(err)
	}

	runningIndex, successIndex := -1, -1
	var runningItem, successItem observation.ActivityItem
	for index, ev := range rec.events {
		if ev.ActivityTimeline == nil || len(ev.ActivityTimeline.Items) == 0 {
			continue
		}
		item := ev.ActivityTimeline.Items[0]
		if item.ToolID != "exec-1" {
			continue
		}
		switch item.Status {
		case observation.ActivityStatusRunning:
			if runningIndex < 0 {
				runningIndex = index
				runningItem = item
			}
		case observation.ActivityStatusSuccess:
			successIndex = index
			successItem = item
		}
	}
	if runningIndex < 0 || successIndex < 0 || runningIndex >= successIndex {
		t.Fatalf("activity timeline event order running=%d success=%d events=%#v", runningIndex, successIndex, rec.events)
	}
	if runningItem.Label != "sleep 10s" || runningItem.Payload["command"] != "sleep 10s" || runningItem.EndedAtUnixMS != 0 {
		t.Fatalf("running item = %#v", runningItem)
	}
	if successItem.Label != "sleep 10s" ||
		successItem.Payload["command"] != "sleep 10s" ||
		successItem.Payload["exit_code"] != 0 ||
		successItem.EndedAtUnixMS < successItem.StartedAtUnixMS {
		t.Fatalf("success item = %#v", successItem)
	}

	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var callDetail, resultDetail *ThreadDetailEvent
	for i := range detail.Events {
		ev := &detail.Events[i]
		switch ev.Kind {
		case ThreadDetailEventToolCall:
			callDetail = ev
		case ThreadDetailEventToolResult:
			resultDetail = ev
		}
	}
	if callDetail == nil || resultDetail == nil {
		t.Fatalf("detail events missing tool call/result: %#v", detail.Events)
	}
	if resultDetail.CreatedAt.Sub(callDetail.CreatedAt) < 10*time.Millisecond {
		t.Fatalf("detail timestamps did not preserve tool runtime: call=%s result=%s", callDetail.CreatedAt, resultDetail.CreatedAt)
	}
	if callDetail.Message == nil || callDetail.Message.Activity == nil || callDetail.Message.Activity.Payload["command"] != "sleep 10s" {
		t.Fatalf("call detail activity = %#v", callDetail.Message)
	}
	if resultDetail.ActivityTimeline == nil || resultDetail.ActivityTimeline.RunID != "run-1" || resultDetail.ActivityTimeline.TurnID != "turn-1" {
		t.Fatalf("result detail activity identity = %#v", resultDetail.ActivityTimeline)
	}

	var projected *observation.ActivityTimeline
	for i := range result.Projection.Segments {
		if result.Projection.Segments[i].ActivityTimeline != nil {
			projected = result.Projection.Segments[i].ActivityTimeline
			break
		}
	}
	if projected == nil || len(projected.Items) != 1 {
		t.Fatalf("projection activity = %#v", result.Projection)
	}
	projectedItem := projected.Items[0]
	if projectedItem.Label != "sleep 10s" ||
		projectedItem.Payload["command"] != "sleep 10s" ||
		projectedItem.EndedAtUnixMS-projectedItem.StartedAtUnixMS < 10 {
		t.Fatalf("projected item = %#v", projectedItem)
	}
}

func TestHostEmitsParallelToolResultBeforeSlowSiblingAndPersistsDetailInCallOrder(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "read-1", Name: "slow_read", Args: `{"text":"wait"}`},
				{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"curl https://example.test"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:         "terminal_exec",
			InputSchema:  runtimeEchoSchema(),
			ReadOnly:     true,
			ParallelSafe: true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow},
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(runtimeEchoArgs)
				if !ok {
					return nil, fmt.Errorf("unexpected args type %T", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    args.Text,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": args.Text},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    "curl https://example.test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "curl https://example.test"},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:process:tp_fast",
					State:       tools.PendingToolResultRunning,
					Summary:     "Terminal process is running",
					Instruction: "Read it later.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:         "slow_read",
			InputSchema:  runtimeEchoSchema(),
			ReadOnly:     true,
			ParallelSafe: true,
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(ctx context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			close(slowStarted)
			select {
			case <-releaseSlow:
				return tools.Result{Text: inv.Args.Text}, nil
			case <-ctx.Done():
				return tools.Result{}, ctx.Err()
			}
		},
	)); err != nil {
		t.Fatal(err)
	}
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: "run"})
		done <- err
	}()
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for slow sibling")
	}
	if !eventuallyRuntimeToolResult(rec, "exec-1") {
		close(releaseSlow)
		t.Fatal("pending tool result event was not emitted before slow sibling finished")
	}
	close(releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run")
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	var toolResults []string
	for _, ev := range detail.Events {
		if ev.Kind == ThreadDetailEventToolResult && ev.ToolResult != nil {
			toolResults = append(toolResults, ev.ToolResult.CallID)
		}
	}
	if !slices.Equal(toolResults, []string{"read-1", "exec-1"}) {
		t.Fatalf("durable tool result order = %v, want call order", toolResults)
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
		Config:               runtimeGatewayConfig("base"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
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
	result, err := host.RunTurn(context.Background(), RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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

func TestHostRunTurnPreservesDistinctRunAndTurnIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var modelRequests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		modelRequests = append(modelRequests, req)
		events := make(chan ModelEvent, 3)
		switch req.Step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "write-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		default:
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	var permission tools.PermissionRequest
	var invocation tools.Invocation[runtimeEchoArgs]
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			PermissionFor: func(req tools.PermissionRequest) (tools.PermissionSpec, error) {
				permission = req
				return tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}}, nil
			},
		},
		nil,
		func(inv tools.Invocation[runtimeEchoArgs]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: inv.Args.Text}}, nil
		},
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			invocation = inv
			return tools.Result{Text: "wrote " + inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	var surfaceRequests []ToolSurfaceRequest
	var approval tools.ApprovalRequest
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			surfaceRequests = append(surfaceRequests, req)
			return ToolSurface{
				Tools:       registry,
				HostContext: map[string]string{"surface": "runtime-test"},
			}, nil
		},
		Approver: func(_ context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
			approval = req
			return tools.PermissionDecisionAllow, nil
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
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-parent",
		ThreadID: "thread",
		TurnID:   "turn-msg",
		Input:    "write",
		Labels: RunLabels{
			Correlation: map[string]string{"message_id": "turn-msg"},
			Host:        map[string]string{"product_run_id": "run-parent"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != "turn-msg" || result.RunID != "run-parent" || result.Status != TurnStatusCompleted {
		t.Fatalf("result = %#v", result)
	}
	if result.ActivityTimeline.RunID != "run-parent" ||
		result.ActivityTimeline.ThreadID != "thread" ||
		result.ActivityTimeline.TurnID != "turn-msg" ||
		result.ActivityTimeline.TraceID != "run-parent" {
		t.Fatalf("activity timeline identity = %#v", result.ActivityTimeline)
	}
	var turnModelRequests []ModelRequest
	for _, req := range modelRequests {
		if req.Step <= 0 {
			continue
		}
		turnModelRequests = append(turnModelRequests, req)
		if req.RunID != "run-parent" ||
			req.ThreadID != "thread" ||
			req.TurnID != "turn-msg" ||
			req.TraceID != "run-parent" ||
			req.PromptScopeID != "thread" {
			t.Fatalf("model request identity = %#v", req)
		}
	}
	if len(turnModelRequests) != 2 {
		t.Fatalf("model requests = %#v", modelRequests)
	}
	if len(surfaceRequests) == 0 {
		t.Fatalf("missing tool surface requests")
	}
	for _, req := range surfaceRequests {
		if req.RunID != "run-parent" ||
			req.ThreadID != "thread" ||
			req.TurnID != "turn-msg" ||
			req.TraceID != "run-parent" ||
			req.PromptScopeID != "thread" {
			t.Fatalf("tool surface request identity = %#v", req)
		}
	}
	if permission.RunID != "run-parent" ||
		permission.ThreadID != "thread" ||
		permission.TurnID != "turn-msg" ||
		permission.PromptScopeID != "thread" ||
		permission.Step != 1 {
		t.Fatalf("permission request identity = %#v", permission)
	}
	if invocation.RunID != "run-parent" ||
		invocation.ThreadID != "thread" ||
		invocation.TurnID != "turn-msg" ||
		invocation.PromptScopeID != "thread" ||
		invocation.Step != 1 ||
		invocation.HostContext["surface"] != "runtime-test" {
		t.Fatalf("tool invocation identity = %#v", invocation)
	}
	if approval.RunID != "run-parent" ||
		approval.ThreadID != "thread" ||
		approval.TurnID != "turn-msg" ||
		approval.PromptScopeID != "thread" ||
		approval.Step != 1 ||
		approval.HostContext["surface"] != "runtime-test" {
		t.Fatalf("approval request identity = %#v", approval)
	}
	records, err := store.prompt.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	var turnRecords []cache.ProviderRequestRecord
	for _, record := range records {
		if record.RunID == "run-parent" {
			turnRecords = append(turnRecords, record)
		}
	}
	if len(turnRecords) != 2 {
		t.Fatalf("provider request records = %#v", records)
	}
	for _, record := range turnRecords {
		if record.RunID != "run-parent" ||
			record.ThreadID != "thread" ||
			record.TurnID != "turn-msg" ||
			record.PromptScopeID != "thread" ||
			!strings.HasPrefix(record.ID, "run-parent:req:") {
			t.Fatalf("provider request record identity = %#v", record)
		}
	}
}

func TestHostRunTurnCanceledProjectionSettlesPendingActivity(t *testing.T) {
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Pending: &tools.PendingToolResult{
				Handle:      "terminal:job:123",
				State:       tools.PendingToolResultRunning,
				Summary:     "Command is running",
				Instruction: "Wait for completion before reusing this handle.",
			}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	secondRequestStarted := make(chan struct{})
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests++
		step := requests
		mu.Unlock()
		switch step {
		case 1:
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"npm test"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
			close(events)
			return events, nil
		default:
			events := make(chan ModelEvent)
			close(secondRequestStarted)
			go func() {
				<-ctx.Done()
				close(events)
			}()
			return events, nil
		}
	})

	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		Approver:             allowRuntimeTools,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(context.Background(), StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	type runOutcome struct {
		result TurnResult
		err    error
	}
	done := make(chan runOutcome, 1)
	go func() {
		result, err := host.RunTurn(runCtx, RunTurnRequest{RunID: "run-canceled", ThreadID: "thread", TurnID: "turn-canceled", Input: "run pending work"})
		done <- runOutcome{result: result, err: err}
	}()

	select {
	case <-secondRequestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second provider request did not start")
	}
	cancelRun()

	var outcome runOutcome
	select {
	case outcome = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTurn did not return after cancellation")
	}
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("RunTurn err = %v, want context.Canceled", outcome.err)
	}
	if outcome.result.Status != TurnStatusCancelled {
		t.Fatalf("result status = %s, want cancelled; result=%#v", outcome.result.Status, outcome.result)
	}
	var toolItem observation.ActivityItem
	for _, segment := range outcome.result.Projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.Status == observation.ActivityStatusRunning || item.Status == observation.ActivityStatusPending {
				t.Fatalf("projection retained non-terminal item: %#v", item)
			}
			if item.ToolID == "exec-1" {
				toolItem = item
			}
		}
	}
	if toolItem.ToolID == "" {
		t.Fatalf("projection missing pending tool item: %#v", outcome.result.Projection)
	}
	if toolItem.Status != observation.ActivityStatusCanceled || toolItem.EndedAtUnixMS == 0 {
		t.Fatalf("tool item = %#v, want canceled terminal item", toolItem)
	}
	if item := runtimeLiveProjectionItem(rec.snapshot(), "exec-1"); item.ToolID != "exec-1" ||
		item.Status != observation.ActivityStatusCanceled ||
		item.EndedAtUnixMS == 0 {
		t.Fatalf("live canceled projection item = %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := toolItem.Metadata[key]; ok {
			t.Fatalf("tool item retained pending metadata %q: %#v", key, toolItem.Metadata)
		}
	}
}

func TestSubAgentActivityTimelineProjectsStatusSummary(t *testing.T) {
	base := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	snapshots := []agentharness.SubAgentSnapshot{
		{ThreadID: "completed", TaskName: "completed task", TaskDescription: "Check the completed path.", ParentThreadID: "parent", Status: agentharness.SubAgentStatusCompleted, LastMessage: "done", CreatedAt: base.Add(-8 * time.Minute), UpdatedAt: base.Add(-7 * time.Minute)},
		{ThreadID: "running", TaskName: "running task", TaskDescription: "Keep checking the running path.", ParentThreadID: "parent", Status: agentharness.SubAgentStatusRunning, LastMessage: "working", CreatedAt: base.Add(-6 * time.Minute), UpdatedAt: base.Add(-1 * time.Minute)},
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
	foundDisplay := false
	foundDescription := false
	for _, item := range timeline.Items {
		if item.Payload["subagent_id"] == "completed" {
			foundDescription = item.Payload["task_description"] == "Check the completed path."
		}
		if item.Payload["subagent_id"] == "running" {
			foundDisplay = item.Label == "running task" &&
				item.Description == "Keep checking the running path." &&
				item.Description != "working"
		}
	}
	if !foundDescription {
		t.Fatalf("subagent task description missing from payload: %#v", timeline.Items)
	}
	if !foundDisplay {
		t.Fatalf("subagent timeline display did not use task name/description: %#v", timeline.Items)
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
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("gateway child done"), nil
	})
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}

	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Review API",
		TaskDescription: "Review the runtime API boundary.",
		Message:         "review the runtime API",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
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
	if req.RunID == "" || req.TurnID == "" {
		t.Fatalf("child execution identity should be populated: %#v", req)
	}
	if req.RunID == "child" || string(req.RunID) == string(req.TurnID) || !strings.HasPrefix(string(req.RunID), "run-") {
		t.Fatalf("child execution run id should be generated independently from child thread/turn: %#v", req)
	}
	records, err := store.prompt.ProviderRequests(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	var childExecutionRecords []cache.ProviderRequestRecord
	for _, record := range records {
		if record.Step > 0 {
			childExecutionRecords = append(childExecutionRecords, record)
		}
	}
	if len(childExecutionRecords) != 1 {
		t.Fatalf("child prompt records = %#v", records)
	}
	if childExecutionRecords[0].RunID != string(req.RunID) ||
		childExecutionRecords[0].TurnID != string(req.TurnID) ||
		childExecutionRecords[0].ThreadID != "child" ||
		!strings.HasPrefix(childExecutionRecords[0].ID, string(req.RunID)+":req:") {
		t.Fatalf("child prompt record identity = %#v, request=%#v", childExecutionRecords[0], req)
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
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Review API",
		TaskDescription: "Review the runtime API boundary.",
		Message:         "review the runtime API",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spawned.ThreadID != "child" || spawned.ParentThreadID != "parent" || spawned.Path != "/root/review_api" || spawned.TaskDescription != "Review the runtime API boundary." {
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
	if len(listed) != 1 || listed[0].TaskDescription != "Review the runtime API boundary." || listed[0].HostProfileRef != "reviewer" || listed[0].LastMessage != "child done" {
		t.Fatalf("listed = %#v", listed)
	}
	if listed[0].ForkMode != SubAgentForkNone {
		t.Fatalf("listed fork mode = %q, want %q", listed[0].ForkMode, SubAgentForkNone)
	}
	timeline, err := host.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Timeline.Items) != 1 ||
		timeline.Timeline.Items[0].Label != "review_api" ||
		timeline.Timeline.Items[0].Description != "Review the runtime API boundary." ||
		timeline.Timeline.Items[0].Payload["fork_mode"] != string(SubAgentForkNone) ||
		timeline.Timeline.Items[0].Payload["task_description"] != "Review the runtime API boundary." {
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
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(stringArgs)
				if !ok {
					t.Fatalf("args=%T, want stringArgs", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    "Read " + args.Value,
					Renderer: observation.ActivityRendererFile,
					Payload:  map[string]any{"path": args.Value},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "file content", Activity: &observation.ActivityPresentation{Description: "Read completed"}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		Tools:                registry,
		IDGenerator:          deterministicIDs(),
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
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolCall); got.ActivityTimeline != nil {
		t.Fatalf("completed tool call row should not duplicate result activity: %#v", got.ActivityTimeline)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "" || got.ToolResult.Preview != "file content" || got.ToolResult.ContentSHA256 == "" || got.ToolResult.Status != string(observation.ActivityStatusSuccess) {
		t.Fatalf("default detail should expose only safe tool result preview and keep hash: %#v", got)
	} else if got.ActivityTimeline == nil {
		t.Fatalf("default detail should expose activity without raw: %#v", got)
	} else if err := observation.ValidateActivityTimeline(*got.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid: %v", err)
	} else if len(got.ActivityTimeline.Items) != 1 || got.ActivityTimeline.Items[0].Status != observation.ActivityStatusSuccess || got.ActivityTimeline.Items[0].Description != "Read completed" {
		t.Fatalf("activity timeline = %#v", got.ActivityTimeline)
	} else if got.ActivityTimeline.RunID == "" || string(got.ActivityTimeline.RunID) == string(got.TurnID) || !strings.HasPrefix(got.ActivityTimeline.RunID, "run-") {
		t.Fatalf("activity timeline run identity = %#v event=%#v", got.ActivityTimeline, got)
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
	mu.Lock()
	requestsBeforeMaintenance := requests
	mu.Unlock()
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	listed, err := maintenance.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ThreadID != "child" || listed[0].LastMessage != "child summary" {
		t.Fatalf("maintenance list = %#v", listed)
	}
	timeline, err := maintenance.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{ParentThreadID: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Timeline.Items) != 1 || timeline.Timeline.Items[0].Payload["thread_id"] != "child" {
		t.Fatalf("maintenance timeline = %#v", timeline)
	}
	maintenanceDetail, err := maintenance.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if len(maintenanceDetail.Events) == 0 || maintenanceDetail.Snapshot.ThreadID != "child" {
		t.Fatalf("maintenance detail = %#v", maintenanceDetail)
	}
	maintenanceEvents, err := maintenance.ListSubAgentDetailEvents(ctx, ListSubAgentDetailEventsRequest{ParentThreadID: "parent", ChildThreadID: "child", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(maintenanceEvents.Events) != 1 || maintenanceEvents.NextOrdinal == 0 {
		t.Fatalf("maintenance detail events = %#v", maintenanceEvents)
	}
	mu.Lock()
	requestsAfterMaintenance := requests
	mu.Unlock()
	if requestsAfterMaintenance != requestsBeforeMaintenance {
		t.Fatalf("maintenance read triggered provider requests: before=%d after=%d", requestsBeforeMaintenance, requestsAfterMaintenance)
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

func TestHostSQLiteStorePersistsSubAgentDetailActivity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		requests++
		events := make(chan ModelEvent, 2)
		if requests == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
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
			Activity: func(inv tools.Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(stringArgs)
				if !ok {
					t.Fatalf("args=%T, want stringArgs", inv.Args)
				}
				return &observation.ActivityPresentation{Label: "Read " + args.Value, Renderer: observation.ActivityRendererFile}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "file content", Activity: &observation.ActivityPresentation{Description: "Read persisted"}}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		Store:                store,
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Tools:                registry,
		Approver:             allowRuntimeTools,
		IDGenerator:          deterministicIDs(),
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
			SystemPrompt: "test",
		},
		Store: reopenedStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	detail, err := reopened.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	result := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolResult)
	if result.ToolResult == nil || result.ToolResult.Status != string(observation.ActivityStatusSuccess) {
		t.Fatalf("reopened result detail = %#v", result)
	}
	if result.ActivityTimeline == nil {
		t.Fatalf("reopened detail missing activity timeline: %#v", detail.Events)
	}
	if err := observation.ValidateActivityTimeline(*result.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid after reopen: %v", err)
	}
	if len(result.ActivityTimeline.Items) != 1 || result.ActivityTimeline.Items[0].Description != "Read persisted" {
		t.Fatalf("reopened activity timeline = %#v", result.ActivityTimeline)
	}
	if call := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventToolCall); call.ActivityTimeline != nil {
		t.Fatalf("reopened completed call row duplicated activity: %#v", call.ActivityTimeline)
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
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		IDGenerator:          deterministicIDs(),
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
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"}); err != nil {
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
			if err := host.DeleteThread(ctx, "missing"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("DeleteThread err = %v, want ErrThreadNotFound", err)
			}
		})
	}
}

func TestHostPublicNotFoundErrors(t *testing.T) {
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

	if _, err := host.ReadThread(ctx, "missing"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "missing", TurnID: "turn-1", RunID: "run-1"}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadTurnProjection err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		ThreadID: "missing",
		RunID:    "pending-run",
		Status:   PendingToolCompletionCompleted,
		Summary:  "done",
		Handle:   "terminal:job:123",
	}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("CompletePendingTool err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "missing",
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementCompleted,
		Summary:    "done",
	}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("SettlePendingTool err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "missing-child",
	}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("ReadSubAgentDetail err = %v, want ErrSubAgentNotFound", err)
	}
	if _, err := host.ListSubAgentDetailEvents(ctx, ListSubAgentDetailEventsRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "missing-child",
	}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("ListSubAgentDetailEvents err = %v, want ErrSubAgentNotFound", err)
	}
}

func TestHostReadTurnProjectionFromDurableDetail(t *testing.T) {
	ctx := context.Background()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "projected answer",
			SystemPrompt: "test",
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
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if projection.ThreadID != "thread" || projection.TurnID != "turn-1" || projection.RunID != "run-1" {
		t.Fatalf("projection identity = %#v", projection)
	}
	if runtimeProjectionAssistantText(projection) != "projected answer" {
		t.Fatalf("projection text = %q", runtimeProjectionAssistantText(projection))
	}
	if runtimeProjectionAssistantText(result.Projection) != runtimeProjectionAssistantText(projection) {
		t.Fatalf("read projection differs from turn result: result=%#v read=%#v", result.Projection, projection)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "missing-turn", RunID: "run-missing"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("ReadTurnProjection err = %v, want ErrTurnNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "wrong-run"}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("ReadTurnProjection wrong run err = %v, want ErrRunNotFound", err)
	}
	if _, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1"}); err == nil || !strings.Contains(err.Error(), "run id is required") {
		t.Fatalf("ReadTurnProjection without run id err = %v, want required run id", err)
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
	if result.RunID != "turn-start" || result.ID != "turn-complete" {
		t.Fatalf("completion execution identity = %#v", result)
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
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "thread", Status: PendingToolCompletionCompleted, Summary: "done", Handle: "terminal:job:123"}); err == nil || !strings.Contains(err.Error(), "run id is required") {
		t.Fatalf("err = %v", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "missing", RunID: "pending-run", Status: PendingToolCompletionCompleted, Summary: "done", Handle: "terminal:job:123"}); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{ThreadID: "thread", RunID: "pending-run", Status: PendingToolCompletionStatus("bogus"), Summary: "done", Handle: "terminal:job:123"}); err == nil || !strings.Contains(err.Error(), "invalid status") {
		t.Fatalf("err = %v", err)
	}
}

func TestHostSettlePendingToolAppendsDetailWithoutProviderTurn(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    "npm test",
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": "npm test"},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:123",
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Wait for completion.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	longAssistantAfterPending := "command started " + strings.Repeat("after pending settlement keeps full assistant text ", 12) + "final settlement sentence."
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.Contains(string(req.RunID), "thread-title") {
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "Pending command"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		}
		mu.Lock()
		requests++
		step := requests
		mu.Unlock()
		events := make(chan ModelEvent, 2)
		switch step {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "exec-1", Name: "terminal_exec", Args: `{"text":"npm test"}`}}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: longAssistantAfterPending}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		default:
			t.Fatalf("unexpected provider request after settlement: %#v", req)
		}
		close(events)
		return events, nil
	})

	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		Tools:                registry,
		Approver:             allowRuntimeTools,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	run, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: "run pending command"})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != TurnStatusCompleted || run.Output != longAssistantAfterPending {
		t.Fatalf("run = %#v", run)
	}
	if item := runtimeProjectionToolItem(run.Projection, "exec-1"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("pending item should remain running before explicit settlement: %#v", item)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	if _, err := maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "other-run",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementCompleted,
		Summary:    "wrong run",
		Output:     "exit 0",
	}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("wrong-run settlement err = %v, want ErrRunNotFound", err)
	}

	settled, err := maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementCompleted,
		Summary:    "command completed",
		Output:     "exit 0",
		Activity:   &observation.ActivityPresentation{Label: "command completed", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if settled.Event.Kind != ThreadDetailEventToolResult ||
		settled.Event.ToolResult == nil ||
		settled.Event.ToolResult.Status != string(observation.ActivityStatusSuccess) ||
		settled.Event.ToolResult.Content != "exit 0" {
		t.Fatalf("settlement event = %#v", settled.Event)
	}
	item := runtimeProjectionToolItem(settled.Projection, "exec-1")
	if item.Status != observation.ActivityStatusSuccess || item.Label != "command completed" || item.Payload["exit_code"] != 0 {
		t.Fatalf("settled projection item = %#v", item)
	}
	again, err := maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementCompleted,
		Summary:    "command completed",
		Output:     "exit 0",
		Activity:   &observation.ActivityPresentation{Label: "command completed", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.Event.ID != settled.Event.ID {
		t.Fatalf("idempotent public settlement returned a different event: first=%#v again=%#v", settled.Event, again.Event)
	}
	_, err = maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementFailed,
		Summary:    "command failed",
	})
	if !errors.Is(err, agentharness.ErrPendingToolSettlementConflict) {
		t.Fatalf("conflicting public settlement err = %v, want conflict", err)
	}
	if got := runtimeProjectionAssistantText(settled.Projection); got != longAssistantAfterPending {
		t.Fatalf("settled projection assistant text length=%d, want full %d\ntext=%q", len([]rune(got)), len([]rune(longAssistantAfterPending)), got)
	}
	readProjection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "other-run"}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("ReadTurnProjection wrong run err = %v, want ErrRunNotFound", err)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-1"); item.Status != observation.ActivityStatusSuccess || item.Label != "command completed" {
		t.Fatalf("read projection item = %#v", item)
	}
	for _, key := range []string{"pending_tool_result", "pending_handle", "pending_state"} {
		if _, ok := item.Metadata[key]; ok {
			t.Fatalf("settled projection retained %q metadata: %#v", key, item.Metadata)
		}
	}
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if gotRequests != 2 {
		t.Fatalf("provider requests = %d, want only original run requests", gotRequests)
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
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		Approver:             allowRuntimeTools,
		Sink:                 rec,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "run tools"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "Before first tool.After first tool, before second tool.Final answer." {
		t.Fatalf("result = %#v", result)
	}
	if got := runtimeProjectionSegmentKinds(result.Projection.Segments); !slices.Equal(got, []ThreadTurnProjectionSegmentKind{
		ThreadTurnProjectionSegmentAssistantText,
		ThreadTurnProjectionSegmentActivityTimeline,
		ThreadTurnProjectionSegmentAssistantText,
		ThreadTurnProjectionSegmentActivityTimeline,
		ThreadTurnProjectionSegmentAssistantText,
	}) {
		t.Fatalf("projection segments = %#v", result.Projection.Segments)
	}
	if result.Projection.Segments[1].ActivityTimeline == nil ||
		len(result.Projection.Segments[1].ActivityTimeline.Items) != 1 ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].ToolID != "call-1" {
		t.Fatalf("first projection activity = %#v", result.Projection.Segments[1])
	}
	readProjection, err := host.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := runtimeProjectionSegmentKinds(readProjection.Segments); !slices.Equal(got, runtimeProjectionSegmentKinds(result.Projection.Segments)) {
		t.Fatalf("read projection segments = %#v, want %#v", got, runtimeProjectionSegmentKinds(result.Projection.Segments))
	}
	if item := runtimeProjectionToolItem(readProjection, "call-2"); item.ToolID != "call-2" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("read projection call-2 item = %#v", item)
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
	for _, ev := range detail.Events {
		if ev.Kind == ThreadDetailEventToolCall && ev.ToolCall != nil && ev.ToolCall.ID == "call-1" && ev.ActivityTimeline != nil {
			t.Fatalf("thread detail completed call row should not duplicate result activity: %#v", ev.ActivityTimeline)
		}
		if ev.Kind != ThreadDetailEventToolResult || ev.ToolResult == nil || ev.ToolResult.CallID != "call-1" {
			continue
		}
		if ev.ToolResult.Status != string(observation.ActivityStatusSuccess) {
			t.Fatalf("thread detail tool result status = %#v", ev.ToolResult)
		}
		if ev.ActivityTimeline == nil {
			t.Fatalf("thread detail tool result should include activity timeline: %#v", ev)
		}
		if err := observation.ValidateActivityTimeline(*ev.ActivityTimeline); err != nil {
			t.Fatalf("thread detail activity timeline invalid: %v", err)
		}
	}

	var committed []string
	committedEvents := 0
	var liveProjections []ThreadTurnProjection
	for _, ev := range rec.events {
		if ev.Committed == nil {
			continue
		}
		committedEvents++
		if ev.Projection == nil {
			t.Fatalf("committed event missing live projection: %#v", ev)
		}
		liveProjections = append(liveProjections, *ev.Projection)
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
	if len(liveProjections) != committedEvents {
		t.Fatalf("live projections=%d, want one per committed event %d", len(liveProjections), committedEvents)
	}
	finalLiveProjection := liveProjections[len(liveProjections)-1]
	if got := runtimeProjectionSegmentKinds(finalLiveProjection.Segments); !slices.Equal(got, runtimeProjectionSegmentKinds(result.Projection.Segments)) {
		t.Fatalf("final live projection segments = %#v, want %#v", got, runtimeProjectionSegmentKinds(result.Projection.Segments))
	}
	if runtimeProjectionAssistantText(finalLiveProjection) != result.Output {
		t.Fatalf("final live projection text = %q, want %q", runtimeProjectionAssistantText(finalLiveProjection), result.Output)
	}
	if item := runtimeProjectionToolItem(finalLiveProjection, "call-1"); item.ToolID != "call-1" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("final live projection call-1 item = %#v", item)
	}
	if item := runtimeProjectionToolItem(finalLiveProjection, "call-2"); item.ToolID != "call-2" || item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("final live projection call-2 item = %#v", item)
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
	rec := &runtimeEventRecorder{}
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
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
	runErr := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{
			RunID:    "turn-1",
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
	if item := runtimeLiveProjectionItem(rec.snapshot(), "call-1"); item.ToolID != "call-1" ||
		item.Status != observation.ActivityStatusWaiting ||
		!item.RequiresApproval ||
		item.ApprovalState != "requested" {
		t.Fatalf("live approval projection item = %#v", item)
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
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "private input"}); err != nil {
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

func TestHostRunTurnProjectionUsesRawAssistantContent(t *testing.T) {
	ctx := context.Background()
	fullAnswer := "Here are browser desktop options:\n\n" +
		"### 1. **HeyPuter/puter**\n" +
		"### 2. **linuxserver/docker-webtop**\n" +
		"The Webtop image can be based on Ubuntu/Alpine/Arch/Fedora and still stay readable in the final answer.\n\n" +
		strings.Repeat("This sentence keeps the answer longer than the preview budget. ", 12) +
		"Final sentence that must survive the canonical turn projection."
	if len([]rune(fullAnswer)) <= 500 {
		t.Fatalf("test fixture must exceed preview budget, got %d runes", len([]rune(fullAnswer)))
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: fullAnswer,
			SystemPrompt: "test",
		},
		Store:       NewMemoryStore(),
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "find options"})
	if err != nil {
		t.Fatal(err)
	}
	projected := runtimeProjectionAssistantText(result.Projection)
	if projected != fullAnswer {
		t.Fatalf("projection assistant text length=%d, want full %d\ntext=%q", len([]rune(projected)), len([]rune(fullAnswer)), projected)
	}
	if strings.Contains(projected, "HeyPuterputer") ||
		strings.Contains(projected, "linuxserverdocker-webtop") ||
		strings.Contains(projected, "UbuntuFedora") {
		t.Fatalf("projection assistant text was path-redacted: %q", projected)
	}

	preview, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	assistantPreview := firstRuntimeThreadDetailEvent(preview.Events, ThreadDetailEventAssistantMessage)
	if assistantPreview.Message == nil || assistantPreview.Message.Content != "" || assistantPreview.Metadata["raw_omitted"] != "true" {
		t.Fatalf("preview detail event = %#v", assistantPreview)
	}
	if len([]rune(assistantPreview.Message.Preview)) >= len([]rune(fullAnswer)) {
		t.Fatalf("preview detail should remain bounded: %d >= %d", len([]rune(assistantPreview.Message.Preview)), len([]rune(fullAnswer)))
	}

	raw, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	assistantRaw := firstRuntimeThreadDetailEvent(raw.Events, ThreadDetailEventAssistantMessage)
	if assistantRaw.Message == nil || assistantRaw.Message.Content != fullAnswer {
		t.Fatalf("raw detail event = %#v", assistantRaw)
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

func TestHostProjectionTreatsCoreControlSignalAsControl(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 2)
		events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{
			ID:   "done",
			Name: "task_complete",
			Args: `{"result":"all done"}`,
		}}}
		events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		close(events)
		return events, nil
	})
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:      "run-1",
		ThreadID:   "thread",
		TurnID:     "turn-1",
		Input:      "finish",
		Completion: TurnCompletionExplicitSignal,
		Signals: TurnSignalSpec{
			Definitions: CoreControlDefinitions(true),
			Project:     ProjectCoreControlSignal,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Signal == nil || result.Signal.Name != "task_complete" {
		t.Fatalf("result = %#v", result)
	}
	if got := runtimeProjectionSegmentKinds(result.Projection.Segments); !slices.Equal(got, []ThreadTurnProjectionSegmentKind{
		ThreadTurnProjectionSegmentControlSignal,
		ThreadTurnProjectionSegmentActivityTimeline,
	}) {
		t.Fatalf("projection segments = %#v", result.Projection.Segments)
	}
	if result.Projection.Segments[1].ActivityTimeline == nil ||
		len(result.Projection.Segments[1].ActivityTimeline.Items) != 1 ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].Kind != observation.ActivityKindControl ||
		result.Projection.Segments[1].ActivityTimeline.Items[0].ToolName != "task_complete" {
		t.Fatalf("control activity = %#v", result.Projection.Segments[1])
	}
	detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	call := firstRuntimeThreadDetailEvent(detail.Events, ThreadDetailEventToolCall)
	if call.Message == nil || call.Message.Kind != "control_signal" {
		t.Fatalf("control call detail = %#v", call)
	}
	if call.ActivityTimeline == nil ||
		len(call.ActivityTimeline.Items) != 1 ||
		call.ActivityTimeline.Items[0].Kind != observation.ActivityKindControl {
		t.Fatalf("control detail activity = %#v", call.ActivityTimeline)
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
	thread, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-1", ThreadID: "thread", TurnID: "turn-1", Input: "hello"})
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
	if _, err := host.ReadThread(ctx, "parent"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("parent read err=%v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadThread(ctx, "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("child read err=%v, want ErrThreadNotFound", err)
	}
	if requests, err := store.prompt.ProviderRequests(ctx, "child"); err != nil || len(requests) != 0 {
		t.Fatalf("child prompt ledger after delete = %#v, %v", requests, err)
	}
	if _, exists := artifacts.Ref(ref.ID); exists {
		t.Fatalf("child artifact should be deleted")
	}
}

func TestThreadMaintenanceHostDeletesThreadTreeWithoutProviderConfig(t *testing.T) {
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
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	if _, ok := maintenance.(Host); ok {
		t.Fatalf("ThreadMaintenanceHost must not expose provider execution methods")
	}
	if summary, err := maintenance.EnsureThread(ctx, EnsureThreadRequest{ThreadID: "parent"}); err != nil || summary.ID != "parent" {
		t.Fatalf("EnsureThread summary=%#v err=%v", summary, err)
	}
	if closed, err := maintenance.CloseSubAgents(ctx, CloseSubAgentsRequest{ParentThreadID: "parent", Reason: "cleanup"}); err != nil || len(closed.Snapshots) != 1 {
		t.Fatalf("CloseSubAgents result=%#v err=%v", closed, err)
	}
	if err := maintenance.DeleteThread(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadThread(ctx, "parent"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread(parent) err = %v, want ErrThreadNotFound", err)
	}
	if _, err := host.ReadThread(ctx, "child"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("ReadThread(child) err = %v, want ErrThreadNotFound", err)
	}
}

func TestThreadMaintenanceHostRequiresStore(t *testing.T) {
	if _, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{}); err == nil || !strings.Contains(err.Error(), "store is required") {
		t.Fatalf("NewThreadMaintenanceHost err = %v, want store required", err)
	}
}

type runtimeEchoArgs struct {
	Text string `json:"text"`
}

func runtimeGatewayConfig(systemPrompt string) config.Config {
	return config.Config{
		SystemPrompt: strings.TrimSpace(systemPrompt),
		ContextPolicy: config.ContextPolicy{
			ContextWindowTokens: config.DefaultContextWindowTokens,
		},
	}
}

func runtimeGatewayIdentity(model string) ModelGatewayIdentity {
	return ModelGatewayIdentity{Provider: "runtime-test-gateway", Model: strings.TrimSpace(model)}
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
	mu     sync.Mutex
	events []Event
}

func (r *runtimeEventRecorder) EmitEvent(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *runtimeEventRecorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
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

func firstRuntimeThreadDetailEvent(events []ThreadDetailEvent, kind ThreadDetailEventKind) ThreadDetailEvent {
	for _, event := range events {
		if event.Kind == kind {
			return event
		}
	}
	return ThreadDetailEvent{}
}

func runtimeProjectionSegmentKinds(segments []ThreadTurnProjectionSegment) []ThreadTurnProjectionSegmentKind {
	out := make([]ThreadTurnProjectionSegmentKind, 0, len(segments))
	for _, segment := range segments {
		out = append(out, segment.Kind)
	}
	return out
}

func runtimeProjectionAssistantText(projection ThreadTurnProjection) string {
	var out strings.Builder
	for _, segment := range projection.Segments {
		if segment.Kind == ThreadTurnProjectionSegmentAssistantText {
			out.WriteString(segment.Text)
		}
	}
	return out.String()
}

func runtimeProjectionToolItem(projection ThreadTurnProjection, toolID string) observation.ActivityItem {
	for _, segment := range projection.Segments {
		if segment.ActivityTimeline == nil {
			continue
		}
		for _, item := range segment.ActivityTimeline.Items {
			if item.ToolID == toolID {
				return item
			}
		}
	}
	return observation.ActivityItem{}
}

func runtimeLiveProjectionItem(events []Event, toolID string) observation.ActivityItem {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Projection == nil {
			continue
		}
		if item := runtimeProjectionToolItem(*events[i].Projection, toolID); item.ToolID != "" {
			return item
		}
	}
	return observation.ActivityItem{}
}

func eventuallyRuntimeToolResult(rec *runtimeEventRecorder, toolID string) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range rec.snapshot() {
			if ev.Type != "tool_result" || ev.ToolID != toolID {
				continue
			}
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func eventuallyThreadDetailToolResult(ctx context.Context, t *testing.T, host Host, threadID string, toolID string, status observation.ActivityStatus) bool {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		detail, err := host.ListThreadDetailEvents(ctx, ListThreadDetailEventsRequest{ThreadID: ThreadID(threadID), IncludeRaw: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range detail.Events {
			if event.Kind != ThreadDetailEventToolResult || event.ToolResult == nil {
				continue
			}
			if event.ToolResult.CallID == toolID && event.ToolResult.Status == string(status) {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
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
