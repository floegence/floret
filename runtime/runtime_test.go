package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/floegence/floret/internal/storage"
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

func TestHostRunTurnReportsTerminalProjectionUnavailableWithoutDiscardingResult(t *testing.T) {
	ctx := context.Background()
	repo := &terminalProjectionFailureRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	store := NewMemoryStore()
	store.repo = repo
	recorder := &terminalProjectionFailureRecorder{repo: repo}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "configured",
			SystemPrompt: "test",
		},
		Store:       store,
		Sink:        recorder,
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
		t.Fatalf("RunTurn err = %v, want nil", err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "configured" {
		t.Fatalf("result terminal facts = %#v", result)
	}
	if result.ID != "turn-1" || result.RunID != "run-1" || result.Metrics.LLMRequests != 1 {
		t.Fatalf("result execution facts = %#v", result)
	}
	if result.ProjectionStatus != TurnProjectionStatusUnavailable || result.Projection != nil || strings.TrimSpace(result.ProjectionError) == "" {
		t.Fatalf("projection outcome = %#v, want unavailable diagnostic", result)
	}
	if err := result.ValidateProjection(); err != nil {
		t.Fatalf("projection outcome validation: %v", err)
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

func TestHostRunTurnRecoversInterruptedActiveLease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "continued",
			SystemPrompt: "test",
		},
		Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.EnsureThread(ctx, EnsureThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-interrupted", sessiontree.TurnStarted, map[string]string{"run_id": "run-interrupted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-interrupted", session.Message{Role: session.User, Content: "start delegated work"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "thread", "turn-interrupted", session.Message{
		Role:       session.Assistant,
		Content:    "tool_call",
		ToolCallID: "call-wait",
		ToolName:   "subagents",
		ToolArgs:   `{"action":"wait","ids":["child"]}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "thread", "turn-interrupted", sessiontree.TurnSavePoint, map[string]string{"reason": "run_result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendFailure(ctx, store.repo, "thread", "turn-interrupted", context.Canceled.Error()); err != nil {
		t.Fatal(err)
	}
	leaseRepo := store.repo.(sessiontree.TurnLeaseRepo)
	if err := leaseRepo.AcquireTurnLease(ctx, sessiontree.TurnLease{
		ThreadID:  "thread",
		TurnID:    "turn-interrupted",
		OwnerID:   "dead-owner",
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	result, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-continue", ThreadID: "thread", TurnID: "turn-continue", Input: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.Output != "continued" {
		t.Fatalf("result = %#v", result)
	}
	if _, ok, err := leaseRepo.ActiveTurnLease(ctx, "thread"); err != nil || ok {
		t.Fatalf("active lease should be released after runtime recovery, ok=%v err=%v", ok, err)
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LatestTurnID != "turn-continue" || snapshot.Status != ThreadStatusCompleted {
		t.Fatalf("snapshot = %#v", snapshot)
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

func TestHostRunTurnEnforcesCumulativeInputTokenLimit(t *testing.T) {
	ctx := context.Background()
	gateway := runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		events <- ModelEvent{Type: ModelEventUsage, Usage: ProviderUsage{InputTokens: 101, OutputTokens: 500, TotalTokens: 601, Available: true}}
		events <- ModelEvent{Type: ModelEventDelta, Text: "over budget"}
		events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
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

	result, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    "hello",
		Limits:   TurnLimits{MaxInputTokens: 100},
	})
	if err == nil || !strings.Contains(err.Error(), "input token budget exceeded") {
		t.Fatalf("RunTurn err = %v, want input token budget exceeded", err)
	}
	if result.Status != TurnStatusFailed || result.Metrics.ProviderUsage.InputTokens != 101 {
		t.Fatalf("result = %#v", result)
	}
}

func TestHostRunTurnProjectsSupplementalContextOnlyIntoCurrentProviderRequest(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("ok " + string(req.TurnID)), nil
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
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    "what is this process",
		SupplementalContext: []TurnSupplementalContextItem{{
			Kind:      "process_snapshot",
			Title:     "Codex (Service)",
			Text:      "Selected from the process monitor.",
			Sensitive: true,
			Metadata: map[string]string{
				"captured_at": "2026-07-10T10:00:00Z",
				"cpu":         "0.0",
				"memory":      "549 MiB",
				"name":        "Codex (Service)",
				"pid":         "12264",
				"platform":    "darwin",
				"username":    "tangjianyin",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != TurnStatusCompleted {
		t.Fatalf("first result = %#v", first)
	}
	second, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-2", ThreadID: "thread", TurnID: "turn-2", Input: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != TurnStatusCompleted {
		t.Fatalf("second result = %#v", second)
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
	inputIndex := -1
	supplementalIndex := -1
	supplementalContent := ""
	inputCount := 0
	for i, msg := range firstReq.Messages {
		if msg.Role == "user" && msg.Content == "what is this process" {
			inputIndex = i
			inputCount++
		}
		if strings.Contains(msg.Content, "Host-provided supplemental context") {
			supplementalIndex = i
			supplementalContent = msg.Content
			if msg.Role != "user" {
				t.Fatalf("supplemental message role = %q, want user", msg.Role)
			}
		}
	}
	if inputCount != 1 || inputIndex < 0 {
		t.Fatalf("user input was not preserved as a distinct message: %#v", firstReq.Messages)
	}
	if supplementalIndex <= inputIndex {
		t.Fatalf("supplemental context should follow the current user input: input=%d supplemental=%d messages=%#v", inputIndex, supplementalIndex, firstReq.Messages)
	}
	for _, want := range []string{"process_snapshot", "Codex (Service)", "pid: 12264", "username: tangjianyin", "sensitive: true", "Selected from the process monitor."} {
		if !strings.Contains(supplementalContent, want) {
			t.Fatalf("supplemental context missing %q: %s", want, supplementalContent)
		}
	}
	for _, msg := range secondReq.Messages {
		if strings.Contains(msg.Content, "Host-provided supplemental context") || strings.Contains(msg.Content, "12264") {
			t.Fatalf("supplemental context leaked into follow-up request: %#v", secondReq.Messages)
		}
	}
	snapshot, err := host.ReadThread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range snapshot.Messages {
		if strings.Contains(msg.Content, "Host-provided supplemental context") || strings.Contains(msg.Content, "12264") {
			t.Fatalf("supplemental context leaked into durable thread snapshot: %#v", snapshot.Messages)
		}
	}
}

func TestHostRunTurnIgnoresEmptySupplementalContext(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		return runtimeGatewayEvents("ok"), nil
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
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		RunID:    "run-1",
		ThreadID: "thread",
		TurnID:   "turn-1",
		Input:    "hello",
		SupplementalContext: []TurnSupplementalContextItem{
			{},
			{Kind: " ", Title: " ", Text: " ", Metadata: map[string]string{" ": " "}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	req, ok := findRuntimeModelRequest(requests, "thread", "turn-1", 1)
	if !ok {
		t.Fatalf("missing gateway request: %#v", requests)
	}
	for _, msg := range req.Messages {
		if strings.Contains(msg.Content, "Host-provided supplemental context") {
			t.Fatalf("empty supplemental context changed request messages: %#v", req.Messages)
		}
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
		strings.TrimSpace(string(status.Status)) == "" {
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
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			ReadOnly:    true,
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
			Name:        "slow_read",
			InputSchema: runtimeEchoSchema(),
			ReadOnly:    true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
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
		func(_ context.Context, inv tools.Invocation[stringArgs]) (tools.Result, error) {
			inv.UpdateActivity(tools.ActivityUpdate{
				Activity: &observation.ActivityPresentation{
					Label:    "Reading README.md",
					Renderer: observation.ActivityRendererTerminal,
					Payload: map[string]any{
						"latest_output": "reading\n",
						"status":        "running",
					},
				},
			})
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
	if defaultDetail.Context.Model.Provider != "runtime-test-gateway" || defaultDetail.Context.Model.Model != "fake-model" {
		t.Fatalf("detail context model = %#v", defaultDetail.Context.Model)
	}
	if defaultDetail.Context.Policy.ContextWindowTokens != config.DefaultContextWindowTokens || defaultDetail.Context.Policy.ReservedOutputTokens != config.DefaultReservedOutputTokens {
		t.Fatalf("detail context policy = %#v", defaultDetail.Context.Policy)
	}
	if defaultDetail.Context.Usage == nil || defaultDetail.Context.Usage.ContextPressure.ContextWindowTokens != config.DefaultContextWindowTokens || defaultDetail.Context.Usage.Provider != "runtime-test-gateway" {
		t.Fatalf("detail context usage = %#v", defaultDetail.Context.Usage)
	}
	contextJSON, err := json.Marshal(defaultDetail.Context)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"recent_tail_tokens", "recent_user_tokens", "compacted_context_target_tokens", "compaction_window_id"} {
		if strings.Contains(string(contextJSON), forbidden) {
			t.Fatalf("detail context leaked internal field %q: %s", forbidden, string(contextJSON))
		}
	}
	for _, ev := range defaultDetail.Events {
		switch ev.Type {
		case "subagent_context_policy", "subagent_context_status", "subagent_context_compaction":
			t.Fatalf("hidden context entry leaked into detail events: %#v", ev)
		}
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolCall); got.ToolCall == nil || got.ToolCall.ArgsJSON != "" || got.ToolCall.ArgsPreview == "" || got.ToolCall.ArgsHash == "" {
		t.Fatalf("default detail should expose only safe args preview and keep hash: %#v", got)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolCall); got.ActivityTimeline != nil {
		t.Fatalf("completed tool call row should not duplicate result activity: %#v", got.ActivityTimeline)
	}
	if got := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolResult); got.ToolResult == nil || got.ToolResult.Content != "" || got.ToolResult.Preview != "file content" || got.ToolResult.ContentSHA256 == "" || got.ToolResult.Status != string(observation.ActivityStatusSuccess) {
		t.Fatalf("default detail should expose only safe tool result preview and keep hash: %#v", got)
	} else if got.ActivityTimeline != nil {
		t.Fatalf("tool result row should not expose stale per-event activity: %#v", got.ActivityTimeline)
	}
	if activity := firstRuntimeSubAgentDetailEvent(defaultDetail.Events, SubAgentDetailEventToolActivity); activity.ActivityTimeline != nil {
		t.Fatalf("tool activity row should not expose stale per-event activity: %#v", activity.ActivityTimeline)
	}
	if err := observation.ValidateActivityTimeline(defaultDetail.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid: %v", err)
	}
	readItem := runtimeSubAgentActivityItem(defaultDetail.ActivityTimeline, "read-1")
	if readItem.Status != observation.ActivityStatusSuccess || readItem.Description != "Read completed" || readItem.Payload["latest_output"] != "reading" {
		t.Fatalf("canonical activity item did not merge running update into success result: %#v", readItem)
	}
	if defaultDetail.ActivityTimeline.RunID == "" || !strings.HasPrefix(defaultDetail.ActivityTimeline.RunID, "run-") {
		t.Fatalf("activity timeline run identity = %#v item=%#v", defaultDetail.ActivityTimeline, readItem)
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
	if item := runtimeSubAgentActivityItem(next.ActivityTimeline, "read-1"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("paged detail should still expose canonical activity timeline: %#v", next.ActivityTimeline)
	}
	if next.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || next.Context.Usage == nil {
		t.Fatalf("paged detail should carry canonical context snapshot: %#v", next.Context)
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
	if maintenanceDetail.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || maintenanceDetail.Context.Usage == nil {
		t.Fatalf("maintenance detail context = %#v want %#v", maintenanceDetail.Context, defaultDetail.Context)
	}
	maintenanceEvents, err := maintenance.ListSubAgentDetailEvents(ctx, ListSubAgentDetailEventsRequest{ParentThreadID: "parent", ChildThreadID: "child", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(maintenanceEvents.Events) != 1 || maintenanceEvents.NextOrdinal == 0 {
		t.Fatalf("maintenance detail events = %#v", maintenanceEvents)
	}
	if maintenanceEvents.Context.Policy.ContextWindowTokens != defaultDetail.Context.Policy.ContextWindowTokens || maintenanceEvents.Context.Usage == nil {
		t.Fatalf("maintenance detail events context = %#v", maintenanceEvents.Context)
	}
	mu.Lock()
	requestsAfterMaintenance := requests
	mu.Unlock()
	if requestsAfterMaintenance != requestsBeforeMaintenance {
		t.Fatalf("maintenance read triggered provider requests: before=%d after=%d", requestsBeforeMaintenance, requestsAfterMaintenance)
	}
}

func TestHostReadsSubAgentDetailRawMessageContentContract(t *testing.T) {
	ctx := context.Background()
	longMission := "inspect the complete delegated output " + strings.Repeat("mission context ", 80) + "mission tail"
	longAnswer := "complete subagent report " + strings.Repeat("evidence section ", 80) + "https://example.test/full-final-output"
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: longAnswer,
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
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
		TaskName:       "Raw Contract",
		Message:        longMission,
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}

	previewOnly, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	inputPreview := firstRuntimeSubAgentDetailEvent(previewOnly.Events, SubAgentDetailEventInput)
	if inputPreview.Message == nil || inputPreview.Message.Content != "" || inputPreview.Message.Preview == "" || !strings.HasSuffix(inputPreview.Message.Preview, "...") {
		t.Fatalf("preview input should omit raw content and keep bounded preview: %#v", inputPreview)
	}
	if strings.Contains(inputPreview.Message.Preview, "mission tail") {
		t.Fatalf("preview input exposed tail raw content: %q", inputPreview.Message.Preview)
	}
	assistantPreview := firstRuntimeSubAgentDetailEvent(previewOnly.Events, SubAgentDetailEventAssistantMessage)
	if assistantPreview.Message == nil || assistantPreview.Message.Content != "" || assistantPreview.Message.Preview == "" || !strings.HasSuffix(assistantPreview.Message.Preview, "...") {
		t.Fatalf("preview assistant should omit raw content and keep bounded preview: %#v", assistantPreview)
	}
	if strings.Contains(assistantPreview.Message.Preview, "full-final-output") {
		t.Fatalf("preview assistant exposed tail raw content: %q", assistantPreview.Message.Preview)
	}

	raw, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	inputRaw := firstRuntimeSubAgentDetailEvent(raw.Events, SubAgentDetailEventInput)
	if inputRaw.Message == nil || inputRaw.Message.Content != longMission || inputRaw.Message.Preview == "" || inputRaw.Message.Preview == inputRaw.Message.Content {
		t.Fatalf("raw input should keep full content and bounded preview: %#v", inputRaw)
	}
	assistantRaw := firstRuntimeSubAgentDetailEvent(raw.Events, SubAgentDetailEventAssistantMessage)
	if assistantRaw.Message == nil || assistantRaw.Message.Content != longAnswer || assistantRaw.Message.Preview == "" || assistantRaw.Message.Preview == assistantRaw.Message.Content {
		t.Fatalf("raw assistant should keep full content and bounded preview: %#v", assistantRaw)
	}

	page, err := host.ListSubAgentDetailEvents(ctx, ListSubAgentDetailEventsRequest{
		ParentThreadID: "parent",
		ChildThreadID:  "child",
		AfterOrdinal:   assistantRaw.Ordinal - 1,
		Limit:          1,
		IncludeRaw:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Kind != SubAgentDetailEventAssistantMessage || page.Events[0].Message == nil || page.Events[0].Message.Content != longAnswer {
		t.Fatalf("paged raw assistant event = %#v", page.Events)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	maintenanceRaw, err := maintenance.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	maintenanceAssistant := firstRuntimeSubAgentDetailEvent(maintenanceRaw.Events, SubAgentDetailEventAssistantMessage)
	if maintenanceAssistant.Message == nil || maintenanceAssistant.Message.Content != longAnswer || maintenanceAssistant.Message.Preview == maintenanceAssistant.Message.Content {
		t.Fatalf("maintenance raw assistant should keep full content and bounded preview: %#v", maintenanceAssistant)
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
	longAnswer := "persisted child report " + strings.Repeat("stored evidence ", 80) + "https://example.test/reopened-full-output"
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: longAnswer,
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
	if got := firstRuntimeSubAgentDetailEvent(detail.Events, SubAgentDetailEventAssistantMessage); got.Message == nil || got.Message.Content != longAnswer || got.Message.Preview == got.Message.Content || !strings.Contains(got.Message.Content, "reopened-full-output") {
		t.Fatalf("reopened detail = %#v", detail.Events)
	}
}

func TestThreadMaintenanceHostListsSubAgentsAfterHostRestart(t *testing.T) {
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
			FakeResponse: "restart child done",
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
		ParentThreadID:  "parent",
		ParentTurnID:    "parent-turn",
		ThreadID:        "child",
		TaskName:        "Restart Review",
		TaskDescription: "Verify subagent listing after runtime restart.",
		Message:         "check restart list",
		HostProfileRef:  "reviewer",
		ForkMode:        SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{
		ParentThreadID: "parent",
		ChildThreadIDs: []ThreadID{"child"},
		Timeout:        2 * time.Second,
	}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: reopenedStore})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	listed, err := maintenance.ListSubAgents(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("maintenance list = %#v", listed)
	}
	child := listed[0]
	if child.ThreadID != "child" ||
		child.ParentThreadID != "parent" ||
		child.ParentTurnID != "parent-turn" ||
		child.TaskName != "restart_review" ||
		child.TaskDescription != "Verify subagent listing after runtime restart." ||
		child.HostProfileRef != "reviewer" ||
		child.ForkMode != SubAgentForkNone ||
		child.Status != SubAgentStatusCompleted ||
		child.LastMessage != "restart child done" ||
		child.CreatedAt.IsZero() ||
		child.UpdatedAt.IsZero() ||
		!child.CanSendInput ||
		child.CanInterrupt ||
		!child.CanClose {
		t.Fatalf("maintenance child snapshot = %#v", child)
	}

	timeline, err := maintenance.ListSubAgentActivityTimeline(ctx, ListSubAgentActivityTimelineRequest{
		ParentThreadID: "parent",
		Meta: observation.ActivityRunMeta{
			RunID:    "parent-run",
			ThreadID: "parent",
			TurnID:   "parent-turn",
			TraceID:  "parent-run",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.ValidateActivityTimeline(timeline.Timeline); err != nil {
		t.Fatalf("maintenance activity timeline invalid: %v", err)
	}
	if len(timeline.Timeline.Items) != 1 {
		t.Fatalf("maintenance activity timeline = %#v", timeline.Timeline)
	}
	item := timeline.Timeline.Items[0]
	if item.Payload["thread_id"] != "child" ||
		item.Payload["subagent_id"] != "child" ||
		item.Payload["parent_thread_id"] != "parent" ||
		item.Payload["parent_turn_id"] != "parent-turn" ||
		item.Payload["task_name"] != "restart_review" ||
		item.Payload["task_description"] != "Verify subagent listing after runtime restart." ||
		item.Payload["status"] != string(SubAgentStatusCompleted) ||
		item.Payload["can_send_input"] != true ||
		item.Payload["can_interrupt"] != false ||
		item.Payload["can_close"] != true {
		t.Fatalf("maintenance activity payload = %#v", item.Payload)
	}
	for _, key := range []string{"operation", "action", "delegation_runtime"} {
		if _, ok := item.Payload[key]; ok {
			t.Fatalf("maintenance activity payload leaked product key %q: %#v", key, item.Payload)
		}
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
	if result.ActivityTimeline != nil {
		t.Fatalf("reopened result row should not expose per-event activity: %#v", result.ActivityTimeline)
	}
	if err := observation.ValidateActivityTimeline(detail.ActivityTimeline); err != nil {
		t.Fatalf("activity timeline invalid after reopen: %v", err)
	}
	if item := runtimeSubAgentActivityItem(detail.ActivityTimeline, "read-1"); item.Status != observation.ActivityStatusSuccess || item.Description != "Read persisted" {
		t.Fatalf("reopened activity timeline = %#v", detail.ActivityTimeline)
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

func TestThreadMaintenanceHostClosesChildrenAfterFailedParentTurn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		switch req.ThreadID {
		case "parent":
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "starting children"}
			events <- ModelEvent{Type: ModelEventError, Err: errors.New("parent failed")}
			close(events)
			return events, nil
		case "completed":
			return runtimeGatewayEvents("completed child"), nil
		default:
			events := make(chan ModelEvent)
			go func() {
				defer close(events)
				<-ctx.Done()
			}()
			return events, nil
		}
	})
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		IDGenerator:          deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
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
	failed, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-parent-failed", ThreadID: "parent", TurnID: "turn-parent-failed", Input: "coordinate children"})
	if err == nil || failed.Status != TurnStatusFailed {
		t.Fatalf("failed parent turn = %#v err=%v, want failed result and error", failed, err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	closed, err := maintenance.CloseSubAgents(ctx, CloseSubAgentsRequest{ParentThreadID: "parent", Reason: "parent_failed"})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Closed != 1 || len(closed.Snapshots) != 2 {
		t.Fatalf("CloseSubAgents result=%#v", closed)
	}
	byID := map[ThreadID]SubAgentSnapshot{}
	for _, snapshot := range closed.Snapshots {
		byID[snapshot.ThreadID] = snapshot
	}
	if byID["completed"].Status != SubAgentStatusCompleted || byID["completed"].Closed {
		t.Fatalf("completed snapshot = %#v", byID["completed"])
	}
	if byID["running"].Status != SubAgentStatusClosed || !byID["running"].Closed || byID["running"].CanClose {
		t.Fatalf("running snapshot = %#v", byID["running"])
	}
	detail, err := maintenance.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(detail.Events, func(ev SubAgentDetailEvent) bool {
		return ev.Type == "subagent_lifecycle" && ev.Metadata["reason"] == "parent_failed"
	}) {
		t.Fatalf("running detail missing parent_failed lifecycle: %#v", detail.Events)
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
	if runtimeProjectionAssistantText(*result.Projection) != runtimeProjectionAssistantText(projection) {
		t.Fatalf("read projection differs from turn result: result=%#v read=%#v", result.Projection, projection)
	}
	if projection.ThroughOrdinal <= 0 || projection.ThroughOrdinal != result.Projection.ThroughOrdinal {
		t.Fatalf("read ThroughOrdinal=%d, result=%d", projection.ThroughOrdinal, result.Projection.ThroughOrdinal)
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

func TestRuntimeEventValidateRejectsUnknownPublicState(t *testing.T) {
	t.Parallel()

	if err := (Event{Type: observation.EventTypeStepStart}).Validate(); err != nil {
		t.Fatalf("valid runtime event: %v", err)
	}
	if err := (Event{Type: "future_event"}).Validate(); err == nil {
		t.Fatal("unknown runtime event type validated")
	}
	if err := (Event{
		Type: observation.EventTypeProviderUsage,
		ContextStatus: &observation.ContextStatus{
			Phase:  observation.ContextPhaseProjectedRequest,
			Status: "future_status",
		},
	}).Validate(); err == nil {
		t.Fatal("runtime event with unknown context status validated")
	}
}

func TestTurnProjectionOutcomeValidation(t *testing.T) {
	t.Parallel()

	projection := &ThreadTurnProjection{ThreadID: "thread", TurnID: "turn", RunID: "run", ThroughOrdinal: 1}
	tests := []struct {
		name    string
		result  TurnResult
		wantErr bool
	}{
		{name: "ready", result: TurnResult{ProjectionStatus: TurnProjectionStatusReady, Projection: projection}},
		{name: "unavailable", result: TurnResult{ProjectionStatus: TurnProjectionStatusUnavailable, ProjectionError: "detail read failed"}},
		{name: "unknown status", result: TurnResult{ProjectionStatus: "future", Projection: projection}, wantErr: true},
		{name: "ready without projection", result: TurnResult{ProjectionStatus: TurnProjectionStatusReady}, wantErr: true},
		{name: "ready with error", result: TurnResult{ProjectionStatus: TurnProjectionStatusReady, Projection: projection, ProjectionError: "unexpected"}, wantErr: true},
		{name: "unavailable with projection", result: TurnResult{ProjectionStatus: TurnProjectionStatusUnavailable, Projection: projection, ProjectionError: "detail read failed"}, wantErr: true},
		{name: "unavailable without error", result: TurnResult{ProjectionStatus: TurnProjectionStatusUnavailable}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.ValidateProjection()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateProjection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestThreadMaintenanceHostForkThreadPreservesProjectionWithNewIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	host, err := NewHost(HostOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "projected answer",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-source", ThreadID: "source", TurnID: "turn-source", Input: "hello"}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.Thread.ID != "fork" || !forked.Thread.CanAppendMessage {
		t.Fatalf("forked thread = %#v", forked.Thread)
	}
	if len(forked.Turns) != 1 {
		t.Fatalf("forked turns = %#v, want one", forked.Turns)
	}
	if forked.OperationID != "fork-operation" {
		t.Fatalf("operation id = %q", forked.OperationID)
	}
	ref := forked.Turns[0]
	if ref.SourceTurnID != "turn-source" || ref.SourceRunID != "run-source" {
		t.Fatalf("source identity = %#v", ref)
	}
	if ref.DestinationTurnID == "" || ref.DestinationRunID == "" || ref.DestinationTurnID == ref.SourceTurnID || ref.DestinationRunID == ref.SourceRunID {
		t.Fatalf("destination identity was not rewritten: %#v", ref)
	}
	projection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{
		ThreadID: "fork",
		TurnID:   ref.DestinationTurnID,
		RunID:    ref.DestinationRunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projection.Status != TurnStatusCompleted || runtimeProjectionAssistantText(projection) != "projected answer" {
		t.Fatalf("fork projection = %#v", projection)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-fork-next", ThreadID: "fork", TurnID: "turn-fork-next", Input: "continue"}); err != nil {
		t.Fatalf("RunTurn on fork: %v", err)
	}
}

func TestThreadMaintenanceHostForkThreadPreservesSQLiteProjectionAfterReopen(t *testing.T) {
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
			FakeResponse: "sqlite projected answer",
			SystemPrompt: "test",
		},
		Store:       store,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-source", ThreadID: "source", TurnID: "turn-source", Input: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	forkStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: forkStore})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatal(err)
	}
	if len(forked.Turns) != 1 {
		t.Fatalf("forked turns = %#v, want one", forked.Turns)
	}
	ref := forked.Turns[0]
	if ref.SourceTurnID != "turn-source" || ref.SourceRunID != "run-source" {
		t.Fatalf("source identity = %#v", ref)
	}
	if ref.DestinationTurnID == "" || ref.DestinationRunID == "" || ref.DestinationTurnID == ref.SourceTurnID || ref.DestinationRunID == ref.SourceRunID {
		t.Fatalf("destination identity was not rewritten: %#v", ref)
	}

	reopenedStore, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: reopenedStore})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replayed, err := reopened.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "source", DestinationThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, forked) {
		t.Fatalf("replayed fork = %#v, want %#v", replayed, forked)
	}
	projection, err := reopened.ReadTurnProjection(ctx, ReadTurnProjectionRequest{
		ThreadID: "fork",
		TurnID:   ref.DestinationTurnID,
		RunID:    ref.DestinationRunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projection.Status != TurnStatusCompleted || runtimeProjectionAssistantText(projection) != "sqlite projected answer" {
		t.Fatalf("fork projection = %#v", projection)
	}
}

func TestThreadMaintenanceHostForkThreadRejectsOperationAndDestinationConflicts(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "source-turn", session.Message{Role: session.User, Content: "pinned"}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	request := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
	first, err := maintenance.ForkThread(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "later-turn", session.Message{Role: session.User, Content: "later"}); err != nil {
		t.Fatal(err)
	}
	replayed, err := maintenance.ForkThread(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed fork = %#v, want %#v", replayed, first)
	}
	forkPath, err := store.repo.Path(ctx, "fork", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(forkPath) != 1 || forkPath[0].Message.Content != "pinned" {
		t.Fatalf("fork path drifted with source: %#v", forkPath)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "different"}); !errors.Is(err, ErrForkOperationConflict) {
		t.Fatalf("request conflict error = %v", err)
	}

	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "occupied"}); err != nil {
		t.Fatal(err)
	}
	conflictingRequest := ForkThreadRequest{OperationID: "destination-operation", SourceThreadID: "source", DestinationThreadID: "occupied"}
	if _, err := maintenance.ForkThread(ctx, conflictingRequest); !errors.Is(err, ErrForkDestinationConflict) {
		t.Fatalf("destination conflict error = %v", err)
	}
	if _, err := maintenance.ForkThread(ctx, conflictingRequest); !errors.Is(err, ErrForkDestinationConflict) {
		t.Fatalf("persisted destination conflict error = %v", err)
	}
}

func TestThreadMaintenanceHostForkThreadValidatesCompletedTargets(t *testing.T) {
	ctx := context.Background()
	t.Run("missing", func(t *testing.T) {
		store := NewMemoryStore()
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := store.repo.DeleteThread(ctx, "fork"); err != nil {
			t.Fatal(err)
		}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, ErrForkOperationTargetMissing) {
			t.Fatalf("missing target error = %v", err)
		}
	})
	t.Run("marker mismatch", func(t *testing.T) {
		store := NewMemoryStore()
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
			t.Fatal(err)
		}
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		meta, err := store.repo.Thread(ctx, "fork")
		if err != nil {
			t.Fatal(err)
		}
		meta.ForkOperationNodeID = "different-node"
		if err := store.repo.UpdateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, ErrForkDestinationConflict) {
			t.Fatalf("marker conflict error = %v", err)
		}
	})
}

func TestThreadMaintenanceHostForkThreadRecoversAtOperationBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("after plan save", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failPrepareAfterSave: true}
		store.forkOperations = faults
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("destination exists before retry: %v", err)
		}
		if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "later-turn", session.Message{Role: session.User, Content: "later"}); err != nil {
			t.Fatal(err)
		}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
		path, err := store.repo.Path(ctx, "fork", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(path) != 1 || path[0].Message.Content != "source" {
			t.Fatalf("prepared fork used changed source path: %#v", path)
		}
	})

	t.Run("before root", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkRepoFaultStore{Repo: store.repo, list: store.repo.(sessiontree.ThreadListRepo), failAt: 1}
		store.repo = faults
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := maintenance.ForkThread(ctx, req); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("between root and terminal child", func(t *testing.T) {
		store := newForkTestStore(t, true)
		faults := &forkRepoFaultStore{Repo: store.repo, list: store.repo.(sessiontree.ThreadListRepo), failAt: 2}
		store.repo = faults
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); err != nil {
			t.Fatalf("root was not committed: %v", err)
		}
		first, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		second, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("replayed fork = %#v, want %#v", second, first)
		}
		children, err := maintenance.ListSubAgents(ctx, "fork")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 {
			t.Fatalf("terminal children = %#v", children)
		}
	})

	t.Run("before completed record", func(t *testing.T) {
		store := newForkTestStore(t, false)
		faults := &forkOperationFaultStore{ForkOperationStore: store.forkOperations, failUpdate: true}
		store.forkOperations = faults
		maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
		if err != nil {
			t.Fatal(err)
		}
		defer maintenance.Close()
		req := ForkThreadRequest{OperationID: "operation", SourceThreadID: "source", DestinationThreadID: "fork"}
		if _, err := maintenance.ForkThread(ctx, req); !errors.Is(err, errInjectedForkFailure) {
			t.Fatalf("first error = %v", err)
		}
		if _, err := store.repo.Thread(ctx, "fork"); err != nil {
			t.Fatalf("destination was not committed: %v", err)
		}
		first, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		second, err := maintenance.ForkThread(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("replayed fork = %#v, want %#v", second, first)
		}
	})
}

func TestThreadMaintenanceHostForkThreadClonesTerminalSubAgents(t *testing.T) {
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
	defer host.Close()
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-parent", ThreadID: "parent", TurnID: "turn-parent", Input: "coordinate"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{
		ParentThreadID: "parent",
		ParentTurnID:   "turn-parent",
		ThreadID:       "child",
		TaskName:       "Review API",
		Message:        "review the runtime API",
		ForkMode:       SubAgentForkNone,
	}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: 2 * time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("WaitSubAgents err=%v waited=%#v", err, waited)
	}
	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	forked, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-operation", SourceThreadID: "parent", DestinationThreadID: "parent-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if len(forked.Turns) != 1 || forked.Turns[0].DestinationTurnID == "" {
		t.Fatalf("forked turns = %#v", forked.Turns)
	}
	children, err := maintenance.ListSubAgents(ctx, "parent-fork")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].ThreadID == "child" || children[0].Status != SubAgentStatusCompleted {
		t.Fatalf("forked children = %#v", children)
	}
	if children[0].ParentTurnID != forked.Turns[0].DestinationTurnID {
		t.Fatalf("forked child parent turn = %q, want %q", children[0].ParentTurnID, forked.Turns[0].DestinationTurnID)
	}
	detail, err := maintenance.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{
		ParentThreadID: "parent-fork",
		ChildThreadID:  children[0].ThreadID,
		IncludeRaw:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtimeSubAgentDetailAssistantText(detail) != "child done" {
		t.Fatalf("forked child detail = %#v", detail.Events)
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
	var invocation tools.Invocation[runtimeEchoArgs]
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "terminal_exec",
			InputSchema: runtimeEchoSchema(),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			invocation = inv
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

	settlementRepo := &settlementProjectionFailureRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	store := NewMemoryStore()
	store.repo = settlementRepo
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ToolSurfaceProvider: func(_ context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
			return ToolSurface{
				Tools: registry,
				HostContext: map[string]string{
					"child_run_id": "run_child_audit",
					"surface":      "runtime-test",
				},
			}, nil
		},
		Approver:    allowRuntimeTools,
		IDGenerator: deterministicIDs(),
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
	if item := runtimeProjectionToolItem(*run.Projection, "exec-1"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("pending item should remain running before explicit settlement: %#v", item)
	}
	if invocation.RunID != "run-1" ||
		invocation.HostContext["child_run_id"] != "run_child_audit" ||
		invocation.HostContext["child_run_id"] == string(invocation.RunID) {
		t.Fatalf("invocation identity/host context = %#v", invocation)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	if _, err := maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "run_child_audit",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettlementCompleted,
		Summary:    "wrong host correlation run",
		Output:     "exit 0",
	}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("host-correlation settlement err = %v, want ErrRunNotFound", err)
	}
	if readAfterWrong, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"}); err != nil {
		t.Fatalf("ReadTurnProjection after wrong run settlement: %v", err)
	} else if item := runtimeProjectionToolItem(readAfterWrong, "exec-1"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("wrong host-correlation settlement changed projection: %#v", item)
	}

	settlementRepo.arm.Store(true)
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
	if settled.ProjectionStatus != TurnProjectionStatusUnavailable || settled.Projection != nil || settled.ProjectionError == "" {
		t.Fatalf("settlement projection outcome = %#v, want unavailable", settled)
	}
	if err := settled.ValidateProjection(); err != nil {
		t.Fatalf("settlement projection validation: %v", err)
	}
	readProjection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	item := runtimeProjectionToolItem(readProjection, "exec-1")
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
	if got := runtimeProjectionAssistantText(readProjection); got != longAssistantAfterPending {
		t.Fatalf("settled projection assistant text length=%d, want full %d\ntext=%q", len([]rune(got)), len([]rune(longAssistantAfterPending)), got)
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

func TestHostSettlePendingToolOnlyUpdatesExplicitPendingTarget(t *testing.T) {
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
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			args := inv.Args
			command := strings.TrimSpace(args.Text)
			if command == "" {
				command = "command"
			}
			return tools.Result{
				Activity: &observation.ActivityPresentation{
					Label:    command,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": command},
				},
				Pending: &tools.PendingToolResult{
					Handle:      "terminal:job:" + strings.ReplaceAll(command, " ", "-"),
					State:       tools.PendingToolResultRunning,
					Summary:     "Command is running",
					Instruction: "Wait for completion.",
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var requests int
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.Contains(string(req.RunID), "thread-title") {
			events := make(chan ModelEvent, 2)
			events <- ModelEvent{Type: ModelEventDelta, Text: "Pending commands"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		}
		requests++
		events := make(chan ModelEvent, 3)
		switch requests {
		case 1:
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "exec-a", Name: "terminal_exec", Args: `{"text":"npm test"}`},
				{ID: "exec-b", Name: "terminal_exec", Args: `{"text":"npm lint"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		case 2:
			events <- ModelEvent{Type: ModelEventDelta, Text: "Both commands are now running under the host."}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		default:
			t.Fatalf("unexpected provider request after pending commands: %#v", req)
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
	run, err := host.RunTurn(ctx, RunTurnRequest{RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: "run commands"})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != TurnStatusCompleted {
		t.Fatalf("run status=%q, want completed", run.Status)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-a"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-a should remain running before settlement: %#v", item)
	}
	if item := runtimeProjectionToolItem(*run.Projection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-b should remain running before settlement: %#v", item)
	}

	maintenance, err := NewThreadMaintenanceHost(ThreadMaintenanceHostOptions{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	settled, err := maintenance.SettlePendingTool(ctx, PendingToolSettlementRequest{
		ThreadID:   "thread",
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-a",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:npm-test",
		Status:     PendingToolSettlementCompleted,
		Summary:    "npm test completed",
		Output:     "ok",
		Activity:   &observation.ActivityPresentation{Label: "npm test", Renderer: observation.ActivityRendererTerminal, Payload: map[string]any{"command": "npm test", "exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if item := runtimeProjectionToolItem(*settled.Projection, "exec-a"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("exec-a settled item = %#v, want success", item)
	}
	if item := runtimeProjectionToolItem(*settled.Projection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("exec-b should remain running after exec-a settlement: %#v", item)
	}

	readProjection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "thread", TurnID: "turn-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-a"); item.Status != observation.ActivityStatusSuccess {
		t.Fatalf("read projection exec-a = %#v, want success", item)
	}
	if item := runtimeProjectionToolItem(readProjection, "exec-b"); item.Status != observation.ActivityStatusRunning {
		t.Fatalf("read projection exec-b = %#v, want still running", item)
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
	for i, projection := range liveProjections {
		if projection.ThroughOrdinal <= 0 {
			t.Fatalf("live projection %d ThroughOrdinal=%d, want positive", i, projection.ThroughOrdinal)
		}
		if i > 0 && projection.ThroughOrdinal <= liveProjections[i-1].ThroughOrdinal {
			t.Fatalf("live projection ordinals did not advance: previous=%d current=%d", liveProjections[i-1].ThroughOrdinal, projection.ThroughOrdinal)
		}
	}
	finalLiveProjection := liveProjections[len(liveProjections)-1]
	if finalLiveProjection.ThroughOrdinal != result.Projection.ThroughOrdinal {
		t.Fatalf("final live ThroughOrdinal=%d, result=%d", finalLiveProjection.ThroughOrdinal, result.Projection.ThroughOrdinal)
	}
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
		approval.BatchIndex != 0 ||
		approval.BatchSize != 1 ||
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

func TestHostPendingApprovalSnapshotKeepsModelBatchOrder(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name:        "write_note",
			InputSchema: runtimeEchoSchema(),
			Effects:     []tools.Effect{tools.EffectWrite},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			return tools.Result{Text: inv.Args.Text}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		events := make(chan ModelEvent, 3)
		if req.Step == 1 {
			events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{
				{ID: "call-a", Name: "write_note", Args: `{"text":"a"}`},
				{ID: "call-b", Name: "write_note", Args: `{"text":"b"}`},
			}}
			events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
			events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	requested := make(chan tools.ApprovalRequest, 2)
	release := make(chan struct{})
	host, err := NewHost(HostOptions{
		Config:               runtimeGatewayConfig("test"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
		Tools:                registry,
		Approver: func(ctx context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
			requested <- req
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
	if _, err := host.StartThread(ctx, StartThreadRequest{ThreadID: "thread-batch"}); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() {
		_, err := host.RunTurn(ctx, RunTurnRequest{RunID: "turn-batch", ThreadID: "thread-batch", TurnID: "turn-batch", Input: "write both"})
		runErr <- err
	}()
	seen := map[string]tools.ApprovalRequest{}
	for range 2 {
		select {
		case req := <-requested:
			seen[req.ID] = req
		case <-time.After(2 * time.Second):
			t.Fatalf("approval requests did not enter concurrently: %#v", seen)
		}
	}
	pending, err := host.ListPendingApprovals(ctx, ListPendingApprovalsRequest{ThreadID: "thread-batch"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Approvals) != 2 ||
		pending.Approvals[0].ToolCallID != "call-a" || pending.Approvals[0].BatchIndex != 0 || pending.Approvals[0].BatchSize != 2 ||
		pending.Approvals[1].ToolCallID != "call-b" || pending.Approvals[1].BatchIndex != 1 || pending.Approvals[1].BatchSize != 2 {
		t.Fatalf("pending approvals = %#v", pending.Approvals)
	}
	close(release)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch run")
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
	projected := runtimeProjectionAssistantText(*result.Projection)
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
	deleteData := store.deleteData
	var deleteCalls int
	var deleteRequest storage.DeleteThreadTreeDataRequest
	store.deleteData = func(ctx context.Context, req storage.DeleteThreadTreeDataRequest) error {
		deleteCalls++
		deleteRequest = req
		return deleteData(ctx, req)
	}
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
	if deleteCalls != 1 || !slices.Equal(deleteRequest.ThreadIDs, []string{"parent", "child"}) || !slices.Equal(deleteRequest.PromptScopeIDs, []string{"parent", "child"}) {
		t.Fatalf("delete calls = %d request = %#v", deleteCalls, deleteRequest)
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
	var seq atomic.Int64
	return func(prefix string) string {
		return fmt.Sprintf("%s-deterministic-%d", prefix, seq.Add(1))
	}
}

var errInjectedForkFailure = errors.New("injected fork failure")

type forkOperationFaultStore struct {
	storage.ForkOperationStore
	mu                   sync.Mutex
	failPrepareAfterSave bool
	failUpdate           bool
}

func (s *forkOperationFaultStore) PrepareForkOperation(ctx context.Context, rec storage.ForkOperationRecord) (storage.ForkOperationRecord, bool, error) {
	stored, created, err := s.ForkOperationStore.PrepareForkOperation(ctx, rec)
	if err != nil {
		return storage.ForkOperationRecord{}, false, err
	}
	s.mu.Lock()
	fail := s.failPrepareAfterSave
	s.failPrepareAfterSave = false
	s.mu.Unlock()
	if fail {
		return storage.ForkOperationRecord{}, false, errInjectedForkFailure
	}
	return stored, created, nil
}

func (s *forkOperationFaultStore) UpdateForkOperation(ctx context.Context, rec storage.ForkOperationRecord) error {
	s.mu.Lock()
	fail := s.failUpdate
	s.failUpdate = false
	s.mu.Unlock()
	if fail {
		return errInjectedForkFailure
	}
	return s.ForkOperationStore.UpdateForkOperation(ctx, rec)
}

type forkRepoFaultStore struct {
	sessiontree.Repo
	list   sessiontree.ThreadListRepo
	mu     sync.Mutex
	calls  int
	failAt int
}

func (r *forkRepoFaultStore) Fork(ctx context.Context, opts sessiontree.ForkOptions) (sessiontree.ThreadMeta, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	fail := r.failAt == call
	r.mu.Unlock()
	if fail {
		return sessiontree.ThreadMeta{}, errInjectedForkFailure
	}
	return r.Repo.Fork(ctx, opts)
}

func (r *forkRepoFaultStore) ListThreads(ctx context.Context, opts sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	return r.list.ListThreads(ctx, opts)
}

func newForkTestStore(t *testing.T, withTerminalChild bool) *Store {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC)
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "source-turn", session.Message{Role: session.User, Content: "source"}); err != nil {
		t.Fatal(err)
	}
	if withTerminalChild {
		if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
			ID:             "child",
			ParentThreadID: "source",
			ParentTurnID:   "source-turn",
			TaskName:       "review",
			AgentPath:      "/root/review",
			ForkMode:       string(SubAgentForkNone),
			Closed:         true,
			Status:         string(SubAgentStatusClosed),
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

type terminalProjectionFailureRepo struct {
	*sessiontree.MemoryRepo
	failPath     atomic.Bool
	postRunPaths atomic.Int64
}

func (r *terminalProjectionFailureRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if r.failPath.Load() && r.postRunPaths.Add(1) > 1 {
		return nil, errors.New("injected terminal projection read failure")
	}
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

type terminalProjectionFailureRecorder struct {
	repo *terminalProjectionFailureRepo
}

type settlementProjectionFailureRepo struct {
	*sessiontree.MemoryRepo
	arm      atomic.Bool
	failPath atomic.Bool
}

func (r *settlementProjectionFailureRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	appended, err := r.MemoryRepo.Append(ctx, entry, opts)
	if err == nil && r.arm.Swap(false) {
		r.failPath.Store(true)
	}
	return appended, err
}

func (r *settlementProjectionFailureRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if r.failPath.Swap(false) {
		return nil, errors.New("injected settlement projection read failure")
	}
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

func (r *terminalProjectionFailureRecorder) EmitEvent(ev Event) {
	if ev.Type == "run_end" {
		r.repo.failPath.Store(true)
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

func runtimeSubAgentActivityItem(timeline observation.ActivityTimeline, toolID string) observation.ActivityItem {
	for _, item := range timeline.Items {
		if item.ToolID == toolID {
			return item
		}
	}
	return observation.ActivityItem{}
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

func runtimeSubAgentDetailAssistantText(detail SubAgentDetail) string {
	var out strings.Builder
	for _, event := range detail.Events {
		if event.Kind != SubAgentDetailEventAssistantMessage || event.Message == nil {
			continue
		}
		out.WriteString(event.Message.Content)
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
