package runtime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

type publicModelGateway func(context.Context, runtime.ModelRequest) (<-chan runtime.ModelEvent, error)

func (f publicModelGateway) StreamModel(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
	return f(ctx, req)
}

func TestRunProjectedTurnFromPublicPackages(t *testing.T) {
	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			FakeResponse: "public facade ok",
			SystemPrompt: "test",
		},
		Store: runtime.NewMemoryStore(),
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-1",
		ThreadID:      "thread-1",
		TurnID:        "turn-1",
		TraceID:       "trace-1",
		PromptScopeID: "thread-1",
		History: []runtime.TranscriptMessage{{
			Role:    "user",
			Content: "hello",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != runtime.TurnStatusCompleted || result.Output != "public facade ok" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Transcript) != 2 {
		t.Fatalf("transcript = %#v", result.Transcript)
	}
	if result.Metrics.LLMRequests != 1 || result.Metrics.ProviderUsage.Source == "" {
		t.Fatalf("metrics = %#v", result.Metrics)
	}
}

func TestRunProjectedTurnWithPublicModelGateway(t *testing.T) {
	registry := tools.NewRegistry()
	err := registry.Register(tools.Define[map[string]any](
		tools.Definition{
			Name:        "lookup",
			Title:       "Lookup",
			Description: "Lookup test data.",
			InputSchema: tools.StrictObject(map[string]any{}, nil),
			ReadOnly:    true,
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{Text: "unused"}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}

	var sawSystem bool
	var sawTool bool
	var sawPreviousState bool
	previousState := &runtime.ModelState{
		Kind:       "openai_responses",
		ID:         "resp_prev",
		Attributes: map[string]string{"cursor": "one"},
	}
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		if req.RunID != "run-gateway" || req.ThreadID != "thread-gateway" || req.TurnID != "turn-gateway" || req.TraceID != "trace-gateway" || req.PromptScopeID != "thread-gateway" {
			t.Fatalf("request identity = %#v", req)
		}
		if req.Labels.Correlation["message_id"] != "msg-gateway" || req.Labels.Host["workspace_id"] != "ws-gateway" {
			t.Fatalf("request labels = %#v", req.Labels)
		}
		if req.PreviousState == nil || req.PreviousState.Kind != "openai_responses" || req.PreviousState.ID != "resp_prev" || req.PreviousState.Attributes["cursor"] != "one" {
			t.Fatalf("request previous state = %#v", req.PreviousState)
		}
		req.PreviousState.Attributes["cursor"] = "mutated"
		sawPreviousState = true
		for _, msg := range req.Messages {
			if msg.Role == "system" && strings.Contains(msg.Content, "gateway system") {
				sawSystem = true
			}
		}
		for _, def := range req.Tools {
			if def.Name == "lookup" && def.Strict {
				sawTool = true
			}
		}
		events := make(chan runtime.ModelEvent, 3)
		events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "gateway ok"}
		events <- runtime.ModelEvent{
			Type: runtime.ModelEventUsage,
			Usage: runtime.ProviderUsage{
				InputTokens:  2,
				OutputTokens: 3,
			},
		}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})

	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
		Tools:        registry,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-gateway",
		ThreadID:      "thread-gateway",
		TurnID:        "turn-gateway",
		TraceID:       "trace-gateway",
		PromptScopeID: "thread-gateway",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
		Labels: runtime.RunLabels{
			Correlation: map[string]string{"message_id": "msg-gateway"},
			Host:        map[string]string{"workspace_id": "ws-gateway"},
		},
		PreviousProviderState: previousState,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawSystem || !sawTool || !sawPreviousState {
		t.Fatalf("gateway saw system=%v tool=%v previous_state=%v", sawSystem, sawTool, sawPreviousState)
	}
	if previousState.Attributes["cursor"] != "one" {
		t.Fatalf("previous state was aliased: %#v", previousState)
	}
	if result.Output != "gateway ok" || result.Metrics.ProviderUsage.InputTokens != 2 || result.Metrics.ProviderUsage.OutputTokens != 3 {
		t.Fatalf("result = %#v", result)
	}
}

func TestRunProjectedTurnProjectsPublicControlSignalActivity(t *testing.T) {
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		var sawControlTool bool
		for _, def := range req.Tools {
			if def.Name == "host_wait" && def.Annotations["kind"] == "control" {
				sawControlTool = true
			}
		}
		if !sawControlTool {
			t.Fatalf("tools missing host_wait control definition: %#v", req.Tools)
		}
		events := make(chan runtime.ModelEvent, 2)
		events <- runtime.ModelEvent{
			Type: runtime.ModelEventToolCalls,
			ToolCalls: []tools.ToolCall{{
				ID:   "control-call-1",
				Name: "host_wait",
				Args: `{"prompt_id":"p1","question":"Pick a file","secret":"token abc"}`,
			}},
			Reason: "tool_calls",
		}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "tool_calls"}
		close(events)
		return events, nil
	})

	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-control",
		ThreadID:      "thread-control",
		TurnID:        "turn-control",
		TraceID:       "trace-control",
		PromptScopeID: "thread-control",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
		Signals: runtime.TurnSignalSpec{
			Definitions: []tools.ToolDefinition{{
				Name:        "host_wait",
				Title:       "Host wait",
				Description: "Wait for host input.",
				InputSchema: tools.StrictObject(map[string]any{
					"prompt_id": tools.String("prompt id"),
					"question":  tools.String("question"),
					"secret":    tools.String("secret"),
				}, []string{"prompt_id", "question"}),
				Strict:      true,
				Annotations: map[string]any{"kind": "control"},
			}},
			Project: func(call tools.ToolCall) (runtime.TurnSignal, bool, error) {
				return runtime.TurnSignal{
					Disposition: runtime.SignalWaiting,
					Name:        call.Name,
					CallID:      call.ID,
					OutputText:  "Pick a file",
					Payload:     map[string]any{"prompt_id": "p1", "secret": "token abc"},
					Activity: &observation.ActivityPresentation{
						Label:       "Waiting for host input",
						Description: "Pick a file",
						Renderer:    observation.ActivityRendererQuestion,
						Payload: map[string]any{
							"prompt_id": "p1",
							"mode":      "select",
						},
					},
				}, true, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != runtime.TurnStatusWaiting || result.Signal == nil || result.Signal.Name != "host_wait" {
		t.Fatalf("result = %#v", result)
	}
	timeline := result.ActivityTimeline
	if timeline.Summary.Status != observation.ActivityStatusWaiting || !timeline.Summary.NeedsAttention {
		t.Fatalf("timeline summary = %#v", timeline.Summary)
	}
	if len(timeline.Items) != 1 {
		t.Fatalf("timeline items = %#v", timeline.Items)
	}
	item := timeline.Items[0]
	if item.Kind != observation.ActivityKindControl || item.ToolName != "host_wait" || item.ToolID != "control-call-1" {
		t.Fatalf("control item = %#v", item)
	}
	if item.Status != observation.ActivityStatusWaiting || item.Metadata["control_disposition"] != "waiting" || item.Metadata["args_hash"] == "" {
		t.Fatalf("control item status/metadata = %#v", item)
	}
	if item.Label != "Waiting for host input" || item.Description != "Pick a file" || item.Renderer != observation.ActivityRendererQuestion {
		t.Fatalf("control presentation = %#v", item)
	}
	if item.Payload["prompt_id"] != "p1" || item.Payload["mode"] != "select" {
		t.Fatalf("control payload = %#v", item.Payload)
	}
	data, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token abc"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("timeline leaked %q: %s", forbidden, data)
		}
	}
}
