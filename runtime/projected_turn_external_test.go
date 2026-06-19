package runtime_test

import (
	"context"
	"encoding/json"
	"slices"
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
		events <- runtime.ModelEvent{
			Type:          runtime.ModelEventDone,
			Reason:        "stop",
			ResponseState: &runtime.ModelState{Kind: "openai_responses", ID: "resp_next", Attributes: map[string]string{"cursor": "two"}},
		}
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
	if result.ProviderState == nil || result.ProviderState.Kind != "openai_responses" || result.ProviderState.ID != "resp_next" || result.ProviderState.Attributes["cursor"] != "two" {
		t.Fatalf("result provider state = %#v", result.ProviderState)
	}
	result.ProviderState.Attributes["cursor"] = "mutated-result"
	second, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "gateway system",
		},
		ModelGateway: publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
			if req.PreviousState == nil || req.PreviousState.Attributes["cursor"] != "mutated-result" {
				t.Fatalf("second request previous state = %#v", req.PreviousState)
			}
			events := make(chan runtime.ModelEvent, 2)
			events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "second ok"}
			events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
			close(events)
			return events, nil
		}),
		Store: runtime.NewMemoryStore(),
	}, runtime.ProjectedTurnRequest{
		RunID:                 "run-gateway-2",
		ThreadID:              "thread-gateway",
		TurnID:                "turn-gateway-2",
		TraceID:               "trace-gateway-2",
		PromptScopeID:         "thread-gateway",
		History:               []runtime.TranscriptMessage{{Role: "user", Content: "hello again"}},
		PreviousProviderState: result.ProviderState,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != runtime.TurnStatusCompleted {
		t.Fatalf("second result = %#v", second)
	}
}

type collectingEventSink struct {
	events []runtime.Event
}

func (s *collectingEventSink) EmitEvent(ev runtime.Event) {
	s.events = append(s.events, ev)
}

func TestRunProjectedTurnEmitsPublicStreamObservations(t *testing.T) {
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 4)
		events <- runtime.ModelEvent{Type: runtime.ModelEventReasoning, Text: "hidden chain"}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "visible "}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "answer"}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	sink := &collectingEventSink{}
	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
		Sink:         sink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-stream",
		ThreadID:      "thread-stream",
		TurnID:        "turn-stream",
		TraceID:       "trace-stream",
		PromptScopeID: "thread-stream",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "visible answer" {
		t.Fatalf("output = %q", result.Output)
	}
	var got []runtime.StreamObservationType
	var text strings.Builder
	var reasoning strings.Builder
	for _, ev := range sink.events {
		if ev.Stream == nil {
			continue
		}
		got = append(got, ev.Stream.Type)
		switch ev.Stream.Type {
		case runtime.StreamObservationAssistantDelta:
			text.WriteString(ev.Stream.Text)
		case runtime.StreamObservationReasoningDelta:
			reasoning.WriteString(ev.Stream.Text)
		case runtime.StreamObservationModelStreamDone:
			if ev.Stream.FinishReason != "stop" {
				t.Fatalf("finish stream = %#v", ev.Stream)
			}
		}
		if ev.Message != "" && (strings.Contains(ev.Message, "visible") || strings.Contains(ev.Message, "hidden chain")) {
			t.Fatalf("sanitized event message leaked stream text: %#v", ev)
		}
	}
	want := []runtime.StreamObservationType{
		runtime.StreamObservationReasoningDelta,
		runtime.StreamObservationAssistantDelta,
		runtime.StreamObservationAssistantDelta,
		runtime.StreamObservationModelStreamDone,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("stream types = %#v, want %#v; events=%#v", got, want, sink.events)
	}
	if text.String() != "visible answer" || reasoning.String() != "hidden chain" {
		t.Fatalf("stream text=%q reasoning=%q", text.String(), reasoning.String())
	}
}

func TestRunProjectedTurnEmitsPublicSourceObservations(t *testing.T) {
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 3)
		events <- runtime.ModelEvent{Type: runtime.ModelEventSources, Sources: []runtime.SourceRef{{
			Title: "Example docs",
			URL:   "https://example.test/docs",
		}}}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "answer"}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	sink := &collectingEventSink{}
	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
		Sink:         sink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-sources",
		ThreadID:      "thread-sources",
		TurnID:        "turn-sources",
		TraceID:       "trace-sources",
		PromptScopeID: "thread-sources",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "answer" {
		t.Fatalf("output = %q", result.Output)
	}
	var sources []runtime.SourceRef
	for _, ev := range sink.events {
		sources = append(sources, ev.Sources...)
		if ev.Stream != nil && ev.Stream.Type == runtime.StreamObservationAssistantDelta && ev.Stream.Text == "Example docs" {
			t.Fatalf("source title leaked as stream text: %#v", ev)
		}
	}
	if len(sources) != 1 || sources[0].Title != "Example docs" || sources[0].URL != "https://example.test/docs" {
		t.Fatalf("sources = %#v", sources)
	}
}

func TestRunProjectedTurnStreamObservationLabelsStayPublic(t *testing.T) {
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 2)
		events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "ok"}
		events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	sink := &collectingEventSink{}
	_, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
		Sink:         sink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-stream-labels",
		ThreadID:      "thread-stream-labels",
		TurnID:        "turn-stream-labels",
		TraceID:       "trace-stream-labels",
		PromptScopeID: "thread-stream-labels",
		Labels: runtime.RunLabels{
			Correlation: map[string]string{"turn": "turn-stream-labels"},
			Host:        map[string]string{"secret": "token secret-value"},
		},
		History: []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawStream bool
	for _, ev := range sink.events {
		if ev.Stream == nil {
			continue
		}
		sawStream = true
		if ev.Stream.Labels.Correlation["turn"] != "turn-stream-labels" {
			t.Fatalf("stream correlation labels = %#v", ev.Stream.Labels.Correlation)
		}
		if len(ev.Stream.Labels.Host) != 0 {
			t.Fatalf("stream should not expose host labels: %#v", ev.Stream.Labels.Host)
		}
		if strings.Contains(strings.Join(mapsValues(ev.Stream.Labels.Correlation), " "), "secret-value") {
			t.Fatalf("stream labels leaked host secret: %#v", ev.Stream.Labels)
		}
	}
	if !sawStream {
		t.Fatalf("missing stream observations: %#v", sink.events)
	}
}

func mapsValues(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func TestRunProjectedTurnEmitsRetryAndAbortStreamObservations(t *testing.T) {
	retryGateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 1)
		if req.Step == 1 {
			events <- runtime.ModelEvent{Type: runtime.ModelEventEmpty}
		} else {
			events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	retrySink := &collectingEventSink{}
	_, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:                config.ProviderFake,
			Model:                   "fake-model",
			SystemPrompt:            "test",
			MaxEmptyProviderRetries: 1,
		},
		ModelGateway: retryGateway,
		Store:        runtime.NewMemoryStore(),
		Sink:         retrySink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-retry",
		ThreadID:      "thread-retry",
		TurnID:        "turn-retry",
		TraceID:       "trace-retry",
		PromptScopeID: "thread-retry",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
	})
	if err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("retry result err = %v", err)
	}
	if !hasStreamType(retrySink.events, runtime.StreamObservationModelRetry) {
		t.Fatalf("retry stream missing: %#v", retrySink.events)
	}
	if !hasStreamType(retrySink.events, runtime.StreamObservationModelStreamAbort) {
		t.Fatalf("abort stream missing after failed retry: %#v", retrySink.events)
	}

	cancelGateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 1)
		events <- runtime.ModelEvent{Type: runtime.ModelEventError, Err: context.Canceled, Reason: "cancelled"}
		close(events)
		return events, nil
	})
	cancelSink := &collectingEventSink{}
	_, err = runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: cancelGateway,
		Store:        runtime.NewMemoryStore(),
		Sink:         cancelSink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-cancel",
		ThreadID:      "thread-cancel",
		TurnID:        "turn-cancel",
		TraceID:       "trace-cancel",
		PromptScopeID: "thread-cancel",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "hello"}},
	})
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("cancel result err = %v", err)
	}
	if !hasStreamType(cancelSink.events, runtime.StreamObservationModelStreamAbort) {
		t.Fatalf("cancel abort stream missing: %#v", cancelSink.events)
	}
}

func hasStreamType(events []runtime.Event, typ runtime.StreamObservationType) bool {
	return slices.ContainsFunc(events, func(ev runtime.Event) bool {
		return ev.Stream != nil && ev.Stream.Type == typ
	})
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

func TestCoreControlHelpersProjectAskUserAndTaskComplete(t *testing.T) {
	defs := runtime.CoreControlDefinitions(true)
	if len(defs) != 2 || defs[0].Name != runtime.CoreControlAskUser || defs[1].Name != runtime.CoreControlTaskComplete {
		t.Fatalf("core control definitions = %#v", defs)
	}
	if defs[0].Annotations["kind"] != "control" || !defs[0].Strict {
		t.Fatalf("ask_user definition = %#v", defs[0])
	}
	assertObjectRequiredArrays(t, "ask_user", defs[0].InputSchema)
	ask, ok, err := runtime.ProjectCoreControlSignal(tools.ToolCall{
		ID:   "ask-1",
		Name: runtime.CoreControlAskUser,
		Args: `{"question":"Which file?"}`,
	})
	if err != nil || !ok || ask.Disposition != runtime.SignalWaiting || ask.OutputText != "Which file?" || ask.Payload["question"] != "Which file?" {
		t.Fatalf("ask signal = %#v ok=%v err=%v", ask, ok, err)
	}
	if got := runtime.ProviderSafeCoreControlText(ask); got != "Agent requested user input: Which file?" {
		t.Fatalf("ask provider text = %q", got)
	}
	done, ok, err := runtime.ProjectCoreControlSignal(tools.ToolCall{
		ID:   "done-1",
		Name: runtime.CoreControlTaskComplete,
		Args: `{"output":"All done."}`,
	})
	if err != nil || !ok || done.Disposition != runtime.SignalTerminal || done.OutputText != "All done." || done.Payload["output"] != "All done." {
		t.Fatalf("done signal = %#v ok=%v err=%v", done, ok, err)
	}
	if got := runtime.ProviderSafeCoreControlText(done); got != "Agent completed the task: All done." {
		t.Fatalf("done provider text = %q", got)
	}
	_, ok, err = runtime.ProjectCoreControlSignal(tools.ToolCall{
		ID:   "bad-ask",
		Name: runtime.CoreControlAskUser,
		Args: `{}`,
	})
	if !ok || err == nil || !strings.Contains(err.Error(), "question is required") {
		t.Fatalf("invalid ask projection ok=%v err=%v", ok, err)
	}
}

func assertObjectRequiredArrays(t *testing.T, path string, schema map[string]any) {
	t.Helper()
	if typ, _ := schema["type"].(string); typ == "object" {
		switch required := schema["required"].(type) {
		case []any:
			for _, item := range required {
				if _, ok := item.(string); !ok {
					t.Fatalf("%s required item = %#v, want string", path, item)
				}
			}
		case []string:
		default:
			t.Fatalf("%s required = %#v, want array", path, schema["required"])
		}
		raw, err := json.Marshal(schema)
		if err != nil {
			t.Fatalf("marshal %s: %v", path, err)
		}
		if strings.Contains(string(raw), `"required":null`) {
			t.Fatalf("%s required must not marshal as null: %s", path, raw)
		}
		if !strings.Contains(string(raw), `"required":[`) {
			t.Fatalf("%s required array missing: %s", path, raw)
		}
	}
	properties, _ := schema["properties"].(map[string]any)
	for name, raw := range properties {
		if child, ok := raw.(map[string]any); ok {
			assertObjectRequiredArrays(t, path+"."+name, child)
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		assertObjectRequiredArrays(t, path+"[]", items)
	}
}

func TestRunProjectedTurnUsesPublicPermissionResourcesAndApprover(t *testing.T) {
	registry := tools.NewRegistry()
	err := registry.Register(tools.Define[map[string]any](
		tools.Definition{
			Name:        "write_note",
			Title:       "Write note",
			Description: "Write a note.",
			InputSchema: tools.StrictObject(map[string]any{
				"path": tools.String("path"),
				"text": tools.String("text"),
			}, []string{"path", "text"}),
			Effects:     []tools.Effect{tools.EffectWrite},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk, ResourceKinds: []string{"file"}},
			Destructive: true,
		},
		nil,
		func(inv tools.Invocation[map[string]any]) ([]tools.ResourceRef, error) {
			return []tools.ResourceRef{{Kind: "file", Value: strings.TrimSpace(inv.Args["path"].(string))}}, nil
		},
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			return tools.Result{Text: "wrote note"}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	gateway := publicModelGateway(func(ctx context.Context, req runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
		events := make(chan runtime.ModelEvent, 3)
		if req.Step == 1 {
			events <- runtime.ModelEvent{
				Type: runtime.ModelEventToolCalls,
				ToolCalls: []tools.ToolCall{{
					ID:   "write-1",
					Name: "write_note",
					Args: `{"path":"notes/today.md","text":"hello"}`,
				}},
				Reason: "tool_calls",
			}
			events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "tool_calls"}
		} else {
			events <- runtime.ModelEvent{Type: runtime.ModelEventDelta, Text: "done"}
			events <- runtime.ModelEvent{Type: runtime.ModelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	var approval tools.ApprovalRequest
	sink := &collectingEventSink{}
	result, err := runtime.RunProjectedTurn(context.Background(), runtime.ProjectedTurnOptions{
		Config: config.Config{
			Provider:     config.ProviderFake,
			Model:        "fake-model",
			SystemPrompt: "test",
		},
		ModelGateway: gateway,
		Store:        runtime.NewMemoryStore(),
		Tools:        registry,
		Approver: func(_ context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
			approval = req
			return tools.PermissionDecisionAllow, nil
		},
		Sink: sink,
	}, runtime.ProjectedTurnRequest{
		RunID:         "run-approval",
		ThreadID:      "thread-approval",
		TurnID:        "turn-approval",
		TraceID:       "trace-approval",
		PromptScopeID: "thread-approval",
		History:       []runtime.TranscriptMessage{{Role: "user", Content: "write"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != runtime.TurnStatusCompleted || result.Metrics.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	if approval.Name != "write_note" || approval.ID != "write-1" || !approval.Destructive || len(approval.Resources) != 1 || approval.Resources[0].Value != "notes/today.md" {
		t.Fatalf("approval request = %#v", approval)
	}
	var approvalEvents []string
	for _, ev := range sink.events {
		switch ev.Type {
		case "tool_approval_requested", "tool_approval_approved":
			approvalEvents = append(approvalEvents, ev.Type)
			if _, ok := ev.Metadata["approval_id"]; ok {
				t.Fatalf("approval metadata should be sanitized in public event: %#v", ev.Metadata)
			}
			if _, ok := ev.Metadata["resources"]; ok {
				t.Fatalf("approval resources should be sanitized in public event: %#v", ev.Metadata)
			}
			if ev.Metadata["approval_id_hash"] == "" || ev.ArgsHash == "" {
				t.Fatalf("approval public event missing hashes: args=%q metadata=%#v", ev.ArgsHash, ev.Metadata)
			}
		}
	}
	if !slices.Equal(approvalEvents, []string{"tool_approval_requested", "tool_approval_approved"}) {
		t.Fatalf("approval events = %#v; all=%#v", approvalEvents, sink.events)
	}
}
