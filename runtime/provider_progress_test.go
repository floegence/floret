package runtime

import (
	"github.com/floegence/floret/v7/florettest"
	"testing"
	"time"

	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func TestRequestProgressAndPreviewsAreAttemptFenced(t *testing.T) {
	service, actor, sink, _ := liveActivityFixture(t)
	started := time.Unix(100, 0)
	emit := func(kind observation.EventType, at time.Time, stream *StreamObservation, epoch int) {
		sink.EmitEvent(Event{Type: kind, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Timestamp: at, Stream: stream, attemptLogicalID: "logical-live", attemptID: "attempt", attemptEpoch: epoch})
	}
	emit(observation.EventTypeProviderRequest, started, nil, 1)
	request := service.currentView(actor).RunProgress.Request
	if request == nil || !request.StartedAt.Equal(started) || request.FirstOutputAt != nil {
		t.Fatalf("request start: %+v", request)
	}
	emit(observation.EventTypeProviderDelta, started.Add(time.Second), &StreamObservation{Type: StreamObservationAssistantDelta}, 1)
	if service.currentView(actor).RunProgress.Phase != ThreadRunPhaseWaitingResponse {
		t.Fatal("empty event ended waiting")
	}
	n := 4
	stream := &StreamObservation{Type: StreamObservationToolCallDelta, ToolCallStream: &ToolCallStream{ID: "edit", Name: "editing_tool"}, LogicalRequestID: "logical-live", AttemptID: "attempt", AttemptEpoch: 1, EstimatedOutputTokens: 13, ArgumentPreviewUpdated: true, ArgumentPreview: &tools.ArgumentPreview{DisplayName: "main.go", Additions: &n}}
	emit(observation.EventTypeProviderToolCallDelta, started.Add(4*time.Second), stream, 1)
	view := service.currentView(actor)
	if len(view.Items) != 0 || len(view.ToolCallPreviews) != 1 || view.RunProgress.Request.EstimatedOutputTokens != 13 || view.RunProgress.Request.FirstOutputAt == nil {
		t.Fatalf("generation became invocation or lost progress: %+v", view)
	}
	*view.ToolCallPreviews[0].Preview.Additions = 99
	*view.RunProgress.Request.FirstOutputAt = started
	if *service.currentView(actor).ToolCallPreviews[0].Preview.Additions != 4 || service.currentView(actor).RunProgress.Request.FirstOutputAt.Equal(started) {
		t.Fatal("mutable snapshots alias actor state")
	}
	actor.state.view.RunProgress = nil // approval hides run indicator, not preview
	emit(observation.EventTypeToolDispatchStarted, started.Add(5*time.Second), nil, 1)
	if len(service.currentView(actor).ToolCallPreviews) != 1 {
		t.Fatal("execution lost frozen preview")
	}
	emit(observation.EventTypeToolResult, started.Add(6*time.Second), nil, 1)
	// ToolResult must name the exact call, never clear another invocation.
	sink.EmitEvent(Event{Type: observation.EventTypeToolResult, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", ToolID: "edit"})
	if len(service.currentView(actor).ToolCallPreviews) != 0 {
		t.Fatal("actual result retained preview")
	}
	emit(observation.EventTypeProviderRequest, started.Add(7*time.Second), nil, 2)
	emit(observation.EventTypeProviderToolCallDelta, started.Add(8*time.Second), stream, 1)
	view = service.currentView(actor)
	if len(view.ToolCallPreviews) != 0 || view.RunProgress.Request.EstimatedOutputTokens != 0 || view.RunProgress.Request.FirstOutputAt != nil {
		t.Fatal("retired attempt contaminated new request")
	}
}

func TestTransportProgressIsNonSemanticAndAttemptFenced(t *testing.T) {
	service, actor, sink, _ := liveActivityFixture(t)
	at := time.Unix(100, 0)
	emit := func(kind observation.EventType, epoch int, transport *observation.ProviderTransportProgress) {
		sink.EmitEvent(Event{Type: kind, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", Timestamp: at, Transport: transport, attemptLogicalID: "logical-live", attemptID: "attempt", attemptEpoch: epoch})
	}
	failed := &observation.ProviderTransportProgress{LogicalRequestID: "logical-live", AttemptID: "attempt", AttemptEpoch: 1, Observation: provider.TransportObservation{Transport: provider.TransportWebSocket, Phase: provider.TransportFailed, NextTransport: provider.TransportHTTPSSE, Reason: "connection_lost"}}
	emit(observation.EventTypeProviderRequest, 1, nil)
	emit(observation.EventTypeProviderTransport, 1, failed)
	view := service.currentView(actor)
	if view.RunProgress.Transport == nil || view.RunProgress.Request.FirstOutputAt != nil || view.RunProgress.Request.LastProgressAt != nil || len(view.Items) != 0 {
		t.Fatalf("transport became output: %+v", view)
	}
	view.RunProgress.Transport.Reason = "mutated"
	if service.currentView(actor).RunProgress.Transport.Reason != "connection_lost" {
		t.Fatal("mutable transport snapshot aliases actor")
	}
	emit(observation.EventTypeProviderRetry, 1, nil)
	if service.currentView(actor).RunProgress.Transport == nil {
		t.Fatal("retry lost switch hint")
	}
	emit(observation.EventTypeProviderRequest, 2, nil)
	next := *failed
	next.AttemptEpoch = 2
	next.Transport = provider.TransportHTTPSSE
	next.Phase = provider.TransportSending
	next.NextTransport = ""
	next.Reason = ""
	emit(observation.EventTypeProviderTransport, 2, &next)
	emit(observation.EventTypeProviderTransport, 1, failed)
	sink.EmitEvent(Event{Type: observation.EventTypeProviderTransport, ThreadID: "thread-live", RunID: "title", Transport: failed})
	view = service.currentView(actor)
	if view.RunProgress.Transport.Transport != provider.TransportHTTPSSE || view.RunProgress.Transport.AttemptEpoch != 2 || view.RunProgress.Request.EstimatedOutputTokens != 0 {
		t.Fatal("stale or auxiliary observation replaced main request")
	}
	emit(observation.EventTypeProviderFinish, 2, nil)
	if service.currentView(actor).RunProgress.Transport != nil {
		t.Fatal("terminal retained transport progress")
	}
}

func TestTransportObservationTraversesProviderEngineAndRuntime(t *testing.T) {
	recorder := &runtimeEventRecorder{}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "transport", StateCompatibilityKey: "v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{
		{Type: provider.EventTransport, Transport: &provider.TransportObservation{Transport: provider.TransportHTTPSSE, Phase: provider.TransportReceiving}},
		{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"},
	}})
	agent, err := testAgent(gateway, WithAgentEventSink(recorder))
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "transport-create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "transport-send", Input: UserInput{Text: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.LastOutcome != nil && v.Activity == ThreadActivityIdle })
	if view.Failure != nil || view.RunProgress != nil {
		t.Fatalf("terminal=%+v", view)
	}
	found := false
	for _, ev := range recorder.snapshot() {
		if ev.Type == observation.EventTypeProviderTransport {
			found = true
			if ev.Transport == nil || ev.Transport.AttemptEpoch != 1 || ev.Transport.AttemptID == "" || ev.Transport.LogicalRequestID == "" || ev.ThreadID != created.ThreadID || ev.RunID == "" || ev.TurnID == "" || ev.Stream != nil || ev.ProviderLastProgressAt != nil {
				t.Fatalf("invalid transport event: %+v", ev)
			}
			if err := ev.Validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !found {
		t.Fatal("transport observation lost across provider bridge")
	}
}
