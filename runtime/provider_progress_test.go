package runtime

import (
	"testing"
	"time"

	"github.com/floegence/floret/v7/observation"
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
