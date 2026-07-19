package testui

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/observation"
)

type agentStream struct {
	seq    int64
	ch     chan AgentStreamEvent
	closed atomic.Bool
}

func newAgentStream(buffer int) *agentStream {
	if buffer <= 0 {
		buffer = 256
	}
	return &agentStream{ch: make(chan AgentStreamEvent, buffer)}
}

func (s *agentStream) Events() <-chan AgentStreamEvent {
	return s.ch
}

func (s *agentStream) EmitAgentStream(ev AgentStreamEvent) {
	if s == nil || s.closed.Load() {
		return
	}
	if ev.At.IsZero() {
		ev.At = time.Now()
	}
	ev.Sequence = atomic.AddInt64(&s.seq, 1)
	select {
	case s.ch <- ev:
	default:
	}
}

func (s *agentStream) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	close(s.ch)
}

type streamingEventRecorder struct {
	mu     sync.Mutex
	events []event.Event
	sink   AgentStreamSink
}

func (r *streamingEventRecorder) SetStreamSink(sink AgentStreamSink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sink = sink
}

func (r *streamingEventRecorder) Emit(ev event.Event) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	events := append([]event.Event(nil), r.events...)
	sink := r.sink
	r.mu.Unlock()
	if sink == nil {
		return
	}
	activity := activityTimelineForObservation(observation.ActivityRunMeta{RunID: ev.RunID, ThreadID: ev.ThreadID, TurnID: ev.TurnID, TraceID: ev.TraceID}, eventsForRun(events, ev.RunID), time.Now())
	switch ev.Type {
	case event.ProviderDelta:
		emitEngineStreamEvent(sink, AgentStreamProviderDelta, ev)
	case event.ProviderUsage:
		if status, ok := contextStatusFromEngineEvent(ev); ok {
			sink.EmitAgentStream(AgentStreamEvent{
				Type:          AgentStreamContextStatus,
				SessionID:     ev.ThreadID,
				TurnID:        ev.TurnID,
				Step:          ev.Step,
				At:            ev.Timestamp,
				ContextStatus: &status,
				EngineEvent:   &ev,
			})
		}
	case event.ProviderFinish:
		emitEngineStreamEvent(sink, AgentStreamProviderDelta, ev)
	case event.ContextCompact:
		if compact, ok := compactionEventFromEngineEvent(ev); ok {
			sink.EmitAgentStream(AgentStreamEvent{
				Type:        AgentStreamContextCompaction,
				SessionID:   ev.ThreadID,
				TurnID:      ev.TurnID,
				Step:        ev.Step,
				At:          ev.Timestamp,
				Compaction:  &compact,
				EngineEvent: &ev,
				Message:     compact.SummaryPreview,
				Error:       compact.Error,
			})
		}
	case event.ContextCompactDebug:
		if debug, ok := compactionDebugEventFromEngineEvent(ev); ok {
			sink.EmitAgentStream(AgentStreamEvent{
				Type:            AgentStreamContextCompactionDebug,
				SessionID:       ev.ThreadID,
				TurnID:          ev.TurnID,
				Step:            ev.Step,
				At:              ev.Timestamp,
				CompactionDebug: &debug,
				EngineEvent:     &ev,
				Message:         string(debug.Stage),
				Error:           debug.Error,
			})
		}
	case event.ToolCall, event.HostedToolCall:
		emitEngineStreamEvent(sink, AgentStreamToolCall, ev, activity)
	case event.ToolResult, event.HostedToolResult:
		emitEngineStreamEvent(sink, AgentStreamToolResult, ev, activity)
	case event.ToolApprovalRequested, event.ToolApprovalApproved, event.ToolApprovalRejected, event.ToolApprovalTimedOut, event.ToolApprovalCanceled, event.BudgetExceeded, event.RunEnd:
		emitEngineStreamEvent(sink, AgentStreamActivity, ev, activity)
	}
}

func (r *streamingEventRecorder) Snapshot() []event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.Event(nil), r.events...)
}

func emitEngineStreamEvent(sink AgentStreamSink, typ AgentStreamEventType, ev event.Event, activity ...observation.ActivityTimeline) {
	if sink == nil {
		return
	}
	evCopy := ev
	stream := AgentStreamEvent{
		Type:        typ,
		SessionID:   ev.ThreadID,
		TurnID:      ev.TurnID,
		Step:        ev.Step,
		At:          ev.Timestamp,
		EngineEvent: &evCopy,
		Message:     evCopy.Message,
		Error:       evCopy.Err,
	}
	if len(activity) > 0 {
		stream.ActivityTimeline = &activity[0]
	}
	if ev.Type == event.ToolResult || ev.Type == event.HostedToolResult {
		stream.Message = evCopy.Result
	}
	sink.EmitAgentStream(stream)
}

func agentStreamEventForResult(result AgentRunResponse) AgentStreamEventType {
	if result.Status == string(engine.Completed) || result.Status == string(engine.Waiting) {
		return AgentStreamTurnCompleted
	}
	return AgentStreamTurnFailed
}
