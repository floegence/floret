package testui

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/sessiontree"
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
	sink := r.sink
	r.mu.Unlock()
	if sink == nil {
		return
	}
	switch ev.Type {
	case event.ProviderDelta:
		emitEngineStreamEvent(sink, AgentStreamProviderDelta, ev)
	case event.ToolCall:
		emitEngineStreamEvent(sink, AgentStreamToolCall, ev)
	case event.ToolResult:
		emitEngineStreamEvent(sink, AgentStreamToolResult, ev)
	}
}

func (r *streamingEventRecorder) Snapshot() []event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.Event(nil), r.events...)
}

type streamingHarnessRecorder struct {
	mu       sync.Mutex
	events   []agentharness.HarnessEvent
	repo     sessiontree.Repo
	threadID string
	sink     AgentStreamSink
}

func (r *streamingHarnessRecorder) SetStreamSink(sink AgentStreamSink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sink = sink
}

func (r *streamingHarnessRecorder) EmitHarness(ev agentharness.HarnessEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	sink := r.sink
	repo := r.repo
	threadID := r.threadID
	r.mu.Unlock()
	if sink == nil {
		return
	}
	switch ev.Type {
	case agentharness.EventTurnStarted:
		sink.EmitAgentStream(AgentStreamEvent{
			Type:      AgentStreamTurnStarted,
			SessionID: threadID,
			TurnID:    ev.TurnID,
			At:        ev.Timestamp,
			Metadata:  ev.Metadata,
		})
	case agentharness.EventEntryAppended:
		streamEntryAppended(context.Background(), sink, repo, threadID, ev)
	}
}

func (r *streamingHarnessRecorder) Snapshot() []agentharness.HarnessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]agentharness.HarnessEvent(nil), r.events...)
}

func streamEntryAppended(ctx context.Context, sink AgentStreamSink, repo sessiontree.Repo, threadID string, ev agentharness.HarnessEvent) {
	if repo == nil || ev.EntryID == "" {
		return
	}
	entry, err := repo.Entry(ctx, threadID, ev.EntryID)
	if err != nil {
		return
	}
	observed := publicObservedEntries(observeEntries([]sessiontree.Entry{entry}))[0]
	eventType := AgentStreamAssistantMessageAppended
	switch entry.Type {
	case sessiontree.EntryUserMessage:
		eventType = AgentStreamUserMessageAppended
	case sessiontree.EntryAssistantMessage:
		eventType = AgentStreamAssistantMessageAppended
	case sessiontree.EntryToolCall:
		eventType = AgentStreamToolCall
	case sessiontree.EntryToolResult:
		eventType = AgentStreamToolResult
	case sessiontree.EntryTurnMarker:
		if entry.TurnStatus == sessiontree.TurnSavePoint {
			eventType = AgentStreamTurnSavePoint
		} else {
			return
		}
	case sessiontree.EntryRunFailure:
		eventType = AgentStreamTurnFailed
	default:
		return
	}
	sink.EmitAgentStream(AgentStreamEvent{
		Type:      eventType,
		SessionID: threadID,
		TurnID:    entry.TurnID,
		EntryID:   entry.ID,
		At:        entry.CreatedAt,
		Entry:     &observed,
		Error:     entry.Error,
	})
}

func emitEngineStreamEvent(sink AgentStreamSink, typ AgentStreamEventType, ev event.Event) {
	if sink == nil {
		return
	}
	evCopy := event.Sanitize(ev)
	stream := AgentStreamEvent{
		Type:        typ,
		SessionID:   ev.SessionID,
		TurnID:      ev.RunID,
		Step:        ev.Step,
		At:          ev.Timestamp,
		EngineEvent: &evCopy,
		Message:     evCopy.Message,
		Error:       evCopy.Err,
	}
	if ev.Type == event.ToolResult {
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
