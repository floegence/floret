package event

import (
	"sync"
	"time"
)

type Type string

const (
	StepStart       Type = "step_start"
	ProviderRequest Type = "provider_request"
	ProviderDelta   Type = "provider_delta"
	ProviderRetry   Type = "provider_retry"
	ToolCall        Type = "tool_call"
	ToolResult      Type = "tool_result"
	ContextCompact  Type = "context_compact"
	StepEnd         Type = "step_end"
	RunEnd          Type = "run_end"
)

type Event struct {
	Type      Type
	RunID     string
	Step      int
	Message   string
	ToolID    string
	ToolName  string
	Args      string
	Result    string
	Err       string
	Timestamp time.Time
}

type Sink interface {
	Emit(Event)
}

type Recorder struct {
	mu     sync.Mutex
	Events []Event
}

func (r *Recorder) Emit(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, e)
}

func (r *Recorder) Snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.Events...)
}
