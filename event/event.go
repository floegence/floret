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
	ProviderUsage   Type = "provider_usage"
	ProviderFinish  Type = "provider_finish"
	ProviderRetry   Type = "provider_retry"
	ToolCall        Type = "tool_call"
	ToolResult      Type = "tool_result"
	ContextCompact  Type = "context_compact"
	ContextContinue Type = "context_continue"
	BudgetExceeded  Type = "budget_exceeded"
	StepEnd         Type = "step_end"
	RunEnd          Type = "run_end"
)

type Event struct {
	Type               Type       `json:"type"`
	TraceID            string     `json:"trace_id,omitempty"`
	RunID              string     `json:"run_id"`
	SessionID          string     `json:"session_id,omitempty"`
	Step               int        `json:"step,omitempty"`
	Provider           string     `json:"provider,omitempty"`
	Model              string     `json:"model,omitempty"`
	Message            string     `json:"message,omitempty"`
	ToolID             string     `json:"tool_id,omitempty"`
	ToolName           string     `json:"tool_name,omitempty"`
	ToolKind           string     `json:"tool_kind,omitempty"`
	Args               string     `json:"args,omitempty"`
	ArgsHash           string     `json:"args_hash,omitempty"`
	Result             string     `json:"result,omitempty"`
	Err                string     `json:"err,omitempty"`
	FinishReason       string     `json:"finish_reason,omitempty"`
	RawFinishReason    string     `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool       `json:"finish_inferred,omitempty"`
	CompletionReason   string     `json:"completion_reason,omitempty"`
	ContinuationReason string     `json:"continuation_reason,omitempty"`
	Duration           int64      `json:"duration_ms,omitempty"`
	Metrics            any        `json:"metrics,omitempty"`
	Metadata           any        `json:"metadata,omitempty"`
	Artifacts          []Artifact `json:"artifacts,omitempty"`
	Timestamp          time.Time  `json:"timestamp"`
}

type Artifact struct {
	Kind string `json:"kind,omitempty"`
	Path string `json:"path,omitempty"`
	MIME string `json:"mime,omitempty"`
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
