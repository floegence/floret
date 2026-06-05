package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"sync"
	"time"
)

type Type string

const (
	StepStart         Type = "step_start"
	ProviderRequest   Type = "provider_request"
	ProviderDelta     Type = "provider_delta"
	ProviderReasoning Type = "provider_reasoning"
	ProviderUsage     Type = "provider_usage"
	ProviderFinish    Type = "provider_finish"
	ProviderRetry     Type = "provider_retry"
	ToolCall          Type = "tool_call"
	ToolResult        Type = "tool_result"
	HostedToolCall    Type = "hosted_tool_call"
	HostedToolResult  Type = "hosted_tool_result"
	ContextCompact    Type = "context_compact"
	ContextContinue   Type = "context_continue"
	BudgetExceeded    Type = "budget_exceeded"
	StepEnd           Type = "step_end"
	RunEnd            Type = "run_end"
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

type Sensitivity string

const (
	SensitivityPublic Sensitivity = "public"
	SensitivityRaw    Sensitivity = "raw"
)

type Redactor func(string) string

type SinkPolicy struct {
	AllowRaw bool
	Redactor Redactor
}

func Sanitize(e Event) Event {
	return sanitize(e, SinkPolicy{})
}

func sanitize(e Event, policy SinkPolicy) Event {
	if policy.AllowRaw {
		if policy.Redactor != nil {
			e.Message = policy.Redactor(e.Message)
			e.Args = policy.Redactor(e.Args)
			e.Result = policy.Redactor(e.Result)
			e.Err = policy.Redactor(e.Err)
		}
		return e
	}
	if e.ArgsHash == "" && e.Args != "" {
		e.ArgsHash = StableHash(e.Args)
	}
	switch e.Type {
	case ProviderDelta, ProviderReasoning:
		e.Message = ""
	case ToolCall, HostedToolCall:
		e.Args = ""
	case ToolResult, HostedToolResult:
		e.Result = ""
		e.Err = ""
	case ContextCompact:
		e.Result = ""
	}
	e.Err = Redact(e.Err)
	e.Message = Redact(e.Message)
	e.Args = ""
	e.Result = Redact(e.Result)
	e.Metadata = sanitizeMetadata(e.Metadata)
	for i := range e.Artifacts {
		e.Artifacts[i].Path = SafePathLabel(e.Artifacts[i].Path)
	}
	return e
}

type SerialSink struct {
	mu     sync.Mutex
	Inner  Sink
	Policy SinkPolicy
}

func (s *SerialSink) Emit(e Event) {
	if s == nil || s.Inner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Inner.Emit(sanitize(e, s.Policy))
}

func NewSerialSink(inner Sink, policy SinkPolicy) *SerialSink {
	return &SerialSink{Inner: inner, Policy: policy}
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

func SafePathLabel(path string) string {
	if path == "" {
		return ""
	}
	base := filepath.Base(path)
	if base == "." || base == string(filepath.Separator) || base == "" {
		base = "artifact"
	}
	sum := sha256.Sum256([]byte(path))
	return base + "#" + hex.EncodeToString(sum[:])[:12]
}

func sanitizeMetadata(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return safeStringLabel(v)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, item := range v {
			out[key] = safeStringLabel(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = sanitizeMetadata(item)
		}
		return out
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			out[i] = safeStringLabel(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizeMetadata(item)
		}
		return out
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "[redacted]"
		}
		return safeStringLabel(string(data))
	}
}

func safeStringLabel(value string) string {
	if value == "" {
		return ""
	}
	redacted := Redact(value)
	return "[redacted]#" + StableHash(redacted)[:12]
}
