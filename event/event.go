package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Type string

const (
	StepStart              Type = "step_start"
	ProviderRequest        Type = "provider_request"
	ProviderDelta          Type = "provider_delta"
	ProviderReasoning      Type = "provider_reasoning"
	ProviderUsage          Type = "provider_usage"
	ProviderFinish         Type = "provider_finish"
	ProviderRetry          Type = "provider_retry"
	ToolCall               Type = "tool_call"
	ToolResult             Type = "tool_result"
	HostedToolCall         Type = "hosted_tool_call"
	HostedToolResult       Type = "hosted_tool_result"
	MCPServerConnecting    Type = "mcp_server_connecting"
	MCPServerReady         Type = "mcp_server_ready"
	MCPServerFailed        Type = "mcp_server_failed"
	MCPToolsListed         Type = "mcp_tools_listed"
	MCPToolCall            Type = "mcp_tool_call"
	MCPToolResult          Type = "mcp_tool_result"
	SkillDetected          Type = "skill_detected"
	SkillLoaded            Type = "skill_loaded"
	SkillBlocked           Type = "skill_blocked"
	SkillInstallRequired   Type = "skill_install_required"
	SkillDisclosureApplied Type = "skill_disclosure_applied"
	ContextCompact         Type = "context_compact"
	ContextContinue        Type = "context_continue"
	BudgetExceeded         Type = "budget_exceeded"
	StepEnd                Type = "step_end"
	RunEnd                 Type = "run_end"
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
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	Path      string `json:"path,omitempty"`
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

func SanitizeWithPolicy(e Event, policy SinkPolicy) Event {
	return sanitize(e, policy)
}

func SanitizePathRefs(e Event) Event {
	return sanitizePathRefs(e)
}

func sanitize(e Event, policy SinkPolicy) Event {
	e = sanitizePathRefs(e)
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
	return e
}

func sanitizePathRefs(e Event) Event {
	e.Message = SafePathRefsText(e.Message)
	e.Args = SafePathRefsText(e.Args)
	e.Result = SafePathRefsText(e.Result)
	e.Err = SafePathRefsText(e.Err)
	e.Metadata = sanitizePathMetadata(e.Metadata)
	for i := range e.Artifacts {
		if e.Artifacts[i].Path != "" {
			e.Artifacts[i].Path = SafePathLabel(e.Artifacts[i].Path)
		}
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

var pathRefPattern = regexp.MustCompile(`(?:/[^\s,"'<>]+|[A-Za-z]:\\[^\s,"'<>]+)`)

func SafePathRefsText(value string) string {
	if value == "" {
		return ""
	}
	matches := pathRefPattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	var out strings.Builder
	last := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		text := value[start:end]
		if preservePathRef(value, start, text) {
			continue
		}
		path := strings.TrimRight(text, ".,;:!?)")
		if path == "" {
			continue
		}
		out.WriteString(value[last:start])
		out.WriteString(SafePathLabel(path))
		out.WriteString(text[len(path):])
		last = end
	}
	if last == 0 {
		return value
	}
	out.WriteString(value[last:])
	return out.String()
}

func preservePathRef(value string, start int, text string) bool {
	if strings.HasPrefix(text, "/artifacts/") {
		return true
	}
	if start > 0 && value[start-1] == ':' {
		schemeStart := start - 1
		for schemeStart > 0 {
			r := value[schemeStart-1]
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				schemeStart--
				continue
			}
			break
		}
		switch strings.ToLower(value[schemeStart : start-1]) {
		case "http", "https", "ws", "wss":
			return true
		}
	}
	return false
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
			out[key] = sanitizeMetadataString(key, item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = sanitizeMetadataWithKey(key, item)
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

func sanitizePathMetadata(value any) any {
	return sanitizePathMetadataWithKey("", value)
}

func sanitizePathMetadataWithKey(key string, value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if metadataKeyIsPath(key) {
			return SafePathLabel(v)
		}
		return SafePathRefsText(v)
	case map[string]string:
		out := make(map[string]string, len(v))
		for itemKey, item := range v {
			out[itemKey] = sanitizePathMetadataWithKey(itemKey, item).(string)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for itemKey, item := range v {
			out[itemKey] = sanitizePathMetadataWithKey(itemKey, item)
		}
		return out
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			if metadataKeyIsPath(key) {
				out[i] = SafePathLabel(item)
			} else {
				out[i] = SafePathRefsText(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizePathMetadataWithKey(key, item)
		}
		return out
	default:
		return value
	}
}

func metadataKeyIsPath(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return key == "path" ||
		key == "workdir" ||
		key == "cwd" ||
		key == "dir" ||
		key == "directory" ||
		strings.HasSuffix(key, "_path") ||
		strings.HasSuffix(key, "_dir") ||
		strings.HasSuffix(key, "_directory")
}

func sanitizeMetadataWithKey(key string, value any) any {
	switch v := value.(type) {
	case string:
		return sanitizeMetadataString(key, v)
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			out[i] = sanitizeMetadataString(key, item)
		}
		return out
	default:
		return sanitizeMetadata(value)
	}
}

func sanitizeMetadataString(key, value string) string {
	if publicMetadataStringKey(key) && safeMetadataToken(value) {
		return value
	}
	return safeStringLabel(value)
}

func publicMetadataStringKey(key string) bool {
	switch key {
	case "server_id", "skill_id", "tool_name", "remote_tool", "source_kind", "source_label", "status", "transport", "protocol_version", "failure_category", "next_action", "capability", "permission_mode", "content_hash", "prompt_sha256":
		return true
	default:
		return false
	}
}

func safeMetadataToken(value string) bool {
	if len(value) > 240 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':', ',', '/', ' ', '(', ')':
			continue
		default:
			return false
		}
	}
	return true
}

func safeStringLabel(value string) string {
	if value == "" {
		return ""
	}
	redacted := Redact(value)
	return "[redacted]#" + StableHash(redacted)[:12]
}
