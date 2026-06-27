package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/observation"
)

type Type string

const (
	StepStart              Type = "step_start"
	ProviderRequest        Type = "provider_request"
	ProviderDelta          Type = "provider_delta"
	ProviderReasoning      Type = "provider_reasoning"
	ProviderToolCallStart  Type = "provider_tool_call_start"
	ProviderToolCallDelta  Type = "provider_tool_call_delta"
	ProviderToolCallEnd    Type = "provider_tool_call_end"
	ProviderUsage          Type = "provider_usage"
	ProviderSources        Type = "provider_sources"
	ProviderFinish         Type = "provider_finish"
	ProviderRetry          Type = "provider_retry"
	ToolCall               Type = "tool_call"
	ToolResult             Type = "tool_result"
	ToolApprovalRequested  Type = "tool_approval_requested"
	ToolApprovalApproved   Type = "tool_approval_approved"
	ToolApprovalRejected   Type = "tool_approval_rejected"
	ToolApprovalTimedOut   Type = "tool_approval_timed_out"
	ToolApprovalCanceled   Type = "tool_approval_canceled"
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
	ContextCompactDebug    Type = "context_compact_debug"
	ContextContinue        Type = "context_continue"
	ThreadEntryCommitted   Type = "thread_entry_committed"
	ControlSignal          Type = "control_signal"
	BudgetExceeded         Type = "budget_exceeded"
	StepEnd                Type = "step_end"
	RunEnd                 Type = "run_end"
)

type Event struct {
	Type               Type                              `json:"type"`
	TraceID            string                            `json:"trace_id,omitempty"`
	RunID              string                            `json:"run_id"`
	ThreadID           string                            `json:"thread_id,omitempty"`
	TurnID             string                            `json:"turn_id,omitempty"`
	PromptScopeID      string                            `json:"prompt_scope_id,omitempty"`
	Step               int                               `json:"step,omitempty"`
	Provider           string                            `json:"provider,omitempty"`
	Model              string                            `json:"model,omitempty"`
	Message            string                            `json:"message,omitempty"`
	ToolID             string                            `json:"tool_id,omitempty"`
	ToolName           string                            `json:"tool_name,omitempty"`
	ToolKind           string                            `json:"tool_kind,omitempty"`
	Args               string                            `json:"args,omitempty"`
	ArgsHash           string                            `json:"args_hash,omitempty"`
	Result             string                            `json:"result,omitempty"`
	Err                string                            `json:"err,omitempty"`
	FinishReason       string                            `json:"finish_reason,omitempty"`
	RawFinishReason    string                            `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                              `json:"finish_inferred,omitempty"`
	CompletionReason   string                            `json:"completion_reason,omitempty"`
	ContinuationReason string                            `json:"continuation_reason,omitempty"`
	Duration           int64                             `json:"duration_ms,omitempty"`
	Metrics            any                               `json:"metrics,omitempty"`
	Metadata           any                               `json:"metadata,omitempty"`
	Activity           *observation.ActivityPresentation `json:"activity,omitempty"`
	Artifacts          []Artifact                        `json:"artifacts,omitempty"`
	Sources            []SourceRef                       `json:"sources,omitempty"`
	Payload            any                               `json:"-"`
	Timestamp          time.Time                         `json:"timestamp"`
}

type SourceRef struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url,omitempty"`
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

const sanitizedErrorPresentMetadataKey = "error_present"

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
		if e.Type == ThreadEntryCommitted {
			e.Metadata = withoutMetadataKey(e.Metadata, "detail")
		}
		e.Activity = sanitizeActivityPresentation(e.Activity)
		return e
	}
	if e.ArgsHash == "" && e.Args != "" {
		e.ArgsHash = StableHash(e.Args)
	}
	switch e.Type {
	case ProviderDelta, ProviderReasoning:
		e.Message = ""
	case ProviderToolCallStart, ProviderToolCallDelta, ProviderToolCallEnd:
		e.Args = ""
	case ToolCall, HostedToolCall:
		e.Args = ""
	case ToolResult, HostedToolResult, ToolApprovalRequested, ToolApprovalApproved, ToolApprovalRejected, ToolApprovalTimedOut, ToolApprovalCanceled:
		e.Metadata = withSanitizedMetadataBool(e.Metadata, sanitizedErrorPresentMetadataKey, strings.TrimSpace(e.Err) != "")
		e.Result = ""
		e.Err = ""
	case ControlSignal:
		e.Result = ""
		e.Err = ""
	case ContextCompact:
		e.Result = ""
	case ThreadEntryCommitted:
		e.Message = ""
		e.Args = ""
		e.Result = ""
		e.Err = ""
		e.Metadata = withoutMetadataKey(e.Metadata, "detail")
	}
	e.Err = Redact(e.Err)
	e.Message = Redact(e.Message)
	e.Args = ""
	e.Result = Redact(e.Result)
	e.Metadata = sanitizeMetadata(e.Metadata)
	e.Payload = nil
	e.Activity = sanitizeActivityPresentation(e.Activity)
	e.Sources = sanitizeSourceRefs(e.Sources)
	return e
}

func withoutMetadataKey(value any, key string) any {
	key = strings.TrimSpace(key)
	if key == "" {
		return value
	}
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for itemKey, item := range v {
			if itemKey == key {
				continue
			}
			out[itemKey] = item
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(v))
		for itemKey, item := range v {
			if itemKey == key {
				continue
			}
			out[itemKey] = item
		}
		return out
	default:
		return value
	}
}

func sanitizePathRefs(e Event) Event {
	e.Message = SafePathRefsText(e.Message)
	e.Args = SafePathRefsText(e.Args)
	e.Result = SafePathRefsText(e.Result)
	e.Err = SafePathRefsText(e.Err)
	e.Metadata = sanitizePathMetadata(e.Metadata)
	e.Activity = sanitizeActivityPresentation(e.Activity)
	e.Sources = sanitizeSourceRefs(e.Sources)
	for i := range e.Artifacts {
		if e.Artifacts[i].Path != "" {
			e.Artifacts[i].Path = SafePathLabel(e.Artifacts[i].Path)
		}
	}
	return e
}

func sanitizeSourceRefs(in []SourceRef) []SourceRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]SourceRef, 0, len(in))
	for _, ref := range in {
		title := safeActivityText(ref.Title, 240)
		url := safeSourceURL(ref.URL)
		if title == "" && url == "" {
			continue
		}
		out = append(out, SourceRef{Title: title, URL: url})
	}
	return out
}

func safeSourceURL(value string) string {
	value = strings.TrimSpace(SafePathRefsText(value))
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return Redact(value)
	}
	return safeActivityText(value, 500)
}

func sanitizeActivityPresentation(in *observation.ActivityPresentation) *observation.ActivityPresentation {
	if in == nil {
		return nil
	}
	out := &observation.ActivityPresentation{
		Label:       safeActivityText(in.Label, 200),
		Description: safeActivityText(in.Description, 500),
		Renderer:    in.Renderer,
		Chips:       sanitizeActivityChips(in.Chips),
		TargetRefs:  sanitizeActivityTargetRefs(in.TargetRefs),
		Payload:     sanitizeActivityPayload(in.Payload, 0),
	}
	if out.Label == "" &&
		out.Description == "" &&
		out.Renderer == "" &&
		len(out.Chips) == 0 &&
		len(out.TargetRefs) == 0 &&
		len(out.Payload) == 0 {
		return nil
	}
	return out
}

func safeActivityText(value string, limit int) string {
	value = strings.TrimSpace(SafePathRefsText(value))
	if value == "" {
		return ""
	}
	if limit > 0 && len([]rune(value)) > limit {
		runes := []rune(value)
		value = string(runes[:limit])
	}
	return Redact(value)
}

func sanitizeActivityChips(in []observation.ActivityChip) []observation.ActivityChip {
	if len(in) == 0 {
		return nil
	}
	out := make([]observation.ActivityChip, 0, len(in))
	for _, chip := range in {
		item := observation.ActivityChip{
			Kind:  safeActivityToken(chip.Kind, 64),
			Label: safeActivityText(chip.Label, 120),
			Value: safeActivityText(chip.Value, 120),
			Tone:  safeActivityToken(chip.Tone, 32),
		}
		if item.Kind == "" || item.Label == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func sanitizeActivityTargetRefs(in []observation.ActivityTargetRef) []observation.ActivityTargetRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]observation.ActivityTargetRef, 0, len(in))
	for _, ref := range in {
		item := observation.ActivityTargetRef{
			Kind:  safeActivityToken(ref.Kind, 64),
			Label: safeActivityText(ref.Label, 240),
			URI:   safeActivityURI(ref.URI),
			Path:  safeActivityPath(ref.Path),
			Line:  ref.Line,
		}
		if item.Line < 0 {
			item.Line = 0
		}
		if item.Kind == "" || item.Label == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func sanitizeActivityPayload(in map[string]any, depth int) map[string]any {
	if len(in) == 0 || depth > 4 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = safeActivityToken(key, 80)
		if key == "" {
			continue
		}
		if sanitized, ok := sanitizeActivityValue(key, value, depth+1); ok {
			out[key] = sanitized
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sanitizeActivityValue(key string, value any, depth int) (any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case string:
		return safeActivityPayloadString(key, v), true
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v, true
	case map[string]string:
		if depth > 4 {
			return nil, false
		}
		out := map[string]any{}
		for itemKey, itemValue := range v {
			itemKey = safeActivityToken(itemKey, 80)
			if itemKey == "" {
				continue
			}
			out[itemKey] = safeActivityPayloadString(itemKey, itemValue)
		}
		return out, true
	case map[string]any:
		return sanitizeActivityPayload(v, depth), true
	case []string:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, safeActivityPayloadString(key, item))
		}
		return out, true
	case []map[string]any:
		if depth > 4 {
			return nil, false
		}
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, sanitizeActivityPayload(item, depth))
		}
		return out, true
	case []any:
		if depth > 4 {
			return nil, false
		}
		out := make([]any, 0, len(v))
		for _, item := range v {
			if sanitized, ok := sanitizeActivityValue(key, item, depth+1); ok {
				out = append(out, sanitized)
			}
		}
		return out, true
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "[redacted]", true
		}
		return safeActivityPayloadString(key, string(data)), true
	}
}

func safeActivityPayloadString(key string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if metadataKeyIsPath(key) {
		value = SafePathLabel(value)
	} else {
		value = SafePathRefsText(value)
	}
	value = Redact(value)
	const limit = 8000
	if len([]rune(value)) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit])
}

func safeActivityToken(value string, limit int) string {
	value = strings.TrimSpace(value)
	if value == "" || len([]rune(value)) > limit {
		return ""
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':':
			continue
		default:
			return ""
		}
	}
	return value
}

func safeActivityURI(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len([]rune(value)) > 1000 {
		return ""
	}
	if strings.HasPrefix(value, "http://") ||
		strings.HasPrefix(value, "https://") ||
		strings.HasPrefix(value, "artifact://") {
		return Redact(value)
	}
	return ""
}

func safeActivityPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return SafePathLabel(value)
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
	case []map[string]string:
		out := make([]map[string]string, len(v))
		for i, item := range v {
			out[i] = make(map[string]string, len(item))
			for itemKey, value := range item {
				out[i][itemKey] = sanitizeMetadataString(itemKey, value)
			}
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

func withSanitizedMetadataBool(value any, key string, enabled bool) any {
	if !enabled {
		return value
	}
	switch v := value.(type) {
	case nil:
		return map[string]any{key: true}
	case map[string]any:
		out := make(map[string]any, len(v)+1)
		for itemKey, item := range v {
			out[itemKey] = item
		}
		out[key] = true
		return out
	case map[string]string:
		out := make(map[string]any, len(v)+1)
		for itemKey, item := range v {
			out[itemKey] = item
		}
		out[key] = true
		return out
	default:
		return map[string]any{
			"details": sanitizeMetadata(value),
			key:       true,
		}
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
	case []map[string]string:
		out := make([]map[string]string, len(v))
		for i, item := range v {
			out[i] = make(map[string]string, len(item))
			for itemKey, value := range item {
				out[i][itemKey] = sanitizePathMetadataWithKey(itemKey, value).(string)
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
	case []map[string]string:
		out := make([]map[string]string, len(v))
		for i, item := range v {
			out[i] = make(map[string]string, len(item))
			for itemKey, value := range item {
				out[i][itemKey] = sanitizeMetadataString(itemKey, value)
			}
		}
		return out
	default:
		if value, ok := metadataUnderlyingString(value); ok {
			return sanitizeMetadataString(key, value)
		}
		return sanitizeMetadata(value)
	}
}

func metadataUnderlyingString(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.String {
		return "", false
	}
	return rv.String(), true
}

func sanitizeMetadataString(key, value string) string {
	if publicMetadataTextKey(key) && safeMetadataText(value) && Redact(value) == value {
		return value
	}
	if publicMetadataStringKey(key) && safeMetadataToken(value) && Redact(value) == value {
		return value
	}
	return safeStringLabel(value)
}

func publicMetadataTextKey(key string) bool {
	return key == "next_action"
}

func publicMetadataStringKey(key string) bool {
	if strings.HasPrefix(key, "correlation.") || strings.HasPrefix(key, "host.") {
		return true
	}
	switch key {
	case "server_id", "skill_id", "tool_name", "remote_tool", "source_kind", "source_label", "status", "transport", "protocol_version", "failure_category", "next_action", "capability", "permission_mode", "content_hash", "prompt_sha256", "control_disposition":
		return true
	case "approval_id", "state", "kind", "effect", "effects":
		return true
	case "pending_handle", "pending_state":
		return true
	case "pressure_signal", "pressure_source", "confidence", "estimate_source", "estimate_method", "compaction_trigger", "trigger", "reason", "source":
		return true
	case "operation_id", "request_id", "stage", "phase", "compaction_id", "compaction_window_id", "previous_compaction_id", "provider_state_kind":
		return true
	default:
		return false
	}
}

func safeMetadataText(value string) bool {
	if len(value) > 240 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '_', '-', '.', ':', ',', ' ', '(', ')':
			continue
		default:
			return false
		}
	}
	return true
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
		case '_', '-', '.', ':', ',':
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
