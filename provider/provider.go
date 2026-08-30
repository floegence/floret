// Package provider defines the complete model-provider boundary consumed by
// Floret's runtime. Gateways own transport; the runtime owns the Agent loop.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v6/config"
	"github.com/floegence/floret/v6/identity"
	"github.com/floegence/floret/v6/tools"
)

var (
	// ErrContextOverflow reports that a provider rejected a request because its
	// rendered context exceeded the model limit.
	ErrContextOverflow = errors.New("provider context overflow")
)

// Gateway is the single model-execution path used by every Agent.
type Gateway interface {
	Identity() Identity
	Capabilities() Capabilities
	Stream(context.Context, Request) (<-chan Event, error)
}

// Identity describes one provider/model transport and its opaque-state
// compatibility boundary.
type Identity struct {
	Provider              string `json:"provider"`
	Model                 string `json:"model"`
	StateCompatibilityKey string `json:"state_compatibility_key"`
}

// Validate verifies a complete provider identity.
func (identity Identity) Validate() error {
	if strings.TrimSpace(identity.Provider) == "" || strings.TrimSpace(identity.Model) == "" || strings.TrimSpace(identity.StateCompatibilityKey) == "" {
		return errors.New("provider identity requires provider, model, and state compatibility key")
	}
	return nil
}

// ReasoningSupport declares whether a gateway accepts reasoning policy.
type ReasoningSupport string

const (
	// ReasoningUnsupported declares that the model has no reasoning controls.
	ReasoningUnsupported ReasoningSupport = "unsupported"
	// ReasoningSupported declares that ReasoningCapability is authoritative.
	ReasoningSupported ReasoningSupport = "supported"
)

// AttachmentPayloadMode declares whether Request attachments stay as opaque
// descriptors or are expanded by a prepared request.
type AttachmentPayloadMode string

const (
	// AttachmentDescriptors keeps attachment resource references opaque.
	AttachmentDescriptors AttachmentPayloadMode = "descriptors"
	// AttachmentExpanded requires Gateway to implement RequestPreparer.
	AttachmentExpanded AttachmentPayloadMode = "expanded"
)

// Capabilities describes provider behavior that affects request validation.
type Capabilities struct {
	Reasoning           ReasoningSupport           `json:"reasoning"`
	ReasoningCapability config.ReasoningCapability `json:"reasoning_capability,omitempty"`
	AttachmentPayload   AttachmentPayloadMode      `json:"attachment_payload"`
}

// Validate verifies an explicit capability declaration.
func (capabilities Capabilities) Validate() error {
	if capabilities.AttachmentPayload == "" {
		capabilities.AttachmentPayload = AttachmentDescriptors
	}
	switch capabilities.AttachmentPayload {
	case AttachmentDescriptors, AttachmentExpanded:
	default:
		return fmt.Errorf("unsupported attachment payload mode %q", capabilities.AttachmentPayload)
	}
	switch capabilities.Reasoning {
	case ReasoningUnsupported:
		if !capabilities.ReasoningCapability.IsZero() {
			return errors.New("reasoning-unsupported gateway cannot declare a reasoning capability")
		}
	case ReasoningSupported:
		if capabilities.ReasoningCapability.IsZero() {
			return errors.New("reasoning-supported gateway requires a reasoning capability")
		}
		if err := capabilities.ReasoningCapability.Validate(); err != nil {
			return fmt.Errorf("reasoning capability: %w", err)
		}
	default:
		return errors.New("gateway reasoning support must be explicit")
	}
	return nil
}

// Request is one complete provider-visible model request.
type Request struct {
	RunID            identity.RunID            `json:"run_id"`
	ThreadID         identity.ThreadID         `json:"thread_id,omitempty"`
	TurnID           identity.TurnID           `json:"turn_id,omitempty"`
	TraceID          identity.TraceID          `json:"trace_id,omitempty"`
	PromptScopeID    identity.PromptScopeID    `json:"prompt_scope_id"`
	LogicalRequestID identity.LogicalRequestID `json:"logical_request_id,omitempty"`
	AttemptID        string                    `json:"attempt_id,omitempty"`
	AttemptEpoch     int                       `json:"attempt_epoch,omitempty"`
	Step             int                       `json:"step"`
	Messages         []Message                 `json:"messages"`
	Tools            []tools.ToolDefinition    `json:"tools,omitempty"`
	HostedTools      []HostedToolDefinition    `json:"hosted_tools,omitempty"`
	MaxOutputTokens  int64                     `json:"max_output_tokens,omitempty"`
	Reasoning        config.ReasoningSelection `json:"reasoning,omitempty"`
	PreviousState    *State                    `json:"previous_state,omitempty"`
	Labels           Labels                    `json:"labels,omitempty"`
}

// MarshalJSON omits the optional logical request identity when a low-level
// provider request is constructed before runtime admission has assigned one.
// Non-empty identities still use identity.LogicalRequestID's validation.
func (request Request) MarshalJSON() ([]byte, error) {
	var logical any
	if request.LogicalRequestID != "" {
		logical = request.LogicalRequestID
	}
	return json.Marshal(struct {
		RunID            identity.RunID            `json:"run_id"`
		ThreadID         identity.ThreadID         `json:"thread_id,omitempty"`
		TurnID           identity.TurnID           `json:"turn_id,omitempty"`
		TraceID          identity.TraceID          `json:"trace_id,omitempty"`
		PromptScopeID    identity.PromptScopeID    `json:"prompt_scope_id"`
		LogicalRequestID any                       `json:"logical_request_id,omitempty"`
		AttemptID        string                    `json:"attempt_id,omitempty"`
		AttemptEpoch     int                       `json:"attempt_epoch,omitempty"`
		Step             int                       `json:"step"`
		Messages         []Message                 `json:"messages"`
		Tools            []tools.ToolDefinition    `json:"tools,omitempty"`
		HostedTools      []HostedToolDefinition    `json:"hosted_tools,omitempty"`
		MaxOutputTokens  int64                     `json:"max_output_tokens,omitempty"`
		Reasoning        config.ReasoningSelection `json:"reasoning,omitempty"`
		PreviousState    *State                    `json:"previous_state,omitempty"`
		Labels           Labels                    `json:"labels,omitempty"`
	}{
		RunID: request.RunID, ThreadID: request.ThreadID, TurnID: request.TurnID, TraceID: request.TraceID,
		PromptScopeID: request.PromptScopeID, LogicalRequestID: logical, AttemptID: request.AttemptID,
		AttemptEpoch: request.AttemptEpoch, Step: request.Step, Messages: request.Messages, Tools: request.Tools,
		HostedTools: request.HostedTools, MaxOutputTokens: request.MaxOutputTokens, Reasoning: request.Reasoning,
		PreviousState: request.PreviousState, Labels: request.Labels,
	})
}

// Validate verifies identities and provider-visible message structure.
func (request Request) Validate() error {
	if strings.TrimSpace(request.RunID.String()) == "" || strings.TrimSpace(request.PromptScopeID.String()) == "" {
		return errors.New("provider request requires run and prompt scope identities")
	}
	if request.Step < 0 || request.MaxOutputTokens < 0 {
		return errors.New("provider request limits cannot be negative")
	}
	if len(request.Messages) == 0 {
		return errors.New("provider request requires messages")
	}
	for index, message := range request.Messages {
		if err := message.Validate(); err != nil {
			return fmt.Errorf("provider message %d: %w", index, err)
		}
	}
	return nil
}

// Labels carries opaque correlation and host labels.
type Labels struct {
	Correlation map[string]string `json:"correlation,omitempty"`
	Host        map[string]string `json:"host,omitempty"`
}

// MessageRole is a provider-visible message role.
type MessageRole string

const (
	// RoleSystem identifies a system instruction.
	RoleSystem MessageRole = "system"
	// RoleUser identifies admitted user input.
	RoleUser MessageRole = "user"
	// RoleAssistant identifies assistant output or tool calls.
	RoleAssistant MessageRole = "assistant"
	// RoleTool identifies one local tool result.
	RoleTool MessageRole = "tool"
)

// Message is one typed provider-visible context item.
type Message struct {
	Role        MessageRole  `json:"role"`
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
	Reasoning   string       `json:"reasoning,omitempty"`
	ToolCalls   []ToolCall   `json:"tool_calls,omitempty"`
	ToolResult  *ToolResult  `json:"tool_result,omitempty"`
}

// Validate verifies the role-specific message shape.
func (message Message) Validate() error {
	switch message.Role {
	case RoleSystem:
		if strings.TrimSpace(message.Text) == "" || len(message.Attachments) > 0 || message.Reasoning != "" || len(message.ToolCalls) > 0 || message.ToolResult != nil {
			return errors.New("invalid system message")
		}
	case RoleUser:
		if strings.TrimSpace(message.Text) == "" && len(message.Attachments) == 0 {
			return errors.New("user message requires text or attachments")
		}
		if message.Reasoning != "" || len(message.ToolCalls) > 0 || message.ToolResult != nil {
			return errors.New("invalid user message")
		}
	case RoleAssistant:
		if message.Text == "" && message.Reasoning == "" && len(message.ToolCalls) == 0 {
			return errors.New("assistant message is empty")
		}
		if len(message.Attachments) > 0 || message.ToolResult != nil {
			return errors.New("invalid assistant message")
		}
	case RoleTool:
		if message.ToolResult == nil || message.Text != "" || len(message.Attachments) > 0 || message.Reasoning != "" || len(message.ToolCalls) > 0 {
			return errors.New("invalid tool message")
		}
	default:
		return fmt.Errorf("unsupported message role %q", message.Role)
	}
	return nil
}

// Attachment is an opaque host resource descriptor visible to a gateway.
type Attachment struct {
	ResourceRef string               `json:"resource_ref"`
	Name        string               `json:"name"`
	MIMEType    string               `json:"mime_type"`
	SizeBytes   int64                `json:"size_bytes,omitempty"`
	TextStats   *AttachmentTextStats `json:"text_stats,omitempty"`
}

// AttachmentTextStats is a stable display snapshot for textual attachments.
type AttachmentTextStats struct {
	UnicodeCodePointCount int64 `json:"unicode_code_points"`
	LogicalLineCount      int64 `json:"logical_lines"`
}

// ToolCall is one provider-requested local invocation.
type ToolCall struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Args string `json:"args"`
}

// ToolResult is one provider-visible local tool outcome.
type ToolResult struct {
	CallID   string `json:"call_id"`
	ToolName string `json:"tool_name"`
	Text     string `json:"text,omitempty"`
}

// HostedToolDefinition is one provider-native capability that the local tool
// runtime must not dispatch.
type HostedToolDefinition struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Options     map[string]any `json:"options,omitempty"`
}

// State is opaque provider continuation state persisted by Floret.
type State struct {
	Kind       string            `json:"kind,omitempty"`
	ID         string            `json:"id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// EventType is one streamed provider event kind.
type EventType string

const (
	// EventDelta appends assistant text.
	EventDelta EventType = "delta"
	// EventReasoning appends assistant reasoning.
	EventReasoning EventType = "reasoning"
	// EventToolCallStart begins one streamed tool call.
	EventToolCallStart EventType = "tool_call_start"
	// EventToolCallDelta appends streamed tool-call arguments.
	EventToolCallDelta EventType = "tool_call_delta"
	// EventToolCallEnd ends one streamed tool call.
	EventToolCallEnd EventType = "tool_call_end"
	// EventToolCalls delivers executable local tool calls.
	EventToolCalls EventType = "tool_calls"
	// EventUsage reports normalized provider usage.
	EventUsage EventType = "usage"
	// EventSources reports provider citations.
	EventSources EventType = "sources"
	// EventHostedToolCall reports a provider-native tool invocation.
	EventHostedToolCall EventType = "hosted_tool_call"
	// EventHostedToolResult reports the structured result of a provider-native
	// tool invocation.
	EventHostedToolResult EventType = "hosted_tool_result"
	// EventDone terminates a successful provider step.
	EventDone EventType = "done"
	// EventEmpty terminates an empty provider step.
	EventEmpty EventType = "empty"
	// EventTruncated terminates a length-limited provider step.
	EventTruncated EventType = "truncated"
	// EventError terminates a failed provider step.
	EventError EventType = "error"
)

// ToolCallStream identifies one tool call while its arguments stream.
type ToolCallStream struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// Event carries one streamed provider output.
type Event struct {
	Type           EventType         `json:"type"`
	Text           string            `json:"text,omitempty"`
	ToolCallStream *ToolCallStream   `json:"tool_call_stream,omitempty"`
	ToolCalls      []ToolCall        `json:"tool_calls,omitempty"`
	HostedToolCall *ToolCall         `json:"hosted_tool_call,omitempty"`
	HostedResult   *HostedToolResult `json:"hosted_result,omitempty"`
	Sources        []Source          `json:"sources,omitempty"`
	Reason         string            `json:"reason,omitempty"`
	Usage          Usage             `json:"usage,omitempty"`
	ResponseID     string            `json:"response_id,omitempty"`
	ResponseState  *State            `json:"response_state,omitempty"`
	Err            error             `json:"-"`
}

// HostedToolResult is a provider-neutral projection of provider-native tool
// output.
type HostedToolResult struct {
	Text     string                 `json:"text,omitempty"`
	Results  []HostedToolResultItem `json:"results,omitempty"`
	Error    *HostedToolResultError `json:"error,omitempty"`
	Metadata map[string]any         `json:"metadata,omitempty"`
}

// HostedToolResultItem is one structured provider-native result item.
type HostedToolResultItem struct {
	Title    string         `json:"title,omitempty"`
	URL      string         `json:"url,omitempty"`
	Snippet  string         `json:"snippet,omitempty"`
	Source   string         `json:"source,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// HostedToolResultError describes a provider-native tool failure.
type HostedToolResultError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Source is one provider citation.
type Source struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url,omitempty"`
}

// Usage is normalized provider token and cost usage.
type Usage struct {
	InputTokens       int64   `json:"input_tokens,omitempty"`
	OutputTokens      int64   `json:"output_tokens,omitempty"`
	ReasoningTokens   int64   `json:"reasoning_tokens,omitempty"`
	CacheReadTokens   int64   `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens  int64   `json:"cache_write_tokens,omitempty"`
	TotalTokens       int64   `json:"total_tokens,omitempty"`
	CostUSD           float64 `json:"cost_usd,omitempty"`
	Source            string  `json:"source,omitempty"`
	Available         bool    `json:"available,omitempty"`
	WindowInputTokens int64   `json:"window_input_tokens,omitempty"`
}

// RequestPreparer renders a complete provider request before context limits
// are applied.
type RequestPreparer interface {
	Prepare(context.Context, Request) (PreparedRequest, error)
}

// PreparedRequest is one immutable, single-use rendered provider request.
type PreparedRequest interface {
	Stream(context.Context) (<-chan Event, error)
	TokenEstimate() TokenEstimate
	RenderedPayloadFingerprint() string
	Close() error
}

// TokenEstimate describes the size of a complete rendered request.
type TokenEstimate struct {
	PrefixTokens         int64  `json:"prefix_tokens,omitempty"`
	MessageTokens        int64  `json:"message_tokens,omitempty"`
	ToolDefinitionTokens int64  `json:"tool_definition_tokens,omitempty"`
	EstimatedInputTokens int64  `json:"estimated_input_tokens,omitempty"`
	Source               string `json:"source,omitempty"`
	Method               string `json:"method,omitempty"`
	Confidence           string `json:"confidence,omitempty"`
	Coverage             string `json:"coverage,omitempty"`
}
