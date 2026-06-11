package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/contextpolicy"
)

var ErrContextOverflow = errors.New("provider context overflow")
var ErrStreamMissingTerminal = errors.New("provider stream closed without terminal event")
var ErrStreamNotClosedAfterTerminal = errors.New("provider stream did not close after terminal event")

type Request struct {
	RunID            string
	ThreadID         string
	TurnID           string
	PromptScopeID    string
	TraceID          string
	Step             int
	LogicalRequestID string
	Attempt          int
	OverflowRetried  bool
	Provider         string
	Model            string
	Messages         []session.Message
	Tools            []ToolDefinition
	HostedTools      []HostedToolDefinition
	RawPlan          cache.RawPlan
	Cache            cache.CachePolicy
	ContextPolicy    contextpolicy.Policy
	RequestEstimate  contextpolicy.RequestEstimate
	ContextPressure  contextpolicy.ContextPressure
	MaxOutputTokens  int64
	DisableReasoning bool
	PreviousState    *State
}

type State struct {
	Kind       string            `json:"kind,omitempty"`
	ID         string            `json:"id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type ToolDefinition struct {
	Name         string
	Title        string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any
	Strict       bool
	Annotations  map[string]any
}

type HostedToolDefinition struct {
	Name        string
	Type        string
	Description string
	Parameters  map[string]any
	Options     map[string]any
}

type EventType string

const (
	Delta            EventType = "delta"
	Reasoning        EventType = "reasoning"
	ToolCalls        EventType = "tool_calls"
	Done             EventType = "done"
	Empty            EventType = "empty"
	Truncated        EventType = "truncated"
	Error            EventType = "error"
	UsageEvent       EventType = "usage"
	HostedToolCall   EventType = "hosted_tool_call"
	HostedToolResult EventType = "hosted_tool_result"
)

type FinishReason string

const (
	FinishUnknown       FinishReason = "unknown"
	FinishStop          FinishReason = "stop"
	FinishToolCalls     FinishReason = "tool_calls"
	FinishLength        FinishReason = "length"
	FinishContentFilter FinishReason = "content_filter"
	FinishError         FinishReason = "error"
	FinishCancelled     FinishReason = "cancelled"
)

func NormalizeFinishReason(raw string, hasToolCalls bool, truncated bool, hasText bool) (FinishReason, bool) {
	if hasToolCalls {
		return FinishToolCalls, raw == ""
	}
	if truncated {
		return FinishLength, raw == ""
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "unknown":
		if hasText {
			return FinishStop, true
		}
		return FinishUnknown, true
	case "stop", "end_turn", "stop_sequence", "stop-sequence":
		return FinishStop, false
	case "tool_calls", "tool-calls", "tool_use", "tool-use", "function_call", "function-calls", "function_calls":
		return FinishToolCalls, false
	case "length", "max_tokens", "max-tokens", "max_output_tokens", "max-output-tokens":
		return FinishLength, false
	case "content_filter", "content-filter", "safety", "blocked":
		return FinishContentFilter, false
	case "error":
		return FinishError, false
	case "cancelled", "canceled", "abort", "aborted":
		return FinishCancelled, false
	default:
		if hasText {
			return FinishStop, true
		}
		return FinishUnknown, true
	}
}

func IsTerminalNaturalFinish(reason FinishReason) bool {
	return reason == FinishStop
}

type ToolCall struct {
	ID        string
	Name      string
	Args      string
	Reasoning string
}

type HostedToolResultData struct {
	Text     string                 `json:"text,omitempty"`
	Results  []HostedToolResultItem `json:"results,omitempty"`
	Error    *HostedToolResultError `json:"error,omitempty"`
	Metadata map[string]any         `json:"metadata,omitempty"`
}

type HostedToolResultItem struct {
	Title    string         `json:"title,omitempty"`
	URL      string         `json:"url,omitempty"`
	Snippet  string         `json:"snippet,omitempty"`
	Source   string         `json:"source,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type HostedToolResultError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func (r HostedToolResultData) IsZero() bool {
	return strings.TrimSpace(r.Text) == "" && len(r.Results) == 0 && r.Error == nil && len(r.Metadata) == 0
}

func (r HostedToolResultData) SummaryText() string {
	if strings.TrimSpace(r.Text) != "" {
		return r.Text
	}
	if r.Error != nil {
		if strings.TrimSpace(r.Error.Message) != "" {
			return r.Error.Message
		}
		if strings.TrimSpace(r.Error.Code) != "" {
			return "Hosted tool result error: " + r.Error.Code
		}
	}
	if len(r.Results) == 0 {
		return ""
	}
	var b strings.Builder
	for i, item := range r.Results {
		if i > 0 {
			b.WriteString("\n")
		}
		title := strings.TrimSpace(item.Title)
		if title == "" {
			title = strings.TrimSpace(item.URL)
		}
		if title == "" {
			title = "Result"
		}
		fmt.Fprintf(&b, "%d. %s", i+1, title)
		if url := strings.TrimSpace(item.URL); url != "" && url != title {
			b.WriteString("\n   URL: ")
			b.WriteString(url)
		}
		if snippet := strings.TrimSpace(item.Snippet); snippet != "" {
			b.WriteString("\n   Snippet: ")
			b.WriteString(snippet)
		}
	}
	return b.String()
}

type StreamEvent struct {
	Type          EventType
	Text          string
	ToolCalls     []ToolCall
	ToolCall      ToolCall
	HostedResult  HostedToolResultData
	Reason        string
	Usage         Usage
	ResponseID    string
	ResponseState *State
	Err           error
}

func CloneState(in *State) *State {
	if in == nil {
		return nil
	}
	out := *in
	if in.Attributes != nil {
		out.Attributes = make(map[string]string, len(in.Attributes))
		for key, value := range in.Attributes {
			out.Attributes[key] = value
		}
	}
	return &out
}

// StreamValidator checks provider stream invariants that the engine relies on.
// Providers may emit delta, reasoning, usage, tool call, and hosted tool events
// before exactly one terminal event. They must not emit unknown event types,
// duplicate tool call IDs, incomplete tool calls, hosted results without prior
// hosted calls, or any event after a terminal event.
type StreamValidator struct {
	terminalSeen bool
	toolIDs      map[string]struct{}
	hostedCalls  map[string]string
	hostedDone   map[string]struct{}
}

func (v *StreamValidator) Observe(ev StreamEvent) error {
	if v.terminalSeen {
		return fmt.Errorf("provider emitted %q after terminal event", ev.Type)
	}
	switch ev.Type {
	case Delta, Reasoning, UsageEvent:
		return nil
	case ToolCalls:
		if v.toolIDs == nil {
			v.toolIDs = map[string]struct{}{}
		}
		for _, call := range ev.ToolCalls {
			if err := ValidateToolCall(call); err != nil {
				return err
			}
			if _, ok := v.toolIDs[call.ID]; ok {
				return fmt.Errorf("provider returned duplicate tool call id %q", call.ID)
			}
			v.toolIDs[call.ID] = struct{}{}
		}
		return nil
	case HostedToolCall, HostedToolResult:
		if err := ValidateToolCall(ev.ToolCall); err != nil {
			return err
		}
		if ev.Type == HostedToolCall {
			if v.hostedCalls == nil {
				v.hostedCalls = map[string]string{}
			}
			if _, ok := v.hostedCalls[ev.ToolCall.ID]; ok {
				return fmt.Errorf("provider returned duplicate hosted tool call id %q", ev.ToolCall.ID)
			}
			v.hostedCalls[ev.ToolCall.ID] = ev.ToolCall.Name
			return nil
		}
		name, ok := v.hostedCalls[ev.ToolCall.ID]
		if !ok {
			return fmt.Errorf("provider returned hosted tool result %q without a prior call", ev.ToolCall.ID)
		}
		if name != ev.ToolCall.Name {
			return fmt.Errorf("provider returned hosted tool result %q for %q after call to %q", ev.ToolCall.ID, ev.ToolCall.Name, name)
		}
		if v.hostedDone == nil {
			v.hostedDone = map[string]struct{}{}
		}
		if _, ok := v.hostedDone[ev.ToolCall.ID]; ok {
			return fmt.Errorf("provider returned duplicate hosted tool result id %q", ev.ToolCall.ID)
		}
		v.hostedDone[ev.ToolCall.ID] = struct{}{}
		return nil
	case Empty, Truncated, Done, Error:
		v.terminalSeen = true
		return nil
	default:
		return fmt.Errorf("unknown provider event type %q", ev.Type)
	}
}

func (v *StreamValidator) TerminalSeen() bool {
	return v != nil && v.terminalSeen
}

func (v *StreamValidator) Finish() error {
	if !v.TerminalSeen() {
		return ErrStreamMissingTerminal
	}
	return nil
}

func ValidateToolCall(call ToolCall) error {
	if strings.TrimSpace(call.ID) == "" {
		return errors.New("provider tool call id is required")
	}
	if strings.TrimSpace(call.Name) == "" {
		return errors.New("provider tool call name is required")
	}
	return nil
}

type Provider interface {
	Stream(context.Context, Request) (<-chan StreamEvent, error)
}

type CachePolicyNormalizer interface {
	NormalizeCachePolicy(cache.CachePolicy) (cache.CachePolicy, error)
}

type CacheRetentionDefault interface {
	DefaultCacheRetention() cache.Retention
}

type PayloadHasher interface {
	PayloadHash(Request) (string, error)
}

type EstimateConfidence string

const (
	EstimateExact        EstimateConfidence = "exact"
	EstimateApproximate  EstimateConfidence = "approximate"
	EstimateConservative EstimateConfidence = "conservative"
)

type TokenEstimateMethod = contextpolicy.EstimateMethod

const (
	TokenEstimateGenericPayload          = contextpolicy.EstimateMethodGenericPayload
	TokenEstimateProviderRenderedPayload = contextpolicy.EstimateMethodProviderRenderedPayload
	TokenEstimateOfficialPreflightCount  = contextpolicy.EstimateMethodOfficialPreflightCount
)

// TokenEstimate is a preflight request estimate. It is not provider usage and
// does not imply an official provider token-count API unless Method says so.
type TokenEstimate struct {
	PrefixTokens         int64
	MessageTokens        int64
	ToolDefinitionTokens int64
	EstimatedInputTokens int64
	Source               string
	Method               TokenEstimateMethod
	Confidence           EstimateConfidence
}

// TokenEstimator lets an adapter estimate the fully rendered request before it
// is sent. Implementations should report the calculation Method explicitly.
type TokenEstimator interface {
	EstimateTokens(context.Context, Request) (TokenEstimate, error)
}

func GenericRequestEstimate(req Request) (TokenEstimate, error) {
	prefix, messages := splitSystemMessages(req.Messages)
	prefixTokens, err := estimateMessagesJSON(prefix)
	if err != nil {
		return TokenEstimate{}, err
	}
	messageTokens, err := estimateMessagesJSON(messages)
	if err != nil {
		return TokenEstimate{}, err
	}
	toolDefinitionTokens, err := estimateToolsJSON(req.Tools, req.HostedTools)
	if err != nil {
		return TokenEstimate{}, err
	}
	return TokenEstimate{
		PrefixTokens:         prefixTokens,
		MessageTokens:        messageTokens,
		ToolDefinitionTokens: toolDefinitionTokens,
		EstimatedInputTokens: prefixTokens + messageTokens + toolDefinitionTokens,
		Source:               "generic_request_json",
		Method:               TokenEstimateGenericPayload,
		Confidence:           EstimateConservative,
	}, nil
}

func estimateMessagesJSON(messages []session.Message) (int64, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		return 0, err
	}
	return contextpolicy.EstimateTextTokens(string(raw)), nil
}

func estimateToolsJSON(tools []ToolDefinition, hostedTools []HostedToolDefinition) (int64, error) {
	if len(tools) == 0 && len(hostedTools) == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(map[string]any{
		"tools":        tools,
		"hosted_tools": hostedTools,
	})
	if err != nil {
		return 0, err
	}
	return contextpolicy.EstimateTextTokens(string(raw)), nil
}

func splitSystemMessages(messages []session.Message) ([]session.Message, []session.Message) {
	var prefix []session.Message
	var history []session.Message
	for _, msg := range messages {
		if msg.Role == session.System {
			prefix = append(prefix, msg)
			continue
		}
		history = append(history, msg)
	}
	return prefix, history
}

type UsageSource string

const (
	UsageNative      UsageSource = "native"
	UsageEstimated   UsageSource = "estimated"
	UsageMixed       UsageSource = "mixed"
	UsageUnknown     UsageSource = "unknown"
	UsageUnavailable UsageSource = "unavailable"
)

type Usage struct {
	InputTokens       int64       `json:"input_tokens,omitempty"`
	OutputTokens      int64       `json:"output_tokens,omitempty"`
	ReasoningTokens   int64       `json:"reasoning_tokens,omitempty"`
	CacheReadTokens   int64       `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens  int64       `json:"cache_write_tokens,omitempty"`
	TotalTokens       int64       `json:"total_tokens,omitempty"`
	CostUSD           float64     `json:"cost_usd,omitempty"`
	Source            UsageSource `json:"source,omitempty"`
	Available         bool        `json:"available,omitempty"`
	WindowInputTokens int64       `json:"window_input_tokens,omitempty"`
}

func (u Usage) Normalized() Usage {
	if u.TotalTokens == 0 {
		u.TotalTokens = u.InputTokens + u.OutputTokens + u.ReasoningTokens + u.CacheReadTokens + u.CacheWriteTokens
	}
	if u.Source == "" {
		if u.TotalTokens == 0 && u.CostUSD == 0 {
			u.Source = UsageUnavailable
		} else {
			u.Source = UsageNative
		}
	}
	if u.Source == UsageUnavailable || u.Source == UsageUnknown {
		u.Available = false
		return u
	}
	if u.TotalTokens > 0 || u.CostUSD > 0 || u.InputTokens > 0 || u.OutputTokens > 0 || u.WindowInputTokens > 0 || u.CacheReadTokens > 0 || u.CacheWriteTokens > 0 {
		u.Available = true
	}
	if u.WindowInputTokens <= 0 && u.Available {
		u.WindowInputTokens = u.InputTokens + u.CacheReadTokens + u.CacheWriteTokens
	}
	return u
}

func (u Usage) Add(other Usage) Usage {
	u = u.Normalized()
	other = other.Normalized()
	if !usageHasValues(u) {
		return other
	}
	if !usageHasValues(other) {
		return u
	}
	source := u.Source
	if source == "" || source == UsageUnknown {
		source = other.Source
	}
	if source == UsageUnavailable {
		source = other.Source
	}
	if other.Source != "" && other.Source != UsageUnavailable && source != other.Source {
		source = UsageMixed
	}
	return Usage{
		InputTokens:       u.InputTokens + other.InputTokens,
		OutputTokens:      u.OutputTokens + other.OutputTokens,
		ReasoningTokens:   u.ReasoningTokens + other.ReasoningTokens,
		CacheReadTokens:   u.CacheReadTokens + other.CacheReadTokens,
		CacheWriteTokens:  u.CacheWriteTokens + other.CacheWriteTokens,
		TotalTokens:       u.TotalTokens + other.TotalTokens,
		CostUSD:           u.CostUSD + other.CostUSD,
		Source:            source,
		Available:         u.Available || other.Available,
		WindowInputTokens: u.WindowInputTokens + other.WindowInputTokens,
	}
}

func usageHasValues(u Usage) bool {
	return u.Available ||
		u.InputTokens != 0 ||
		u.OutputTokens != 0 ||
		u.ReasoningTokens != 0 ||
		u.CacheReadTokens != 0 ||
		u.CacheWriteTokens != 0 ||
		u.TotalTokens != 0 ||
		u.CostUSD != 0 ||
		u.WindowInputTokens != 0
}
