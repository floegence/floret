package provider

import (
	"context"
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
	RunID           string
	Step            int
	Provider        string
	Model           string
	Messages        []session.Message
	Tools           []ToolDefinition
	HostedTools     []HostedToolDefinition
	RawPlan         cache.RawPlan
	Cache           cache.CachePolicy
	ContextPolicy   contextpolicy.Policy
	ContextUsage    contextpolicy.Usage
	MaxOutputTokens int64
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

type StreamEvent struct {
	Type       EventType
	Text       string
	ToolCalls  []ToolCall
	ToolCall   ToolCall
	Reason     string
	Usage      Usage
	ResponseID string
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
	case Empty, Truncated, Done:
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

type UsageSource string

const (
	UsageNative    UsageSource = "native"
	UsageEstimated UsageSource = "estimated"
	UsageMixed     UsageSource = "mixed"
	UsageUnknown   UsageSource = "unknown"
)

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	TotalTokens      int64
	CostUSD          float64
	Source           UsageSource
}

func (u Usage) Normalized() Usage {
	if u.TotalTokens == 0 {
		u.TotalTokens = u.InputTokens + u.OutputTokens + u.ReasoningTokens + u.CacheReadTokens + u.CacheWriteTokens
	}
	if u.Source == "" {
		if u.TotalTokens == 0 && u.CostUSD == 0 {
			u.Source = UsageUnknown
		} else {
			u.Source = UsageNative
		}
	}
	return u
}

func (u Usage) Add(other Usage) Usage {
	u = u.Normalized()
	other = other.Normalized()
	source := u.Source
	if source == "" || source == UsageUnknown {
		source = other.Source
	}
	if other.Source != "" && source != other.Source {
		source = UsageMixed
	}
	return Usage{
		InputTokens:      u.InputTokens + other.InputTokens,
		OutputTokens:     u.OutputTokens + other.OutputTokens,
		ReasoningTokens:  u.ReasoningTokens + other.ReasoningTokens,
		CacheReadTokens:  u.CacheReadTokens + other.CacheReadTokens,
		CacheWriteTokens: u.CacheWriteTokens + other.CacheWriteTokens,
		TotalTokens:      u.TotalTokens + other.TotalTokens,
		CostUSD:          u.CostUSD + other.CostUSD,
		Source:           source,
	}
}
