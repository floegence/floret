package provider

import (
	"context"
	"errors"
	"strings"

	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/session"
)

var ErrContextOverflow = errors.New("provider context overflow")

type Request struct {
	RunID           string
	Step            int
	Provider        string
	Model           string
	Messages        []session.Message
	Tools           []ToolDefinition
	HostedTools     []HostedToolDefinition
	RawPlan         promptcache.RawPlan
	Cache           promptcache.CachePolicy
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
	Delta      EventType = "delta"
	ToolCalls  EventType = "tool_calls"
	Done       EventType = "done"
	Empty      EventType = "empty"
	Truncated  EventType = "truncated"
	UsageEvent EventType = "usage"
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
	ID   string
	Name string
	Args string
}

type StreamEvent struct {
	Type       EventType
	Text       string
	ToolCalls  []ToolCall
	Reason     string
	Usage      Usage
	ResponseID string
}

type Provider interface {
	Stream(context.Context, Request) (<-chan StreamEvent, error)
}

type CachePolicyNormalizer interface {
	NormalizeCachePolicy(promptcache.CachePolicy) (promptcache.CachePolicy, error)
}

type CacheRetentionDefault interface {
	DefaultCacheRetention() promptcache.Retention
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
