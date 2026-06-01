package provider

import (
	"context"
	"errors"

	"github.com/floegence/floret/session"
)

var ErrContextOverflow = errors.New("provider context overflow")

type Request struct {
	RunID    string
	Step     int
	Provider string
	Model    string
	Messages []session.Message
	Tools    []ToolDefinition
}

type ToolDefinition struct {
	Name        string
	Description string
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

type ToolCall struct {
	ID       string
	Name     string
	Args     string
	ReadOnly bool
}

type StreamEvent struct {
	Type      EventType
	Text      string
	ToolCalls []ToolCall
	Reason    string
	Usage     Usage
}

type Provider interface {
	Stream(context.Context, Request) (<-chan StreamEvent, error)
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
