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
	Messages []session.Message
}

type EventType string

const (
	Delta     EventType = "delta"
	ToolCalls EventType = "tool_calls"
	Done      EventType = "done"
	Empty     EventType = "empty"
	Truncated EventType = "truncated"
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
}

type Provider interface {
	Stream(context.Context, Request) (<-chan StreamEvent, error)
}
