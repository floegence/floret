package harness

import (
	"context"
	"sync"
	"time"

	"github.com/floegence/floret/internal/provider"
)

type ScriptedProvider struct {
	mu       sync.Mutex
	Steps    [][]provider.StreamEvent
	Requests []provider.Request
	Errs     map[int]error
}

func NewScriptedProvider(steps ...[]provider.StreamEvent) *ScriptedProvider {
	return &ScriptedProvider{Steps: steps, Errs: map[int]error{}}
}

func Step(events ...provider.StreamEvent) []provider.StreamEvent {
	return events
}

func Text(text string) provider.StreamEvent {
	return provider.StreamEvent{Type: provider.Delta, Text: text}
}

func Tool(id, name, args string) provider.StreamEvent {
	return provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: id, Name: name, Args: args}}}
}

func Usage(usage provider.Usage) provider.StreamEvent {
	return provider.StreamEvent{Type: provider.UsageEvent, Usage: usage}
}

func Truncated(reason string) provider.StreamEvent {
	return provider.StreamEvent{Type: provider.Truncated, Reason: reason}
}

func Empty() provider.StreamEvent {
	return provider.StreamEvent{Type: provider.Empty}
}

func Done() provider.StreamEvent {
	return DoneReason("stop")
}

func DoneReason(reason string) provider.StreamEvent {
	return provider.StreamEvent{Type: provider.Done, Reason: reason}
}

func Hang() provider.StreamEvent {
	return provider.StreamEvent{Type: "hang"}
}

func (p *ScriptedProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.Requests = append(p.Requests, req)
	err := p.Errs[len(p.Requests)]
	var events []provider.StreamEvent
	if len(p.Requests) <= len(p.Steps) {
		events = append([]provider.StreamEvent(nil), p.Steps[len(p.Requests)-1]...)
	}
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ch := make(chan provider.StreamEvent, len(events))
	go func() {
		defer close(ch)
		for _, ev := range events {
			if ev.Type == "hang" {
				<-ctx.Done()
				return
			}
			if ev.Reason != "" && ev.Type == provider.Delta {
				if d, err := time.ParseDuration(ev.Reason); err == nil {
					time.Sleep(d)
				}
			}
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
		}
	}()
	return ch, nil
}
