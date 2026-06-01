package testui

import (
	"context"
	"sync"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type observingProvider struct {
	inner provider.Provider
	mu    sync.Mutex
	reqs  []ObservedProviderRequest
	evs   []ObservedProviderEvent
}

func newObservingProvider(inner provider.Provider) *observingProvider {
	return &observingProvider{inner: inner}
}

func (p *observingProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, ObservedProviderRequest{
		Step:     req.Step,
		Provider: req.Provider,
		Model:    req.Model,
		Messages: observeMessages(req.Messages),
		Tools:    append([]provider.ToolDefinition(nil), req.Tools...),
	})
	p.mu.Unlock()

	stream, err := p.inner.Stream(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan provider.StreamEvent)
	go func() {
		defer close(out)
		for ev := range stream {
			p.recordEvent(req.Step, ev)
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
	}()
	return out, nil
}

func (p *observingProvider) Snapshot() AgentObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return AgentObservation{
		ProviderRequests: append([]ObservedProviderRequest(nil), p.reqs...),
		ProviderEvents:   append([]ObservedProviderEvent(nil), p.evs...),
	}
}

func (p *observingProvider) recordEvent(step int, ev provider.StreamEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evs = append(p.evs, ObservedProviderEvent{
		Step:      step,
		Type:      ev.Type,
		Text:      ev.Text,
		ToolCalls: append([]provider.ToolCall(nil), ev.ToolCalls...),
		Reason:    ev.Reason,
		Usage:     ev.Usage,
	})
}

func observeMessages(messages []session.Message) []ObservedSessionMessage {
	out := make([]ObservedSessionMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, ObservedSessionMessage{
			Role:       string(msg.Role),
			Content:    msg.Content,
			ToolCallID: msg.ToolCallID,
			ToolName:   msg.ToolName,
			ToolArgs:   msg.ToolArgs,
		})
	}
	return out
}
