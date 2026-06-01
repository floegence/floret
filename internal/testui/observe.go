package testui

import (
	"context"
	"sync"

	"github.com/floegence/floret/promptcache"
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

func (p *observingProvider) NormalizeCachePolicy(policy promptcache.CachePolicy) (promptcache.CachePolicy, error) {
	if normalizer, ok := p.inner.(provider.CachePolicyNormalizer); ok {
		return normalizer.NormalizeCachePolicy(policy)
	}
	return policy, nil
}

func (p *observingProvider) DefaultCacheRetention() promptcache.Retention {
	if defaults, ok := p.inner.(provider.CacheRetentionDefault); ok {
		return defaults.DefaultCacheRetention()
	}
	return promptcache.RetentionInMemory
}

func (p *observingProvider) PayloadHash(req provider.Request) (string, error) {
	if hasher, ok := p.inner.(provider.PayloadHasher); ok {
		return hasher.PayloadHash(req)
	}
	return req.RawPlan.PayloadHash, nil
}

func (p *observingProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, ObservedProviderRequest{
		Step:        req.Step,
		Provider:    req.Provider,
		Model:       req.Model,
		Messages:    observeMessages(req.Messages),
		Tools:       append([]provider.ToolDefinition(nil), req.Tools...),
		RawSegments: observeRawSegments(req.RawPlan),
		CacheSummary: ObservedCacheSummary{
			Namespace:      req.Cache.Namespace,
			Retention:      string(req.Cache.Retention),
			PrefixHash:     req.RawPlan.PrefixHash,
			PayloadHash:    req.RawPlan.PayloadHash,
			ToolsetID:      req.RawPlan.ToolsetID,
			ToolsetEpoch:   req.RawPlan.ToolsetEpoch,
			ReusedSegments: req.RawPlan.ReusedSegments,
			NewSegments:    req.RawPlan.NewSegments,
		},
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
	if ev.Type == provider.UsageEvent {
		for i := len(p.reqs) - 1; i >= 0; i-- {
			if p.reqs[i].Step == step {
				p.reqs[i].CacheSummary.CacheReadTokens += ev.Usage.CacheReadTokens
				p.reqs[i].CacheSummary.CacheWriteTokens += ev.Usage.CacheWriteTokens
				break
			}
		}
	}
	p.evs = append(p.evs, ObservedProviderEvent{
		Step:      step,
		Type:      ev.Type,
		Text:      ev.Text,
		ToolCalls: append([]provider.ToolCall(nil), ev.ToolCalls...),
		Reason:    ev.Reason,
		Usage:     ev.Usage,
	})
}

func observeRawSegments(plan promptcache.RawPlan) []ObservedRawSegment {
	out := make([]ObservedRawSegment, 0, len(plan.Segments))
	for i, seg := range plan.Segments {
		reused := false
		if i < len(plan.SegmentStates) {
			reused = plan.SegmentStates[i] == "reused"
		}
		out = append(out, ObservedRawSegment{
			ID:         seg.ID,
			Kind:       seg.Kind,
			Role:       seg.Role,
			SHA256:     seg.SHA256,
			ByteLength: seg.ByteLength,
			Epoch:      seg.Epoch,
			Reused:     reused,
			RawPreview: preview(seg.Raw, 240),
		})
	}
	return out
}

func preview(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
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
