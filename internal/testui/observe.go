package testui

import (
	"context"
	"sync"
	"time"

	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

const maxObservedRawSegmentBytes = 16 * 1024

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

func (p *observingProvider) MessageRaw(kind promptcache.SegmentKind, msg session.Message) (string, string, error) {
	if renderer, ok := p.inner.(promptcache.Renderer); ok {
		return renderer.MessageRaw(kind, msg)
	}
	return "", "", nil
}

func (p *observingProvider) ToolRaw(def promptcache.ToolDefinition) (string, string, error) {
	if renderer, ok := p.inner.(promptcache.Renderer); ok {
		return renderer.ToolRaw(def)
	}
	return "", "", nil
}

func (p *observingProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	sessionID := observedSessionID(req)
	threadID := observedThreadID(req)
	turnID := observedTurnID(req)
	p.reqs = append(p.reqs, ObservedProviderRequest{
		RunID:        req.RunID,
		SessionID:    sessionID,
		ThreadID:     threadID,
		TurnID:       turnID,
		Step:         req.Step,
		Provider:     req.Provider,
		Model:        req.Model,
		ObservedAt:   time.Now(),
		Messages:     observeMessages(req.Messages),
		Tools:        append([]provider.ToolDefinition(nil), req.Tools...),
		ContextUsage: req.ContextUsage,
		RawSegments:  observeRawSegments(req.RawPlan),
		CacheSummary: ObservedCacheSummary{
			Namespace:            req.Cache.Namespace,
			Retention:            string(req.Cache.Retention),
			PrefixHash:           req.RawPlan.PrefixHash,
			PayloadHash:          req.RawPlan.PayloadHash,
			ToolsetID:            req.RawPlan.ToolsetID,
			ToolsetEpoch:         req.RawPlan.ToolsetEpoch,
			CompactionGeneration: req.RawPlan.CompactionGeneration,
			CompactionWindowID:   req.RawPlan.CompactionWindowID,
			CompactionEntryID:    req.RawPlan.CompactionEntryID,
			ReusedSegments:       req.RawPlan.ReusedSegments,
			NewSegments:          req.RawPlan.NewSegments,
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
			p.recordEvent(req.RunID, sessionID, req.Step, ev)
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

func (p *observingProvider) recordEvent(runID string, sessionID string, step int, ev provider.StreamEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ev.Type == provider.UsageEvent {
		for i := len(p.reqs) - 1; i >= 0; i-- {
			if p.reqs[i].RunID == runID && p.reqs[i].Step == step {
				p.reqs[i].CacheSummary.CacheReadTokens += ev.Usage.CacheReadTokens
				p.reqs[i].CacheSummary.CacheWriteTokens += ev.Usage.CacheWriteTokens
				break
			}
		}
	}
	p.evs = append(p.evs, ObservedProviderEvent{
		RunID:      runID,
		SessionID:  sessionID,
		Step:       step,
		Type:       ev.Type,
		ObservedAt: time.Now(),
		ResponseID: ev.ResponseID,
		Text:       ev.Text,
		ToolCalls:  append([]provider.ToolCall(nil), ev.ToolCalls...),
		Reason:     ev.Reason,
		Usage:      ev.Usage,
	})
}

func observedSessionID(req provider.Request) string {
	for _, seg := range req.RawPlan.Segments {
		if seg.SessionID != "" {
			return seg.SessionID
		}
	}
	return ""
}

func observedThreadID(req provider.Request) string {
	for _, seg := range req.RawPlan.Segments {
		if seg.ThreadID != "" {
			return seg.ThreadID
		}
	}
	return observedSessionID(req)
}

func observedTurnID(req provider.Request) string {
	for _, seg := range req.RawPlan.Segments {
		if seg.TurnID != "" {
			return seg.TurnID
		}
	}
	return req.RunID
}

func observeRawSegments(plan promptcache.RawPlan) []ObservedRawSegment {
	out := make([]ObservedRawSegment, 0, len(plan.Segments))
	for i, seg := range plan.Segments {
		reused := false
		if i < len(plan.SegmentStates) {
			reused = plan.SegmentStates[i] == "reused"
		}
		raw, truncated := boundedRaw(seg.Raw, maxObservedRawSegmentBytes)
		out = append(out, ObservedRawSegment{
			ID:                   seg.ID,
			RunID:                seg.RunID,
			SessionID:            seg.SessionID,
			ThreadID:             seg.ThreadID,
			TurnID:               seg.TurnID,
			EntryID:              seg.EntryID,
			ParentEntryID:        seg.ParentEntryID,
			Kind:                 seg.Kind,
			Role:                 seg.Role,
			SHA256:               seg.SHA256,
			ByteLength:           seg.ByteLength,
			Epoch:                seg.Epoch,
			Sequence:             seg.Sequence,
			Reused:               reused,
			FragmentType:         seg.FragmentType,
			StructuredRefID:      seg.StructuredRefID,
			CompactionGeneration: seg.CompactionGeneration,
			CompactionWindowID:   seg.CompactionWindowID,
			CompactionEntryID:    seg.CompactionEntryID,
			Fingerprint:          seg.Fingerprint,
			SchemaVersion:        seg.SchemaVersion,
			AdapterVersion:       seg.AdapterVersion,
			Raw:                  raw,
			RawTruncated:         truncated,
			RawPreview:           preview(seg.Raw, 240),
		})
	}
	return out
}

func boundedRaw(value string, max int) (string, bool) {
	if max <= 0 || len(value) <= max {
		return value, false
	}
	return value[:max] + "\n...[truncated in test UI response]", true
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
			Role:                 string(msg.Role),
			Content:              msg.Content,
			ToolCallID:           msg.ToolCallID,
			ToolName:             msg.ToolName,
			ToolArgs:             msg.ToolArgs,
			Kind:                 string(msg.Kind),
			EntryID:              msg.EntryID,
			ParentEntryID:        msg.ParentEntryID,
			CompactionID:         msg.CompactionID,
			CompactionGeneration: msg.CompactionGeneration,
			CompactionWindowID:   msg.CompactionWindowID,
		})
	}
	return out
}
