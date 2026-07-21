package testui

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

const maxObservedRawSegmentBytes = 16 * 1024

type observingProvider struct {
	inner provider.Provider
	mu    sync.Mutex
	reqs  []ObservedProviderRequest
	evs   []ObservedProviderEvent
	sink  AgentStreamSink
}

func newObservingProvider(inner provider.Provider) *observingProvider {
	return &observingProvider{inner: inner}
}

func observedProviderRuntime(observed *observingProvider) provider.Provider {
	if _, ok := observed.inner.(provider.TokenEstimator); ok {
		return estimatingObservingProvider{observingProvider: observed}
	}
	return observed
}

type estimatingObservingProvider struct {
	*observingProvider
}

func (p estimatingObservingProvider) EstimateTokens(ctx context.Context, req provider.Request) (provider.TokenEstimate, error) {
	return p.inner.(provider.TokenEstimator).EstimateTokens(ctx, req)
}

func (p *observingProvider) SetStreamSink(sink AgentStreamSink) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sink = sink
}

func (p *observingProvider) NormalizeCachePolicy(policy cache.CachePolicy) (cache.CachePolicy, error) {
	if normalizer, ok := p.inner.(provider.CachePolicyNormalizer); ok {
		return normalizer.NormalizeCachePolicy(policy)
	}
	return policy, nil
}

func (p *observingProvider) DefaultCacheRetention() cache.Retention {
	if defaults, ok := p.inner.(provider.CacheRetentionDefault); ok {
		return defaults.DefaultCacheRetention()
	}
	return cache.RetentionInMemory
}

func (p *observingProvider) PayloadHash(req provider.Request) (string, error) {
	if hasher, ok := p.inner.(provider.PayloadHasher); ok {
		return hasher.PayloadHash(req)
	}
	return req.RawPlan.PayloadHash, nil
}

func (p *observingProvider) MessageRaw(kind cache.SegmentKind, msg session.Message) (string, string, error) {
	if renderer, ok := p.inner.(cache.Renderer); ok {
		return renderer.MessageRaw(kind, msg)
	}
	return "", "", nil
}

func (p *observingProvider) ToolRaw(def cache.ToolDefinition) (string, string, error) {
	if renderer, ok := p.inner.(cache.Renderer); ok {
		return renderer.ToolRaw(def)
	}
	return "", "", nil
}

func (p *observingProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	observeConversation := observeProviderRequest(req)
	observedRequest := ObservedProviderRequest{
		RunID:                   req.RunID,
		ThreadID:                req.ThreadID,
		TurnID:                  req.TurnID,
		PromptScopeID:           req.PromptScopeID,
		Step:                    req.Step,
		LogicalRequestID:        req.LogicalRequestID,
		Attempt:                 req.Attempt,
		OverflowRetried:         req.OverflowRetried,
		Provider:                req.Provider,
		Model:                   req.Model,
		ObservedAt:              time.Now(),
		Messages:                observeMessages(req.Messages),
		Tools:                   append([]provider.ToolDefinition(nil), req.Tools...),
		HostedTools:             append([]provider.HostedToolDefinition(nil), req.HostedTools...),
		UnavailableCapabilities: unavailableCapabilitiesFromHostedRequest(req),
		RequestEstimate:         req.RequestEstimate,
		ProjectedPressure:       req.ContextPressure,
		RawSegments:             observeRawSegments(req.RawPlan),
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
	}
	p.mu.Lock()
	if observeConversation {
		p.reqs = append(p.reqs, observedRequest)
	}
	sink := p.sink
	p.mu.Unlock()
	if sink != nil && observeConversation {
		reqCopy := observedRequest
		status := contextStatusFromProviderRequest(observedRequest)
		sink.EmitAgentStream(AgentStreamEvent{
			Type:            AgentStreamProviderRequest,
			SessionID:       observedRequest.ThreadID,
			TurnID:          observedRequest.TurnID,
			Step:            observedRequest.Step,
			At:              observedRequest.ObservedAt,
			ProviderRequest: &reqCopy,
		})
		sink.EmitAgentStream(AgentStreamEvent{
			Type:          AgentStreamContextStatus,
			SessionID:     observedRequest.ThreadID,
			TurnID:        observedRequest.TurnID,
			Step:          observedRequest.Step,
			At:            observedRequest.ObservedAt,
			ContextStatus: &status,
		})
	}

	stream, err := p.inner.Stream(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan provider.StreamEvent)
	go func() {
		defer close(out)
		for {
			var ev provider.StreamEvent
			var ok bool
			select {
			case <-ctx.Done():
				return
			case ev, ok = <-stream:
				if !ok {
					return
				}
			}
			if observeConversation {
				p.recordEvent(req.RunID, observedRequest.ThreadID, observedRequest.TurnID, req.Step, ev)
			}
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
	}()
	return out, nil
}

func observeProviderRequest(req provider.Request) bool {
	return req.LogicalRequestID != agentharness.ThreadTitleLogicalRequestID
}

func (p *observingProvider) Snapshot() AgentObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return AgentObservation{
		ProviderRequests: append([]ObservedProviderRequest(nil), p.reqs...),
		ProviderEvents:   append([]ObservedProviderEvent(nil), p.evs...),
	}
}

func (p *observingProvider) recordEvent(runID string, threadID string, turnID string, step int, ev provider.StreamEvent) {
	hostedResult := observedHostedResult(ev.HostedResult)
	observed := ObservedProviderEvent{
		RunID:        runID,
		ThreadID:     threadID,
		TurnID:       turnID,
		Step:         step,
		Type:         ev.Type,
		ObservedAt:   time.Now(),
		ResponseID:   ev.ResponseID,
		Text:         ev.Text,
		Reasoning:    reasoningText(ev),
		ToolCalls:    observedToolCalls(ev),
		HostedResult: hostedResult,
		Metadata:     observedHostedResultMetadata(ev),
		Reason:       ev.Reason,
		Usage:        ev.Usage,
	}
	p.mu.Lock()
	if ev.Type == provider.UsageEvent {
		for i := len(p.reqs) - 1; i >= 0; i-- {
			if p.reqs[i].RunID == runID && p.reqs[i].Step == step {
				p.reqs[i].CacheSummary.CacheReadTokens += ev.Usage.CacheReadTokens
				p.reqs[i].CacheSummary.CacheWriteTokens += ev.Usage.CacheWriteTokens
				break
			}
		}
	}
	p.evs = append(p.evs, observed)
	sink := p.sink
	p.mu.Unlock()
	if sink == nil || (ev.Type != provider.HostedToolCall && ev.Type != provider.HostedToolResult) {
		return
	}
	eventCopy := observed
	typ := AgentStreamToolCall
	message := ""
	if ev.Type == provider.HostedToolResult {
		typ = AgentStreamToolResult
		if hostedResult != nil {
			message = hostedResult.SummaryText()
		}
	}
	sink.EmitAgentStream(AgentStreamEvent{
		Type: typ, SessionID: threadID, TurnID: turnID, Step: step, At: observed.ObservedAt,
		ProviderEvent: &eventCopy, Message: message,
	})
}

func observedHostedResult(result provider.HostedToolResultData) *provider.HostedToolResultData {
	if result.IsZero() {
		return nil
	}
	return &result
}

func observedHostedResultMetadata(ev provider.StreamEvent) map[string]string {
	if ev.Type != provider.HostedToolResult {
		return nil
	}
	result := ev.HostedResult
	out := map[string]string{}
	if len(result.Results) > 0 {
		out["result_count"] = fmt.Sprintf("%d", len(result.Results))
	}
	if result.Error != nil && result.Error.Code != "" {
		out["error_code"] = result.Error.Code
	}
	if text := result.SummaryText(); text != "" {
		out["result_hash"] = providerResultHash(text)
	} else if ev.Text != "" {
		out["result_hash"] = providerResultHash(ev.Text)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func providerResultHash(value string) string {
	if value == "" {
		return ""
	}
	return "sha256:" + cache.StableHash(value)
}

func unavailableCapabilitiesFromHostedRequest(req provider.Request) []string {
	return nil
}

func observedToolCalls(ev provider.StreamEvent) []provider.ToolCall {
	if len(ev.ToolCalls) > 0 {
		return append([]provider.ToolCall(nil), ev.ToolCalls...)
	}
	if ev.ToolCall.ID != "" || ev.ToolCall.Name != "" || ev.ToolCall.Args != "" {
		return []provider.ToolCall{ev.ToolCall}
	}
	return nil
}

func reasoningText(ev provider.StreamEvent) string {
	if ev.Type == provider.Reasoning {
		return ev.Text
	}
	if len(ev.ToolCalls) == 0 {
		return ""
	}
	var out string
	for _, call := range ev.ToolCalls {
		if call.Reasoning == "" {
			continue
		}
		if out != "" {
			out += "\n"
		}
		out += call.Reasoning
	}
	return out
}

func observeRawSegments(plan cache.RawPlan) []ObservedRawSegment {
	out := make([]ObservedRawSegment, 0, len(plan.Segments))
	for i, seg := range plan.Segments {
		reused := false
		if i < len(plan.SegmentStates) {
			reused = plan.SegmentStates[i] == "reused"
		}
		raw, truncated := boundedRaw(seg.Raw, maxObservedRawSegmentBytes)
		out = append(out, ObservedRawSegment{
			ID:                   seg.ID,
			PromptScopeID:        seg.PromptScopeID,
			CreatedByRunID:       seg.CreatedByRunID,
			CreatedByTurnID:      seg.CreatedByTurnID,
			ThreadID:             seg.ThreadID,
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
			Attachments:          append([]session.MessageAttachment(nil), msg.Attachments...),
			References:           append([]session.MessageReference(nil), msg.References...),
			Reasoning:            msg.Reasoning,
			ToolCallID:           msg.ToolCallID,
			ToolName:             msg.ToolName,
			ToolArgs:             msg.ToolArgs,
			Kind:                 string(msg.Kind),
			ToolResult:           observeToolResultView(msg.ToolResult),
			EntryID:              msg.EntryID,
			ParentEntryID:        msg.ParentEntryID,
			CompactionID:         msg.CompactionID,
			CompactionGeneration: msg.CompactionGeneration,
			CompactionWindowID:   msg.CompactionWindowID,
		})
	}
	return out
}

func observeContextProjection(projection sessiontree.ContextProjection) ObservedContextProjection {
	return ObservedContextProjection{
		Messages: observeMessages(projection.Messages),
		Segments: observeContextSegments(projection.Segments),
	}
}

func observeContextSegments(segments []sessiontree.ProjectedSegment) []ObservedContextSegment {
	out := make([]ObservedContextSegment, 0, len(segments))
	for _, seg := range segments {
		out = append(out, ObservedContextSegment{
			EntryID:       seg.EntryID,
			EntryType:     seg.EntryType,
			MessageIndex:  seg.MessageIndex,
			Role:          string(seg.Role),
			ToolCallID:    seg.ToolCallID,
			ToolName:      seg.ToolName,
			TokenEstimate: seg.TokenEstimate,
			ArtifactRefs:  observeArtifactRefs(seg.ArtifactRefs),
			UIPreview:     seg.UIPreview,
		})
	}
	return out
}

func observeArtifactRefs(refs []artifact.Ref) []ObservedArtifactRef {
	out := make([]ObservedArtifactRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, observeArtifactRef(ref))
	}
	return out
}

func observeArtifactRef(ref artifact.Ref) ObservedArtifactRef {
	return ObservedArtifactRef{
		ID:        ref.ID,
		SafeLabel: ref.SafeLabel,
		Kind:      ref.Kind,
		MIME:      ref.MIME,
		SizeBytes: ref.SizeBytes,
		SHA256:    ref.SHA256,
	}
}

func observeToolResultView(view *session.ToolResultView) *ObservedToolResultView {
	if view == nil {
		return nil
	}
	out := &ObservedToolResultView{
		Truncated:     view.Truncated,
		OriginalBytes: view.OriginalBytes,
		VisibleBytes:  view.VisibleBytes,
		OriginalLines: view.OriginalLines,
		VisibleLines:  view.VisibleLines,
		Strategy:      view.Strategy,
		ContentSHA256: view.ContentSHA256,
	}
	if view.FullOutput != nil {
		ref := observeArtifactRef(*view.FullOutput)
		out.FullOutput = &ref
	}
	return out
}
