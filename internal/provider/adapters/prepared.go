package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"sync"
)

type preparedWire struct {
	mu          sync.Mutex
	used        bool
	outputField string
	body        []byte
	estimate    provider.TokenEstimate
	stream      func(context.Context, []byte) (<-chan provider.StreamEvent, error)
}

func freezeWire(req provider.Request, format string, body []byte, stream func(context.Context, []byte) (<-chan provider.StreamEvent, error)) (provider.PreparedRequest, error) {
	estimate, err := estimateRenderedRequest(req, format, body)
	if err != nil {
		return nil, err
	}
	field := "max_tokens"
	if format == "openai_chat" {
		var envelope map[string]json.RawMessage
		_ = json.Unmarshal(body, &envelope)
		if _, ok := envelope["max_completion_tokens"]; ok {
			field = "max_completion_tokens"
		}
	}
	return &preparedWire{body: body, estimate: estimate, stream: stream, outputField: field}, nil
}
func (p *preparedWire) Stream(ctx context.Context) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	used := p.used
	p.used = true
	p.mu.Unlock()
	if used {
		return nil, errors.New("prepared request already used or closed")
	}
	return p.stream(ctx, p.body)
}
func (p *preparedWire) TokenEstimate() provider.TokenEstimate {
	out := p.estimate
	out.Characters = out.Characters.Clone()
	if out.Breakdown.Details != nil {
		d := *out.Breakdown.Details
		d.Items = append([]contextpolicy.ContextTokenDetail(nil), d.Items...)
		out.Breakdown.Details = &d
	}
	return out
}
func (p *preparedWire) PayloadFingerprint() string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(p.body))
}
func (p *preparedWire) Close() error                 { p.mu.Lock(); p.used = true; p.mu.Unlock(); return nil }
func (p *preparedWire) EnforceInputTokenLimit() bool { return true }
func (p *preparedWire) PrepareRetry(ctx context.Context, _ provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &preparedWire{body: p.body, estimate: p.estimate, stream: p.stream, outputField: p.outputField}, nil
}

func (p OpenAICompatibleProvider) PrepareRequest(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	req.Cache = policy
	if err := validateOpenAICompatibleHostedTools(req.HostedTools); err != nil {
		return nil, err
	}
	body, err := p.chatRequestBody(req)
	if err != nil {
		return nil, err
	}
	return freezeWire(req, "openai_chat", body, p.streamRendered)
}
func (p *preparedWire) WithMaxOutputTokens(ctx context.Context, limit int64) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("output limit must be positive")
	}
	p.mu.Lock()
	used := p.used
	p.mu.Unlock()
	if used {
		return nil, errors.New("prepared request already used or closed")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(p.body, &envelope); err != nil {
		return nil, err
	}
	envelope[p.outputField], _ = json.Marshal(limit)
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	return &preparedWire{body: body, estimate: p.estimate, stream: p.stream, outputField: p.outputField}, nil
}
