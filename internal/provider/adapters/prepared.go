package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider"
	"sync"
)

type preparedWire struct {
	mu       sync.Mutex
	used     bool
	body     []byte
	estimate provider.TokenEstimate
	stream   func(context.Context, []byte) (<-chan provider.StreamEvent, error)
}

func freezeWire(req provider.Request, format string, body []byte, stream func(context.Context, []byte) (<-chan provider.StreamEvent, error)) (provider.PreparedRequest, error) {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	estimate, err := estimateRenderedRequest(req, format, decoded)
	if err != nil {
		return nil, err
	}
	return &preparedWire{body: body, estimate: estimate, stream: stream}, nil
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
func (p *preparedWire) TokenEstimate() provider.TokenEstimate { return p.estimate }
func (p *preparedWire) PayloadFingerprint() string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(p.body))
}
func (p *preparedWire) Close() error                 { p.mu.Lock(); p.used = true; p.mu.Unlock(); return nil }
func (p *preparedWire) EnforceInputTokenLimit() bool { return true }
func (p *preparedWire) PrepareRetry(ctx context.Context, _ provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &preparedWire{body: p.body, estimate: p.estimate, stream: p.stream}, nil
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
func (p AnthropicProvider) PrepareRequest(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	req.Cache = policy
	if err := validateAnthropicHostedTools(req.HostedTools); err != nil {
		return nil, err
	}
	body, err := p.anthropicRequestBody(req)
	if err != nil {
		return nil, err
	}
	return freezeWire(req, "anthropic_messages", body, p.streamRendered)
}
