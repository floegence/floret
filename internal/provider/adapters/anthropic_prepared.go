package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/toolrender"
)

type AnthropicAttachment struct {
	Kind, MIMEType string
	Data           []byte
	URL, Text      string
}

func (p AnthropicProvider) resolveAttachments(ctx context.Context, req provider.Request, body *anthropicRequest) error {
	messages, err := provider.MessagesWithEphemeralUser(req.Messages, req.EphemeralUser)
	if err != nil {
		return err
	}
	index := 0
	for _, m := range messages {
		if m.Role == session.System {
			continue
		}
		attachments := m.Attachments
		if m.ToolResult != nil {
			attachments = append(append([]session.MessageAttachment(nil), attachments...), m.ToolResult.Attachments...)
		}
		if len(attachments) > 0 {
			if p.ResolveAttachment == nil {
				return errors.New("Anthropic attachment requires an authorized resolver")
			}
			if index >= len(body.Messages) {
				return errors.New("Anthropic attachments do not match rendered messages")
			}
			blocks, err := anthropicBlocks(body.Messages[index].Content)
			if err != nil {
				return err
			}
			var extra []anthropicContentBlock
			for _, a := range attachments {
				v, err := p.ResolveAttachment(ctx, a)
				if err != nil {
					return err
				}
				block, err := anthropicAttachmentBlock(v)
				if err != nil {
					return err
				}
				extra = append(extra, block)
			}
			if m.Role == session.Tool {
				if len(blocks) != 1 || blocks[0].Type != "tool_result" {
					return errors.New("Anthropic attachment has no owning tool result")
				}
				var content []anthropicContentBlock
				var text string
				if json.Unmarshal(blocks[0].Content, &text) == nil {
					if text != "" {
						content = append(content, anthropicContentBlock{Type: "text", Text: text})
					}
				} else if json.Unmarshal(blocks[0].Content, &content) != nil {
					return errors.New("invalid Anthropic tool result content")
				}
				blocks[0].Content, _ = json.Marshal(append(content, extra...))
			} else {
				blocks = append(blocks, extra...)
			}
			body.Messages[index].Content = blocks
		}
		index++
	}
	return nil
}
func anthropicAttachmentBlock(a AnthropicAttachment) (anthropicContentBlock, error) {
	n := 0
	for _, present := range []bool{len(a.Data) > 0, a.URL != "", a.Text != ""} {
		if present {
			n++
		}
	}
	if n != 1 {
		return anthropicContentBlock{}, errors.New("Anthropic attachment requires exactly one content source")
	}
	var source any
	switch a.Kind {
	case "text":
		if a.Text == "" || !utf8.ValidString(a.Text) {
			return anthropicContentBlock{}, errors.New("Anthropic manifest requires text")
		}
		return anthropicContentBlock{Type: "text", Text: a.Text}, nil
	case "image":
		if a.URL != "" {
			u, e := url.Parse(a.URL)
			if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
				return anthropicContentBlock{}, errors.New("invalid authorized image URL")
			}
			source = map[string]any{"type": "url", "url": a.URL}
		} else {
			switch a.MIMEType {
			case "image/jpeg", "image/png", "image/gif", "image/webp":
			default:
				return anthropicContentBlock{}, errors.New("unsupported Anthropic image MIME type")
			}
			if len(a.Data) == 0 {
				return anthropicContentBlock{}, errors.New("Anthropic image requires bytes")
			}
			source = map[string]any{"type": "base64", "media_type": a.MIMEType, "data": base64.StdEncoding.EncodeToString(a.Data)}
		}
	case "document":
		if len(a.Data) == 0 {
			return anthropicContentBlock{}, errors.New("Anthropic document requires inline bytes")
		}
		if a.MIMEType == "application/pdf" {
			source = map[string]any{"type": "base64", "media_type": a.MIMEType, "data": base64.StdEncoding.EncodeToString(a.Data)}
		} else if strings.HasPrefix(a.MIMEType, "text/") || a.MIMEType == "application/json" || a.MIMEType == "application/xml" || a.MIMEType == "application/yaml" || a.MIMEType == "application/x-yaml" || a.MIMEType == "application/toml" || a.MIMEType == "application/markdown" {
			if !utf8.Valid(a.Data) {
				return anthropicContentBlock{}, errors.New("Anthropic text document must be UTF-8")
			}
			source = map[string]any{"type": "text", "media_type": "text/plain", "data": string(a.Data)}
		} else {
			return anthropicContentBlock{}, errors.New("unsupported Anthropic document MIME type")
		}
	default:
		return anthropicContentBlock{}, errors.New("unsupported Anthropic attachment kind")
	}
	raw, err := json.Marshal(source)
	return anthropicContentBlock{Type: a.Kind, Source: raw}, err
}

type anthropicPrepared struct {
	mu                  sync.Mutex
	used                bool
	adapter             AnthropicProvider
	body                []byte
	estimate            provider.TokenEstimate
	history             anthropicHistory
	stateAllowed        bool
	attempt             string
	measurementIdentity string
}

func (p AnthropicProvider) PrepareRequest(ctx context.Context, req provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	if req.EphemeralUser != nil {
		policy.Enabled = false
	}
	req.Cache = policy
	if err = validateAnthropicHostedTools(req.HostedTools); err != nil {
		return nil, err
	}
	maxTokens, err := p.maxTokensForRequest(req)
	if err != nil {
		return nil, err
	}
	body, err := p.buildAnthropicRequest(req, maxTokens)
	if err != nil {
		return nil, err
	}
	if err = p.resolveAttachments(ctx, req, &body); err != nil {
		return nil, err
	}
	canonical, err := canonicalAnthropicMessages(body.Messages)
	if err != nil {
		return nil, err
	}
	identity := anthropicHash([]string{p.Endpoint, p.Model, p.StateIdentity, p.APIKey})
	previous := req.PreviousState
	allowed := !req.DisableProviderState && req.EphemeralUser == nil
	if !allowed {
		previous = nil
	}
	history, err := prepareAnthropicHistory(canonical, previous, identity)
	if err != nil {
		return nil, err
	}
	history.cacheMarked = req.Cache.Enabled && p.Cache.AnthropicCacheControl
	body.Messages = history.input
	body.Stream = true
	applyAnthropicCache(&body, anthropicCacheControlFor(req, p.Cache), history.bridge)
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	raw, err = toolrender.OrderSchemas("anthropic_messages", raw)
	if err != nil {
		return nil, err
	}
	if p.MaxRequestBytes > 0 && int64(len(raw)) > p.MaxRequestBytes {
		return nil, &anthropicRequestByteLimitError{int64(len(raw)), p.MaxRequestBytes}
	}
	estimate, err := estimateRenderedRequest(req, "anthropic_messages", raw)
	if err != nil {
		return nil, err
	}
	return &anthropicPrepared{adapter: p, body: raw, estimate: estimate, history: history, stateAllowed: allowed, attempt: req.AttemptID, measurementIdentity: identity}, nil
}
func (p *anthropicPrepared) Stream(ctx context.Context) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	used := p.used
	p.used = true
	p.mu.Unlock()
	if used {
		return nil, errors.New("prepared request already used or closed")
	}
	return p.adapter.streamAnthropic(ctx, p)
}
func (p *anthropicPrepared) TokenEstimate() provider.TokenEstimate {
	x := p.estimate
	x.Characters = x.Characters.Clone()
	if x.Breakdown.Details != nil {
		d := *x.Breakdown.Details
		d.Items = append(d.Items[:0:0], d.Items...)
		x.Breakdown.Details = &d
	}
	return x
}
func (p *anthropicPrepared) PayloadFingerprint() string   { return "sha256:" + anthropicRawHash(p.body) }
func anthropicRawHash(raw []byte) string                  { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
func (p *anthropicPrepared) Close() error                 { p.mu.Lock(); p.used = true; p.mu.Unlock(); return nil }
func (p *anthropicPrepared) EnforceInputTokenLimit() bool { return true }
func (p *anthropicPrepared) clone(body []byte, attempt string) *anthropicPrepared {
	return &anthropicPrepared{adapter: p.adapter, body: body, estimate: p.estimate, history: p.history, stateAllowed: p.stateAllowed, attempt: attempt, measurementIdentity: p.measurementIdentity}
}
func (p *anthropicPrepared) PrepareRetry(ctx context.Context, next provider.Request) (provider.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.clone(p.body, next.AttemptID), nil
}
func (p *anthropicPrepared) WithMaxOutputTokens(ctx context.Context, limit int64) (provider.PreparedRequest, error) {
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
	var body map[string]json.RawMessage
	if err := json.Unmarshal(p.body, &body); err != nil {
		return nil, err
	}
	body["max_tokens"], _ = json.Marshal(limit)
	var thinking anthropicThinking
	_ = json.Unmarshal(body["thinking"], &thinking)
	if thinking.BudgetTokens >= limit {
		return nil, errors.New("Anthropic thinking budget exceeds output limit")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	raw, err = toolrender.OrderSchemas("anthropic_messages", raw)
	if err != nil {
		return nil, err
	}
	if p.adapter.MaxRequestBytes > 0 && int64(len(raw)) > p.adapter.MaxRequestBytes {
		return nil, &anthropicRequestByteLimitError{int64(len(raw)), p.adapter.MaxRequestBytes}
	}
	return p.clone(raw, p.attempt), nil
}
func (p *anthropicPrepared) InputMeasurementPrefix() requestmeasure.Prefix {
	x, _ := requestmeasure.RenderedPrefix(anthropicMeasuredBody(p.body), "anthropic_messages", p.measurementIdentity, "input")
	return x
}
func (p *anthropicPrepared) MeasurementSuffix(prefix requestmeasure.Prefix) ([]byte, bool) {
	return requestmeasure.RenderedSuffix(anthropicMeasuredBody(p.body), "anthropic_messages", p.measurementIdentity, prefix)
}

type anthropicRequestByteLimitError struct{ actual, limit int64 }

func (e *anthropicRequestByteLimitError) Error() string {
	return fmt.Sprintf("Anthropic request byte limit exceeded: actual_bytes=%d limit_bytes=%d", e.actual, e.limit)
}
func (e *anthropicRequestByteLimitError) RequestByteLimit() (int64, int64) { return e.actual, e.limit }
func (e *anthropicRequestByteLimitError) Unwrap() error                    { return provider.ErrContextOverflow }
