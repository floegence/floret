package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type AnthropicProvider struct {
	Endpoint   string
	APIKey     string
	Model      string
	MaxTokens  int64
	CostModel  modelcatalog.Model
	Cache      modelcatalog.CacheCapability
	HTTPClient *http.Client
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int64              `json:"max_tokens"`
	System    any                `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
	Tools     []anthropicTool    `json:"tools,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type anthropicTool struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description,omitempty"`
	InputSchema  map[string]any         `json:"input_schema"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicContentBlock struct {
	Type         string                 `json:"type"`
	Text         string                 `json:"text,omitempty"`
	ID           string                 `json:"id,omitempty"`
	Name         string                 `json:"name,omitempty"`
	Input        json.RawMessage        `json:"input,omitempty"`
	ToolUseID    string                 `json:"tool_use_id,omitempty"`
	Content      string                 `json:"content,omitempty"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicCacheControl struct {
	Type string `json:"type"`
	TTL  string `json:"ttl,omitempty"`
}

type anthropicUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

type anthropicResponse struct {
	Content    []anthropicContentBlock `json:"content"`
	StopReason string                  `json:"stop_reason"`
	Usage      anthropicUsage          `json:"usage"`
	Error      *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func (p AnthropicProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	if p.Endpoint == "" {
		return nil, fmt.Errorf("anthropic endpoint is required")
	}
	if p.APIKey == "" {
		return nil, fmt.Errorf("anthropic api key is required")
	}
	if p.Model == "" {
		return nil, fmt.Errorf("anthropic model is required")
	}
	normalizedCache, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	req.Cache = normalizedCache
	maxTokens := p.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	body, err := json.Marshal(p.buildAnthropicRequest(req, maxTokens))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("x-api-key", p.APIKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")
	httpReq.Header.Set("content-type", "application/json")
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if httpResp.StatusCode == http.StatusRequestEntityTooLarge || httpResp.StatusCode == http.StatusBadRequest && looksLikeContextOverflow(respBody) {
		return nil, provider.ErrContextOverflow
	}
	var parsed anthropicResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("decode anthropic response: %w", err)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("anthropic provider error: %s", parsed.Error.Message)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("anthropic provider status %d", httpResp.StatusCode)
	}
	ch := make(chan provider.StreamEvent, 4)
	if len(parsed.Content) == 0 {
		ch <- provider.StreamEvent{Type: provider.Empty}
		close(ch)
		return ch, nil
	}
	var calls []provider.ToolCall
	for _, block := range parsed.Content {
		switch block.Type {
		case "text":
			if block.Text != "" {
				ch <- provider.StreamEvent{Type: provider.Delta, Text: block.Text}
			}
		case "tool_use":
			calls = append(calls, provider.ToolCall{ID: block.ID, Name: block.Name, Args: string(block.Input)})
		}
	}
	if usage := normalizeAnthropicUsage(parsed.Usage, p.CostModel); usage.TotalTokens > 0 || usage.CostUSD > 0 {
		ch <- provider.StreamEvent{Type: provider.UsageEvent, Usage: usage}
	}
	if len(calls) > 0 {
		ch <- provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}
	}
	if parsed.StopReason == "max_tokens" {
		ch <- provider.StreamEvent{Type: provider.Truncated, Reason: "max_tokens"}
	} else {
		ch <- provider.StreamEvent{Type: provider.Done, Reason: parsed.StopReason}
	}
	close(ch)
	return ch, nil
}

func (p AnthropicProvider) buildAnthropicRequest(req provider.Request, maxTokens int64) anthropicRequest {
	messages := req.Messages
	if len(req.RawPlan.Segments) > 0 {
		messages = nil
	}
	cacheControl := anthropicCacheControlFor(req, p.Cache)
	system := renderAnthropicSystem(messages, cacheControl)
	renderedMessages := renderAnthropicMessages(messages, cacheControl)
	renderedTools := renderAnthropicTools(req.Tools, cacheControl)
	if len(req.RawPlan.Segments) > 0 {
		system = renderAnthropicSystemFromRawPlan(req.RawPlan, cacheControl, system)
		renderedMessages = renderAnthropicMessagesFromRawPlan(req.RawPlan, renderedMessages)
		renderedTools = renderAnthropicToolsFromRawPlan(req.RawPlan, cacheControl, renderedTools)
	}
	return anthropicRequest{
		Model:     p.Model,
		MaxTokens: maxTokens,
		System:    system,
		Messages:  renderedMessages,
		Tools:     renderedTools,
	}
}

func (p AnthropicProvider) NormalizeCachePolicy(policy promptcache.CachePolicy) (promptcache.CachePolicy, error) {
	if !policy.Enabled || policy.Retention == promptcache.RetentionNone {
		policy.Enabled = false
		policy.Retention = promptcache.RetentionNone
		return policy, nil
	}
	if policy.Retention == "" {
		policy.Retention = promptcache.RetentionShort
	}
	switch policy.Retention {
	case promptcache.RetentionShort, promptcache.RetentionLong:
		return policy, nil
	case promptcache.RetentionInMemory:
		return promptcache.CachePolicy{}, fmt.Errorf("anthropic prompt cache retention %q is unsupported; use %q or %q", policy.Retention, promptcache.RetentionShort, promptcache.RetentionLong)
	case promptcache.RetentionDay:
		return promptcache.CachePolicy{}, fmt.Errorf("anthropic prompt cache retention %q is unsupported; use %q or %q", policy.Retention, promptcache.RetentionShort, promptcache.RetentionLong)
	default:
		return promptcache.CachePolicy{}, fmt.Errorf("unsupported prompt cache retention %q", policy.Retention)
	}
}

func (p AnthropicProvider) DefaultCacheRetention() promptcache.Retention {
	return promptcache.RetentionShort
}

func (p AnthropicProvider) PayloadHash(req provider.Request) (string, error) {
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return "", err
	}
	req.Cache = policy
	maxTokens := p.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	body, err := json.Marshal(p.buildAnthropicRequest(req, maxTokens))
	if err != nil {
		return "", err
	}
	return promptcache.StableHash(string(body)), nil
}

func (p AnthropicProvider) MessageRaw(kind promptcache.SegmentKind, msg session.Message) (string, string, error) {
	if msg.Role == session.System {
		block := anthropicContentBlock{Type: "text", Text: msg.Content}
		raw, err := promptcache.CanonicalJSON(block)
		return raw, promptcache.FragmentAnthropicSystem, err
	}
	rendered := renderAnthropicMessages([]session.Message{msg}, nil)
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := promptcache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	_ = kind
	return raw, promptcache.FragmentAnthropicMessage, nil
}

func (p AnthropicProvider) ToolRaw(def promptcache.ToolDefinition) (string, string, error) {
	rendered := renderAnthropicTools([]provider.ToolDefinition{{Name: def.Name, Description: def.Description}}, nil)
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := promptcache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	return raw, promptcache.FragmentAnthropicTool, nil
}

func anthropicSystemBlocks(messages []session.Message, cacheControl *anthropicCacheControl) []anthropicContentBlock {
	var blocks []anthropicContentBlock
	for _, msg := range messages {
		if msg.Role == session.System {
			blocks = append(blocks, anthropicContentBlock{Type: "text", Text: msg.Content})
		}
	}
	if cacheControl != nil && len(blocks) > 0 {
		blocks[len(blocks)-1].CacheControl = cacheControl
	}
	return blocks
}

func renderAnthropicSystem(messages []session.Message, cacheControl *anthropicCacheControl) any {
	blocks := anthropicSystemBlocks(messages, cacheControl)
	if len(blocks) == 0 {
		return nil
	}
	if cacheControl == nil {
		if len(blocks) == 1 {
			return blocks[0].Text
		}
		return blocks
	}
	return blocks
}

func renderAnthropicSystemFromRawPlan(plan promptcache.RawPlan, cacheControl *anthropicCacheControl, fallback any) any {
	var blocks []anthropicContentBlock
	for _, segment := range plan.Segments {
		if segment.FragmentType != promptcache.FragmentAnthropicSystem {
			continue
		}
		var block anthropicContentBlock
		if err := json.Unmarshal([]byte(segment.Raw), &block); err == nil {
			blocks = append(blocks, block)
		}
	}
	if len(blocks) == 0 {
		return fallback
	}
	if cacheControl != nil {
		blocks[len(blocks)-1].CacheControl = cacheControl
		return blocks
	}
	if len(blocks) == 1 {
		return blocks[0].Text
	}
	return blocks
}

func renderAnthropicMessages(messages []session.Message, _ *anthropicCacheControl) []anthropicMessage {
	out := make([]anthropicMessage, 0, len(messages))
	for _, msg := range messages {
		switch msg.Role {
		case session.System:
			continue
		case session.User:
			out = append(out, anthropicMessage{Role: "user", Content: msg.Content})
		case session.Assistant:
			if msg.ToolCallID != "" && msg.ToolName != "" {
				input := json.RawMessage(msg.ToolArgs)
				if !json.Valid(input) {
					input, _ = json.Marshal(map[string]string{"input": msg.ToolArgs})
				}
				out = append(out, anthropicMessage{Role: "assistant", Content: []anthropicContentBlock{{
					Type:  "tool_use",
					ID:    msg.ToolCallID,
					Name:  msg.ToolName,
					Input: input,
				}}})
				continue
			}
			out = append(out, anthropicMessage{Role: "assistant", Content: msg.Content})
		case session.Tool:
			out = append(out, anthropicMessage{Role: "user", Content: []anthropicContentBlock{{
				Type:      "tool_result",
				ToolUseID: msg.ToolCallID,
				Content:   msg.Content,
			}}})
		}
	}
	return out
}

func renderAnthropicMessagesFromRawPlan(plan promptcache.RawPlan, fallback []anthropicMessage) []anthropicMessage {
	var out []anthropicMessage
	for _, segment := range plan.Segments {
		if segment.FragmentType != promptcache.FragmentAnthropicMessage {
			continue
		}
		var msg anthropicMessage
		if err := json.Unmarshal([]byte(segment.Raw), &msg); err == nil {
			out = append(out, msg)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func renderAnthropicTools(defs []provider.ToolDefinition, cacheControl *anthropicCacheControl) []anthropicTool {
	tools := make([]anthropicTool, 0, len(defs))
	for i, def := range defs {
		if def.Name == "" {
			continue
		}
		toolCacheControl := (*anthropicCacheControl)(nil)
		if cacheControl != nil && i == len(defs)-1 {
			toolCacheControl = cacheControl
		}
		tools = append(tools, anthropicTool{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": true,
			},
			CacheControl: toolCacheControl,
		})
	}
	return tools
}

func renderAnthropicToolsFromRawPlan(plan promptcache.RawPlan, cacheControl *anthropicCacheControl, fallback []anthropicTool) []anthropicTool {
	var out []anthropicTool
	for _, segment := range plan.Segments {
		if segment.FragmentType != promptcache.FragmentAnthropicTool {
			continue
		}
		var tool anthropicTool
		if err := json.Unmarshal([]byte(segment.Raw), &tool); err == nil {
			out = append(out, tool)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	if cacheControl != nil {
		out[len(out)-1].CacheControl = cacheControl
	}
	return out
}

func anthropicCacheControlFor(req provider.Request, capability modelcatalog.CacheCapability) *anthropicCacheControl {
	if !req.Cache.Enabled || !capability.AnthropicCacheControl {
		return nil
	}
	control := &anthropicCacheControl{Type: "ephemeral"}
	if req.Cache.Retention == "1h" {
		control.TTL = "1h"
	}
	return control
}

func normalizeAnthropicUsage(payload anthropicUsage, model modelcatalog.Model) provider.Usage {
	usage := provider.Usage{
		InputTokens:      payload.InputTokens,
		OutputTokens:     payload.OutputTokens,
		CacheReadTokens:  payload.CacheReadInputTokens,
		CacheWriteTokens: payload.CacheCreationInputTokens,
		Source:           provider.UsageNative,
	}.Normalized()
	if usage.InputTokens >= usage.CacheReadTokens {
		usage.InputTokens -= usage.CacheReadTokens
	}
	if model.ID != "" {
		usage.CostUSD = modelcatalog.CostForUsage(model, usage)
	}
	return usage.Normalized()
}
