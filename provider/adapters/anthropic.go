package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/session"
)

type AnthropicProvider struct {
	Endpoint   string
	APIKey     string
	Model      string
	MaxTokens  int64
	CostModel  catalog.Model
	Cache      catalog.CacheCapability
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
	Type         string                 `json:"type,omitempty"`
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
	Content      json.RawMessage        `json:"content,omitempty"`
	Query        string                 `json:"query,omitempty"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicWebSearchResultItem struct {
	Type             string `json:"type"`
	URL              string `json:"url"`
	Title            string `json:"title"`
	PageAge          string `json:"page_age"`
	EncryptedContent string `json:"encrypted_content"`
}

type anthropicWebSearchResultError struct {
	Type      string `json:"type"`
	ErrorCode string `json:"error_code"`
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
	if err := validateAnthropicHostedTools(req.HostedTools); err != nil {
		return nil, err
	}
	normalizedCache, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	req.Cache = normalizedCache
	body, err := p.anthropicRequestBody(req)
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
	ch := make(chan provider.StreamEvent, len(parsed.Content)+4)
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
		case "server_tool_use":
			ch <- provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: block.ID, Name: block.Name, Args: block.Query}}
		case "web_search_tool_result":
			result := decodeAnthropicWebSearchResult(block.Content)
			ch <- provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: provider.ToolCall{ID: block.ToolUseID, Name: searchcap.ToolWebSearch}, Text: result.Text, HostedResult: result}
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

func decodeAnthropicWebSearchResult(raw json.RawMessage) provider.HostedToolResultData {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return provider.HostedToolResultData{}
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			return provider.HostedToolResultData{Text: text}
		}
	}
	if raw[0] == '[' {
		var items []anthropicWebSearchResultItem
		if err := json.Unmarshal(raw, &items); err == nil {
			return anthropicSearchItemsResult(items)
		}
	}
	if raw[0] == '{' {
		var errPayload anthropicWebSearchResultError
		if err := json.Unmarshal(raw, &errPayload); err == nil && errPayload.Type == "web_search_tool_result_error" {
			return provider.HostedToolResultData{
				Text: fmt.Sprintf("Anthropic web_search failed: %s", errPayload.ErrorCode),
				Error: &provider.HostedToolResultError{
					Code:    errPayload.ErrorCode,
					Message: fmt.Sprintf("Anthropic web_search failed: %s", errPayload.ErrorCode),
				},
			}
		}
	}
	return provider.HostedToolResultData{Text: string(raw)}
}

func anthropicSearchItemsResult(items []anthropicWebSearchResultItem) provider.HostedToolResultData {
	results := make([]provider.HostedToolResultItem, 0, len(items))
	for _, item := range items {
		if item.Type != "" && item.Type != "web_search_result" {
			continue
		}
		metadata := map[string]any{}
		if item.PageAge != "" {
			metadata["page_age"] = item.PageAge
		}
		if item.EncryptedContent != "" {
			metadata["encrypted_content"] = item.EncryptedContent
		}
		if len(metadata) == 0 {
			metadata = nil
		}
		results = append(results, provider.HostedToolResultItem{
			Title:    item.Title,
			URL:      item.URL,
			Source:   item.URL,
			Metadata: metadata,
		})
	}
	result := provider.HostedToolResultData{Results: results}
	result.Text = result.SummaryText()
	return result
}

func (p AnthropicProvider) buildAnthropicRequest(req provider.Request, maxTokens int64) (anthropicRequest, error) {
	anthropicReq, err := p.contextAnthropicRequestWithCacheControl(req, anthropicCacheControlFor(req, p.Cache))
	if err != nil {
		return anthropicRequest{}, err
	}
	anthropicReq.Model = p.Model
	anthropicReq.MaxTokens = maxTokens
	return anthropicReq, nil
}

func (p AnthropicProvider) contextAnthropicRequest(req provider.Request) (anthropicRequest, error) {
	return p.contextAnthropicRequestWithCacheControl(req, nil)
}

func (p AnthropicProvider) contextAnthropicRequestWithCacheControl(req provider.Request, cacheControl *anthropicCacheControl) (anthropicRequest, error) {
	messages := req.Messages
	if len(req.RawPlan.Segments) > 0 {
		messages = nil
	}
	system := renderAnthropicSystem(messages, cacheControl)
	renderedMessages := renderAnthropicMessages(messages, cacheControl)
	renderedTools := append(renderAnthropicTools(req.Tools, cacheControl), renderAnthropicHostedTools(req.HostedTools)...)
	if len(req.RawPlan.Segments) > 0 {
		var err error
		system, err = renderAnthropicSystemFromRawPlan(req.RawPlan, cacheControl)
		if err != nil {
			return anthropicRequest{}, err
		}
		renderedMessages, err = renderAnthropicMessagesFromRawPlan(req.RawPlan)
		if err != nil {
			return anthropicRequest{}, err
		}
		renderedTools, err = renderAnthropicToolsFromRawPlan(req.RawPlan, len(req.Tools), cacheControl, renderAnthropicHostedTools(req.HostedTools))
		if err != nil {
			return anthropicRequest{}, err
		}
	}
	return anthropicRequest{
		System:   system,
		Messages: renderedMessages,
		Tools:    renderedTools,
	}, nil
}

func (p AnthropicProvider) NormalizeCachePolicy(policy cache.CachePolicy) (cache.CachePolicy, error) {
	if !policy.Enabled || policy.Retention == cache.RetentionNone {
		policy.Enabled = false
		policy.Retention = cache.RetentionNone
		return policy, nil
	}
	if policy.Retention == "" {
		policy.Retention = cache.RetentionShort
	}
	switch policy.Retention {
	case cache.RetentionShort, cache.RetentionLong:
		return policy, nil
	case cache.RetentionInMemory:
		return cache.CachePolicy{}, fmt.Errorf("anthropic prompt cache retention %q is unsupported; use %q or %q", policy.Retention, cache.RetentionShort, cache.RetentionLong)
	case cache.RetentionDay:
		return cache.CachePolicy{}, fmt.Errorf("anthropic prompt cache retention %q is unsupported; use %q or %q", policy.Retention, cache.RetentionShort, cache.RetentionLong)
	default:
		return cache.CachePolicy{}, fmt.Errorf("unsupported prompt cache retention %q", policy.Retention)
	}
}

func (p AnthropicProvider) DefaultCacheRetention() cache.Retention {
	return cache.RetentionShort
}

func (p AnthropicProvider) PayloadHash(req provider.Request) (string, error) {
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return "", err
	}
	req.Cache = policy
	if err := validateAnthropicHostedTools(req.HostedTools); err != nil {
		return "", err
	}
	body, err := p.anthropicRequestBody(req)
	if err != nil {
		return "", err
	}
	return cache.StableHash(string(body)), nil
}

func (p AnthropicProvider) EstimateTokens(_ context.Context, req provider.Request) (provider.TokenEstimate, error) {
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	req.Cache = policy
	if err := validateAnthropicHostedTools(req.HostedTools); err != nil {
		return provider.TokenEstimate{}, err
	}
	anthropicReq, err := p.contextAnthropicRequest(req)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	return estimateRenderedParts("anthropic_rendered_json", anthropicReq.System, anthropicReq.Messages, anthropicReq.Tools)
}

func (p AnthropicProvider) maxTokensForRequest(req provider.Request) (int64, error) {
	maxTokens := p.MaxTokens
	if req.MaxOutputTokens > 0 {
		maxTokens = int64(req.MaxOutputTokens)
	} else if req.ContextPolicy.MaxOutputTokens > 0 {
		maxTokens = int64(req.ContextPolicy.MaxOutputTokens)
	}
	if maxTokens <= 0 {
		return 0, fmt.Errorf("anthropic max output tokens are required for model %q; set FLORET_MAX_OUTPUT_TOKENS or add model catalog max_tokens", p.Model)
	}
	return maxTokens, nil
}

func (p AnthropicProvider) anthropicRequestBody(req provider.Request) ([]byte, error) {
	maxTokens, err := p.maxTokensForRequest(req)
	if err != nil {
		return nil, err
	}
	anthropicReq, err := p.buildAnthropicRequest(req, maxTokens)
	if err != nil {
		return nil, err
	}
	return json.Marshal(anthropicReq)
}

func (p AnthropicProvider) MessageRaw(kind cache.SegmentKind, msg session.Message) (string, string, error) {
	if msg.Role == session.System {
		block := anthropicContentBlock{Type: "text", Text: msg.Content}
		raw, err := cache.CanonicalJSON(block)
		return raw, cache.FragmentAnthropicSystem, err
	}
	rendered := renderAnthropicMessages([]session.Message{msg}, nil)
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := cache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	_ = kind
	return raw, cache.FragmentAnthropicMessage, nil
}

func (p AnthropicProvider) ToolRaw(def cache.ToolDefinition) (string, string, error) {
	rendered := renderAnthropicTools([]provider.ToolDefinition{{
		Name:         def.Name,
		Title:        def.Title,
		Description:  def.Description,
		InputSchema:  def.InputSchema,
		OutputSchema: def.OutputSchema,
		Strict:       def.Strict,
		Annotations:  def.Annotations,
	}}, nil)
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := cache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	return raw, cache.FragmentAnthropicTool, nil
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

func renderAnthropicSystemFromRawPlan(plan cache.RawPlan, cacheControl *anthropicCacheControl) (any, error) {
	var blocks []anthropicContentBlock
	for _, segment := range plan.Segments {
		if segment.Kind != cache.SegmentSystem {
			continue
		}
		if segment.FragmentType != cache.FragmentAnthropicSystem {
			return nil, fmt.Errorf("anthropic raw system segment %q uses unsupported fragment type %q", segment.ID, segment.FragmentType)
		}
		var block anthropicContentBlock
		if err := json.Unmarshal([]byte(segment.Raw), &block); err != nil {
			return nil, fmt.Errorf("decode anthropic raw system segment %q: %w", segment.ID, err)
		}
		blocks = append(blocks, block)
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	if cacheControl != nil {
		blocks[len(blocks)-1].CacheControl = cacheControl
		return blocks, nil
	}
	if len(blocks) == 1 {
		return blocks[0].Text, nil
	}
	return blocks, nil
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
			content, _ := json.Marshal(msg.Content)
			out = append(out, anthropicMessage{Role: "user", Content: []anthropicContentBlock{{
				Type:      "tool_result",
				ToolUseID: msg.ToolCallID,
				Content:   content,
			}}})
		}
	}
	return out
}

func renderAnthropicMessagesFromRawPlan(plan cache.RawPlan) ([]anthropicMessage, error) {
	var out []anthropicMessage
	for _, segment := range plan.Segments {
		if segment.Kind == cache.SegmentSystem || segment.Kind == cache.SegmentToolset {
			continue
		}
		if segment.FragmentType != cache.FragmentAnthropicMessage {
			return nil, fmt.Errorf("anthropic raw plan segment %q uses unsupported fragment type %q", segment.ID, segment.FragmentType)
		}
		var msg anthropicMessage
		if err := json.Unmarshal([]byte(segment.Raw), &msg); err != nil {
			return nil, fmt.Errorf("decode anthropic raw message segment %q: %w", segment.ID, err)
		}
		out = append(out, msg)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("anthropic raw plan has no message segments")
	}
	return out, nil
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
			Name:         def.Name,
			Description:  def.Description,
			InputSchema:  inputSchemaForAnthropic(def),
			CacheControl: toolCacheControl,
		})
	}
	return tools
}

func inputSchemaForAnthropic(def provider.ToolDefinition) map[string]any {
	if def.InputSchema != nil {
		return def.InputSchema
	}
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{},
		"required":             []string{},
		"additionalProperties": false,
	}
}

func validateAnthropicHostedTools(defs []provider.HostedToolDefinition) error {
	for _, def := range defs {
		shape, _ := def.Options["wire_shape"].(string)
		wireShape := searchcap.HostedWireShape(shape)
		if def.Name != searchcap.ToolWebSearch || def.Type != searchcap.ToolWebSearch || wireShape != searchcap.WireShapeAnthropicServerWebSearch {
			return fmt.Errorf("anthropic provider does not support hosted tool %s/%s with wire shape %q", def.Type, def.Name, shape)
		}
	}
	return nil
}

func renderAnthropicHostedTools(defs []provider.HostedToolDefinition) []anthropicTool {
	out := make([]anthropicTool, 0, len(defs))
	for _, def := range defs {
		shape, _ := def.Options["wire_shape"].(string)
		if def.Name != searchcap.ToolWebSearch || def.Type != searchcap.ToolWebSearch || searchcap.HostedWireShape(shape) != searchcap.WireShapeAnthropicServerWebSearch {
			continue
		}
		out = append(out, anthropicTool{
			Type:        "web_search_20250305",
			Name:        searchcap.ToolWebSearch,
			Description: "Search the web using the provider-hosted search capability.",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "required": []string{}, "additionalProperties": false},
		})
	}
	return out
}

func renderAnthropicToolsFromRawPlan(plan cache.RawPlan, expectedLocalTools int, cacheControl *anthropicCacheControl, hostedTools []anthropicTool) ([]anthropicTool, error) {
	var out []anthropicTool
	for _, segment := range plan.Segments {
		if segment.Kind != cache.SegmentToolset {
			continue
		}
		if segment.FragmentType != cache.FragmentAnthropicTool {
			return nil, fmt.Errorf("anthropic raw toolset segment %q uses unsupported fragment type %q", segment.ID, segment.FragmentType)
		}
		var tool anthropicTool
		if err := json.Unmarshal([]byte(segment.Raw), &tool); err != nil {
			return nil, fmt.Errorf("decode anthropic raw tool segment %q: %w", segment.ID, err)
		}
		out = append(out, tool)
	}
	if expectedLocalTools > 0 && len(out) != expectedLocalTools {
		return nil, fmt.Errorf("anthropic raw plan has %d toolset segments for %d requested local tools", len(out), expectedLocalTools)
	}
	out = append(out, hostedTools...)
	if cacheControl != nil {
		for i := len(out) - 1; i >= 0; i-- {
			if out[i].Type == "" {
				out[i].CacheControl = cacheControl
				break
			}
		}
	}
	return out, nil
}

func anthropicCacheControlFor(req provider.Request, capability catalog.CacheCapability) *anthropicCacheControl {
	if !req.Cache.Enabled || !capability.AnthropicCacheControl {
		return nil
	}
	control := &anthropicCacheControl{Type: "ephemeral"}
	if req.Cache.Retention == "1h" {
		control.TTL = "1h"
	}
	return control
}

func normalizeAnthropicUsage(payload anthropicUsage, model catalog.Model) provider.Usage {
	usage := provider.Usage{
		InputTokens:       payload.InputTokens,
		OutputTokens:      payload.OutputTokens,
		CacheReadTokens:   payload.CacheReadInputTokens,
		CacheWriteTokens:  payload.CacheCreationInputTokens,
		Source:            provider.UsageNative,
		WindowInputTokens: payload.InputTokens + payload.CacheCreationInputTokens + payload.CacheReadInputTokens,
	}.Normalized()
	if model.ID != "" {
		usage.CostUSD = catalog.CostForUsage(model, usage)
	}
	return usage.Normalized()
}
