package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/provider/catalog"
	"github.com/floegence/floret/v7/internal/searchcap"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/toolrender"
	"github.com/floegence/floret/v7/tools"
)

type AnthropicProvider struct {
	StateIdentity     string
	MaxRequestBytes   int64
	Temperature       *float64
	TopP              *float64
	ResolveAttachment func(context.Context, session.MessageAttachment) (AnthropicAttachment, error)
	Endpoint          string
	APIKey            string
	Model             string
	MaxTokens         int64
	CostModel         catalog.Model
	Cache             catalog.CacheCapability
	HTTPClient        *http.Client
}

type anthropicRequest struct {
	Stream       bool                   `json:"stream,omitempty"`
	Temperature  *float64               `json:"temperature,omitempty"`
	TopP         *float64               `json:"top_p,omitempty"`
	Model        string                 `json:"model"`
	MaxTokens    int64                  `json:"max_tokens"`
	System       any                    `json:"system,omitempty"`
	Messages     []anthropicMessage     `json:"messages"`
	Tools        []anthropicTool        `json:"tools,omitempty"`
	Thinking     *anthropicThinking     `json:"thinking,omitempty"`
	OutputConfig *anthropicOutputConfig `json:"output_config,omitempty"`
}

type anthropicThinking struct {
	Type         string `json:"type"`
	BudgetTokens int64  `json:"budget_tokens,omitempty"`
}

type anthropicOutputConfig struct {
	Effort string `json:"effort,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type anthropicTool struct {
	Strict       bool                   `json:"strict,omitempty"`
	Name         string                 `json:"name"`
	Type         string                 `json:"type,omitempty"`
	Description  string                 `json:"description,omitempty"`
	InputSchema  map[string]any         `json:"input_schema"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicContentBlock struct {
	extra        map[string]json.RawMessage
	Thinking     string                 `json:"thinking,omitempty"`
	Signature    string                 `json:"signature,omitempty"`
	Data         string                 `json:"data,omitempty"`
	Source       json.RawMessage        `json:"source,omitempty"`
	Citations    json.RawMessage        `json:"citations,omitempty"`
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
	inputReported            bool
	outputReported           bool
	readReported             bool
	writeReported            bool
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

type anthropicResponse struct {
	ID         string                  `json:"id"`
	Content    []anthropicContentBlock `json:"content"`
	StopReason string                  `json:"stop_reason"`
	Usage      anthropicUsage          `json:"usage"`
	Error      *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func (p AnthropicProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	prepared, err := p.PrepareRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return prepared.Stream(ctx)
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
	result := provider.HostedToolResultData{ResultsProvided: true, Results: results}
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
	anthropicReq.Temperature = p.Temperature
	anthropicReq.TopP = p.TopP
	if err := p.applyAnthropicReasoning(&anthropicReq, req, maxTokens); err != nil {
		return anthropicRequest{}, err
	}
	return anthropicReq, nil
}

func (p AnthropicProvider) applyAnthropicReasoning(out *anthropicRequest, req provider.Request, maxTokens int64) error {
	if out == nil {
		return nil
	}
	selection := provider.NormalizeReasoningSelection(req.Reasoning)
	if selection.IsZero() || selection.Level == provider.ReasoningLevelDefault && selection.BudgetTokens == 0 {
		return nil
	}
	capability := p.CostModel.Reasoning
	if err := capability.ValidateSelection(selection); err != nil {
		return err
	}
	if selection.Level == provider.ReasoningLevelOff {
		out.Thinking = &anthropicThinking{Type: "disabled"}
		return nil
	}
	if selection.BudgetTokens > 0 {
		if selection.BudgetTokens >= maxTokens {
			return fmt.Errorf("anthropic reasoning budget %d must be less than max_tokens %d", selection.BudgetTokens, maxTokens)
		}
		out.Thinking = &anthropicThinking{Type: "enabled", BudgetTokens: selection.BudgetTokens}
	}
	if selection.Level != "" && selection.Level != provider.ReasoningLevelDefault && selection.Level != provider.ReasoningLevelOn {
		out.OutputConfig = &anthropicOutputConfig{Effort: string(selection.Level)}
		if capability.Kind == provider.ReasoningKindEffort {
			out.Thinking = &anthropicThinking{Type: "adaptive"}
		}
	}
	return nil
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
	if req.EphemeralUser != nil {
		absoluteIndex, err := provider.EphemeralUserMessageIndex(req.Messages, req.EphemeralUser)
		if err != nil {
			return anthropicRequest{}, err
		}
		systemPrefix := absoluteIndex - req.EphemeralUser.HistoryInsertAt
		if len(renderedMessages) != len(req.Messages)-systemPrefix {
			return anthropicRequest{}, errors.New("anthropic raw messages do not match canonical history count")
		}
		ephemeral := renderAnthropicMessages([]session.Message{req.EphemeralUser.Message}, nil)
		if len(ephemeral) != 1 {
			return anthropicRequest{}, errors.New("anthropic ephemeral user message did not render exactly once")
		}
		insertAt := req.EphemeralUser.HistoryInsertAt
		renderedMessages = append(renderedMessages, anthropicMessage{})
		copy(renderedMessages[insertAt+1:], renderedMessages[insertAt:])
		renderedMessages[insertAt] = ephemeral[0]
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
	prepared, err := p.PrepareRequest(context.Background(), req)
	if err != nil {
		return "", err
	}
	defer prepared.Close()
	return cache.StableHash(string(prepared.(*anthropicPrepared).body)), nil
}
func (p AnthropicProvider) EstimateTokens(ctx context.Context, req provider.Request) (provider.TokenEstimate, error) {
	prepared, err := p.PrepareRequest(ctx, req)
	if err != nil {
		return provider.TokenEstimate{}, err
	}
	defer prepared.Close()
	return prepared.TokenEstimate(), nil
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
	prepared, err := p.PrepareRequest(context.Background(), req)
	if err != nil {
		return nil, err
	}
	defer prepared.Close()
	return bytes.Clone(prepared.(*anthropicPrepared).body), nil
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
	rendered := renderAnthropicTools([]tools.ToolDefinition{{
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
	ordered, err := toolrender.OrderTool("anthropic_messages", []byte(raw))
	if err != nil {
		return "", "", err
	}
	return string(ordered), cache.FragmentAnthropicTool, nil
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
	var out []anthropicMessage
	for _, m := range messages {
		if m.Role == session.System {
			continue
		}
		var blocks []anthropicContentBlock
		role := string(m.Role)
		if m.Role == session.Tool {
			content, _ := json.Marshal(m.Content)
			blocks = append(blocks, anthropicContentBlock{Type: "tool_result", ToolUseID: m.ToolCallID, Content: content})
			role = "user"
		} else {
			if m.Content != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "text", Text: m.Content})
			}
			if m.Reasoning != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "floret_reasoning", Thinking: m.Reasoning})
			}
			if m.ToolCallID != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "tool_use", ID: m.ToolCallID, Name: m.ToolName, Input: json.RawMessage(m.ToolArgs)})
			}
		}
		out = append(out, anthropicMessage{Role: role, Content: blocks})
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

func renderAnthropicTools(defs []tools.ToolDefinition, cacheControl *anthropicCacheControl) []anthropicTool {
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
			Strict:       def.Strict,
			Description:  def.Description,
			InputSchema:  inputSchemaForAnthropic(def),
			CacheControl: toolCacheControl,
		})
	}
	return tools
}

func inputSchemaForAnthropic(def tools.ToolDefinition) map[string]any {
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
		InputTokensReported: payload.inputReported, InputTokenScope: "request_input",
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

func (u *anthropicUsage) UnmarshalJSON(raw []byte) error {
	type plain anthropicUsage
	var v plain
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if v.InputTokens < 0 || v.OutputTokens < 0 || v.CacheReadInputTokens < 0 || v.CacheCreationInputTokens < 0 {
		return errors.New("Anthropic usage contains negative tokens")
	}
	total := int64(0)
	for _, n := range []int64{v.InputTokens, v.OutputTokens, v.CacheReadInputTokens, v.CacheCreationInputTokens} {
		if n > math.MaxInt64-total {
			return errors.New("Anthropic usage overflows")
		}
		total += n
	}
	*u = anthropicUsage(v)
	value, ok := fields["input_tokens"]
	u.inputReported = ok && string(value) != "null"
	value, ok = fields["output_tokens"]
	u.outputReported = ok && string(value) != "null"
	value, ok = fields["cache_read_input_tokens"]
	u.readReported = ok && string(value) != "null"
	value, ok = fields["cache_creation_input_tokens"]
	u.writeReported = ok && string(value) != "null"
	return nil
}

// Required empty text/thinking fields must survive native round trips.
func (b anthropicContentBlock) MarshalJSON() ([]byte, error) {
	type plain anthropicContentBlock
	raw, err := json.Marshal(plain(b))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	switch b.Type {
	case "thinking":
		fields["thinking"], _ = json.Marshal(b.Thinking)
	case "text":
		fields["text"], _ = json.Marshal(b.Text)
	}
	for key, value := range b.extra {
		if _, ok := fields[key]; !ok {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
}

func (b *anthropicContentBlock) UnmarshalJSON(raw []byte) error {
	type plain anthropicContentBlock
	var value plain
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*b = anthropicContentBlock(value)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"type", "thinking", "signature", "data", "source", "citations", "text", "id", "name", "input", "tool_use_id", "content", "query", "cache_control"} {
		delete(fields, key)
	}
	if len(fields) > 0 {
		b.extra = fields
	}
	return nil
}
