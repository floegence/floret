package adapters

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type OpenAICompatibleProvider struct {
	Endpoint        string
	APIKey          string
	Model           string
	CostModel       modelcatalog.Model
	Cache           modelcatalog.CacheCapability
	HTTPClient      *http.Client
	StreamResponses bool
}

type chatRequest struct {
	Model                string        `json:"model"`
	Messages             []chatMessage `json:"messages"`
	Stream               bool          `json:"stream"`
	MaxTokens            int64         `json:"max_tokens,omitempty"`
	Tools                []chatTool    `json:"tools,omitempty"`
	ToolChoice           string        `json:"tool_choice,omitempty"`
	PromptCacheKey       string        `json:"prompt_cache_key,omitempty"`
	PromptCacheRetention string        `json:"prompt_cache_retention,omitempty"`
}

type usagePayload struct {
	PromptTokens            int64        `json:"prompt_tokens"`
	CompletionTokens        int64        `json:"completion_tokens"`
	ReasoningTokens         int64        `json:"reasoning_tokens"`
	TotalTokens             int64        `json:"total_tokens"`
	PromptTokensDetails     tokenDetails `json:"prompt_tokens_details"`
	CompletionTokensDetails tokenDetails `json:"completion_tokens_details"`
}

type tokenDetails struct {
	CachedTokens     int64 `json:"cached_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	Name       string         `json:"name,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

type chatTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters"`
	} `json:"function"`
}

type chatToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage usagePayload `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

type chatStreamResponse struct {
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage usagePayload `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func (p OpenAICompatibleProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	if p.Endpoint == "" {
		return nil, fmt.Errorf("openai-compatible endpoint is required")
	}
	if p.APIKey == "" {
		return nil, fmt.Errorf("openai-compatible api key is required")
	}
	if p.Model == "" {
		return nil, fmt.Errorf("openai-compatible model is required")
	}
	if len(req.HostedTools) > 0 {
		return nil, fmt.Errorf("openai-compatible chat provider does not support hosted tools: %s", hostedToolNames(req.HostedTools))
	}
	normalizedCache, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return nil, err
	}
	req.Cache = normalizedCache
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	chatReq := p.buildChatRequest(req)
	body, err := json.Marshal(chatReq)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+p.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if p.StreamResponses {
		return p.streamResponse(httpResp)
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if httpResp.StatusCode == http.StatusRequestEntityTooLarge || httpResp.StatusCode == http.StatusBadRequest && looksLikeContextOverflow(respBody) {
		return nil, provider.ErrContextOverflow
	}
	var parsed chatResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("decode openai-compatible response: %w", err)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("openai-compatible provider error: %s", parsed.Error.Message)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("openai-compatible provider status %d", httpResp.StatusCode)
	}
	ch := make(chan provider.StreamEvent, 4)
	if len(parsed.Choices) == 0 {
		ch <- provider.StreamEvent{Type: provider.Empty}
		close(ch)
		return ch, nil
	}
	choice := parsed.Choices[0]
	if choice.Message.Content != "" {
		ch <- provider.StreamEvent{Type: provider.Delta, Text: choice.Message.Content}
	}
	if usage := normalizeUsage(parsed.Usage, p.CostModel); usage.TotalTokens > 0 || usage.CostUSD > 0 {
		ch <- provider.StreamEvent{Type: provider.UsageEvent, Usage: usage}
	}
	if len(choice.Message.ToolCalls) > 0 {
		calls := make([]provider.ToolCall, 0, len(choice.Message.ToolCalls))
		for _, call := range choice.Message.ToolCalls {
			calls = append(calls, provider.ToolCall{
				ID:   call.ID,
				Name: call.Function.Name,
				Args: call.Function.Arguments,
			})
		}
		ch <- provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}
	}
	if choice.FinishReason == "length" {
		ch <- provider.StreamEvent{Type: provider.Truncated, Reason: "length"}
	} else {
		ch <- provider.StreamEvent{Type: provider.Done, Reason: choice.FinishReason}
	}
	close(ch)
	return ch, nil
}

func (p OpenAICompatibleProvider) NormalizeCachePolicy(policy promptcache.CachePolicy) (promptcache.CachePolicy, error) {
	if !policy.Enabled || policy.Retention == promptcache.RetentionNone {
		policy.Enabled = false
		policy.Retention = promptcache.RetentionNone
		return policy, nil
	}
	if policy.Retention == "" {
		policy.Retention = promptcache.RetentionInMemory
	}
	switch policy.Retention {
	case promptcache.RetentionInMemory:
		return policy, nil
	case promptcache.RetentionDay:
		if !p.Cache.PromptCacheRetention {
			return promptcache.CachePolicy{}, fmt.Errorf("openai-compatible model does not support 24h prompt cache retention")
		}
		return policy, nil
	case promptcache.RetentionShort, promptcache.RetentionLong:
		return promptcache.CachePolicy{}, fmt.Errorf("openai-compatible prompt cache retention %q is unsupported; use %q or %q", policy.Retention, promptcache.RetentionInMemory, promptcache.RetentionDay)
	default:
		return promptcache.CachePolicy{}, fmt.Errorf("unsupported prompt cache retention %q", policy.Retention)
	}
}

func (p OpenAICompatibleProvider) DefaultCacheRetention() promptcache.Retention {
	return promptcache.RetentionInMemory
}

func (p OpenAICompatibleProvider) PayloadHash(req provider.Request) (string, error) {
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return "", err
	}
	req.Cache = policy
	if len(req.HostedTools) > 0 {
		return "", fmt.Errorf("openai-compatible chat provider does not support hosted tools: %s", hostedToolNames(req.HostedTools))
	}
	body, err := json.Marshal(p.buildChatRequest(req))
	if err != nil {
		return "", err
	}
	return promptcache.StableHash(string(body)), nil
}

func (p OpenAICompatibleProvider) buildChatRequest(req provider.Request) chatRequest {
	messages := req.Messages
	if len(req.RawPlan.Segments) > 0 {
		messages = nil
	}
	renderedMessages := renderMessages(messages)
	renderedTools := renderTools(req.Tools)
	if len(req.RawPlan.Segments) > 0 {
		renderedMessages = renderMessagesFromRawPlan(req.RawPlan, renderedMessages)
		renderedTools = renderToolsFromRawPlan(req.RawPlan, renderedTools)
	}
	maxTokens := req.MaxOutputTokens
	if maxTokens <= 0 {
		maxTokens = req.ContextPolicy.MaxOutputTokens
	}
	chatReq := chatRequest{Model: p.Model, Messages: renderedMessages, Stream: p.StreamResponses, MaxTokens: maxTokens, Tools: renderedTools}
	if len(chatReq.Tools) > 0 {
		chatReq.ToolChoice = "auto"
	}
	if req.Cache.Enabled && p.Cache.PromptCacheKey {
		chatReq.PromptCacheKey = req.Cache.Namespace
	}
	if req.Cache.Enabled && p.Cache.PromptCacheRetention {
		chatReq.PromptCacheRetention = string(req.Cache.Retention)
	}
	return chatReq
}

func (p OpenAICompatibleProvider) MessageRaw(kind promptcache.SegmentKind, msg session.Message) (string, string, error) {
	rendered := renderMessages([]session.Message{msg})
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := promptcache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	_ = kind
	return raw, promptcache.FragmentOpenAIMessage, nil
}

func (p OpenAICompatibleProvider) ToolRaw(def promptcache.ToolDefinition) (string, string, error) {
	rendered := renderTools([]provider.ToolDefinition{{
		Name:         def.Name,
		Title:        def.Title,
		Description:  def.Description,
		InputSchema:  def.InputSchema,
		OutputSchema: def.OutputSchema,
		Strict:       def.Strict,
		Annotations:  def.Annotations,
	}})
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := promptcache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	return raw, promptcache.FragmentOpenAITool, nil
}

func (p OpenAICompatibleProvider) streamResponse(httpResp *http.Response) (<-chan provider.StreamEvent, error) {
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
		if httpResp.StatusCode == http.StatusRequestEntityTooLarge || httpResp.StatusCode == http.StatusBadRequest && looksLikeContextOverflow(respBody) {
			return nil, provider.ErrContextOverflow
		}
		return nil, fmt.Errorf("openai-compatible provider status %d", httpResp.StatusCode)
	}
	ch := make(chan provider.StreamEvent, 16)
	go func() {
		defer httpResp.Body.Close()
		defer close(ch)
		type partialTool struct {
			id   string
			name string
			args string
		}
		tools := map[int]partialTool{}
		scanner := bufio.NewScanner(httpResp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, ":") {
				continue
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "[DONE]" {
				return
			}
			var parsed chatStreamResponse
			if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
				ch <- provider.StreamEvent{Type: provider.Empty, Reason: err.Error()}
				return
			}
			if parsed.Error != nil {
				ch <- provider.StreamEvent{Type: provider.Empty, Reason: parsed.Error.Message}
				return
			}
			if usage := normalizeUsage(parsed.Usage, p.CostModel); usage.TotalTokens > 0 || usage.CostUSD > 0 {
				ch <- provider.StreamEvent{Type: provider.UsageEvent, Usage: usage}
			}
			for _, choice := range parsed.Choices {
				if choice.Delta.Content != "" {
					ch <- provider.StreamEvent{Type: provider.Delta, Text: choice.Delta.Content}
				}
				for _, call := range choice.Delta.ToolCalls {
					item := tools[call.Index]
					if call.ID != "" {
						item.id = call.ID
					}
					if call.Function.Name != "" {
						item.name = call.Function.Name
					}
					item.args += call.Function.Arguments
					tools[call.Index] = item
				}
				switch choice.FinishReason {
				case "tool_calls":
					calls := make([]provider.ToolCall, 0, len(tools))
					for i := 0; i < len(tools); i++ {
						item, ok := tools[i]
						if !ok {
							continue
						}
						calls = append(calls, provider.ToolCall{ID: item.id, Name: item.name, Args: item.args})
					}
					ch <- provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}
					ch <- provider.StreamEvent{Type: provider.Done, Reason: choice.FinishReason}
					return
				case "length":
					ch <- provider.StreamEvent{Type: provider.Truncated, Reason: "length"}
					return
				case "stop":
					ch <- provider.StreamEvent{Type: provider.Done, Reason: choice.FinishReason}
					return
				case "content_filter":
					ch <- provider.StreamEvent{Type: provider.Done, Reason: "content_filter"}
					return
				}
			}
		}
	}()
	return ch, nil
}

func renderMessages(messages []session.Message) []chatMessage {
	out := make([]chatMessage, 0, len(messages))
	for _, msg := range messages {
		role := string(msg.Role)
		if msg.Role == session.Tool {
			role = "tool"
		}
		rendered := chatMessage{
			Role:       role,
			Content:    msg.Content,
			ToolCallID: msg.ToolCallID,
			Name:       msg.ToolName,
		}
		if msg.Role == session.Assistant && msg.ToolCallID != "" && msg.ToolName != "" {
			var call chatToolCall
			call.ID = msg.ToolCallID
			call.Type = "function"
			call.Function.Name = msg.ToolName
			call.Function.Arguments = msg.ToolArgs
			rendered.Content = ""
			rendered.ToolCalls = []chatToolCall{call}
			rendered.ToolCallID = ""
			rendered.Name = ""
		}
		out = append(out, rendered)
	}
	return out
}

func renderMessagesFromRawPlan(plan promptcache.RawPlan, fallback []chatMessage) []chatMessage {
	var out []chatMessage
	for _, segment := range plan.Segments {
		if segment.Kind == promptcache.SegmentToolset {
			continue
		}
		if segment.FragmentType == promptcache.FragmentOpenAIMessage {
			var msg chatMessage
			if err := json.Unmarshal([]byte(segment.Raw), &msg); err == nil {
				out = append(out, msg)
				continue
			}
		}
		msg := session.Message{
			Role:       session.Role(segment.Message.Role),
			Content:    segment.Message.Content,
			ToolCallID: segment.Message.ToolCallID,
			ToolName:   segment.Message.ToolName,
			ToolArgs:   segment.Message.ToolArgs,
		}
		if msg.Role == "" {
			continue
		}
		rendered := renderMessages([]session.Message{msg})
		if len(rendered) > 0 {
			out = append(out, rendered[0])
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func renderTools(defs []provider.ToolDefinition) []chatTool {
	tools := make([]chatTool, 0, len(defs))
	for _, def := range defs {
		if def.Name == "" {
			continue
		}
		var tool chatTool
		tool.Type = "function"
		tool.Function.Name = def.Name
		tool.Function.Description = def.Description
		tool.Function.Parameters = def.InputSchema
		if tool.Function.Parameters == nil {
			tool.Function.Parameters = map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"required":             []string{},
				"additionalProperties": false,
			}
		}
		tools = append(tools, tool)
	}
	return tools
}

func hostedToolNames(defs []provider.HostedToolDefinition) string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Type+"/"+def.Name)
	}
	return strings.Join(names, ", ")
}

func renderToolsFromRawPlan(plan promptcache.RawPlan, fallback []chatTool) []chatTool {
	var out []chatTool
	for _, segment := range plan.Segments {
		if segment.FragmentType != promptcache.FragmentOpenAITool {
			continue
		}
		var tool chatTool
		if err := json.Unmarshal([]byte(segment.Raw), &tool); err == nil {
			out = append(out, tool)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func looksLikeContextOverflow(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "window") || strings.Contains(text, "token"))
}

func normalizeUsage(payload usagePayload, model modelcatalog.Model) provider.Usage {
	cacheRead := payload.PromptTokensDetails.CachedTokens + payload.PromptTokensDetails.CacheReadTokens
	cacheWrite := payload.PromptTokensDetails.CacheWriteTokens
	reasoning := payload.ReasoningTokens + payload.CompletionTokensDetails.ReasoningTokens
	input := payload.PromptTokens
	if input >= cacheRead {
		input -= cacheRead
	}
	usage := provider.Usage{
		InputTokens:      input,
		OutputTokens:     payload.CompletionTokens,
		ReasoningTokens:  reasoning,
		CacheReadTokens:  cacheRead,
		CacheWriteTokens: cacheWrite,
		TotalTokens:      payload.TotalTokens,
		Source:           provider.UsageNative,
	}.Normalized()
	if model.ID != "" {
		usage.CostUSD = modelcatalog.CostForUsage(model, usage)
	}
	return usage
}
