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

	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/session"
)

type OpenAICompatibleProvider struct {
	Endpoint        string
	APIKey          string
	Model           string
	CostModel       catalog.Model
	Cache           catalog.CacheCapability
	HTTPClient      *http.Client
	StreamResponses bool
}

type chatRequest struct {
	Model                string         `json:"model"`
	Messages             []chatMessage  `json:"messages"`
	Stream               bool           `json:"stream"`
	MaxTokens            int64          `json:"max_tokens,omitempty"`
	Tools                []chatTool     `json:"tools,omitempty"`
	WebSearchOptions     map[string]any `json:"web_search_options,omitempty"`
	HasWebSearchOptions  bool           `json:"-"`
	ToolChoice           string         `json:"tool_choice,omitempty"`
	PromptCacheKey       string         `json:"prompt_cache_key,omitempty"`
	PromptCacheRetention string         `json:"prompt_cache_retention,omitempty"`
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
	Role             string         `json:"role"`
	Content          string         `json:"content"`
	ReasoningContent string         `json:"reasoning_content,omitempty"`
	ToolCallID       string         `json:"tool_call_id,omitempty"`
	Name             string         `json:"name,omitempty"`
	ToolCalls        []chatToolCall `json:"tool_calls,omitempty"`
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
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
			ToolCalls        []struct {
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
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
			ToolCalls        []struct {
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
	if err := validateOpenAICompatibleHostedTools(req.HostedTools); err != nil {
		return nil, err
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
	chatReq.Messages = normalizeChatToolResults(chatReq.Messages)
	if err := validateChatToolAdjacency(chatReq.Messages); err != nil {
		return nil, err
	}
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
	if len(parsed.Choices) == 0 {
		ch := make(chan provider.StreamEvent, 1)
		ch <- provider.StreamEvent{Type: provider.Empty}
		close(ch)
		return ch, nil
	}
	events := make([]provider.StreamEvent, 0, 5)
	choice := parsed.Choices[0]
	if choice.Message.ReasoningContent != "" {
		events = append(events, provider.StreamEvent{Type: provider.Reasoning, Text: choice.Message.ReasoningContent})
	}
	if choice.Message.Content != "" {
		events = append(events, provider.StreamEvent{Type: provider.Delta, Text: choice.Message.Content})
	}
	if usage := normalizeUsage(parsed.Usage, p.CostModel); usage.TotalTokens > 0 || usage.CostUSD > 0 {
		events = append(events, provider.StreamEvent{Type: provider.UsageEvent, Usage: usage})
	}
	if len(choice.Message.ToolCalls) > 0 {
		calls := make([]provider.ToolCall, 0, len(choice.Message.ToolCalls))
		for _, call := range choice.Message.ToolCalls {
			calls = append(calls, provider.ToolCall{
				ID:        call.ID,
				Name:      call.Function.Name,
				Args:      call.Function.Arguments,
				Reasoning: choice.Message.ReasoningContent,
			})
		}
		events = append(events, provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls})
	}
	if choice.FinishReason == "length" {
		events = append(events, provider.StreamEvent{Type: provider.Truncated, Reason: "length"})
	} else {
		events = append(events, provider.StreamEvent{Type: provider.Done, Reason: choice.FinishReason})
	}
	ch := make(chan provider.StreamEvent, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)
	return ch, nil
}

func (p OpenAICompatibleProvider) NormalizeCachePolicy(policy cache.CachePolicy) (cache.CachePolicy, error) {
	if !policy.Enabled || policy.Retention == cache.RetentionNone {
		policy.Enabled = false
		policy.Retention = cache.RetentionNone
		return policy, nil
	}
	if policy.Retention == "" {
		policy.Retention = cache.RetentionInMemory
	}
	switch policy.Retention {
	case cache.RetentionInMemory:
		return policy, nil
	case cache.RetentionDay:
		if !p.Cache.PromptCacheRetention {
			return cache.CachePolicy{}, fmt.Errorf("openai-compatible model does not support 24h prompt cache retention")
		}
		return policy, nil
	case cache.RetentionShort, cache.RetentionLong:
		return cache.CachePolicy{}, fmt.Errorf("openai-compatible prompt cache retention %q is unsupported; use %q or %q", policy.Retention, cache.RetentionInMemory, cache.RetentionDay)
	default:
		return cache.CachePolicy{}, fmt.Errorf("unsupported prompt cache retention %q", policy.Retention)
	}
}

func (p OpenAICompatibleProvider) DefaultCacheRetention() cache.Retention {
	return cache.RetentionInMemory
}

func (p OpenAICompatibleProvider) PayloadHash(req provider.Request) (string, error) {
	policy, err := p.NormalizeCachePolicy(req.Cache)
	if err != nil {
		return "", err
	}
	req.Cache = policy
	if err := validateOpenAICompatibleHostedTools(req.HostedTools); err != nil {
		return "", err
	}
	chatReq := p.buildChatRequest(req)
	chatReq.Messages = normalizeChatToolResults(chatReq.Messages)
	if err := validateChatToolAdjacency(chatReq.Messages); err != nil {
		return "", err
	}
	body, err := json.Marshal(chatReq)
	if err != nil {
		return "", err
	}
	return cache.StableHash(string(body)), nil
}

func (r chatRequest) MarshalJSON() ([]byte, error) {
	type alias chatRequest
	out := struct {
		alias
		WebSearchOptions *map[string]any `json:"web_search_options,omitempty"`
	}{
		alias: alias(r),
	}
	out.alias.HasWebSearchOptions = false
	out.alias.WebSearchOptions = nil
	if r.HasWebSearchOptions {
		options := r.WebSearchOptions
		if r.WebSearchOptions == nil {
			options = map[string]any{}
		}
		out.WebSearchOptions = &options
	}
	return json.Marshal(out)
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
	if hasOpenAICompatibleHostedSearch(req.HostedTools) {
		chatReq.WebSearchOptions = map[string]any{}
		chatReq.HasWebSearchOptions = true
	}
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

func validateOpenAICompatibleHostedTools(defs []provider.HostedToolDefinition) error {
	for _, def := range defs {
		shape, _ := def.Options["wire_shape"].(string)
		wireShape := searchcap.HostedWireShape(shape)
		if def.Name != searchcap.ToolWebSearch || def.Type != searchcap.ToolWebSearch || wireShape != searchcap.WireShapeOpenAIChatWebSearchOptions {
			return fmt.Errorf("openai-compatible chat provider does not support hosted tool %s/%s with wire shape %q", def.Type, def.Name, shape)
		}
	}
	return nil
}

func hasOpenAICompatibleHostedSearch(defs []provider.HostedToolDefinition) bool {
	for _, def := range defs {
		shape, _ := def.Options["wire_shape"].(string)
		if def.Name == searchcap.ToolWebSearch && def.Type == searchcap.ToolWebSearch && searchcap.HostedWireShape(shape) == searchcap.WireShapeOpenAIChatWebSearchOptions {
			return true
		}
	}
	return false
}

func (p OpenAICompatibleProvider) MessageRaw(kind cache.SegmentKind, msg session.Message) (string, string, error) {
	rendered := renderMessages([]session.Message{msg})
	if len(rendered) == 0 {
		return "", "", nil
	}
	raw, err := cache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	_ = kind
	return raw, cache.FragmentOpenAIMessage, nil
}

func (p OpenAICompatibleProvider) ToolRaw(def cache.ToolDefinition) (string, string, error) {
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
	raw, err := cache.CanonicalJSON(rendered[0])
	if err != nil {
		return "", "", err
	}
	return raw, cache.FragmentOpenAITool, nil
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
		var reasoning strings.Builder
		scanner := bufio.NewScanner(httpResp.Body)
		scanner.Buffer(make([]byte, 32*1024), 4*1024*1024)
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
				if choice.Delta.ReasoningContent != "" {
					reasoning.WriteString(choice.Delta.ReasoningContent)
					ch <- provider.StreamEvent{Type: provider.Reasoning, Text: choice.Delta.ReasoningContent}
				}
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
						calls = append(calls, provider.ToolCall{ID: item.id, Name: item.name, Args: item.args, Reasoning: reasoning.String()})
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
		if err := scanner.Err(); err != nil {
			ch <- provider.StreamEvent{Type: provider.Empty, Reason: err.Error()}
		}
	}()
	return ch, nil
}

func renderMessages(messages []session.Message) []chatMessage {
	out := make([]chatMessage, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
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
		if msg.Role == session.Assistant {
			rendered.ReasoningContent = msg.Reasoning
		}
		if isAssistantToolCall(msg) {
			rendered.Content = ""
			rendered.ToolCallID = ""
			rendered.Name = ""
			var calls []chatToolCall
			for i < len(messages) && isAssistantToolCall(messages[i]) {
				callMsg := messages[i]
				var call chatToolCall
				call.ID = callMsg.ToolCallID
				call.Type = "function"
				call.Function.Name = callMsg.ToolName
				call.Function.Arguments = callMsg.ToolArgs
				calls = append(calls, call)
				if rendered.ReasoningContent == "" && callMsg.Reasoning != "" {
					rendered.ReasoningContent = callMsg.Reasoning
				}
				i++
			}
			i--
			rendered.Content = ""
			rendered.ToolCalls = calls
		}
		out = append(out, rendered)
	}
	return out
}

func isAssistantToolCall(msg session.Message) bool {
	return msg.Role == session.Assistant && msg.ToolCallID != "" && msg.ToolName != ""
}

func validateChatToolAdjacency(messages []chatMessage) error {
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		if msg.Role == "tool" {
			return fmt.Errorf("openai-compatible request has orphan tool result %q without preceding assistant tool_calls", msg.ToolCallID)
		}
		if msg.Role != "assistant" || len(msg.ToolCalls) == 0 {
			continue
		}
		want := make([]string, 0, len(msg.ToolCalls))
		seen := map[string]struct{}{}
		for _, call := range msg.ToolCalls {
			if strings.TrimSpace(call.ID) == "" {
				return fmt.Errorf("openai-compatible request has assistant tool_calls with empty tool_call id")
			}
			if _, ok := seen[call.ID]; ok {
				return fmt.Errorf("openai-compatible request has duplicate assistant tool_call id %q", call.ID)
			}
			seen[call.ID] = struct{}{}
			want = append(want, call.ID)
		}
		for offset, id := range want {
			idx := i + 1 + offset
			if idx >= len(messages) {
				return fmt.Errorf("openai-compatible request assistant tool_calls must be followed by tool messages: missing tool result for %q", id)
			}
			toolMsg := messages[idx]
			if toolMsg.Role != "tool" {
				return fmt.Errorf("openai-compatible request assistant tool_calls must be followed by tool messages: got %q before result for %q", toolMsg.Role, id)
			}
			if strings.TrimSpace(toolMsg.ToolCallID) == "" {
				return fmt.Errorf("openai-compatible request tool result after assistant tool_calls has empty tool_call_id, want %q", id)
			}
			if _, ok := seen[toolMsg.ToolCallID]; !ok {
				return fmt.Errorf("openai-compatible request tool result %q does not match preceding assistant tool_calls", toolMsg.ToolCallID)
			}
			delete(seen, toolMsg.ToolCallID)
			if toolMsg.ToolCallID != id {
				return fmt.Errorf("openai-compatible request tool result order mismatch: got %q, want %q", toolMsg.ToolCallID, id)
			}
		}
		i += len(want)
	}
	return nil
}

func normalizeChatToolResults(messages []chatMessage) []chatMessage {
	out := make([]chatMessage, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		out = append(out, msg)
		if msg.Role != "assistant" || len(msg.ToolCalls) == 0 {
			continue
		}
		needed := make([]string, 0, len(msg.ToolCalls))
		for _, call := range msg.ToolCalls {
			needed = append(needed, call.ID)
		}
		start := i + 1
		end := start
		for end < len(messages) && messages[end].Role == "tool" {
			end++
		}
		toolBatch := messages[start:end]
		if len(toolBatch) == 0 {
			continue
		}
		if reordered, ok := reorderToolBatch(needed, toolBatch); ok {
			out = append(out, reordered...)
		} else {
			out = append(out, toolBatch...)
		}
		i = end - 1
	}
	return out
}

func reorderToolBatch(ids []string, toolBatch []chatMessage) ([]chatMessage, bool) {
	if len(ids) != len(toolBatch) {
		return nil, false
	}
	byID := make(map[string]chatMessage, len(toolBatch))
	for _, toolMsg := range toolBatch {
		if toolMsg.ToolCallID == "" {
			return nil, false
		}
		if _, exists := byID[toolMsg.ToolCallID]; exists {
			return nil, false
		}
		byID[toolMsg.ToolCallID] = toolMsg
	}
	reordered := make([]chatMessage, 0, len(ids))
	for _, id := range ids {
		toolMsg, ok := byID[id]
		if !ok {
			return nil, false
		}
		reordered = append(reordered, toolMsg)
	}
	return reordered, true
}

func renderMessagesFromRawPlan(plan cache.RawPlan, fallback []chatMessage) []chatMessage {
	var sessionMessages []session.Message
	for _, segment := range plan.Segments {
		if segment.Kind == cache.SegmentToolset {
			continue
		}
		msg := session.Message{
			Role:       session.Role(segment.Message.Role),
			Content:    segment.Message.Content,
			Reasoning:  segment.Message.Reasoning,
			ToolCallID: segment.Message.ToolCallID,
			ToolName:   segment.Message.ToolName,
			ToolArgs:   segment.Message.ToolArgs,
		}
		if msg.Role == "" {
			continue
		}
		sessionMessages = append(sessionMessages, msg)
	}
	out := renderMessages(sessionMessages)
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

func renderToolsFromRawPlan(plan cache.RawPlan, fallback []chatTool) []chatTool {
	var out []chatTool
	for _, segment := range plan.Segments {
		if segment.FragmentType != cache.FragmentOpenAITool {
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

func normalizeUsage(payload usagePayload, model catalog.Model) provider.Usage {
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
		usage.CostUSD = catalog.CostForUsage(model, usage)
	}
	return usage
}
