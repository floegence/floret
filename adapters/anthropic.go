package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type AnthropicProvider struct {
	Endpoint   string
	APIKey     string
	Model      string
	MaxTokens  int64
	CostModel  modelcatalog.Model
	HTTPClient *http.Client
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int64              `json:"max_tokens"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
	Tools     []anthropicTool    `json:"tools,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type anthropicTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"input_schema"`
}

type anthropicContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
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
	maxTokens := p.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	body, err := json.Marshal(anthropicRequest{
		Model:     p.Model,
		MaxTokens: maxTokens,
		System:    anthropicSystem(req.Messages),
		Messages:  renderAnthropicMessages(req.Messages),
		Tools:     renderAnthropicTools(req.Tools),
	})
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
		ch <- provider.StreamEvent{Type: provider.Done}
	}
	close(ch)
	return ch, nil
}

func anthropicSystem(messages []session.Message) string {
	for _, msg := range messages {
		if msg.Role == session.System {
			return msg.Content
		}
	}
	return ""
}

func renderAnthropicMessages(messages []session.Message) []anthropicMessage {
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

func renderAnthropicTools(defs []provider.ToolDefinition) []anthropicTool {
	tools := make([]anthropicTool, 0, len(defs))
	for _, def := range defs {
		if def.Name == "" {
			continue
		}
		tools = append(tools, anthropicTool{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": true,
			},
		})
	}
	return tools
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
