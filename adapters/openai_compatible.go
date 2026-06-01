package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type OpenAICompatibleProvider struct {
	Endpoint   string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type chatMessage struct {
	Role       string `json:"role"`
	Content    string `json:"content"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	Name       string `json:"name,omitempty"`
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
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	body, err := json.Marshal(chatRequest{Model: p.Model, Messages: renderMessages(req.Messages), Stream: false})
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
	ch := make(chan provider.StreamEvent, 3)
	if len(parsed.Choices) == 0 {
		ch <- provider.StreamEvent{Type: provider.Empty}
		close(ch)
		return ch, nil
	}
	choice := parsed.Choices[0]
	if choice.Message.Content != "" {
		ch <- provider.StreamEvent{Type: provider.Delta, Text: choice.Message.Content}
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
		ch <- provider.StreamEvent{Type: provider.Done}
	}
	close(ch)
	return ch, nil
}

func renderMessages(messages []session.Message) []chatMessage {
	out := make([]chatMessage, 0, len(messages))
	for _, msg := range messages {
		role := string(msg.Role)
		if msg.Role == session.Tool {
			role = "tool"
		}
		out = append(out, chatMessage{
			Role:       role,
			Content:    msg.Content,
			ToolCallID: msg.ToolCallID,
			Name:       msg.ToolName,
		})
	}
	return out
}

func looksLikeContextOverflow(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "window") || strings.Contains(text, "token"))
}
