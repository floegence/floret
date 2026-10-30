package adapters

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/providerretry"
	"github.com/floegence/floret/v7/internal/providertransport"
	"github.com/floegence/floret/v7/internal/requestmeasure"
)

func (p AnthropicProvider) streamAnthropic(ctx context.Context, prepared *anthropicPrepared) (<-chan provider.StreamEvent, error) {
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(prepared.body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", p.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if response.StatusCode == http.StatusRequestEntityTooLarge || response.StatusCode == 400 && looksLikeContextOverflow(raw) {
			return nil, errors.Join(provider.ErrContextOverflow, protocolFailure("anthropic", response.StatusCode, response.Header, raw, time.Now()))
		}
		return nil, protocolFailure("anthropic", response.StatusCode, response.Header, raw, time.Now())
	}
	out := make(chan provider.StreamEvent)
	go func() {
		defer close(out)
		defer response.Body.Close()
		emit := func(e provider.StreamEvent) bool {
			select {
			case out <- e:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !emit(provider.StreamEvent{Type: provider.TransportEvent, Transport: &providertransport.Observation{Transport: providertransport.HTTPSSE, Phase: providertransport.Receiving}}) {
			return
		}
		var result anthropicResponse
		var err error
		if strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
			result, err = readAnthropicStream(response.Body, emit)
		} else {
			raw, e := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
			err = e
			if len(raw) > 8<<20 {
				err = errors.New("Anthropic response exceeds limit")
			}
			if err == nil {
				err = json.Unmarshal(raw, &result)
			}
			if err == nil && result.Error != nil {
				err = protocolFailure("anthropic", response.StatusCode, response.Header, raw, time.Now())
			}
			if err == nil {
				for _, b := range result.Content {
					emitAnthropicBlock(b, emit)
				}
			}
		}
		usage := normalizeAnthropicUsage(result.Usage, p.CostModel)
		usage.InputTokenScope = "provider_execution"
		usage.OutputIncludesReasoning = true
		usage.OutputTokensReported = result.Usage.outputReported
		usage.Available = result.Usage.inputReported || result.Usage.outputReported
		if err == nil && result.StopReason == "" {
			err = providerretry.WithAdvice(provider.ErrStreamMissingTerminal, providerretry.Advice{Category: providerretry.Interrupted})
		}
		if err == nil {
			err = validateAnthropicNative(result.Content)
		}
		if ctx.Err() != nil {
			return
		}
		var state *provider.State
		if err == nil && prepared.stateAllowed && len(anthropicVisible(result.Content)) > 0 {
			state, err = prepared.history.commit(result.Content)
		}
		if err == nil && result.ID != "" && result.Usage.inputReported && result.Usage.readReported && result.Usage.writeReported {
			var body anthropicRequest
			_ = json.Unmarshal(prepared.body, &body)
			complete := true
			for _, tool := range body.Tools {
				if tool.Type != "" {
					complete = false
				}
			}
			opaque := false
			for _, m := range body.Messages {
				blocks, _ := anthropicBlocks(m.Content)
				for _, b := range blocks {
					if b.Type == "redacted_thinking" || b.Type == "thinking" {
						opaque = true
					}
				}
			}
			// Native Messages usage covers cache partitions. Encrypted blocks and
			// hosted execution require additional evidence and remain unqualified.
			if complete && !opaque {
				usage.Measurement = &requestmeasure.Evidence{AttemptID: prepared.attempt, CompleteInput: true, Input: prepared.InputMeasurementPrefix(), ResponseID: result.ID}
				responseComplete := true
				for _, b := range result.Content {
					if b.Type == "redacted_thinking" || b.Type == "thinking" {
						responseComplete = false
					}
				}
				if state != nil && result.Usage.outputReported && responseComplete {
					body.Messages = append(body.Messages, anthropicMessage{Role: "assistant", Content: result.Content})
					raw, _ := json.Marshal(body)
					usage.Measurement.ResponseEnd, _ = requestmeasure.RenderedPrefix(anthropicMeasuredBody(raw), "anthropic_messages", prepared.measurementIdentity, "response_end")
				}
			}
		}
		if usage.Available {
			if !emit(provider.StreamEvent{Type: provider.UsageEvent, Usage: usage}) {
				return
			}
		}
		if err != nil {
			emit(provider.StreamEvent{Type: provider.Error, Err: err, Reason: err.Error()})
			return
		}
		if len(result.Content) == 0 {
			emit(provider.StreamEvent{Type: provider.Empty})
			return
		}
		var calls []provider.ToolCall
		for _, b := range result.Content {
			if b.Type == "tool_use" {
				calls = append(calls, provider.ToolCall{ID: b.ID, Name: b.Name, Args: string(b.Input)})
			}
		}
		if len(calls) > 0 {
			if !emit(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}) {
				return
			}
		}
		emit(provider.StreamEvent{Type: provider.TransportEvent, Transport: &providertransport.Observation{Transport: providertransport.HTTPSSE, Phase: providertransport.Completed}})
		kind := provider.Done
		if result.StopReason == "max_tokens" {
			kind = provider.Truncated
		}
		emit(provider.StreamEvent{Type: kind, Reason: result.StopReason, ResponseID: result.ID, ResponseState: state})
	}()
	return out, nil
}
func emitAnthropicBlock(b anthropicContentBlock, emit func(provider.StreamEvent) bool) {
	switch b.Type {
	case "text":
		if b.Text != "" {
			emit(provider.StreamEvent{Type: provider.Delta, Text: b.Text})
		}
	case "thinking":
		if b.Thinking != "" {
			emit(provider.StreamEvent{Type: provider.Reasoning, Text: b.Thinking})
		}
	case "server_tool_use":
		emit(provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: b.ID, Name: b.Name, Args: string(b.Input)}})
	case "web_search_tool_result":
		r := decodeAnthropicWebSearchResult(b.Content)
		emit(provider.StreamEvent{Type: provider.HostedToolResult, ToolCall: provider.ToolCall{ID: b.ToolUseID, Name: "web_search"}, Text: r.Text, HostedResult: r})
	}
}
func mergeAnthropicUsage(dst *anthropicUsage, src anthropicUsage) {
	if src.inputReported {
		dst.InputTokens = src.InputTokens
		dst.inputReported = true
	}
	if src.outputReported {
		dst.OutputTokens = src.OutputTokens
		dst.outputReported = true
	}
	if src.readReported {
		dst.CacheReadInputTokens = src.CacheReadInputTokens
		dst.readReported = true
	}
	if src.writeReported {
		dst.CacheCreationInputTokens = src.CacheCreationInputTokens
		dst.writeReported = true
	}
}
func readAnthropicStream(reader io.Reader, emit func(provider.StreamEvent) bool) (anthropicResponse, error) {
	// Bound both individual events and aggregate output, including unknown events.
	reader = io.LimitReader(reader, (32<<20)+1)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	result := anthropicResponse{}
	started := false
	terminal := false
	active := -1
	var data strings.Builder
	var args strings.Builder
	total := 0
	cancelled := false
	send := emit
	emit = func(e provider.StreamEvent) bool {
		if cancelled {
			return false
		}
		if !send(e) {
			cancelled = true
			return false
		}
		return true
	}
	process := func(raw string) error {
		var e struct {
			Type    string                `json:"type"`
			Index   int                   `json:"index"`
			Message anthropicResponse     `json:"message"`
			Block   anthropicContentBlock `json:"content_block"`
			Usage   anthropicUsage        `json:"usage"`
			Delta   struct {
				Type, Text, Thinking, Signature string
				Citation                        json.RawMessage `json:"citation"`
				PartialJSON                     string          `json:"partial_json"`
				StopReason                      string          `json:"stop_reason"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(raw), &e) != nil {
			return errors.New("invalid Anthropic stream event")
		}
		if terminal {
			return errors.New("Anthropic event after terminal")
		}
		switch e.Type {
		case "ping":
			return nil
		case "message_start":
			if started {
				return errors.New("duplicate Anthropic message start")
			}
			started = true
			result = e.Message
			if len(result.Content) != 0 {
				return errors.New("Anthropic stream start must have empty content")
			}
		case "content_block_start":
			if !started || active >= 0 || e.Index != len(result.Content) {
				return errors.New("invalid Anthropic block start")
			}
			active = e.Index
			args.Reset()
			result.Content = append(result.Content, e.Block)
			if e.Block.Type != "server_tool_use" && e.Block.Type != "web_search_tool_result" {
				emitAnthropicBlock(e.Block, emit)
			}
			if e.Block.Type == "tool_use" {
				emit(provider.StreamEvent{Type: provider.ToolCallStart, ToolCallStream: provider.ToolCallStream{ID: e.Block.ID, Name: e.Block.Name}})
			}
		case "content_block_delta":
			if active < 0 || e.Index != active {
				return errors.New("Anthropic delta has no active block")
			}
			b := &result.Content[active]
			switch e.Delta.Type {
			case "text_delta":
				if b.Type != "text" {
					return errors.New("invalid text delta")
				}
				b.Text += e.Delta.Text
				emit(provider.StreamEvent{Type: provider.Delta, Text: e.Delta.Text})
			case "thinking_delta":
				if b.Type != "thinking" {
					return errors.New("invalid thinking delta")
				}
				b.Thinking += e.Delta.Thinking
				emit(provider.StreamEvent{Type: provider.Reasoning, Text: e.Delta.Thinking})
			case "citations_delta":
				if b.Type != "text" || !json.Valid(e.Delta.Citation) {
					return errors.New("invalid Anthropic citation delta")
				}
				var citations []json.RawMessage
				if len(b.Citations) > 0 && json.Unmarshal(b.Citations, &citations) != nil {
					return errors.New("invalid Anthropic citations")
				}
				citations = append(citations, e.Delta.Citation)
				b.Citations, _ = json.Marshal(citations)
			case "signature_delta":
				if b.Type != "thinking" {
					return errors.New("invalid signature delta")
				}
				b.Signature += e.Delta.Signature
			case "input_json_delta":
				if b.Type != "tool_use" && b.Type != "server_tool_use" {
					return errors.New("invalid tool delta")
				}
				args.WriteString(e.Delta.PartialJSON)
				if b.Type == "tool_use" {
					emit(provider.StreamEvent{Type: provider.ToolCallDelta, Text: e.Delta.PartialJSON, ToolCallStream: provider.ToolCallStream{ID: b.ID, Name: b.Name}})
				}
			default:
				return fmt.Errorf("unsupported Anthropic delta %q", e.Delta.Type)
			}
		case "content_block_stop":
			if active < 0 || e.Index != active {
				return errors.New("invalid Anthropic block stop")
			}
			b := &result.Content[active]
			if args.Len() > 0 {
				b.Input = json.RawMessage(args.String())
			}
			if err := validateAnthropicNative([]anthropicContentBlock{*b}); err != nil {
				return err
			}
			if b.Type == "server_tool_use" || b.Type == "web_search_tool_result" {
				emitAnthropicBlock(*b, emit)
			}
			if b.Type == "tool_use" {
				emit(provider.StreamEvent{Type: provider.ToolCallEnd, ToolCallStream: provider.ToolCallStream{ID: b.ID, Name: b.Name}})
			}
			active = -1
		case "message_delta":
			if !started || active >= 0 {
				return errors.New("invalid Anthropic message delta")
			}
			if e.Delta.StopReason != "" {
				result.StopReason = e.Delta.StopReason
			}
			mergeAnthropicUsage(&result.Usage, e.Usage)
		case "message_stop":
			if !started || active >= 0 || result.StopReason == "" {
				return provider.ErrStreamMissingTerminal
			}
			terminal = true
		case "error":
			return protocolFailure("anthropic", http.StatusOK, nil, []byte(raw), time.Now())
		default:
			// Anthropic permits additive event types; known block deltas remain strict.
			return nil
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		total += len(line) + 1
		if total > 32<<20 {
			return result, errors.New("Anthropic stream exceeds limit")
		}
		if cancelled {
			return result, context.Canceled
		}
		if line == "" {
			if data.Len() > 0 {
				if err := process(data.String()); err != nil {
					return result, err
				}
				data.Reset()
				if terminal {
					return result, nil
				}
				if cancelled {
					return result, context.Canceled
				}
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte(10)
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if data.Len() > 0 {
		if err := process(data.String()); err != nil {
			return result, err
		}
	}
	if !terminal {
		return result, providerretry.WithAdvice(provider.ErrStreamMissingTerminal, providerretry.Advice{Category: providerretry.Interrupted})
	}
	return result, nil
}
