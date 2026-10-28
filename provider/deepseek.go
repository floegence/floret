package provider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/tools"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/configbridge"
	"github.com/floegence/floret/v7/internal/provider/catalog"
)

// DeepSeekOptions configures DeepSeek's stateless Responses transport. BaseURL
// is the API root. Sampling and output format apply to every request made by
// this gateway; ResponseFormat accepts empty, text, or json_object.
// Opaque response state belongs to Floret and must be passed back unchanged.
type DeepSeekOptions struct {
	// ImageFiles opts into temporary file references; nil preserves inline transport.
	// The owner must match this gateway endpoint and credentials.
	ImageFiles *DeepSeekImageFiles
	// MaxRequestBytes bounds the final JSON body; zero disables the host limit.
	MaxRequestBytes       int64
	Model                 string
	BaseURL               string
	APIKey                string
	StateCompatibilityKey string
	HTTPClient            *http.Client
	Temperature           *float64
	TopP                  *float64
	ResponseFormat        string
	// ResolveAttachment resolves an authorized opaque attachment into image bytes.
	// It runs during Prepare (or Stream for direct callers), never during replay
	// of a prepared request. Nil preserves descriptor-only behavior. The gateway
	// accepts images only when the selected model advertises image input.
	ResolveAttachment func(context.Context, Attachment) ([]byte, error)
}

type deepSeekGateway struct {
	imageFiles                       *DeepSeekImageFiles
	maxRequestBytes                  int64
	identity                         Identity
	capabilities                     Capabilities
	endpoint, apiKey, responseFormat string
	client                           *http.Client
	temperature, topP                *float64
	resolveAttachment                func(context.Context, Attachment) ([]byte, error)
	supportsImages                   bool
}

// NewDeepSeek constructs the single DeepSeek Responses execution path, including
// streaming reasoning and validated stateless history replay. Hosted web_search
// is supported by Pro; Flash ignores hosted search and uses the independent Search API.
// Image input is an optional per-model capability resolved by the host.
func NewDeepSeek(options DeepSeekOptions) (Gateway, error) {
	if options.MaxRequestBytes < 0 {
		return nil, errors.New("request byte limit must not be negative")
	}
	model, ok := catalog.FindModel(catalog.ProviderDeepSeek, strings.TrimSpace(options.Model))
	if !ok {
		return nil, fmt.Errorf("unsupported DeepSeek model %q", options.Model)
	}
	capabilities := Capabilities{Reasoning: ReasoningSupported, ReasoningCapability: configbridge.PublicReasoningCapability(model.Reasoning), AttachmentPayload: AttachmentDescriptors}
	identity, capabilities, baseURL, apiKey, err := validateOfficialOptions("deepseek", options.Model, options.BaseURL, options.APIKey, options.StateCompatibilityKey, capabilities)
	if err != nil {
		return nil, fmt.Errorf("DeepSeek gateway: %w", err)
	}
	if options.Temperature != nil && !(*options.Temperature >= 0 && *options.Temperature <= 2) {
		return nil, errors.New("DeepSeek temperature must be between 0 and 2")
	}
	if options.TopP != nil && !(*options.TopP >= 0 && *options.TopP <= 1) {
		return nil, errors.New("DeepSeek top_p must be between 0 and 1")
	}
	switch options.ResponseFormat {
	case "", "text", "json_object":
	default:
		return nil, fmt.Errorf("unsupported DeepSeek response format %q", options.ResponseFormat)
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	clone := func(p *float64) *float64 {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	if options.ResolveAttachment != nil {
		capabilities.AttachmentPayload = AttachmentExpanded
	}
	if options.ImageFiles != nil && (options.ImageFiles.endpoint != baseURL || options.ImageFiles.apiKey != apiKey) {
		return nil, errors.New("DeepSeek image file owner differs from gateway endpoint or credentials")
	}
	return &deepSeekGateway{imageFiles: options.ImageFiles, maxRequestBytes: options.MaxRequestBytes, resolveAttachment: options.ResolveAttachment, supportsImages: slices.Contains(model.Input, "image"), identity: identity, capabilities: capabilities, endpoint: baseURL + "/responses", apiKey: apiKey, client: client, temperature: clone(options.Temperature), topP: clone(options.TopP), responseFormat: options.ResponseFormat}, nil
}
func (g *deepSeekGateway) Identity() Identity { return g.identity }
func (g *deepSeekGateway) Capabilities() Capabilities {
	out := g.capabilities
	out.OutputControl = OutputControlClient
	out.ReasoningCapability.SupportedLevels = slices.Clone(out.ReasoningCapability.SupportedLevels)
	return out
}

const deepSeekStateKind = "deepseek_responses_v1"

// The opaque transcript retains provider-native items, including server search
// receipts. Canonical prefix hashes bind it to the exact projected conversation;
// current system instructions always come from the current request.
type deepSeekHistory struct {
	Compatibility string            `json:"compatibility"`
	Prefix        []string          `json:"prefix"`
	Input         []json.RawMessage `json:"input"`
}

type deepSeekInput struct {
	Type      string `json:"type"`
	Role      string `json:"role,omitempty"`
	Content   any    `json:"content,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	Output    any    `json:"output,omitempty"`
}
type deepSeekRendered struct {
	input  []json.RawMessage
	prefix []string
	ends   []int
}

func deepSeekHash(value any) string {
	raw, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func deepSeekSignature(item deepSeekInput) string {
	if item.Type == "function_call" {
		var args any
		if json.Unmarshal([]byte(item.Arguments), &args) == nil {
			b, _ := json.Marshal(args)
			item.Arguments = string(b)
		}
	}
	return deepSeekHash(item)
}
func (r *deepSeekRendered) add(item deepSeekInput, canonical bool) {
	raw, _ := json.Marshal(item)
	r.input = append(r.input, raw)
	if canonical {
		r.prefix = append(r.prefix, deepSeekSignature(item))
		r.ends = append(r.ends, len(r.input))
	}
}
func (g *deepSeekGateway) renderMessages(ctx context.Context, messages []Message, images map[string]deepSeekResolvedImage) (deepSeekRendered, []json.RawMessage, error) {
	// Canonical conversation may split one provider answer around hosted activity.
	// Adjacent assistant fragments and calls still represent one provider response.
	merged := make([]Message, 0, len(messages))
	for _, message := range messages {
		if len(merged) > 0 && message.Role == RoleAssistant && message.ToolResult == nil && len(message.Attachments) == 0 {
			previous := &merged[len(merged)-1]
			if previous.Role == RoleAssistant && previous.ToolResult == nil && len(previous.Attachments) == 0 {
				previous.Text += message.Text
				previous.Reasoning += message.Reasoning
				previous.ToolCalls = append(append([]ToolCall(nil), previous.ToolCalls...), message.ToolCalls...)
				continue
			}
		}
		merged = append(merged, message)
	}
	var out deepSeekRendered
	var system []json.RawMessage
	pending := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range merged {
		var content any = m.Text
		if len(m.Attachments) > 0 {
			if m.Role != RoleUser || !g.supportsImages || g.resolveAttachment == nil {
				return out, nil, errors.New("DeepSeek model does not accept resolved image input")
			}
			parts := []deepSeekImageContent{}
			if m.Text != "" {
				parts = append(parts, deepSeekImageContent{Type: "input_text", Text: m.Text})
			}
			for _, attachment := range m.Attachments {
				switch attachment.MIMEType {
				case "image/png", "image/jpeg", "image/gif", "image/webp":
				default:
					return out, nil, fmt.Errorf("unsupported DeepSeek image MIME type %q", attachment.MIMEType)
				}
				if err := ctx.Err(); err != nil {
					return out, nil, err
				}
				data, err := g.resolveAttachment(ctx, attachment)
				if err != nil {
					return out, nil, fmt.Errorf("resolve DeepSeek attachment: %w", err)
				}
				if len(data) == 0 || (attachment.SizeBytes > 0 && int64(len(data)) != attachment.SizeBytes) {
					return out, nil, errors.New("resolved DeepSeek image size differs from attachment")
				}
				ref := deepSeekImageReference{Attachment: attachment, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
				images[deepSeekHash(ref)] = deepSeekResolvedImage{mime: attachment.MIMEType, data: data}
				parts = append(parts, deepSeekImageContent{Type: "image_reference", Image: &ref})
			}
			content = parts
		}
		if len(pending) > 0 && m.Role != RoleTool && !(m.Role == RoleAssistant && m.Text == "" && len(m.ToolCalls) > 0) {
			return out, nil, errors.New("DeepSeek tool calls require results before the next message")
		}
		if m.Role == RoleSystem {
			raw, _ := json.Marshal(deepSeekInput{Type: "message", Role: "system", Content: m.Text})
			system = append(system, raw)
			continue
		}
		if m.Reasoning != "" {
			out.add(deepSeekInput{Type: "reasoning", Content: []map[string]string{{"type": "reasoning_text", "text": m.Reasoning}}}, m.Text == "" && len(m.ToolCalls) == 0)
		}
		if m.Text != "" || len(m.Attachments) > 0 {
			out.add(deepSeekInput{Type: "message", Role: string(m.Role), Content: content}, true)
		}
		for _, c := range m.ToolCalls {
			if c.ID == "" || c.Name == "" || !json.Valid([]byte(c.Args)) || seen[c.ID] {
				return out, nil, errors.New("invalid or duplicate DeepSeek function call")
			}
			seen[c.ID] = true
			pending[c.ID] = true
			out.add(deepSeekInput{Type: "function_call", CallID: c.ID, Name: c.Name, Arguments: c.Args}, true)
		}
		if m.ToolResult != nil {
			c := m.ToolResult
			if !pending[c.CallID] {
				return out, nil, errors.New("orphan or duplicate DeepSeek function result")
			}
			delete(pending, c.CallID)
			if len(c.Attachments) == 0 {
				out.add(deepSeekInput{Type: "function_call_output", CallID: c.CallID, Output: c.Text}, true)
				continue
			}
			if !g.supportsImages || g.resolveAttachment == nil {
				return out, nil, errors.New("DeepSeek tool result contains images but the model has no image resolver")
			}
			parts := make([]deepSeekImageContent, 0, 1+len(c.Attachments))
			if c.Text != "" {
				parts = append(parts, deepSeekImageContent{Type: "input_text", Text: c.Text})
			}
			for _, attachment := range c.Attachments {
				switch attachment.MIMEType {
				case "image/png", "image/jpeg", "image/gif", "image/webp":
				default:
					return out, nil, fmt.Errorf("DeepSeek tool result has unsupported image MIME type %q", attachment.MIMEType)
				}
				data, err := g.resolveAttachment(ctx, attachment)
				if err != nil {
					return out, nil, fmt.Errorf("resolve DeepSeek tool result image %q: %w", attachment.ResourceRef, err)
				}
				if len(data) == 0 {
					return out, nil, errors.New("DeepSeek tool result image is empty")
				}
				ref := deepSeekImageReference{Attachment: attachment, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
				images[deepSeekHash(ref)] = deepSeekResolvedImage{mime: attachment.MIMEType, data: data}
				parts = append(parts, deepSeekImageContent{Type: "image_reference", Image: &ref})
			}
			out.add(deepSeekInput{Type: "function_call_output", CallID: c.CallID, Output: parts}, true)
		}
	}
	if len(pending) > 0 {
		return out, nil, errors.New("DeepSeek function call is missing its result")
	}
	return out, system, nil
}

func (g *deepSeekGateway) render(ctx context.Context, req Request) ([]byte, deepSeekHistory, error) {
	var history deepSeekHistory
	if req.Reasoning.BudgetTokens != 0 {
		return nil, history, errors.New("DeepSeek Responses does not support a reasoning token budget")
	}
	if err := req.Validate(); err != nil {
		return nil, history, err
	}
	if err := configbridge.ReasoningCapability(g.capabilities.ReasoningCapability).ValidateSelection(configbridge.ReasoningSelection(req.Reasoning)); err != nil {
		return nil, history, err
	}
	images := map[string]deepSeekResolvedImage{}
	rendered, system, err := g.renderMessages(ctx, req.Messages, images)
	if err != nil {
		return nil, history, err
	}
	history = deepSeekHistory{Compatibility: deepSeekHash(g.identity), Prefix: rendered.prefix, Input: rendered.input}
	if state := req.PreviousState; state != nil {
		if state.Kind != deepSeekStateKind || strings.TrimSpace(state.ID) == "" {
			return nil, history, errors.New("invalid DeepSeek response state kind or identity")
		}
		var previous deepSeekHistory
		decoder := json.NewDecoder(strings.NewReader(state.Attributes["history"]))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&previous); err != nil {
			return nil, history, fmt.Errorf("invalid DeepSeek response history: %w", err)
		}
		if previous.Compatibility != history.Compatibility || len(previous.Prefix) > len(rendered.prefix) || !slices.Equal(previous.Prefix, rendered.prefix[:len(previous.Prefix)]) || len(previous.Input) == 0 {
			return nil, history, errors.New("DeepSeek response history does not match canonical context")
		}
		for _, raw := range previous.Input {
			var item deepSeekInput
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, history, errors.New("invalid DeepSeek history item")
			}
			switch item.Type {
			case "message", "reasoning", "function_call", "function_call_output", "web_search_call":
			default:
				return nil, history, errors.New("unsupported DeepSeek history item")
			}
		}
		end := 0
		if len(previous.Prefix) > 0 {
			end = rendered.ends[len(previous.Prefix)-1]
		}
		history.Input = append(slices.Clone(previous.Input), rendered.input[end:]...)
	}
	input, err := g.expandImages(ctx, append(slices.Clone(system), history.Input...), images)
	if err != nil {
		return nil, history, err
	}
	body := map[string]any{"model": g.identity.Model, "input": input, "stream": true}
	if req.MaxOutputTokens > 0 {
		body["max_output_tokens"] = req.MaxOutputTokens
	}
	if g.temperature != nil {
		body["temperature"] = *g.temperature
	}
	if g.topP != nil {
		body["top_p"] = *g.topP
	}
	if g.responseFormat != "" {
		body["text"] = map[string]any{"format": map[string]string{"type": g.responseFormat}}
	}
	level := config.NormalizeReasoningSelection(req.Reasoning).Level
	if level != "" && level != config.ReasoningLevelDefault {
		effort := string(level)
		if level == config.ReasoningLevelOff {
			effort = "none"
		}
		body["reasoning"] = map[string]string{"effort": effort}
	}
	var definitions []any
	names := map[string]bool{}
	for _, tool := range req.Tools {
		if !validDeepSeekToolName(tool.Name) || names[tool.Name] {
			return nil, history, errors.New("invalid or duplicate DeepSeek tool name")
		}
		names[tool.Name] = true
		definitions = append(definitions, map[string]any{"type": "function", "name": tool.Name, "description": tool.Description, "parameters": tool.InputSchema, "strict": tool.Strict})
	}
	hosted := false
	for _, tool := range req.HostedTools {
		if tool.Name != "web_search" || tool.Type != "web_search" || hosted || names[tool.Name] || len(tool.Parameters) > 0 {
			return nil, history, errors.New("DeepSeek supports only one native web_search tool")
		}
		for key, value := range tool.Options {
			if key != "wire_shape" || value != "deepseek_responses_web_search" {
				return nil, history, fmt.Errorf("unsupported DeepSeek hosted tool option %q", key)
			}
		}
		hosted = true
		definitions = append(definitions, map[string]string{"type": "web_search"})
	}
	if len(definitions) > 0 {
		body["tools"] = definitions
		body["tool_choice"] = "auto"
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, history, err
	}
	raw, err = OrderToolSchemas(RequestFormatOpenAIResponses, raw)
	if err == nil {
		err = CheckRequestBytes(raw, g.maxRequestBytes)
	}
	return raw, history, err
}

func (g *deepSeekGateway) Stream(ctx context.Context, req Request) (<-chan Event, error) {
	body, history, err := g.render(ctx, req)
	if err != nil {
		return nil, err
	}
	return g.streamRendered(ctx, body, history, req.ReusePolicy == nil || req.ReusePolicy.ProviderStateAllowed)
}

func (g *deepSeekGateway) streamRendered(ctx context.Context, body []byte, history deepSeekHistory, stateAllowed bool) (<-chan Event, error) {
	if err := CheckRequestBytes(body, g.maxRequestBytes); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+g.apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	response, err := g.client.Do(request)
	receivedAt := time.Now()
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		lower := strings.ToLower(string(raw))
		overflow := response.StatusCode == 413 || response.StatusCode == 400 && (strings.Contains(lower, "context_length_exceeded") || strings.Contains(lower, "context window") || strings.Contains(lower, "maximum context length"))
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &envelope)
		failure := &ProviderHTTPError{
			Provider:   "DeepSeek",
			StatusCode: response.StatusCode,
			Code:       sanitizeProviderErrorText(envelope.Error.Code),
			Message:    sanitizeProviderErrorText(envelope.Error.Message),
		}
		if g.imageFiles != nil && (response.StatusCode == 400 || response.StatusCode == 404 || response.StatusCode == 410) && (envelope.Error.Code == "file_not_found" || envelope.Error.Code == "file_expired") {
			if err := g.imageFiles.invalidateBody(ctx, body); err != nil {
				return nil, err
			}
			return nil, &RequestPreparationError{Cause: failure}
		}
		if overflow {
			// Preserve the established recovery classification while exposing
			// whether the server rejected transport bytes (413) or model context.
			return nil, errors.Join(failure, ErrContextOverflow)
		}
		return nil, WithRetryAdvice(failure, HTTPRetryAdvice(response.StatusCode, envelope.Error.Code, response.Header, receivedAt))
	}
	out := make(chan Event, 32)
	go func() {
		defer close(out)
		defer response.Body.Close()
		g.readStream(ctx, response.Body, history, out, stateAllowed)
	}()
	return out, nil
}

func sanitizeProviderErrorText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	if len(value) > 1000 {
		value = value[:1000]
	}
	return value
}

type deepSeekOutput struct {
	Type      string                 `json:"type"`
	ID        string                 `json:"id"`
	Status    string                 `json:"status"`
	Role      string                 `json:"role"`
	CallID    string                 `json:"call_id"`
	Name      string                 `json:"name"`
	Arguments string                 `json:"arguments"`
	Error     *HostedToolResultError `json:"error"`
	Content   []struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		Annotations []struct {
			Type       string `json:"type"`
			URL        string `json:"url"`
			Title      string `json:"title"`
			StartIndex *int   `json:"start_index"`
			EndIndex   *int   `json:"end_index"`
		} `json:"annotations"`
	} `json:"content"`
	Action struct {
		Type    string                 `json:"type"`
		URL     string                 `json:"url,omitempty"`
		Pattern string                 `json:"pattern,omitempty"`
		Query   string                 `json:"query"`
		Queries []string               `json:"queries"`
		Sources []HostedToolResultItem `json:"sources,omitempty"`
	} `json:"action"`
}
type deepSeekResponse struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	Output []json.RawMessage `json:"output"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	IncompleteDetails struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Usage *struct {
		Input        int64 `json:"input_tokens"`
		Output       int64 `json:"output_tokens"`
		Total        int64 `json:"total_tokens"`
		InputDetails struct {
			Cached int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
		OutputDetails struct {
			Reasoning int64 `json:"reasoning_tokens"`
		} `json:"output_tokens_details"`
	} `json:"usage"`
}

func (g *deepSeekGateway) readStream(ctx context.Context, body io.Reader, history deepSeekHistory, out chan<- Event, stateAllowed bool) {
	emit := func(event Event) bool {
		select {
		case <-ctx.Done():
			return false
		case out <- event:
			return true
		}
	}
	directEmit := emit
	fail := func(err error) { directEmit(Event{Type: EventError, Err: err}) }
	var text, reasoning strings.Builder
	nativeChannels := map[EventType]bool{}
	anonymousChannels := map[EventType]bool{}
	streamedParts := map[string]string{}
	nativeParts := map[string]OutputPart{}
	nativePositions := map[int]string{}
	partKey := func(part OutputPart, kind EventType) string {
		return fmt.Sprintf("%s:%d:%d:%s", part.ItemID, part.OutputIndex, part.ContentIndex, kind)
	}
	emitText := func(kind EventType, value string, part *OutputPart) {
		if value == "" {
			return
		}
		if part != nil {
			streamedParts[partKey(*part, kind)] += value
			nativeChannels[kind] = true
		} else {
			anonymousChannels[kind] = true
		}
		emit(Event{Type: kind, Text: value, OutputPart: part})
	}

	partFor := func(id string) *OutputPart {
		if p, ok := nativeParts[id]; ok {
			copy := p
			copy.ContentIndex = 0
			return &copy
		}
		return nil
	}
	calls := map[string]*ToolCall{}
	ended := map[string]bool{}
	searches := map[string]deepSeekOutput{}
	searchDone := map[string]bool{}
	search := func(item deepSeekOutput) deepSeekOutput {
		previous, exists := searches[item.ID]
		if item.Action.Type == "" {
			item.Action.Type = previous.Action.Type
		}
		if item.Action.Query == "" {
			item.Action.Query = previous.Action.Query
		}
		if item.Action.Queries == nil {
			item.Action.Queries = previous.Action.Queries
		}
		if item.Action.URL == "" {
			item.Action.URL = previous.Action.URL
		}
		if item.Action.Pattern == "" {
			item.Action.Pattern = previous.Action.Pattern
		}
		if item.Action.Sources == nil {
			item.Action.Sources = previous.Action.Sources
		}
		searches[item.ID] = item
		if !exists {
			args, _ := json.Marshal(item.Action)
			emit(Event{Type: EventHostedToolCall, OutputPart: partFor(item.ID), HostedToolCall: &ToolCall{ID: item.ID, Name: "web_search", Args: string(args)}})
		}
		return item
	}
	finishSearch := func(item deepSeekOutput) {
		item = search(item)
		if searchDone[item.ID] {
			return
		}
		searchDone[item.ID] = true
		result := &HostedToolResult{ResultsProvided: item.Action.Sources != nil, Text: "Web search completed", Metadata: map[string]any{"status": item.Status}}
		if item.Status == "failed" || item.Status == "incomplete" || item.Error != nil {
			result.Text = "Web search failed"
			result.Error = item.Error
			if result.Error == nil {
				result.Error = &HostedToolResultError{Code: "search_failed", Message: result.Text}
			}
		}
		for _, source := range item.Action.Sources {
			title := source.Title
			if source.URL != "" {
				result.Results = append(result.Results, HostedToolResultItem{Title: title, URL: source.URL, Snippet: source.Snippet})
			}
		}
		args, _ := json.Marshal(item.Action)
		emit(Event{Type: EventHostedToolResult, OutputPart: partFor(item.ID), HostedToolCall: &ToolCall{ID: item.ID, Name: "web_search", Args: string(args)}, HostedResult: result})
	}
	finishCall := func(item deepSeekOutput) error {
		if item.ID == "" || item.CallID == "" || item.Name == "" || !json.Valid([]byte(item.Arguments)) {
			return errors.New("invalid DeepSeek function call output")
		}
		c := calls[item.ID]
		if c == nil {
			c = &ToolCall{ID: item.CallID, Name: item.Name}
			calls[item.ID] = c
			emit(Event{Type: EventToolCallStart, OutputPart: partFor(item.ID), ToolCallStream: &ToolCallStream{ID: c.ID, Name: c.Name}})
		}
		if c.ID != item.CallID || c.Name != item.Name {
			return errors.New("DeepSeek function call identity changed")
		}
		c.Args = item.Arguments
		if !ended[item.ID] {
			ended[item.ID] = true
			emit(Event{Type: EventToolCallEnd, OutputPart: partFor(item.ID), ToolCallStream: &ToolCallStream{ID: c.ID, Name: c.Name}})
		}
		return nil
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	var data []string
	dispatch := func(payload string) bool {
		var event struct {
			Type         string           `json:"type"`
			OutputIndex  *int             `json:"output_index"`
			ContentIndex int              `json:"content_index"`
			Delta        string           `json:"delta"`
			ItemID       string           `json:"item_id"`
			Item         deepSeekOutput   `json:"item"`
			Response     deepSeekResponse `json:"response"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			fail(fmt.Errorf("decode DeepSeek SSE: %w", err))
			return true
		}
		var part *OutputPart
		itemID := event.ItemID
		if itemID == "" {
			itemID = event.Item.ID
		}
		if itemID != "" {
			previous, known := nativeParts[itemID]
			if event.OutputIndex != nil {
				if *event.OutputIndex < 0 || event.ContentIndex < 0 || known && previous.OutputIndex != *event.OutputIndex {
					fail(errors.New("invalid or changed DeepSeek output item position"))
					return true
				}
				if other, exists := nativePositions[*event.OutputIndex]; exists && other != itemID {
					fail(errors.New("DeepSeek output position has conflicting items"))
					return true
				}
				nativePositions[*event.OutputIndex] = itemID
				previous = OutputPart{ItemID: itemID, OutputIndex: *event.OutputIndex}
				nativeParts[itemID] = previous
				known = true
			}
			if known {
				previous.ContentIndex = event.ContentIndex
				part = &previous
			}
		}

		switch event.Type {
		case "response.output_text.delta":
			text.WriteString(event.Delta)
			emitText(EventDelta, event.Delta, part)
		case "response.reasoning_text.delta":
			reasoning.WriteString(event.Delta)
			emitText(EventReasoning, event.Delta, part)
		case "response.output_item.added":
			item := event.Item
			switch item.Type {
			case "function_call":
				if item.ID == "" || item.CallID == "" || item.Name == "" || calls[item.ID] != nil {
					fail(errors.New("invalid DeepSeek function call start"))
					return true
				}
				calls[item.ID] = &ToolCall{ID: item.CallID, Name: item.Name, Args: item.Arguments}
				emit(Event{Type: EventToolCallStart, OutputPart: part, ToolCallStream: &ToolCallStream{ID: item.CallID, Name: item.Name}})
			case "web_search_call":
				search(item)
			}
		case "response.function_call_arguments.delta":
			c := calls[event.ItemID]
			if c == nil {
				fail(errors.New("DeepSeek arguments without function call"))
				return true
			}
			c.Args += event.Delta
			emit(Event{Type: EventToolCallDelta, OutputPart: part, Text: event.Delta, ToolCallStream: &ToolCallStream{ID: c.ID, Name: c.Name}})
		case "response.output_item.done":
			switch event.Item.Type {
			case "function_call":
				if err := finishCall(event.Item); err != nil {
					fail(err)
					return true
				}
			case "web_search_call":
				finishSearch(event.Item)
			}
		case "response.web_search_call.in_progress", "response.web_search_call.searching":
			search(deepSeekOutput{ID: event.ItemID})
		case "response.failed", "error":
			if event.Response.Error != nil && event.Response.Error.Code == "context_length_exceeded" {
				fail(errors.Join(&ProviderHTTPError{Provider: "DeepSeek", StatusCode: 200, Code: event.Response.Error.Code, Message: sanitizeProviderErrorText(event.Response.Error.Message)}, ErrContextOverflow))
			} else {
				fail(errors.New("DeepSeek response failed"))
			}
			return true
		case "response.completed", "response.incomplete":
			r := event.Response
			if r.ID == "" || r.Status != strings.TrimPrefix(event.Type, "response.") || r.Error != nil {
				fail(errors.New("invalid DeepSeek terminal response"))
				return true
			}
			directEmit := emit
			var terminalEvents []Event
			emit = func(e Event) bool { terminalEvents = append(terminalEvents, e); return true }
			defer func() { emit = directEmit }()
			finalParts := map[string]bool{}
			finalItems := map[string]bool{}
			var finalText, finalReasoning strings.Builder
			var finalCalls []ToolCall
			finalIDs := map[string]bool{}
			var sources []Source
			for outputIndex, raw := range r.Output {
				var item deepSeekOutput
				if err := json.Unmarshal(raw, &item); err != nil {
					fail(err)
					return true
				}
				if item.ID != "" {
					if finalItems[item.ID] {
						fail(errors.New("duplicate DeepSeek terminal output item"))
						return true
					}
					finalItems[item.ID] = true
					if previous, ok := nativeParts[item.ID]; ok && previous.OutputIndex != outputIndex {
						fail(errors.New("DeepSeek terminal item position changed"))
						return true
					}
					nativeParts[item.ID] = OutputPart{ItemID: item.ID, OutputIndex: outputIndex}
				}
				switch item.Type {
				case "message":
					for contentIndex, part := range item.Content {
						if part.Type != "output_text" {
							fail(fmt.Errorf("unsupported DeepSeek message part %q", part.Type))
							return true
						}
						offset := utf8.RuneCountInString(finalText.String())
						finalText.WriteString(part.Text)
						native := OutputPart{ItemID: item.ID, OutputIndex: outputIndex, ContentIndex: contentIndex}
						if item.ID != "" {
							key := partKey(native, EventDelta)
							finalParts[key] = true
							prior := streamedParts[key]
							if !strings.HasPrefix(part.Text, prior) {
								fail(errors.New("DeepSeek terminal text part disagrees with streamed deltas"))
								return true
							}
							if !anonymousChannels[EventDelta] {
								if suffix := part.Text[len(prior):]; suffix != "" {
									emit(Event{Type: EventDelta, Text: suffix, OutputPart: &native})
								}
							}
						} else if nativeChannels[EventDelta] {
							fail(errors.New("DeepSeek terminal text omitted output identity"))
							return true
						} else if !anonymousChannels[EventDelta] && part.Text != "" {
							emit(Event{Type: EventDelta, Text: part.Text})
						}
						for _, a := range part.Annotations {
							if a.Type == "url_citation" && a.URL != "" {
								source := citation.Source{Title: a.Title, URL: a.URL, StartIndex: a.StartIndex, EndIndex: a.EndIndex}
								refs := citation.Fragment([]citation.Source{source}, part.Text, 0, len(part.Text), true, nil)
								for _, ref := range refs {
									if ref.StartIndex != nil {
										*ref.StartIndex += offset
										*ref.EndIndex += offset
									}
									sources = append(sources, Source(ref))
								}
							}
						}
					}
				case "reasoning":
					for contentIndex, part := range item.Content {
						if part.Type != "reasoning_text" {
							fail(fmt.Errorf("unsupported DeepSeek reasoning part %q", part.Type))
							return true
						}
						finalReasoning.WriteString(part.Text)
						native := OutputPart{ItemID: item.ID, OutputIndex: outputIndex, ContentIndex: contentIndex}
						if item.ID != "" {
							key := partKey(native, EventReasoning)
							finalParts[key] = true
							prior := streamedParts[key]
							if !strings.HasPrefix(part.Text, prior) {
								fail(errors.New("DeepSeek terminal reasoning part disagrees with streamed deltas"))
								return true
							}
							if !anonymousChannels[EventReasoning] {
								if suffix := part.Text[len(prior):]; suffix != "" {
									emit(Event{Type: EventReasoning, Text: suffix, OutputPart: &native})
								}
							}
						} else if nativeChannels[EventReasoning] {
							fail(errors.New("DeepSeek terminal reasoning omitted output identity"))
							return true
						} else if !anonymousChannels[EventReasoning] && part.Text != "" {
							emit(Event{Type: EventReasoning, Text: part.Text})
						}
					}
				case "function_call":
					if r.Status == "incomplete" {
						continue
					}
					if err := finishCall(item); err != nil {
						fail(err)
						return true
					}
					if finalIDs[item.CallID] {
						fail(errors.New("duplicate DeepSeek terminal function call"))
						return true
					}
					finalIDs[item.CallID] = true
					finalCalls = append(finalCalls, *calls[item.ID])
				case "web_search_call":
					if item.ID == "" || item.Status == "" {
						fail(errors.New("invalid DeepSeek completed search"))
						return true
					}
					finishSearch(item)
				default:
					fail(fmt.Errorf("unsupported DeepSeek output item %q", item.Type))
					return true
				}
			}
			if r.Status == "completed" {
				for _, call := range calls {
					if !finalIDs[call.ID] {
						fail(errors.New("DeepSeek terminal response omitted a streamed function call"))
						return true
					}
				}
			}
			for key := range streamedParts {
				if !finalParts[key] {
					fail(errors.New("DeepSeek terminal omitted a streamed output part"))
					return true
				}
			}
			for _, channel := range []struct {
				kind            EventType
				streamed, final string
			}{
				{EventReasoning, reasoning.String(), finalReasoning.String()}, {EventDelta, text.String(), finalText.String()},
			} {
				if anonymousChannels[channel.kind] {
					if nativeChannels[channel.kind] {
						fail(errors.New("DeepSeek mixed identified and unidentified deltas in one channel"))
						return true
					}
					if !strings.HasPrefix(channel.final, channel.streamed) {
						fail(errors.New("DeepSeek terminal output disagrees with streamed deltas"))
						return true
					}
					if suffix := channel.final[len(channel.streamed):]; suffix != "" {
						emit(Event{Type: channel.kind, Text: suffix})
					}

				}
			}

			if len(sources) > 0 {
				refs := make([]citation.Source, len(sources))
				for i, s := range sources {
					refs[i] = citation.Source(s)
				}
				sources = nil
				for _, s := range citation.Normalize(refs) {
					sources = append(sources, Source(s))
				}
				emit(Event{Type: EventSources, Sources: sources})
			}
			if u := r.Usage; u != nil {
				cached, reason := u.InputDetails.Cached, u.OutputDetails.Reasoning
				if u.Input < 0 || u.Output < 0 || cached < 0 || cached > u.Input || reason < 0 || reason > u.Output {
					fail(errors.New("invalid DeepSeek usage"))
					return true
				}
				emit(Event{Type: EventUsage, Usage: Usage{InputTokensReported: true, InputTokenScope: "request_input", InputTokens: u.Input - cached, CacheReadTokens: cached, OutputTokens: u.Output - reason, ReasoningTokens: reason, TotalTokens: u.Input + u.Output, WindowInputTokens: u.Input, Source: "native", Available: true}})
			}
			terminal := Event{Type: EventDone, Reason: "stop", ResponseID: r.ID}
			if r.Status == "incomplete" {
				terminal.Type = EventTruncated
				terminal.Reason = r.IncompleteDetails.Reason
				if terminal.Reason != "max_output_tokens" {
					fail(fmt.Errorf("unsupported DeepSeek incomplete reason %q", terminal.Reason))
					return true
				}
			}
			if len(finalCalls) > 0 {
				emit(Event{Type: EventToolCalls, ToolCalls: finalCalls})
				terminal.Reason = "tool_calls"
			}
			{
				if finalText.Len() > 0 {
					history.Prefix = append(history.Prefix, deepSeekSignature(deepSeekInput{Type: "message", Role: "assistant", Content: finalText.String()}))
				}
				if finalText.Len() == 0 && len(finalCalls) == 0 && finalReasoning.Len() > 0 {
					history.Prefix = append(history.Prefix, deepSeekSignature(deepSeekInput{Type: "reasoning", Content: []map[string]string{{"type": "reasoning_text", "text": finalReasoning.String()}}}))
				}
				for _, c := range finalCalls {
					history.Prefix = append(history.Prefix, deepSeekSignature(deepSeekInput{Type: "function_call", CallID: c.ID, Name: c.Name, Arguments: c.Args}))
				}
				for _, raw := range r.Output {
					var item deepSeekOutput
					_ = json.Unmarshal(raw, &item)
					if r.Status == "incomplete" && item.Type == "function_call" {
						continue
					}
					history.Input = append(history.Input, raw)
				}
				raw, _ := json.Marshal(history)
				if stateAllowed {
					terminal.ResponseState = &State{Kind: deepSeekStateKind, ID: r.ID, Attributes: map[string]string{"history": string(raw)}}
				}
			}
			emit = directEmit
			for _, queued := range terminalEvents {
				if !emit(queued) {
					return true
				}
			}
			emit(terminal)
			return true
		}
		return ctx.Err() != nil
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if len(data) > 0 {
				if dispatch(strings.Join(data, "\n")) {
					return
				}
				data = nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if len(data) > 0 && dispatch(strings.Join(data, "\n")) {
		return
	}
	if err := scanner.Err(); err != nil {
		fail(fmt.Errorf("read DeepSeek SSE: %w", err))
	} else if ctx.Err() != nil {
		fail(ctx.Err())
	} else {
		fail(errors.New("DeepSeek stream closed without a terminal response"))
	}
}

// Prepare freezes the exact Responses body, including opaque replay items, so
// request estimates cover the complete text and image input actually sent.
func (g *deepSeekGateway) Prepare(ctx context.Context, req Request) (PreparedRequest, error) {
	body, history, err := g.render(ctx, req)
	if err != nil {
		return nil, err
	}
	estimate, err := EstimateRenderedRequest(RenderedRequest{Provider: "deepseek", Model: g.identity.Model, Format: RequestFormatOpenAIResponses, Payload: body, Attribution: deepSeekAttribution(body, req)})
	if err != nil {
		return nil, err
	}
	return &deepSeekPrepared{gateway: g, body: body, history: history, stateAllowed: req.ReusePolicy == nil || req.ReusePolicy.ProviderStateAllowed, fingerprint: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), estimate: estimate}, nil
}

// Bind attribution by the provider's ordered user projection, never by parsing
// prompt text. Native replay may insert reasoning and tool facts, not user input.
func deepSeekAttribution(body []byte, req Request) []RenderedContentAttribution {
	var envelope struct {
		Input []struct {
			Role    string
			Content json.RawMessage
		}
		Tools []json.RawMessage
	}
	if json.Unmarshal(body, &envelope) != nil {
		return nil
	}

	out := []RenderedContentAttribution{}
	for _, role := range []MessageRole{RoleSystem, RoleUser} {
		var originals []Message
		for _, m := range req.Messages {
			if m.Role == role && (m.Text != "" || len(m.Attachments) > 0) {
				originals = append(originals, m)
			}
		}
		u := 0
		for i, item := range envelope.Input {
			if item.Role != string(role) {
				continue
			}
			if u >= len(originals) {
				break
			}
			m := originals[u]
			u++
			if m.Attribution == nil {
				continue
			}
			path := fmt.Sprintf("/input/%d/content", i)
			if len(item.Content) > 0 && item.Content[0] == '[' {
				path += "/0/text"
			}
			out = append(out, RenderedContentAttribution{Path: path, Category: m.Attribution.Category, Parts: append([]ContentAttributionPart(nil), m.Attribution.Parts...)})
		}
		if u != len(originals) {
			out = append(out, RenderedContentAttribution{Path: "/missing_canonical_attribution"})
		}
	}

	for i, t := range req.Tools {
		source, _ := t.Annotations["source"].(string)
		ns, _ := t.Annotations["namespace"].(string)
		out = append(out, RenderedContentAttribution{Path: fmt.Sprintf("/tools/%d", i), Category: "tool_definition", Key: toolAttributionKey(t), Source: source, Namespace: ns})
	}
	return out
}

func toolAttributionKey(t tools.ToolDefinition) string {
	if name, ok := t.Annotations["canonical_name"].(string); ok && name != "" {
		return name
	}
	return t.Name
}

type deepSeekPrepared struct {
	stateAllowed bool
	mu           sync.Mutex
	gateway      *deepSeekGateway
	body         []byte
	history      deepSeekHistory
	fingerprint  string
	estimate     TokenEstimate
	used, closed bool
}

func (p *deepSeekPrepared) Stream(ctx context.Context) (<-chan Event, error) {
	p.mu.Lock()
	if p.used || p.closed {
		p.mu.Unlock()
		return nil, errors.New("DeepSeek prepared request is closed or already streamed")
	}
	p.used = true
	body, history := p.body, p.history
	p.mu.Unlock()
	return p.gateway.streamRendered(ctx, body, history, p.stateAllowed)
}
func (p *deepSeekPrepared) TokenEstimate() TokenEstimate       { return cloneTokenEstimate(p.estimate) }
func (p *deepSeekPrepared) RenderedPayloadFingerprint() string { return p.fingerprint }
func (p *deepSeekPrepared) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func validDeepSeekToolName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func (p *deepSeekPrepared) PrepareRetry(ctx context.Context, _ Request) (PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &deepSeekPrepared{gateway: p.gateway, body: p.body, history: p.history, stateAllowed: p.stateAllowed, fingerprint: p.fingerprint, estimate: p.estimate}, nil
}

func (p *deepSeekPrepared) WithMaxOutputTokens(ctx context.Context, limit int64) (PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("output limit must be positive")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.used || p.closed {
		return nil, errors.New("DeepSeek prepared request is closed or already streamed")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(p.body, &envelope); err != nil {
		return nil, err
	}
	envelope["max_output_tokens"], _ = json.Marshal(limit)
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if err := CheckRequestBytes(body, p.gateway.maxRequestBytes); err != nil {
		return nil, err
	}
	return &deepSeekPrepared{gateway: p.gateway, body: body, history: p.history, stateAllowed: p.stateAllowed, fingerprint: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), estimate: p.estimate}, nil
}
