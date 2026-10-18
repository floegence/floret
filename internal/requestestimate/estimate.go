// Package requestestimate counts frozen provider input without retaining text.
package requestestimate

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"strings"
)

type Estimate struct {
	Characters              *contextpolicy.ContextCharacterBreakdown
	Prefix, Messages, Tools int64
	Media                   int64
	Source, Method          string
	Breakdown               contextpolicy.ContextBreakdown
}
type counter struct {
	provider, model, encoding string
	margin                    int64
	opaque                    bool
	media                     *int64
}

func Count(provider, model, format string, payload []byte) (Estimate, error) {
	return CountAttributed(provider, model, format, payload, nil)
}

func CountAttributed(provider, model, format string, payload []byte, attrs []Attribution) (Estimate, error) {
	if strings.TrimSpace(model) == "" || len(payload) == 0 {
		return Estimate{}, errors.New("request estimate requires a model and input")
	}
	var body map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil || body == nil || !json.Valid(payload) {
		return Estimate{}, errors.New("request estimate requires a JSON object")
	}
	media := int64(0)
	c := newCounter(provider, model, &media)
	switch format {
	case "openai_chat", "anthropic_messages":
		if err := sanitizeMessages(body["messages"], &c); err != nil {
			return Estimate{}, err
		}
	case "openai_responses":
		if list, ok := body["input"].([]any); ok {
			for _, item := range list {
				if m, ok := item.(map[string]any); ok {
					field := "content"
					if m["type"] == "function_call_output" {
						field = "output"
					}
					if err := c.parts(m[field]); err != nil {
						return Estimate{}, err
					}
				}
			}
		}
	case "input_json":
		c.encoding = ""
		c.margin = 25
		c.opaque = true
		if body["messages"] != nil {
			if err := sanitizeMessages(body["messages"], &c); err != nil {
				return Estimate{}, err
			}
		}
	default:
		return Estimate{}, errors.New("unsupported request estimate format")
	}
	stripCacheControl(body["system"])
	stripCacheControl(body["tools"])
	// Typed media placeholders never occur in caller JSON. Opaque transports keep
	// their byte bounds, while image-like text in schemas and arguments stays text.
	var mediaCost func(any) int64
	mediaCost = func(v any) int64 {
		switch v := v.(type) {
		case mediaBudget:
			return int64(v)
		case []any:
			var n int64
			for _, x := range v {
				n += mediaCost(x)
			}
			return n
		case map[string]any:
			var n int64
			for _, x := range v {
				n += mediaCost(x)
			}
			return n
		}
		return 0
	}
	media = mediaCost(body)
	collected, err := collectContent(c, format, body, attrs)
	if err != nil {
		return Estimate{}, err
	}
	b := collected.ContextBreakdown
	if media > 0 {
		b.AttributionCoverage = "partial"
		if b.Limitation == "" {
			b.Limitation = "media_input"
		}
	}
	prefix := b.SystemTokens
	definitions := b.ToolDefinitionTokens
	messages := b.UserTokens + b.AssistantTokens + b.ToolCallTokens + b.ToolResultTokens + b.ModelContextTokens + b.CompactionTokens + b.ReasoningTokens
	margin := func(n int64) int64 { return n + (n*c.margin+99)/100 }
	source := c.encoding
	if source == "" {
		source = "proxy_bpe"
	}
	method := "provider_rendered_payload_estimate"
	if c.opaque {
		method = "generic_payload_estimate"
	}
	estimate := Estimate{Characters: collected.characters, Prefix: margin(prefix), Tools: margin(definitions) + collected.toolFraming, Messages: margin(messages) + b.FramingTokens - collected.toolFraming + media + b.OpaqueBytes, Media: media, Source: "rendered_" + source + "_content_v4", Method: method, Breakdown: b}
	if b.Diagnostic != "" {
		estimate.Breakdown = contextpolicy.ContextBreakdown{Diagnostic: b.Diagnostic}
		estimate.Characters = nil
	}
	return estimate, nil
}

type mediaBudget int64

func sanitizeMessages(value any, c *counter) error {
	list, ok := value.([]any)
	if !ok {
		return errors.New("request messages must be an array")
	}
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return errors.New("request message must be an object")
		}
		if err := c.parts(m["content"]); err != nil {
			return err
		}
	}
	return nil
}

// Inspect only protocol content parts. Tool arguments, schemas and literal text
// may contain image-like keys and must never be mistaken for attachments.
func (c counter) parts(value any) error {
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	for _, item := range list {
		part, ok := item.(map[string]any)
		if !ok {
			continue
		}
		delete(part, "cache_control")
		kind, _ := part["type"].(string)
		switch kind {
		case "image_url", "input_image":
			if c.opaque {
				raw, _ := json.Marshal(part)
				for k := range part {
					delete(part, k)
				}
				part["type"] = kind
				part["opaque_file_budget"] = mediaBudget(max(4096, int64(len(raw))))
				continue
			}
			url, _ := part["image_url"].(string)
			detail, _ := part["detail"].(string)
			if image, ok := part["image_url"].(map[string]any); ok {
				url, _ = image["url"].(string)
				detail, _ = image["detail"].(string)
			}
			n, err := c.image(url, detail)
			if err != nil {
				return err
			}
			part["image_url"] = mediaBudget(n)
		case "image":
			if c.opaque {
				raw, _ := json.Marshal(part)
				for k := range part {
					delete(part, k)
				}
				part["type"] = kind
				part["opaque_file_budget"] = mediaBudget(max(4096, int64(len(raw))))
				continue
			}
			source, _ := part["source"].(map[string]any)
			url, _ := source["url"].(string)
			if data, ok := source["data"].(string); ok {
				mime, _ := source["media_type"].(string)
				url = "data:" + mime + ";base64," + data
			}
			n, err := c.image(url, "")
			if err != nil {
				return err
			}
			part["source"] = mediaBudget(n)
		case "tool_result":
			if err := c.parts(part["content"]); err != nil {
				return err
			}
		case "input_file", "file", "document", "audio", "input_audio", "video":
			// Opaque documents may expand into text and page images. Local text BPE
			// must not silently discount their encoded bytes as if they were prose.
			raw, err := json.Marshal(part)
			if err != nil {
				return errors.New("invalid file input")
			}
			for key := range part {
				switch key {
				case "filename", "title", "text", "caption", "description":
					continue
				}
				delete(part, key)
			}
			part["type"] = kind
			part["opaque_file_budget"] = mediaBudget(max(4096, int64(len(raw))))
		}
	}
	return nil
}

// Cache hints change billing/reuse, not model input. Do not recurse into tool
// schemas, where a property named cache_control is ordinary model-visible text.
func stripCacheControl(value any) {
	if list, ok := value.([]any); ok {
		for _, item := range list {
			if block, ok := item.(map[string]any); ok {
				delete(block, "cache_control")
			}
		}
	}
}
