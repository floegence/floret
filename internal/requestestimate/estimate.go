// Package requestestimate owns provider-neutral request counting. It accepts
// rendered provider input and never resolves remote resources or stores text.
package requestestimate

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/deepseektokenizer"
	"github.com/floegence/floret/v7/internal/openaitokenizer"
)

type Estimate struct {
	Prefix, Messages, Tools int64
	Source, Method          string
}

type counter struct {
	provider, model, encoding string
	margin                    int64
	opaque                    bool
}

func Count(provider, model, format string, payload []byte) (Estimate, error) {
	if strings.TrimSpace(model) == "" || len(payload) == 0 {
		return Estimate{}, errors.New("request estimate requires a model and input")
	}
	var body map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil || body == nil || !json.Valid(payload) {
		return Estimate{}, errors.New("request estimate requires a JSON object")
	}
	c := counter{provider: strings.ToLower(strings.TrimSpace(provider)), model: strings.TrimSpace(model), margin: 25}
	c.encoding = openaitokenizer.Encoding(c.provider, c.model)
	if c.provider == "openrouter" && strings.HasPrefix(c.model, "deepseek/") {
		c.provider = "deepseek"
		c.model = strings.TrimPrefix(c.model, "deepseek/")
	}
	if c.provider == "deepseek" && (c.model == "deepseek-v4-pro" || c.model == "deepseek-v4-flash" || c.model == "deepseek-v4-flash-vision-exp") {
		c.encoding = "deepseek_v4"
	}
	if c.encoding != "" {
		c.margin = 10
	}
	source := c.encoding
	if source == "" {
		source = "proxy_bpe"
	}
	estimate := Estimate{Source: "rendered_" + source + "_media_v1", Method: "provider_rendered_payload_estimate"}
	var prefix, messages, tools any
	switch format {
	case "openai_chat":
		messages = body["messages"]
		tools = body["tools"]
		if err := sanitizeMessages(messages, &c); err != nil {
			return Estimate{}, err
		}
		var system []any
		if list, ok := messages.([]any); ok {
			var history []any
			for _, item := range list {
				message, _ := item.(map[string]any)
				if message["role"] == "system" || message["role"] == "developer" {
					system = append(system, item)
				} else {
					history = append(history, item)
				}
			}
			messages = history
		}
		if len(system) > 0 || body["response_format"] != nil {
			prefix = map[string]any{"messages": system, "response_format": body["response_format"]}
		}
	case "openai_responses":
		messages = body["input"]
		tools = body["tools"]
		if list, ok := messages.([]any); ok {
			for _, item := range list {
				if m, ok := item.(map[string]any); ok {
					switch m["type"] {
					case "function_call_output":
						if err := c.parts(m["output"]); err != nil {
							return Estimate{}, err
						}
					default:
						if err := c.parts(m["content"]); err != nil {
							return Estimate{}, err
						}
					}
				}
			}
		}
		prefix = map[string]any{"instructions": body["instructions"], "text": body["text"]}
	case "anthropic_messages":
		messages = body["messages"]
		tools = body["tools"]
		prefix = body["system"]
		if err := sanitizeMessages(messages, &c); err != nil {
			return Estimate{}, err
		}
	case "input_json":
		// Remote/custom transports expose complete model input but not the final
		// provider rendering. Do not claim official tokenization of opaque input.
		c.encoding = ""
		c.margin = 25
		c.opaque = true
		messages = body
		estimate.Source = "input_json_proxy_bpe_v1"
		estimate.Method = "generic_payload_estimate"
	default:
		return Estimate{}, errors.New("unsupported request estimate format")
	}
	stripCacheControl(prefix)
	stripCacheControl(tools)
	var err error
	if estimate.Prefix, err = c.value(prefix); err != nil {
		return Estimate{}, err
	}
	if estimate.Messages, err = c.value(messages); err != nil {
		return Estimate{}, err
	}
	if estimate.Tools, err = c.value(tools); err != nil {
		return Estimate{}, err
	}
	// Protocol envelopes and tool separators are not identical to their JSON form.
	// Keep explicit framing allowance in addition to the text margin.
	estimate.Messages += 16
	if list, ok := messages.([]any); ok {
		estimate.Messages += int64(len(list)) * 12
	}
	if list, ok := tools.([]any); ok && len(list) > 0 {
		estimate.Tools += 32 + int64(len(list))*16
	}
	return estimate, nil
}

func (c counter) value(value any) (int64, error) {
	if value == nil {
		return 0, nil
	}
	// Media budgets are carried by private placeholders and added independently
	// of text tokenization. They cannot be supplied by input JSON.
	media := int64(0)
	var collect func(any) any
	collect = func(v any) any {
		switch v := v.(type) {
		case string:
			if c.opaque && strings.HasPrefix(v, "data:") {
				// Opaque transports do not expose typed media positions. Keep
				// their transport-byte bound and allow for small encoded images.
				budget := max(int64(len(v)), 4096)
				if strings.HasPrefix(v, "data:image/") {
					if n, err := c.image(v, ""); err == nil {
						budget = max(budget, n)
					}
				}
				media += budget
				return ""
			}
		case mediaBudget:
			media += int64(v)
			return ""
		case map[string]any:
			for key, item := range v {
				v[key] = collect(item)
			}
		case []any:
			for i, item := range v {
				v[i] = collect(item)
			}
		}
		return v
	}
	value = collect(value)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return 0, errors.New("cannot encode request estimate input")
	}
	text := strings.TrimSuffix(buf.String(), "\n")
	var n int64
	var err error
	switch c.encoding {
	case "deepseek_v4":
		n, err = deepseektokenizer.Count(text)
	case "cl100k_base", "o200k_base":
		n, err = openaitokenizer.Count(c.encoding, text)
	default:
		a, e := openaitokenizer.Count("cl100k_base", text)
		if e != nil {
			return 0, e
		}
		b, e := openaitokenizer.Count("o200k_base", text)
		if e != nil {
			return 0, e
		}
		n = max(a, b)
	}
	if err != nil {
		return 0, err
	}
	return n + (n*c.margin+99)/100 + media, nil
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
		case "input_file", "file", "document":
			// Opaque documents may expand into text and page images. Local text BPE
			// must not silently discount their encoded bytes as if they were prose.
			raw, err := json.Marshal(part)
			if err != nil {
				return errors.New("invalid file input")
			}
			for key := range part {
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
