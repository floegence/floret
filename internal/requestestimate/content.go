package requestestimate

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/deepseektokenizer"
	"github.com/floegence/floret/v7/internal/openaitokenizer"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

// Attribution is metadata over a decoded frozen payload, never model input.
type Attribution struct {
	Path, Category, Key, Source, Namespace string
	Parts                                  []Part
}
type Part struct {
	Category   string
	Start, End int
	Key        string
}

// Published DeepSeek V4 tool template; only trusted protocol markers are special.
//
//go:embed deepseek_tools.txt
var deepSeekToolTemplate string

func (c counter) trustedTemplate(text string) (int64, error) {
	n := int64(0)
	for _, token := range []string{"<｜begin▁of▁sentence｜>", "<｜end▁of▁sentence｜>", "<｜User｜>", "<｜Assistant｜>", "<think>", "</think>", "｜DSML｜"} {
		n += int64(strings.Count(text, token))
		text = strings.ReplaceAll(text, token, "\x00")
	}
	for _, part := range strings.Split(text, "\x00") {
		v, err := c.raw(part)
		if err != nil {
			return 0, err
		}
		n += v
	}
	return n, nil
}

func newCounter(provider, model string, media *int64) counter {
	c := counter{provider: strings.ToLower(strings.TrimSpace(provider)), model: strings.TrimSpace(model), margin: 25, media: media}
	if c.provider == "openrouter" && strings.HasPrefix(c.model, "deepseek/") {
		c.provider = "deepseek"
		c.model = strings.TrimPrefix(c.model, "deepseek/")
	}
	c.encoding = openaitokenizer.Encoding(c.provider, c.model)
	if c.provider == "deepseek" {
		switch c.model {
		case "deepseek-flash", "deepseek-v4-pro", "deepseek-v4-flash", "deepseek-v4-flash-vision-exp":
			c.encoding = "deepseek_v4"
		}
	}
	if c.encoding != "" {
		c.margin = 10
	}
	return c
}

// raw counts decoded text. Callers serialize structured schema values once.
func (c counter) raw(text string) (int64, error) {
	if text == "" {
		return 0, nil
	}
	switch c.encoding {
	case "deepseek_v4":
		return deepseektokenizer.Count(text)
	case "cl100k_base", "o200k_base":
		return openaitokenizer.Count(c.encoding, text)
	default:
		a, err := openaitokenizer.Count("cl100k_base", text)
		if err != nil {
			return 0, err
		}
		b, err := openaitokenizer.Count("o200k_base", text)
		return max(a, b), err
	}
}

func schemaText(value any) (string, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(value); err != nil {
		return "", errors.New("cannot encode model input value")
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

type collector struct {
	c       counter
	out     collectedContent
	attrs   map[string]Attribution
	used    map[string]bool
	invalid bool
}

type collectedContent struct {
	contextpolicy.ContextBreakdown
	toolFraming int64
	characters  *contextpolicy.ContextCharacterBreakdown
}

func (s *collector) limit(reason string) {
	s.out.AttributionCoverage = "partial"
	if s.out.characters != nil {
		found := false
		for _, l := range s.out.characters.Limitations {
			found = found || l == reason
		}
		if !found {
			s.out.characters.Limitations = append(s.out.characters.Limitations, reason)
		}
	}
	if s.out.Limitation == "" {
		s.out.Limitation = reason
	}
}

func (s *collector) detail(category, key, source, namespace string, n int64) {
	if s.out.Details == nil {
		s.out.Details = &contextpolicy.ContextBreakdownDetails{}
	}
	for i := range s.out.Details.Items {
		p := &s.out.Details.Items[i]
		if p.Category == category && p.Key == key && p.Source == source && p.Namespace == namespace {
			p.Tokens += n
			return
		}
	}
	s.out.Details.Items = append(s.out.Details.Items, contextpolicy.ContextTokenDetail{Category: category, Key: key, Source: source, Namespace: namespace, Tokens: n})
}
func (s *collector) add(category string, n int64) {
	switch category {
	case "system":
		s.out.SystemTokens += n
	case "assistant":
		s.out.AssistantTokens += n
	case "tool_call":
		s.out.ToolCallTokens += n
	case "tool_result":
		s.out.ToolResultTokens += n
	case "tool_definition":
		s.out.ToolDefinitionTokens += n
	case "model_context":
		s.out.ModelContextTokens += n
	case "compaction":
		s.out.CompactionTokens += n
	case "reasoning":
		s.out.ReasoningTokens += n
	case "framing":
		s.out.FramingTokens += n
	default:
		s.out.UserTokens += n
	}
}
func (s *collector) text(path, category, text string) error {
	a, ok := s.attrs[path]
	if ok {
		s.used[path] = true
		if a.Category != "model_context" && a.Category != "compaction" && a.Category != "system" && a.Category != "user" {
			s.invalid = true
			ok = false
		} else {
			category = a.Category
		}
	}
	if ok && len(a.Parts) > 0 {
		offset := 0
		valid := true
		for _, p := range a.Parts {
			if (p.Category != "" && p.Category != "system" && p.Category != "user" && p.Category != "model_context" && p.Category != "compaction") || p.Start != offset || p.End <= p.Start || p.End > len(text) || !utf8.ValidString(text[p.Start:p.End]) {
				valid = false
				break
			}
			offset = p.End
		}
		if offset != len(text) {
			valid = false
		}

		if valid {
			ends := make([]int, len(a.Parts))
			for i, p := range a.Parts {
				ends[i] = p.End
			}
			var counts []int64
			var err error
			if s.c.encoding == "deepseek_v4" {
				counts, err = deepseektokenizer.CountParts(text, ends)
			} else {
				encoding := s.c.encoding
				if encoding == "" {
					x, e := openaitokenizer.Count("cl100k_base", text)
					if e != nil {
						return e
					}
					y, e := openaitokenizer.Count("o200k_base", text)
					if e != nil {
						return e
					}
					encoding = "cl100k_base"
					if y > x {
						encoding = "o200k_base"
					}
				}
				counts, err = openaitokenizer.CountParts(encoding, text, ends)
			}
			if err != nil {
				return err
			}
			for i, p := range a.Parts {
				partCategory := category
				if p.Category != "" {
					partCategory = p.Category
				}
				s.add(partCategory, counts[i])
				s.characters(partCategory, text[p.Start:p.End], p.Key, "", "")
				if partCategory == "model_context" {
					s.detail(partCategory, p.Key, "", "", counts[i])
				}
			}
			return nil
		}

		// Invalid optional attribution must never lower the actual safety budget.
		s.invalid = true
	}

	n, err := s.c.raw(text)
	if err != nil {
		return err
	}
	s.add(category, n)
	characterText := text
	if category == "tool_call" && (strings.HasSuffix(path, "/arguments") || strings.HasSuffix(path, "/args_json")) {
		var value any
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if decoder.Decode(&value) == nil && json.Valid([]byte(text)) {
			if canonical, err := schemaText(value); err == nil {
				characterText = canonical
			}
		}
	}
	s.characters(category, characterText, "", "", "")
	if category == "model_context" {
		s.detail(category, "", "", "", n)
	}
	return nil
}
func (s *collector) structured(category string, value any) error {
	if value == nil {
		return nil
	}
	text, err := schemaText(value)
	if err != nil {
		return err
	}
	n, err := s.c.raw(text)
	if err != nil {
		return err
	}
	s.add(category, n)
	s.characters(category, text, "", "", "")
	return nil
}
func (s *collector) parts(path, category string, value any) error {
	if text, ok := value.(string); ok {
		return s.text(path, category, text)
	}
	list, ok := value.([]any)
	if !ok {
		if value != nil {
			return s.structured(category, value)
		}
		return nil
	}
	for i, v := range list {
		p, ok := v.(map[string]any)
		if !ok {
			return errors.New("invalid model content part")
		}
		base := fmt.Sprintf("%s/%d", path, i)
		kind, _ := p["type"].(string)
		switch kind {
		case "text", "input_text", "output_text", "reasoning_text", "summary_text":
			text, _ := p["text"].(string)
			if err := s.text(base+"/text", category, text); err != nil {
				return err
			}
		case "reasoning":
			text, _ := p["text"].(string)
			if err := s.text(base+"/text", "reasoning", text); err != nil {
				return err
			}
		case "tool_call":
			for _, key := range []string{"tool_name", "args_json"} {
				text, _ := p[key].(string)
				if err := s.text(base+"/"+key, "tool_call", text); err != nil {
					return err
				}
			}
		case "thinking":
			text, _ := p["thinking"].(string)
			if err := s.text(base+"/thinking", "reasoning", text); err != nil {
				return err
			}
		case "tool_use":
			name, _ := p["name"].(string)
			if err := s.text(base+"/name", "tool_call", name); err != nil {
				return err
			}
			if err := s.structured("tool_call", p["input"]); err != nil {
				return err
			}
		case "tool_result":
			if text, ok := p["text"].(string); ok {
				if err := s.text(base+"/text", "tool_result", text); err != nil {
					return err
				}
			}
			if err := s.parts(base+"/content", "tool_result", p["content"]); err != nil {
				return err
			}
		case "image_url", "input_image", "image", "input_file", "file", "document", "audio", "input_audio", "video":
			s.limit("media_input")
			for _, key := range []string{"filename", "title", "text", "caption", "description"} {
				if text, ok := p[key].(string); ok {
					if err := s.text(base+"/"+key, category, text); err != nil {
						return err
					}
				}
			}
		case "refusal":
			if text, ok := p["refusal"].(string); ok {
				if err := s.text(base+"/refusal", category, text); err != nil {
					return err
				}
			}
		case "redacted_thinking":
			s.limit("opaque_reasoning")
			if data, ok := p["data"].(string); ok {
				s.out.OpaqueBytes += int64(len(data))
			}
		default:
			// Unknown provider content has no established token interpretation.
			s.limit("unknown_content")
			raw, _ := json.Marshal(p)
			s.out.OpaqueBytes += int64(len(raw))
		}
	}
	return nil
}

func collectContent(c counter, format string, body map[string]any, attrs []Attribution) (collectedContent, error) {
	s := collector{c: c, out: collectedContent{ContextBreakdown: contextpolicy.ContextBreakdown{Source: "prepared_content_v4", Tokenizer: c.encoding, Quality: "token_estimate", AttributionCoverage: "complete"}}, attrs: map[string]Attribution{}, used: map[string]bool{}}
	s.out.characters = &contextpolicy.ContextCharacterBreakdown{Source: "prepared_characters_v1", Unit: "unicode_code_points"}
	if c.encoding == "" {
		s.out.Tokenizer = "proxy_bpe"
	}
	if format == "input_json" {
		s.limit("opaque_transport")
		s.out.characters = nil
	}
	for _, a := range attrs {
		if _, ok := s.attrs[a.Path]; ok {
			s.invalid = true
		}
		s.attrs[a.Path] = a
	}
	var values any
	key := "messages"
	if format == "openai_responses" {
		key = "input"
	}
	values = body[key]
	if format == "anthropic_messages" {
		if err := s.parts("/system", "system", body["system"]); err != nil {
			return s.out, err
		}
	}
	if format == "openai_responses" {
		if err := s.parts("/instructions", "system", body["instructions"]); err != nil {
			return s.out, err
		}
	}
	if text, ok := values.(string); ok {
		if err := s.text("/"+key, "user", text); err != nil {
			return s.out, err
		}
	} else if values != nil {
		list, ok := values.([]any)
		if !ok {
			return s.out, errors.New("request messages must be an array")
		}
		for i, v := range list {
			m, ok := v.(map[string]any)
			if !ok {
				return s.out, errors.New("request message must be an object")
			}
			path := fmt.Sprintf("/%s/%d", key, i)
			role, _ := m["role"].(string)
			category := role
			if role == "developer" {
				category = "system"
			}
			if role == "tool" {
				category = "tool_result"
			}
			kind, _ := m["type"].(string)
			switch kind {
			case "function_call":
				for _, k := range []string{"name", "arguments"} {
					if text, ok := m[k].(string); ok {
						if err := s.text(path+"/"+k, "tool_call", text); err != nil {
							return s.out, err
						}
					}
				}
			case "function_call_output":
				if err := s.parts(path+"/output", "tool_result", m["output"]); err != nil {
					return s.out, err
				}
			case "reasoning":
				if data, ok := m["encrypted_content"].(string); ok {
					s.out.OpaqueBytes += int64(len(data))
					s.limit("opaque_reasoning")
				}
				for _, k := range []string{"content", "summary"} {
					if err := s.parts(path+"/"+k, "reasoning", m[k]); err != nil {
						return s.out, err
					}
				}
			case "web_search_call":
				s.limit("hosted_context")
				if m["action"] != nil {
					text, err := schemaText(m["action"])
					if err != nil {
						return s.out, err
					}
					s.characters("hosted_context", text, "", "", "")
				}
			default:
				if err := s.parts(path+"/content", category, m["content"]); err != nil {
					return s.out, err
				}
				for _, k := range []string{"reasoning_content", "reasoning"} {
					if text, ok := m[k].(string); ok {
						if err := s.text(path+"/"+k, "reasoning", text); err != nil {
							return s.out, err
						}
					}
				}
				if calls, ok := m["tool_calls"].([]any); ok {
					for j, v := range calls {
						call, _ := v.(map[string]any)
						f, _ := call["function"].(map[string]any)
						for _, k := range []string{"name", "arguments"} {
							if text, ok := f[k].(string); ok {
								if err := s.text(fmt.Sprintf("%s/tool_calls/%d/function/%s", path, j, k), "tool_call", text); err != nil {
									return s.out, err
								}
							}
						}
					}
				}
			}
			if c.encoding != "deepseek_v4" {
				s.out.FramingTokens += 12
			} else {
				switch kind {
				case "function_call":
					s.out.FramingTokens += 30
				case "function_call_output":
					s.out.FramingTokens += 10
				case "reasoning":
					s.out.FramingTokens += 2
				default:
					if role != "system" {
						s.out.FramingTokens += 3
					}
				}
			}
		}
	}
	var tools func(string, []any) error
	tools = func(path string, list []any) error {
		for i, v := range list {
			p := fmt.Sprintf("%s/%d", path, i)
			tool, ok := v.(map[string]any)
			if !ok {
				return errors.New("invalid tool definition")
			}
			if nested, ok := tool["tools"].([]any); ok {
				if err := tools(p+"/tools", nested); err != nil {
					return err
				}
				continue
			}
			a, found := s.attrs[p]
			if found {
				s.used[p] = true
				if a.Category != "tool_definition" || a.Key == "" {
					s.invalid = true
				}
			}
			if kind, _ := tool["type"].(string); strings.HasPrefix(kind, "web_search") {
				s.limit("hosted_context")
			}
			def := tool
			if f, ok := tool["function"].(map[string]any); ok {
				def = f
			}
			clean := map[string]any{}
			for k, v := range def {
				if k != "cache_control" && k != "annotations" {
					clean[k] = v
				}
			}
			text, err := schemaText(clean)
			if err != nil {
				return err
			}
			n, err := c.raw(text)
			if err != nil {
				return err
			}
			s.add("tool_definition", n)
			name, _ := def["name"].(string)
			source, namespace := "", ""
			if found {
				name, source, namespace = a.Key, a.Source, a.Namespace
			}
			if name == "" {
				name, _ = def["type"].(string)
				source = "provider"
			}
			if name == "" {
				name = fmt.Sprintf("tool_%d", i)
			}
			s.detail("tool_definition", name, source, namespace, n)
			s.characters("tool_definition", text, name, source, namespace)
			if c.encoding != "deepseek_v4" {
				s.out.FramingTokens += 16
				s.out.toolFraming += 16
			}
		}
		return nil
	}
	if list, ok := body["tools"].([]any); ok {
		if err := tools("/tools", list); err != nil {
			return s.out, err
		}
		if len(list) > 0 {
			if c.encoding != "deepseek_v4" {
				s.out.FramingTokens += 32
				s.out.toolFraming += 32
			} else {
				n, err := c.trustedTemplate(deepSeekToolTemplate)
				if err != nil {
					return s.out, err
				}
				s.out.FramingTokens += n
				s.out.toolFraming += n
			}
		}
	}
	for _, k := range []string{"response_format", "text"} {
		value := body[k]
		if k == "text" {
			if config, ok := value.(map[string]any); ok {
				value = config["format"]
			}
		}
		if err := s.structured("system", value); err != nil {
			return s.out, err
		}
	}
	if c.encoding != "deepseek_v4" {
		s.out.FramingTokens += 16
	} else {
		s.out.FramingTokens += 3
	}
	if format == "input_json" && values == nil {
		if err := s.structured("user", body); err != nil {
			return s.out, err
		}
	}
	if s.out.Details != nil {
		sort.Slice(s.out.Details.Items, func(i, j int) bool {
			a, b := s.out.Details.Items[i], s.out.Details.Items[j]
			return strings.Join([]string{a.Category, a.Key, a.Source, a.Namespace}, "\x00") < strings.Join([]string{b.Category, b.Key, b.Source, b.Namespace}, "\x00")
		})
	}
	for path := range s.attrs {
		if !s.used[path] {
			s.invalid = true
		}
	}
	if s.out.characters != nil {
		sort.Slice(s.out.characters.Categories, func(i, j int) bool {
			return s.out.characters.Categories[i].Category < s.out.characters.Categories[j].Category
		})
		for i := range s.out.characters.Categories {
			d := s.out.characters.Categories[i].Details
			sort.Slice(d, func(i, j int) bool {
				a, b := d[i], d[j]
				return strings.Join([]string{a.Key, a.Source, a.Namespace}, "\x00") < strings.Join([]string{b.Key, b.Source, b.Namespace}, "\x00")
			})
		}
		sort.Strings(s.out.characters.Limitations)
	}
	if s.invalid {
		s.out.Diagnostic = "attribution_mismatch"
		return s.out, nil
	}
	return s.out, nil
}

// characters shares the token collector's decoded fragments and attribution.
// Protocol framing and opaque budgets never pass through this method.
func (s *collector) characters(category, text, key, source, namespace string) {
	b := s.out.characters
	if b == nil {
		return
	}
	switch category {
	case "system", "user", "assistant", "tool_definition", "tool_call", "tool_result", "model_context", "compaction", "reasoning", "hosted_context":
	default:
		category = "unlabelled"
	}
	n := int64(utf8.RuneCountInString(text))
	b.TotalCharacters += n
	index := -1
	for i := range b.Categories {
		if b.Categories[i].Category == category {
			index = i
			break
		}
	}
	if index < 0 {
		index = len(b.Categories)
		b.Categories = append(b.Categories, contextpolicy.ContextCharacterCategory{Category: category})
	}
	c := &b.Categories[index]
	c.Characters += n
	if category != "tool_definition" && category != "model_context" {
		return
	}
	for i := range c.Details {
		d := &c.Details[i]
		if d.Key == key && d.Source == source && d.Namespace == namespace {
			d.Characters += n
			return
		}
	}
	c.Details = append(c.Details, contextpolicy.ContextCharacterDetail{Key: key, Source: source, Namespace: namespace, Characters: n})
}
