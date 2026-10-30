package adapters

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider"
	"io"
	"strings"
)

const anthropicStateKind = "anthropic-messages-v1"
const anthropicStateLimit = 8 << 20

type anthropicHistoryRecord struct {
	Start       int                     `json:"start"`
	Prefix      string                  `json:"prefix"`
	Visible     string                  `json:"visible"`
	CacheMarked bool                    `json:"cache_marked,omitempty"`
	Output      []anthropicContentBlock `json:"output"`
}
type anthropicHistory struct {
	canonical   []anthropicMessage
	records     []anthropicHistoryRecord
	input       []anthropicMessage
	identity    string
	bridge      int
	cacheMarked bool
}

func anthropicHash(value any) string {
	raw, _ := json.Marshal(value)
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&normalized) == nil {
		raw, _ = json.Marshal(normalized)
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func anthropicBlocks(value any) ([]anthropicContentBlock, error) {
	if text, ok := value.(string); ok {
		if text == "" {
			return nil, nil
		}
		return []anthropicContentBlock{{Type: "text", Text: text}}, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var blocks []anthropicContentBlock
	err = json.Unmarshal(raw, &blocks)
	return blocks, err
}

// Fingerprints bind canonical text, reasoning, and tool identities; the exact
// native block order is retained only in opaque history.
func anthropicVisible(blocks []anthropicContentBlock) []anthropicContentBlock {
	var text, reasoning strings.Builder
	var calls []anthropicContentBlock
	for _, b := range blocks {
		switch b.Type {
		case "text":
			text.WriteString(b.Text)
		case "thinking", "floret_reasoning":
			reasoning.WriteString(b.Thinking)
		case "tool_use":
			calls = append(calls, anthropicContentBlock{Type: "tool_use", ID: b.ID, Name: b.Name, Input: b.Input})
		}
	}
	var out []anthropicContentBlock
	if text.Len() > 0 {
		out = append(out, anthropicContentBlock{Type: "text", Text: text.String()})
	}
	if reasoning.Len() > 0 {
		out = append(out, anthropicContentBlock{Type: "floret_reasoning", Thinking: reasoning.String()})
	}
	return append(out, calls...)
}
func canonicalAnthropicMessages(messages []anthropicMessage) ([]anthropicMessage, error) {
	var out []anthropicMessage
	for _, m := range messages {
		blocks, err := anthropicBlocks(m.Content)
		if err != nil {
			return nil, err
		}
		for i := range blocks {
			blocks[i].CacheControl = nil
		}
		if m.Role == "assistant" && len(out) > 0 && out[len(out)-1].Role == "assistant" {
			old, _ := anthropicBlocks(out[len(out)-1].Content)
			out[len(out)-1].Content = append(old, blocks...)
		} else {
			out = append(out, anthropicMessage{Role: m.Role, Content: blocks})
		}
	}
	for i := range out {
		if out[i].Role == "assistant" {
			b, _ := anthropicBlocks(out[i].Content)
			out[i].Content = anthropicVisible(b)
		}
	}
	return out, nil
}
func prepareAnthropicHistory(canonical []anthropicMessage, previous *provider.State, identity string) (anthropicHistory, error) {
	h := anthropicHistory{canonical: canonical, identity: identity, bridge: -1}
	records := []anthropicHistoryRecord{}
	if previous != nil {
		if previous.Kind != anthropicStateKind || previous.ID != identity || len(previous.Attributes) != 1 || previous.ReusePolicy != provider.StateReuseCanonicalHistory {
			return h, errors.New("incompatible Anthropic native history")
		}
		if err := provider.ValidateState(previous); err != nil {
			return h, err
		}
		raw, ok := previous.Attributes["native_output"]
		if !ok || len(raw) > anthropicStateLimit {
			return h, errors.New("invalid Anthropic native history size")
		}
		d := json.NewDecoder(strings.NewReader(raw))
		d.DisallowUnknownFields()
		if d.Decode(&records) != nil || records == nil || d.Decode(new(any)) != io.EOF {
			return h, errors.New("invalid Anthropic native history records")
		}
	}
	cursor, previousEnd := 0, 0
	pruned := false
	for _, record := range records {
		if record.Start < previousEnd || record.Start < 0 || len(record.Prefix) != 64 || len(record.Visible) != 64 || len(record.Output) == 0 {
			return h, errors.New("invalid Anthropic native history boundary")
		}
		previousEnd = record.Start + 1
		if previousEnd <= record.Start {
			return h, errors.New("invalid Anthropic history boundary")
		}
		if _, err := hex.DecodeString(record.Prefix); err != nil {
			return h, errors.New("invalid Anthropic prefix")
		}
		if _, err := hex.DecodeString(record.Visible); err != nil {
			return h, errors.New("invalid Anthropic output fingerprint")
		}
		if err := validateAnthropicNative(record.Output); err != nil {
			return h, err
		}
		if anthropicHash(anthropicMessage{Role: "assistant", Content: anthropicVisible(record.Output)}) != record.Visible {
			return h, errors.New("Anthropic native output fingerprint mismatch")
		}
		if pruned || record.Start >= len(canonical) || anthropicHash(canonical[:record.Start]) != record.Prefix {
			pruned = true
			continue
		}
		if canonical[record.Start].Role != "assistant" || anthropicHash(canonical[record.Start]) != record.Visible {
			return h, errors.New("Anthropic native output differs from canonical history")
		}
		h.input = append(h.input, canonical[cursor:record.Start]...)
		if record.CacheMarked {
			h.bridge = len(h.input) - 1
		}
		h.input = append(h.input, anthropicMessage{Role: "assistant", Content: record.Output})
		h.records = append(h.records, record)
		cursor = record.Start + 1
	}
	h.input = append(h.input, canonical[cursor:]...)
	// Unsigned historical reasoning remains canonical; it is never forged as
	// a signed block or recast as the assistant's visible answer.
	clean := make([]anthropicMessage, 0, len(h.input))
	bridge := -1
	for i, m := range h.input {
		blocks, _ := anthropicBlocks(m.Content)
		kept := make([]anthropicContentBlock, 0, len(blocks))
		for _, b := range blocks {
			if b.Type != "floret_reasoning" {
				kept = append(kept, b)
			}
		}
		if len(kept) > 0 {
			clean = append(clean, anthropicMessage{Role: m.Role, Content: kept})
		}
		if i == h.bridge {
			bridge = len(clean) - 1
		}
	}
	h.input = clean
	h.bridge = bridge
	return h, nil
}
func validateAnthropicNative(blocks []anthropicContentBlock) error {
	for _, b := range blocks {
		switch b.Type {
		case "thinking":
			if strings.TrimSpace(b.Signature) == "" {
				return errors.New("Anthropic thinking block has no signature")
			}
		case "redacted_thinking":
			if b.Data == "" {
				return errors.New("Anthropic redacted thinking has no data")
			}
		case "text":
		case "tool_use", "server_tool_use":
			if b.ID == "" || b.Name == "" || !json.Valid(b.Input) {
				return errors.New("invalid Anthropic native tool block")
			}
		case "web_search_tool_result":
			if b.ToolUseID == "" || !json.Valid(b.Content) {
				return errors.New("invalid Anthropic native search result")
			}
		default:
			return fmt.Errorf("unsupported Anthropic native block %q", b.Type)
		}
	}
	return nil
}
func (h anthropicHistory) commit(blocks []anthropicContentBlock) (*provider.State, error) {
	if err := validateAnthropicNative(blocks); err != nil {
		return nil, err
	}
	visible := anthropicVisible(blocks)
	if len(visible) == 0 {
		return nil, errors.New("Anthropic native output has no canonical boundary")
	}
	records := append([]anthropicHistoryRecord(nil), h.records...)
	if len(h.canonical) > 0 && h.canonical[len(h.canonical)-1].Role == "assistant" {
		start := len(h.canonical) - 1
		var prefixBlocks []anthropicContentBlock
		if len(records) > 0 && records[len(records)-1].Start == start {
			prefixBlocks = records[len(records)-1].Output
			records = records[:len(records)-1]
		} else {
			prefixBlocks, _ = anthropicBlocks(h.input[len(h.input)-1].Content)
		}
		blocks = append(append([]anthropicContentBlock(nil), prefixBlocks...), blocks...)
		visible = anthropicVisible(blocks)
		records = append(records, anthropicHistoryRecord{Start: start, Prefix: anthropicHash(h.canonical[:start]), Visible: anthropicHash(anthropicMessage{Role: "assistant", Content: visible}), Output: blocks})
	} else {
		records = append(records, anthropicHistoryRecord{Start: len(h.canonical), Prefix: anthropicHash(h.canonical), Visible: anthropicHash(anthropicMessage{Role: "assistant", Content: visible}), Output: blocks, CacheMarked: h.cacheMarked})
	}
	raw, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	if len(raw) > anthropicStateLimit {
		return nil, errors.New("Anthropic native history exceeds state limit")
	}
	return &provider.State{Kind: anthropicStateKind, ID: h.identity, ReusePolicy: provider.StateReuseCanonicalHistory, Attributes: map[string]string{"native_output": string(raw)}}, nil
}
func applyAnthropicCache(body *anthropicRequest, control *anthropicCacheControl, bridge int) {
	for i := range body.Tools {
		body.Tools[i].CacheControl = nil
	}
	var system []anthropicContentBlock
	if body.System != nil {
		system, _ = anthropicBlocks(body.System)
		for i := range system {
			system[i].CacheControl = nil
		}
		body.System = system
	}
	for i := range body.Messages {
		blocks, _ := anthropicBlocks(body.Messages[i].Content)
		for j := range blocks {
			blocks[j].CacheControl = nil
		}
		body.Messages[i].Content = blocks
	}
	if control == nil {
		return
	}
	if len(body.Tools) > 0 {
		for i := len(body.Tools) - 1; i >= 0; i-- {
			if body.Tools[i].Type == "" {
				body.Tools[i].CacheControl = control
				break
			}
		}
	}
	if len(system) > 0 {
		system[len(system)-1].CacheControl = control
		body.System = system
	}
	mark := func(end int) int {
		for i := end; i >= 0; i-- {
			blocks, _ := anthropicBlocks(body.Messages[i].Content)
			for j := len(blocks) - 1; j >= 0; j-- {
				switch blocks[j].Type {
				case "text", "image", "document", "tool_result", "tool_use":
					blocks[j].CacheControl = control
					body.Messages[i].Content = blocks
					return i
				}
			}
		}
		return -1
	}
	tail := mark(len(body.Messages) - 1)
	if bridge >= 0 && bridge < tail {
		mark(bridge)
	}
}

// Moving cache hints do not change model input. Input measurement excludes them.
func anthropicMeasuredBody(raw []byte) []byte {
	var body anthropicRequest
	if json.Unmarshal(raw, &body) != nil {
		return nil
	}
	applyAnthropicCache(&body, nil, -1)
	result, _ := json.Marshal(body)
	return result
}
