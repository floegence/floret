package adapters

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/provider"
	"strings"
	"testing"
)

func historyMessages(t *testing.T, m ...anthropicMessage) []anthropicMessage {
	t.Helper()
	out, err := canonicalAnthropicMessages(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestAnthropicHistoryCanonicalRoundTripAndContinuation(t *testing.T) {
	input := historyMessages(t, anthropicMessage{Role: "user", Content: "question"})
	h, err := prepareAnthropicHistory(input, nil, "account")
	if err != nil {
		t.Fatal(err)
	}
	h.cacheMarked = true
	output := []anthropicContentBlock{{Type: "thinking", Thinking: "", Signature: "opaque"}, {Type: "redacted_thinking", Data: "encrypted"}, {Type: "text", Text: "part one"}}
	state, err := h.commit(output)
	if err != nil {
		t.Fatal(err)
	}
	next := historyMessages(t, input[0], anthropicMessage{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "part one"}}})
	h, err = prepareAnthropicHistory(next, state, "account")
	if err != nil {
		t.Fatal(err)
	}
	state, err = h.commit([]anthropicContentBlock{{Type: "text", Text: "part two"}})
	if err != nil {
		t.Fatal(err)
	}
	next = historyMessages(t, input[0], anthropicMessage{Role: "assistant", Content: "part one"}, anthropicMessage{Role: "assistant", Content: "part two"}, anthropicMessage{Role: "user", Content: "continue"})
	h, err = prepareAnthropicHistory(next, state, "account")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(h.input)
	if strings.Count(string(raw), "opaque") != 1 || !strings.Contains(string(raw), `"thinking":""`) || !strings.Contains(string(raw), "encrypted") || strings.Contains(string(raw), "floret_reasoning") {
		t.Fatalf("native replay=%s", raw)
	}
	if _, err = prepareAnthropicHistory(next, state, "another-account"); err == nil {
		t.Fatal("crossed account boundary")
	}
	var records []anthropicHistoryRecord
	_ = json.Unmarshal([]byte(state.Attributes["native_output"]), &records)
	records = append(records, anthropicHistoryRecord{Start: 100, Prefix: strings.Repeat("0", 64), Visible: strings.Repeat("0", 64), Output: []anthropicContentBlock{{Type: "thinking"}}})
	raw, _ = json.Marshal(records)
	state.Attributes["native_output"] = string(raw)
	if _, err = prepareAnthropicHistory(historyMessages(t, anthropicMessage{Role: "user", Content: "changed"}), state, "account"); err == nil {
		t.Fatal("corruption hidden behind pruned prefix")
	}
}
func TestAnthropicCacheBridgeAcrossLargeAppend(t *testing.T) {
	input := historyMessages(t, anthropicMessage{Role: "user", Content: "first"})
	h, _ := prepareAnthropicHistory(input, nil, "account")
	h.cacheMarked = true
	state, err := h.commit([]anthropicContentBlock{{Type: "text", Text: "answer"}})
	if err != nil {
		t.Fatal(err)
	}
	next := append(input, anthropicMessage{Role: "assistant", Content: "answer"})
	for range 30 {
		next = append(next, anthropicMessage{Role: "user", Content: "added"})
	}
	canonical := historyMessages(t, next...)
	h, err = prepareAnthropicHistory(canonical, state, "account")
	if err != nil {
		t.Fatal(err)
	}
	body := anthropicRequest{System: "system", Tools: []anthropicTool{{Name: "local"}}, Messages: h.input}
	applyAnthropicCache(&body, &anthropicCacheControl{Type: "ephemeral"}, h.bridge)
	raw, _ := json.Marshal(body)
	if strings.Count(string(raw), "cache_control") != 4 {
		t.Fatalf("markers=%s", raw)
	}
	first, _ := anthropicBlocks(body.Messages[0].Content)
	last, _ := anthropicBlocks(body.Messages[len(body.Messages)-1].Content)
	if first[0].CacheControl == nil || last[0].CacheControl == nil {
		t.Fatal("lost proven bridge or new tail")
	}
	applyAnthropicCache(&body, nil, -1)
	raw, _ = json.Marshal(body)
	if strings.Contains(string(raw), "cache_control") {
		t.Fatal("cache disabled left markers")
	}
	// Cache-off state does not invent a previously submitted breakpoint.
	h, _ = prepareAnthropicHistory(input, nil, "account")
	state, _ = h.commit([]anthropicContentBlock{{Type: "text", Text: "answer"}})
	h, err = prepareAnthropicHistory(canonical, state, "account")
	if err != nil || h.bridge != -1 {
		t.Fatalf("invented cache boundary: %+v %v", h, err)
	}
}
func TestAnthropicCanonicalToolArgumentsIgnoreSerializationOrder(t *testing.T) {
	input := historyMessages(t, anthropicMessage{Role: "user", Content: "tool"})
	h, _ := prepareAnthropicHistory(input, nil, "account")
	output := []anthropicContentBlock{{Type: "tool_use", ID: "call", Name: "read", Input: json.RawMessage(`{"z":1, "a":2}`)}}
	state, err := h.commit(output)
	if err != nil {
		t.Fatal(err)
	}
	output[0].Input = json.RawMessage(`{"a":2,"z":1}`)
	next := historyMessages(t, input[0], anthropicMessage{Role: "assistant", Content: output}, anthropicMessage{Role: "user", Content: []anthropicContentBlock{{Type: "tool_result", ToolUseID: "call", Content: json.RawMessage(`"done"`)}}})
	if _, err = prepareAnthropicHistory(next, state, "account"); err != nil {
		t.Fatal(err)
	}
}
func TestAnthropicStreamFailuresRetainUsageWithoutState(t *testing.T) {
	start := `data: {"type":"message_start","message":{"id":"msg","content":[],"usage":{"input_tokens":3,"output_tokens":0}}}` + "\n\n"
	for _, tail := range []string{"", `data: {"type":"error","error":{"type":"overloaded_error"}}` + "\n\n", `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"invalid"}}` + "\n\n"} {
		result, err := readAnthropicStream(strings.NewReader(start+tail), func(provider.StreamEvent) bool { return true })
		if err == nil || result.Usage.InputTokens != 3 || !result.Usage.inputReported {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	for _, raw := range []string{`{"input_tokens":-1}`, `{"input_tokens":9223372036854775807,"output_tokens":1}`} {
		var usage anthropicUsage
		if json.Unmarshal([]byte(raw), &usage) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	var usage anthropicUsage
	_ = json.Unmarshal([]byte(`{"input_tokens":12,"output_tokens":2,"cache_read_input_tokens":10}`), &usage)
	var delta anthropicUsage
	_ = json.Unmarshal([]byte(`{"output_tokens":0}`), &delta)
	mergeAnthropicUsage(&usage, delta)
	if usage.InputTokens != 12 || usage.OutputTokens != 0 || usage.CacheReadInputTokens != 10 {
		t.Fatalf("merge=%+v", usage)
	}
}
