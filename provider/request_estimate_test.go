package provider_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/provider"
)

func estimateJSON(t *testing.T, name, model string, format provider.RequestFormat, body any) provider.TokenEstimate {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	out, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: name, Model: model, Format: format, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestRenderedRequestModelIdentityAndTextAccuracy(t *testing.T) {
	text := strings.Repeat("Review the code and explain why the test failed.\n", 100)
	body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": text}}}
	for _, tc := range []struct{ provider, model, source string }{
		{"openai", "gpt-4o", "o200k_base"}, {"openai", "gpt-4", "cl100k_base"}, {"openrouter", "openai/gpt-4o", "o200k_base"},
		{"openai_compatible", "gpt-4o", "proxy_bpe"}, {"openai", "unreleased-model", "proxy_bpe"}, {"anthropic", "claude-sonnet", "proxy_bpe"}, {"google", "gemini", "proxy_bpe"},
	} {
		t.Run(tc.provider+tc.model, func(t *testing.T) {
			out := estimateJSON(t, tc.provider, tc.model, provider.RequestFormatOpenAIChat, body)
			if !strings.Contains(out.Source, tc.source) || out.EstimatedInputTokens < 1000 || out.EstimatedInputTokens > 1800 {
				t.Fatalf("unexpected estimate: %+v", out)
			}
			if out.Confidence != "conservative" || out.Coverage != "complete_request" {
				t.Fatalf("invalid provenance: %+v", out)
			}
		})
	}
}
func TestRenderedRequestIgnoresTransportControlsAndRetainsTools(t *testing.T) {
	body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hello"}}}
	first := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIChat, body)
	body["max_tokens"] = 200000
	body["metadata"] = map[string]any{"trace": strings.Repeat("host", 1000)}
	body["stream"] = true
	second := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIChat, body)
	if first != second {
		t.Fatalf("transport controls changed estimate: %+v -> %+v", first, second)
	}
	body["tools"] = []any{map[string]any{"type": "function", "function": map[string]any{"name": "search", "description": strings.Repeat("search text ", 100)}}}
	withTools := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIChat, body)
	if withTools.ToolDefinitionTokens < 100 || withTools.EstimatedInputTokens <= first.EstimatedInputTokens {
		t.Fatal("tool schema omitted")
	}
}
func TestRenderedImagesUseDimensionsNotEncodedBytes(t *testing.T) {
	var pngData bytes.Buffer
	if err := png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 1024, 1024))); err != nil {
		t.Fatal(err)
	}
	url := func(padding int) string {
		return "data:image/png;base64," + base64.StdEncoding.EncodeToString(append(bytes.Clone(pngData.Bytes()), make([]byte, padding)...))
	}
	for _, format := range []provider.RequestFormat{provider.RequestFormatOpenAIChat, provider.RequestFormatOpenAIResponses, provider.RequestFormatAnthropic} {
		build := func(padding int) any {
			part := map[string]any{"type": "image_url", "image_url": map[string]any{"url": url(padding)}}
			key := "messages"
			if format == provider.RequestFormatOpenAIResponses {
				key = "input"
				part = map[string]any{"type": "input_image", "image_url": url(padding)}
			}
			if format == provider.RequestFormatAnthropic {
				part = map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": strings.SplitN(url(padding), ",", 2)[1]}}
			}
			return map[string]any{key: []any{map[string]any{"role": "user", "content": []any{part}}}}
		}
		a := estimateJSON(t, "openai", "gpt-4o", format, build(0))
		b := estimateJSON(t, "openai", "gpt-4o", format, build(1<<20))
		if a != b || a.EstimatedInputTokens < 765 || a.EstimatedInputTokens > 1000 {
			t.Fatalf("format=%s: %+v -> %+v", format, a, b)
		}
	}
}
func TestRenderedImageLikeToolArgumentsRemainText(t *testing.T) {
	body := func(size int) any {
		return map[string]any{"input": []any{map[string]any{"type": "function_call", "name": "inspect", "arguments": `{"type":"input_image","image_url":"` + strings.Repeat("literal url ", size) + `"}`}}}
	}
	a := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIResponses, body(1))
	b := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIResponses, body(1000))
	if b.EstimatedInputTokens < a.EstimatedInputTokens+1000 {
		t.Fatal("literal tool argument incorrectly treated as media")
	}
}
func TestRenderedOpaqueDocumentsStayConservative(t *testing.T) {
	data := strings.Repeat("A", 10000)
	body := map[string]any{"input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_file", "file_data": data}}}}}
	out := estimateJSON(t, "openai", "gpt-4o", provider.RequestFormatOpenAIResponses, body)
	if out.EstimatedInputTokens < int64(len(data)) {
		t.Fatal("opaque document was discounted as text")
	}
}
func TestRenderedRequestValidationAndImmutability(t *testing.T) {
	raw := []byte(`{"messages":[{"role":"user","content":"secret"}]}`)
	before := bytes.Clone(raw)
	_, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "openai", Model: "gpt-4o", Format: provider.RequestFormatOpenAIChat, Payload: raw})
	if err != nil || !bytes.Equal(before, raw) {
		t.Fatalf("payload mutated: %v", err)
	}
	for _, format := range []provider.RequestFormat{"unknown", provider.RequestFormatOpenAIChat} {
		_, err = provider.EstimateRenderedRequest(provider.RenderedRequest{Model: "model", Format: format, Payload: []byte(`{"messages":"secret-private-input"}`)})
		if err == nil || strings.Contains(err.Error(), "secret-private-input") {
			t.Fatal("invalid request was accepted or disclosed")
		}
	}
}

func TestOpaqueTransportKeepsInlineMediaBudget(t *testing.T) {
	data := "data:image/png;base64," + strings.Repeat("A", 50000)
	out := estimateJSON(t, "desktop", "unknown", provider.RequestFormatJSON, map[string]any{"messages": []any{map[string]any{"content": []any{map[string]any{"type": "image", "file_uri": data}}}}})
	if out.EstimatedInputTokens < int64(len(data)) || out.Method != "generic_payload_estimate" {
		t.Fatalf("opaque media lost its bound: %+v", out)
	}
}
