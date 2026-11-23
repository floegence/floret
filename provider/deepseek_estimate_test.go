package provider

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestDeepSeekFrozenOutputRefitPreservesEncodedFields(t *testing.T) {
	body, err := json.Marshal(map[string]any{"model": "vision", "input": []any{map[string]any{"role": "user", "content": []any{map[string]string{"type": "input_image", "image_url": "data:image/png;base64," + strings.Repeat("AQID", 1<<16)}, map[string]string{"text": "<&>\u2028"}}}}, "temperature": json.Number("1.0200"), "max_output_tokens": 2})
	if err != nil {
		t.Fatal(err)
	}
	original := &deepSeekPrepared{gateway: &deepSeekGateway{maxRequestBytes: 1 << 20}, body: body}
	limited, err := original.WithMaxOutputTokens(t.Context(), 456)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Replace(body, []byte(`"max_output_tokens":2`), []byte(`"max_output_tokens":456`), 1)
	if bytes.Equal(want, body) || !bytes.Equal(limited.(*deepSeekPrepared).body, want) {
		t.Fatal("output refit changed frozen input")
	}
}

func TestDeepSeekTokenizerRemovesByteInflation(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("Read the code and explain the context budget.\n", 1000),
		strings.Repeat("请检查上下文管理与工具执行的实际用量。\n", 1000),
		strings.Repeat("func main() { fmt.Println(\"context\") }\n", 1000),
	} {
		raw, _ := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "message", "role": "user", "content": text}}})
		estimate, err := EstimateRenderedRequest(RenderedRequest{Provider: "deepseek", Model: "deepseek-flash", Format: RequestFormatOpenAIResponses, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if estimate.EstimatedInputTokens >= int64(len(raw)/2) || estimate.EstimatedInputTokens <= 0 {
			t.Fatalf("tokens=%d bytes=%d", estimate.EstimatedInputTokens, len(raw))
		}
		if estimate.Source != "rendered_deepseek_v4_content_v5" {
			t.Fatal("estimator upgrade must invalidate old calibration")
		}
		t.Logf("wire bytes=%d estimated tokens=%d", len(raw), estimate.EstimatedInputTokens)
	}
}
