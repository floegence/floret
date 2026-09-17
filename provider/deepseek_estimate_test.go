package provider

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDeepSeekTokenizerRemovesByteInflation(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("Read the code and explain the context budget.\n", 1000),
		strings.Repeat("请检查上下文管理与工具执行的实际用量。\n", 1000),
		strings.Repeat("func main() { fmt.Println(\"context\") }\n", 1000),
	} {
		raw, _ := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "message", "role": "user", "content": text}}})
		estimate, err := deepSeekRenderedEstimate(raw)
		if err != nil {
			t.Fatal(err)
		}
		if estimate.EstimatedInputTokens >= int64(len(raw)/2) || estimate.EstimatedInputTokens <= 0 {
			t.Fatalf("tokens=%d bytes=%d", estimate.EstimatedInputTokens, len(raw))
		}
		if estimate.Source != "deepseek_v4_tokenizer_image_tokens_v3" {
			t.Fatal("estimator upgrade must invalidate old calibration")
		}
		t.Logf("wire bytes=%d estimated tokens=%d", len(raw), estimate.EstimatedInputTokens)
	}
}
