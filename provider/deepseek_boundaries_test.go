package provider

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDeepSeekBoundStringPrefixMatchesJSONAndHonorsCancellation(t *testing.T) {
	for _, value := range []string{"ASCII", "<>&\"\\\n\t\x00", "思考🌸\u2028\u2029", "invalid\xffutf8", strings.Repeat("long text ", 1000)} {
		for _, reasoning := range []bool{false, true} {
			item := deepSeekInput{Type: "message", Role: "assistant", Content: value}
			if reasoning {
				item = deepSeekInput{Type: "reasoning", Content: []map[string]string{{"type": "reasoning_text", "text": value}}}
			}
			signature := deepSeekSignature(item)
			got, ok, err := deepSeekBoundStringPrefix(t.Context(), value+"suffix", signature, reasoning, len(value))
			if err != nil || !ok || got != len(value) {
				t.Fatalf("value=%q reasoning=%t boundary=%d matched=%t err=%v", value, reasoning, got, ok, err)
			}
			if _, ok, err := deepSeekBoundStringPrefix(t.Context(), value+"suffix", signature, reasoning, len(value)-1); err != nil || ok {
				t.Fatalf("accepted boundary beyond limit: matched=%t err=%v", ok, err)
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := deepSeekBoundStringPrefix(ctx, "history", strings.Repeat("0", 64), true, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}
