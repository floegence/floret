package requestestimate

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/openaitokenizer"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func validationContentSum(b contextpolicy.ContextBreakdown) int64 {
	return b.SystemTokens + b.UserTokens + b.AssistantTokens + b.ToolCallTokens + b.ToolResultTokens + b.ToolDefinitionTokens + b.ModelContextTokens + b.CompactionTokens + b.ReasoningTokens + b.AttachmentTokens + b.FramingTokens
}

func TestValidationUnknownModelNeverClaimsOfficialVocabulary(t *testing.T) {
	if got := openaitokenizer.Encoding("openai", "gpt-6.1-sol"); got != "" {
		t.Fatalf("unverified model mapping: %s", got)
	}
	raw := []byte(`{"instructions":"Validation only.","input":[{"role":"user","content":"Hello 世界."}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{}}}]}`)
	estimate, err := Count("openai", "gpt-6.1-sol", "openai_responses", raw)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.Breakdown.Quality != "token_estimate" || estimate.Breakdown.Tokenizer != "proxy_bpe" {
		t.Fatalf("proxy weights claimed exact vocabulary: %+v", estimate.Breakdown)
	}
	if estimate.Prefix+estimate.Messages+estimate.Tools <= validationContentSum(estimate.Breakdown) {
		t.Fatal("budget safety margin no longer separate from raw attribution")
	}
}

func TestValidationEncryptedReasoningIsTransportWeightNotTokenCount(t *testing.T) {
	for _, model := range []string{"gpt-4o", "gpt-6.1-sol"} {
		t.Run(model, func(t *testing.T) {
			encrypted := strings.Repeat("A", 7096)
			raw, err := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "reasoning", "encrypted_content": encrypted, "summary": []any{}}, map[string]any{"role": "user", "content": "Continue."}}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := Count("openai", model, "openai_responses", raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.Breakdown.ReasoningTokens != 0 || got.Breakdown.OpaqueBytes != int64(len(encrypted)) || got.Breakdown.AttributionCoverage != "partial" {
				t.Fatalf("opaque transport contract changed: %+v", got.Breakdown)
			}
			textTokens, err := openaitokenizer.Count("o200k_base", encrypted)
			if err != nil {
				t.Fatal(err)
			}
			if textTokens == got.Breakdown.OpaqueBytes {
				t.Fatal("fixture failed to distinguish transport bytes from text tokens")
			}
			t.Logf("model=%s opaque_bytes=%d encoded_string_text_tokens=%d quality=%s; neither is a measurement of decrypted reasoning tokens", model, len(encrypted), textTokens, got.Breakdown.Quality)
		})
	}
}

func TestValidationHostedSearchActionDoesNotMeasureSearchContext(t *testing.T) {
	count := func(action string) Estimate {
		raw, err := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "web_search_call", "action": map[string]any{"type": "search", "query": action}}, map[string]any{"role": "user", "content": "Continue."}}, "tools": []any{map[string]any{"type": "web_search"}}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := Count("openai", "gpt-4o", "openai_responses", raw)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	short, long := count("public status"), count(strings.Repeat("public status ", 1000))
	if validationContentSum(short.Breakdown) != validationContentSum(long.Breakdown) || short.Breakdown.ToolResultTokens != 0 || short.Breakdown.AttributionCoverage != "partial" {
		t.Fatalf("hosted action metadata must not claim measured search content: short=%+v long=%+v", short.Breakdown, long.Breakdown)
	}
	t.Log("hosted search context is not present in the locally counted action metadata; attribution remains unqualified even with a known text vocabulary")
}

func TestToolDefinitionsStayStableAcrossOpaqueHistory(t *testing.T) {
	var previous int64
	for i, size := range []int{0, 7096, 32000} {
		raw, _ := json.Marshal(map[string]any{"instructions": "System.", "input": []any{map[string]any{"type": "reasoning", "encrypted_content": strings.Repeat("a", size)}, map[string]any{"role": "user", "content": "Hello."}}, "tools": []any{map[string]any{"type": "function", "name": "lookup", "parameters": map[string]any{"type": "object"}}}})
		got, err := Count("openai", "gpt-6.1-sol", "openai_responses", raw)
		if err != nil || (i > 0 && got.Breakdown.ToolDefinitionTokens != previous) || got.Breakdown.ReasoningTokens != 0 || got.Breakdown.OpaqueBytes != int64(size) {
			t.Fatalf("opaque changed tool tokens: %+v %v", got, err)
		}
		previous = got.Breakdown.ToolDefinitionTokens
	}
}
