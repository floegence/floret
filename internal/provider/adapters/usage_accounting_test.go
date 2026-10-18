package adapters

import (
	"encoding/json"
	"github.com/floegence/floret/v7/internal/provider/catalog"
	"testing"
)

func TestOpenAIUsageTotalAndReasoningRelationship(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int64
		present bool
	}{
		{`{"prompt_tokens":100,"completion_tokens":20,"completion_tokens_details":{"reasoning_tokens":5}}`, 120, false},
		{`{"prompt_tokens":100,"completion_tokens":20,"completion_tokens_details":{"reasoning_tokens":5},"total_tokens":999}`, 999, true},
		{`{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`, 0, true},
	} {
		var payload usagePayload
		if err := json.Unmarshal([]byte(tc.raw), &payload); err != nil {
			t.Fatal(err)
		}
		got := normalizeUsage(payload, catalog.Model{})
		if got.TotalTokens != tc.want || !got.OutputIncludesReasoning || got.TotalTokensReported != tc.present || !got.Available {
			t.Fatalf("%s: %+v", tc.raw, got)
		}
	}
}
