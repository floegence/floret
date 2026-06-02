package provider

import "testing"

func TestUsageNormalizesTotalAndSource(t *testing.T) {
	got := Usage{InputTokens: 10, OutputTokens: 5}.Normalized()
	if got.TotalTokens != 15 {
		t.Fatalf("total = %d, want 15", got.TotalTokens)
	}
	if got.Source != UsageNative {
		t.Fatalf("source = %q, want native", got.Source)
	}
}

func TestUsageAddPreservesMixedSource(t *testing.T) {
	got := Usage{InputTokens: 10, Source: UsageNative}.Add(Usage{OutputTokens: 5, Source: UsageEstimated})
	if got.InputTokens != 10 || got.OutputTokens != 5 || got.TotalTokens != 15 {
		t.Fatalf("usage = %#v", got)
	}
	if got.Source != UsageMixed {
		t.Fatalf("source = %q, want mixed", got.Source)
	}
}

func TestNormalizeFinishReason(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		hasTools  bool
		truncated bool
		hasText   bool
		want      FinishReason
		inferred  bool
	}{
		{name: "openai stop", raw: "stop", hasText: true, want: FinishStop},
		{name: "anthropic end turn", raw: "end_turn", hasText: true, want: FinishStop},
		{name: "tool calls win", raw: "stop", hasTools: true, want: FinishToolCalls},
		{name: "length", raw: "max_tokens", truncated: true, hasText: true, want: FinishLength},
		{name: "content filter", raw: "content_filter", want: FinishContentFilter},
		{name: "unknown with text infers stop", raw: "weird", hasText: true, want: FinishStop, inferred: true},
		{name: "unknown empty stays unknown", raw: "weird", want: FinishUnknown, inferred: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, inferred := NormalizeFinishReason(tt.raw, tt.hasTools, tt.truncated, tt.hasText)
			if got != tt.want || inferred != tt.inferred {
				t.Fatalf("NormalizeFinishReason = %q/%v, want %q/%v", got, inferred, tt.want, tt.inferred)
			}
		})
	}
}
