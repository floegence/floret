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
