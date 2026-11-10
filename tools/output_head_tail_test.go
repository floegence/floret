package tools

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestOutputHeadTailPreservesBothEndsAndFullSource(t *testing.T) {
	text := "BEGIN-unique\n" + strings.Repeat("middle 世界🙂\n", 3000) + "END-unique"
	got := BuildOutputProjection(Result{Text: text}, OutputPolicy{VisibleMaxBytes: 8000, Strategy: OutputStrategy("head_tail")})
	if !strings.HasPrefix(got.VisibleText, "BEGIN-unique") || !strings.HasSuffix(got.VisibleText, "END-unique") {
		t.Fatalf("preview must retain both ends, got %q ... %q", got.VisibleText[:min(32, len(got.VisibleText))], got.VisibleText[max(0, len(got.VisibleText)-32):])
	}
	if len(got.VisibleText) > 8000 || !utf8.ValidString(got.VisibleText) || !got.Truncated || got.FullOutputPlan == nil || got.FullOutputPlan.Text != text {
		t.Fatal("head-tail projection lost its budget, UTF-8 validity or complete source")
	}
	if !strings.Contains(got.VisibleText, "bytes omitted") {
		t.Fatal("missing explicit middle omission")
	}
}

func TestOutputHeadTailSmallBudgetsAndUnicode(t *testing.T) {
	for _, text := range []string{"", "a", "世界🙂", strings.Repeat("🙂中x", 1000), "a\nb\nc\nd\ne\nf\ng\nh\ni\nj"} {
		for limit := 1; limit <= 128; limit++ {
			t.Run(fmt.Sprintf("%d/%d", len(text), limit), func(t *testing.T) {
				got := BuildOutputProjection(Result{Text: text}, OutputPolicy{VisibleMaxBytes: limit, Strategy: OutputStrategy("head_tail")})
				if len(got.VisibleText) > limit || !utf8.ValidString(got.VisibleText) {
					t.Fatalf("invalid bounded preview: %q", got.VisibleText)
				}
				if len(text) <= limit && got.VisibleText != text {
					t.Fatal("small output must remain exact")
				}
				if got.OriginalBytes != len(text) || got.ContentSHA256 != stableTextHash(text) {
					t.Fatal("source facts changed")
				}
			})
		}
	}
}

func TestOutputHeadTailLineBudget(t *testing.T) {
	text := "first\nsecond\nthird\nfourth\nfifth\nsixth\nlast"
	got := BuildOutputProjection(Result{Text: text}, OutputPolicy{VisibleMaxBytes: 1024, VisibleMaxLines: 5, Strategy: OutputStrategy("head_tail")})
	if !strings.HasPrefix(got.VisibleText, "first") || !strings.HasSuffix(got.VisibleText, "last") || countLines(got.VisibleText) > 5 || !got.Truncated {
		t.Fatalf("invalid line projection: %q", got.VisibleText)
	}
}

func TestOutputHeadTailNoticeBudgets(t *testing.T) {
	for _, notice := range []string{"", " ", "omitted", "omitted\nread full output", "一\n二\n三"} {
		for lines := 1; lines <= 9; lines++ {
			for budget := 1; budget < 100; budget++ {
				got := BuildOutputProjection(Result{Text: strings.Repeat("line 世界\n", 100)}, OutputPolicy{VisibleMaxBytes: budget, VisibleMaxLines: lines, Strategy: OutputHeadTail, TruncationNotice: notice})
				if len(got.VisibleText) > budget || countLines(got.VisibleText) > lines || !utf8.ValidString(got.VisibleText) {
					t.Fatalf("budget=%d lines=%d notice=%q preview=%q", budget, lines, notice, got.VisibleText)
				}
			}
		}
	}
}

func TestOutputFullPlanPreservesPrePreviewFacts(t *testing.T) {
	source := strings.Repeat("original 世界\n", 1000)
	preview := `{"status":"success","preview":"first ... last"}`
	got := BuildOutputProjection(Result{Text: preview, FullOutput: &FullOutputPlan{Text: source}}, OutputPolicy{VisibleMaxBytes: 8000, Strategy: OutputHeadTail})
	if got.VisibleText != preview || got.FullOutputPlan.Text != source || got.OriginalBytes != len(source) || got.ContentSHA256 != stableTextHash(source) || !got.Truncated {
		t.Fatal("separate original lost")
	}
}

func TestOutputReferenceIsStrictAndNotThreadAuthority(t *testing.T) {
	ref := OutputReference{ThreadID: "thread", Kind: "artifact", ID: "artifact"}
	parsed, err := ParseOutputReference(ref.String())
	if err != nil || parsed != ref {
		t.Fatal(parsed, err)
	}
	for _, raw := range []string{"", ref.String() + "=", "output:v2:abc", "output:v1:e30"} {
		if _, err := ParseOutputReference(raw); err == nil {
			t.Fatal("invalid reference accepted")
		}
	}
}
