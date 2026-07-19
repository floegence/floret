package tools

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBuildOutputProjectionIsUTF8Safe(t *testing.T) {
	got := BuildOutputProjection(Result{Name: "demo", Text: "alpha 世界 omega"}, OutputPolicy{VisibleMaxBytes: 9, Strategy: OutputTail, PreserveFull: true})
	if !utf8.ValidString(got.VisibleText) {
		t.Fatalf("projected text is invalid utf8: %q", got.VisibleText)
	}
	if !strings.Contains(got.VisibleText, "omega") {
		t.Fatalf("tail projection = %q", got.VisibleText)
	}
}

func TestBuildOutputProjectionHeadTailByBytesAndLines(t *testing.T) {
	text := "one\ntwo\nthree\nfour"
	head := BuildOutputProjection(Result{Name: "demo", Text: text}, OutputPolicy{VisibleMaxLines: 2, Strategy: OutputHead, PreserveFull: true})
	if head.VisibleText != "one\ntwo" {
		t.Fatalf("head lines = %q", head.VisibleText)
	}
	tail := BuildOutputProjection(Result{Name: "demo", Text: text}, OutputPolicy{VisibleMaxLines: 2, Strategy: OutputTail, PreserveFull: true})
	if tail.VisibleText != "three\nfour" {
		t.Fatalf("tail lines = %q", tail.VisibleText)
	}
	bytes := BuildOutputProjection(Result{Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputHead, PreserveFull: true})
	if bytes.VisibleText != "0123" {
		t.Fatalf("head bytes = %q", bytes.VisibleText)
	}
}

func TestBuildOutputProjectionPlansFullOutputWithoutDurableRef(t *testing.T) {
	got := BuildOutputProjection(Result{CallID: "call-1", Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputTail})
	if got.VisibleText != "6789" || !got.Truncated || got.FullOutput != nil || got.FullOutputPlan == nil {
		t.Fatalf("projection = %#v", got)
	}
	if got.OriginalBytes != 10 || got.VisibleBytes != 4 || got.Strategy != OutputTail {
		t.Fatalf("projection metadata = %#v", got)
	}
	if got.FullOutputPlan.Text != "0123456789" || got.FullOutputPlan.Kind != DefaultArtifactKind || got.FullOutputPlan.MIME != DefaultArtifactMIME {
		t.Fatalf("full output plan = %#v", got.FullOutputPlan)
	}
}

func TestBuildOutputProjectionAllowsExplicitNoPreserve(t *testing.T) {
	got := BuildOutputProjection(Result{Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputTail, PreserveFullSet: true, PreserveFull: false})
	if got.VisibleText != "6789" || !got.Truncated || got.FullOutput != nil || got.FullOutputPlan != nil {
		t.Fatalf("projection = %#v", got)
	}
}
