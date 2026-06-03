package tools

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestResultLimitIsUTF8Safe(t *testing.T) {
	got := ApplyResultLimit(Result{Text: "alpha 世界 omega"}, ResultLimit{MaxBytes: 9, Strategy: "tail"})
	if !utf8.ValidString(got.Text) {
		t.Fatalf("truncated text is invalid utf8: %q", got.Text)
	}
	if !strings.Contains(got.Text, "omega") {
		t.Fatalf("tail truncation = %q", got.Text)
	}
}

func TestResultLimitHeadTailByBytesAndLines(t *testing.T) {
	text := "one\ntwo\nthree\nfour"
	head := ApplyResultLimit(Result{Text: text}, ResultLimit{MaxLines: 2, Strategy: "head"})
	if head.Text != "one\ntwo" {
		t.Fatalf("head lines = %q", head.Text)
	}
	tail := ApplyResultLimit(Result{Text: text}, ResultLimit{MaxLines: 2, Strategy: "tail"})
	if tail.Text != "three\nfour" {
		t.Fatalf("tail lines = %q", tail.Text)
	}
	bytes := ApplyResultLimit(Result{Text: "0123456789"}, ResultLimit{MaxBytes: 4, Strategy: "head"})
	if bytes.Text != "0123" {
		t.Fatalf("head bytes = %q", bytes.Text)
	}
}

func TestResultLimitAddsTruncationMetadata(t *testing.T) {
	got := ApplyResultLimit(Result{Text: "0123456789", Metadata: map[string]any{"kind": "demo"}}, ResultLimit{MaxBytes: 4})
	if got.Text != "6789" {
		t.Fatalf("default tail bytes = %q", got.Text)
	}
	if got.Metadata["kind"] != "demo" || got.Metadata["truncated"] != true || got.Metadata["original_bytes"] != 10 || got.Metadata["returned_bytes"] != 4 || got.Metadata["truncation_strategy"] != "tail" {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
}
