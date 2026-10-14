package citation

import (
	"testing"
	"unicode/utf8"
)

func index(v int) *int { return &v }
func TestSourcesNormalizeAndFragmentUnicode(t *testing.T) {
	text := "你好😀first第二"
	refs := []Source{
		{URL: "https://example.com/a", StartIndex: index(0), EndIndex: index(3)},
		{URL: "https://example.com/a", StartIndex: index(3), EndIndex: index(8)},
		{URL: "https://example.com/b", StartIndex: index(2), EndIndex: index(9)},
		{URL: "https://example.com/c", StartIndex: index(90), EndIndex: index(91)},
		{URL: "https://example.com/d"},
	}
	boundary := len("你好😀")
	first := Fragment(refs, text, 0, boundary, false, []int{boundary})
	last := Fragment(refs, text, boundary, len(text), true, []int{boundary})
	if len(first) != 1 || *first[0].StartIndex != 0 || *first[0].EndIndex != 3 {
		t.Fatalf("first=%+v", first)
	}
	if len(last) != 4 || *last[0].StartIndex != 0 || *last[0].EndIndex != 5 {
		t.Fatalf("last=%+v", last)
	}
	for _, s := range last[1:] {
		if s.StartIndex != nil || s.EndIndex != nil {
			t.Fatal("invalid binding retained")
		}
	}
	*first[0].EndIndex = 1
	if *refs[0].EndIndex != 3 {
		t.Fatal("fragment aliases source")
	}
	if utf8.RuneCountInString(text) != 10 {
		t.Fatal("fixture")
	}
	for _, u := range []string{"javascript:alert(1)", "data:text/plain,a", "file:///a", "https://user:secret@example.com", "https://example.com/\nx", "https:///a"} {
		if len(Normalize([]Source{{URL: u}})) != 0 {
			t.Fatalf("unsafe URL: %q", u)
		}
	}
	duplicate := append(Clone(refs[:1]), refs[0])
	if len(Normalize(duplicate)) != 1 {
		t.Fatal("duplicate annotation retained")
	}
}
