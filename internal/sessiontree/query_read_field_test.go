package sessiontree

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/threadquery"
)

func TestQueryReadFieldsPreserveExactBytes(t *testing.T) {
	parts := []queryPart{{"body", ""}, {"body", strings.Repeat("a中文🌸é\r\n", 5000)}, {"result", ""}, {"result", "one"}, {"body", "tail"}, {"result", ""}, {"result", "two"}}
	for _, field := range queryReadFields(parts) {
		want := (&queryRecord{parts: parts}).text(field.name)
		for _, size := range []int{4, 17, 16384} {
			hash := sha256.New()
			for offset := 0; offset < field.bytes; {
				text := field.chunk(offset, size)
				if len(text) == 0 || len(text) > size || !utf8.ValidString(text) {
					t.Fatalf("%s offset %d: invalid chunk %q", field.name, offset, text)
				}
				hash.Write([]byte(text))
				offset += len(text)
			}
			if fmt.Sprintf("%x", hash.Sum(nil)) != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) {
				t.Fatalf("%s/%d full read hash mismatch", field.name, size)
			}
		}
	}
}

func TestQueryReadFieldHandlesRuneSplitAcrossSources(t *testing.T) {
	text := strings.Repeat("中文🌸a", 100)
	var field queryReadField
	for i := range len(text) {
		field.append(text[i : i+1])
	}
	var got strings.Builder
	for offset := 0; offset < field.bytes; {
		chunk := field.chunk(offset, 17)
		if len(chunk) == 0 || !utf8.ValidString(chunk) {
			t.Fatalf("invalid split source at %d: %q", offset, chunk)
		}
		got.WriteString(chunk)
		offset += len(chunk)
	}
	if got.String() != text {
		t.Fatal("split sources changed bytes")
	}
}

func TestQueryTextPrefixMatchesCanonicalText(t *testing.T) {
	parts := []queryPart{{"body", strings.Repeat("中🌸", 100)}, {"reference_label", "label"}, {"body", "tail"}}
	record := &queryRecord{parts: parts}
	full := record.text("text")
	for limit := 1; limit <= len(full)+5; limit++ {
		end := min(limit, len(full))
		for end > 0 && end < len(full) && !utf8.RuneStart(full[end]) {
			end--
		}
		if got := record.textPrefix(limit); got != full[:end] {
			t.Fatalf("prefix %d differs: %q", limit, got)
		}
	}
}

func BenchmarkQueryReadLargeField(b *testing.B) {
	for _, size := range []int{40 << 10, 400 << 10, 4 << 20, 40 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			record := &queryRecord{key: "record", parts: []queryPart{{"body", strings.Repeat("a", size)}}}
			snapshot := &querySnapshot{recordRefs: map[string]threadquery.RecordRef{"record": "ref"}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				row := queryRow(snapshot, record, threadquery.Filter{})
				fields := queryReadFields(record.parts)
				chunk := fields[0].chunk(size/2, 16<<10)
				if len(row.Snippet) != 256 || len(chunk) != 16<<10 {
					b.Fatal("incorrect bounded read")
				}
			}
		})
	}
}
