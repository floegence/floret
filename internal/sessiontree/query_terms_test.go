package sessiontree

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func referenceQueryDocumentTerms(record *queryRecord) []string {
	terms := map[string]bool{}
	for _, field := range []string{"text", "title", "arguments", "result"} {
		runes := []rune(strings.ToLower(record.text(field)))
		for n := 1; n <= 3; n++ {
			for i := 0; i+n <= len(runes); i++ {
				terms[queryAtomicTerm(field, string(runes[i:i+n]))] = true
			}
		}
	}
	result := make([]string, 0, len(terms))
	for term := range terms {
		result = append(result, term)
	}
	sort.Strings(result)
	return result
}

func TestQueryDocumentTermsMatchJoinedUnicodeOracle(t *testing.T) {
	random := rand.New(rand.NewPCG(173, 817))
	pieces := []string{"", "body", "中文", "🌸", "\r\n", "İKΣ𐐀", "e\u0301", "\x00", "<>&"}
	fields := []string{"body", "reference_label", "reference_text", "title", "arguments", "result"}
	for iteration := range 100 {
		record := &queryRecord{}
		for range 1 + random.IntN(30) {
			record.parts = append(record.parts, queryPart{fields[random.IntN(len(fields))], pieces[random.IntN(len(pieces))]})
		}
		if iteration%2 == 0 {
			record.activities = []*tools.ActivityPresentation{{}}
		}
		if got, want := queryDocumentTerms(record), referenceQueryDocumentTerms(record); !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: got=%q want=%q", iteration, got, want)
		}
	}
	// A byte split must not introduce replacement characters or alter ngrams.
	source := "İ中文🌸e\u0301\r\nXYZ"
	record := &queryRecord{}
	for i := range len(source) {
		record.parts = append(record.parts, queryPart{"body", source[i : i+1]})
	}
	if got, want := queryDocumentTerms(record), referenceQueryDocumentTerms(record); !reflect.DeepEqual(got, want) {
		t.Fatalf("split UTF-8: got=%q want=%q", got, want)
	}
}

func BenchmarkQueryDocumentTermsLargeBody(b *testing.B) {
	for _, size := range []int{40 << 10, 400 << 10, 4 << 20, 40 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			text := strings.Repeat("Alpha 中文🌸\r\n", size/18+1)
			record := &queryRecord{}
			for offset := 0; offset < len(text); offset += 16 << 10 {
				record.parts = append(record.parts, queryPart{"body", text[offset:min(len(text), offset+(16<<10))]})
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(text)))
			b.ResetTimer()
			for range b.N {
				if len(queryDocumentTerms(record)) == 0 {
					b.Fatal("no terms")
				}
			}
		})
	}
}
