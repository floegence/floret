package runtime

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestThreadContentBufferMatchesExactJoinedRuneCount(t *testing.T) {
	random := rand.New(rand.NewPCG(23, 71))
	source := strings.Repeat("a中文🌸é\r\ne\u0301", 400)
	for range 30 {
		var buffer threadContentBuffer
		for offset := 0; offset < len(source); {
			end := min(len(source), offset+1+random.IntN(37))
			buffer.append(source[offset:end])
			if buffer.text.String() != source[:end] || buffer.runes != utf8.RuneCountInString(source[:end]) {
				t.Fatalf("wrong bytes or rune count at %d", end)
			}
			offset = end
		}
	}
}

func TestCanonicalSharedContentPreservesCitationOffsetsAndSources(t *testing.T) {
	texts := []string{"中文🌸", "\r\né", "last"}
	entries := make([]sessiontree.Entry, len(texts))
	want := ""
	for i, text := range texts {
		lo, hi := 0, utf8.RuneCountInString(text)
		entries[i] = sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, CreatedAt: time.Unix(1, 0), Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "source", Sources: []citation.Source{{Title: fmt.Sprint(i), URL: "https://example.com", StartIndex: &lo, EndIndex: &hi}}}}
		want += text
	}
	items, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil || len(items) != 1 || items[0].Text != want || len(items[0].Sources) != 3 || len(items[0].sourceEntryIDs) != 3 {
		t.Fatalf("wrong projection: %+v %v", items, err)
	}
	offset := 0
	for i, source := range items[0].Sources {
		if *source.StartIndex != offset || *source.EndIndex != offset+utf8.RuneCountInString(texts[i]) {
			t.Fatalf("wrong citation %d: %+v", i, source)
		}
		if *entries[i].Message.Sources[0].StartIndex != 0 {
			t.Fatal("projection mutated canonical citation")
		}
		offset += utf8.RuneCountInString(texts[i])
	}
}

func TestThreadContentFactsMatchEveryFragmentBoundary(t *testing.T) {
	text := " a中文🌸é\r\ne\u0301 "
	for first := 1; first < len(text); first++ {
		for second := first; second < len(text); second++ {
			var complete, metadata threadContentBuffer
			for _, part := range []string{text[:first], text[first:second], text[second:]} {
				complete.append(part)
				var fragment threadContentBuffer
				fragment.append(part)
				metadata.appendFacts(sessiontree.CanonicalTextFacts{Bytes: len(part), Runes: fragment.runes, NonSpace: fragment.nonSpace, Head: []byte(part[:min(3, len(part))]), Incomplete: []byte(fragment.incomplete)})
				if metadata.bytes != complete.bytes || metadata.runes != complete.runes || metadata.incomplete != complete.incomplete || metadata.nonSpace != complete.nonSpace {
					t.Fatalf("split %d/%d: metadata=%+v complete=%+v", first, second, metadata, complete)
				}
			}
		}
	}
}
