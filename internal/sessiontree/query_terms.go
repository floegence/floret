package sessiontree

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// visitText supplies the exact public search field as borrowed source segments.
// The separator rule is shared with explicit full-field materialization.
func (record *queryRecord) visitText(field string, visit func(string)) {
	bytes := 0
	for _, part := range record.parts {
		if field != "text" && field != part.field {
			continue
		}
		if bytes > 0 && part.field != "body" {
			visit("\n")
			bytes++
		}
		visit(part.text)
		bytes += len(part.text)
	}
	if len(record.activities) > 0 && (field == "text" || field == "presentation") {
		raw, _ := json.Marshal(record.activities)
		if bytes > 0 {
			visit("\n")
		}
		visit(string(raw))
	}
}

// queryRuneStream carries at most one partial rune between canonical segments.
// This also handles source fragments whose boundary falls inside UTF-8.
type queryRuneStream struct {
	pending [utf8.UTFMax]byte
	count   int
	emit    func(rune)
}

func (stream *queryRuneStream) write(text string) {
	for len(text) > 0 {
		if stream.count > 0 {
			stream.pending[stream.count] = text[0]
			stream.count++
			text = text[1:]
			if utf8.FullRune(stream.pending[:stream.count]) {
				ch, size := utf8.DecodeRune(stream.pending[:stream.count])
				stream.emit(ch)
				stream.count = copy(stream.pending[:], stream.pending[size:stream.count])
			}
			continue
		}
		if text[0] < utf8.RuneSelf {
			stream.emit(rune(text[0]))
			text = text[1:]
			continue
		}
		if !utf8.FullRuneInString(text) {
			stream.count = copy(stream.pending[:], text)
			return
		}
		ch, size := utf8.DecodeRuneInString(text)
		stream.emit(ch)
		text = text[size:]
	}
}

func (stream *queryRuneStream) finish() {
	for stream.count > 0 {
		ch, size := utf8.DecodeRune(stream.pending[:stream.count])
		stream.emit(ch)
		stream.count = copy(stream.pending[:], stream.pending[size:stream.count])
	}
}

// queryTermCollector receives field parts and arbitrary byte fragments. Both
// complete projections and storage-backed rebuilds use these exact separator,
// Unicode and ngram rules without joining source fields.
type queryTermField struct {
	name   string
	bytes  int
	grams  map[[3]rune]struct{}
	stream queryRuneStream
}

type queryTermCollector struct {
	fields []*queryTermField
	active []*queryTermField
}

func newQueryTermCollector() *queryTermCollector {
	collector := &queryTermCollector{}
	for _, name := range []string{"text", "title", "arguments", "result"} {
		field := &queryTermField{name: name, grams: map[[3]rune]struct{}{}}
		previous := [2]rune{}
		count := 0
		field.stream.emit = func(ch rune) {
			ch = unicode.ToLower(ch)
			field.grams[[3]rune{ch, -1, -1}] = struct{}{}
			if count > 0 {
				field.grams[[3]rune{previous[1], ch, -1}] = struct{}{}
			}
			if count > 1 {
				field.grams[[3]rune{previous[0], previous[1], ch}] = struct{}{}
			}
			previous[0], previous[1] = previous[1], ch
			count = min(2, count+1)
		}
		collector.fields = append(collector.fields, field)
	}
	return collector
}

func (collector *queryTermCollector) beginPart(name string) {
	collector.active = collector.active[:0]
	for _, field := range collector.fields {
		if field.name != "text" && field.name != name {
			continue
		}
		if field.bytes > 0 && name != "body" {
			field.stream.write("\n")
			field.bytes++
		}
		collector.active = append(collector.active, field)
	}
}

func (collector *queryTermCollector) write(text string) {
	for _, field := range collector.active {
		field.stream.write(text)
		field.bytes += len(text)
	}
}

func (collector *queryTermCollector) finish() []string {
	result := []string{}
	for _, field := range collector.fields {
		field.stream.finish()
		for gram := range field.grams {
			size := 1
			if gram[1] >= 0 {
				size++
			}
			if gram[2] >= 0 {
				size++
			}
			result = append(result, queryAtomicTerm(field.name, string(gram[:size])))
		}
	}
	sort.Strings(result)
	return result
}

func queryDocumentTerms(record *queryRecord) []string {
	collector := newQueryTermCollector()
	for _, part := range record.parts {
		collector.beginPart(part.field)
		collector.write(part.text)
	}
	if len(record.activities) > 0 {
		collector.beginPart("presentation")
		raw, _ := json.Marshal(record.activities)
		collector.write(string(raw))
	}
	return collector.finish()
}

func (record *queryRecord) text(field string) string {
	var result strings.Builder
	record.visitText(field, func(text string) { result.WriteString(text) })
	return result.String()
}
