package sessiontree

import (
	"sort"
	"strings"
	"unicode/utf8"
)

type queryReadSegment struct {
	end  int
	text string
}

// queryReadField borrows immutable source strings. Reading a small chunk never
// joins the complete field, including messages assembled from many entries.
type queryReadField struct {
	name     string
	bytes    int
	segments []queryReadSegment
}

func queryReadFields(parts []queryPart) []queryReadField {
	var fields []queryReadField
	byName := map[string]int{}
	for _, part := range parts {
		i, found := byName[part.field]
		if !found {
			i = len(fields)
			byName[part.field] = i
			fields = append(fields, queryReadField{name: part.field})
		}
		field := &fields[i]
		if field.bytes > 0 && part.field != "body" {
			field.append("\n")
		}
		field.append(part.text)
	}
	return fields
}

func (field *queryReadField) append(text string) {
	if len(text) > 0 {
		field.bytes += len(text)
		field.segments = append(field.segments, queryReadSegment{end: field.bytes, text: text})
	}
}

func (field queryReadField) chunk(offset, limit int) string {
	end := min(field.bytes, offset+limit)
	i := sort.Search(len(field.segments), func(i int) bool { return field.segments[i].end > offset })
	if i == len(field.segments) {
		return ""
	}
	segment := field.segments[i]
	start := offset - (segment.end - len(segment.text))
	var text string
	if end <= segment.end {
		text = segment.text[start : start+end-offset]
	} else {
		var out strings.Builder
		out.Grow(end - offset)
		for offset < end {
			segment = field.segments[i]
			n := min(end-offset, len(segment.text)-start)
			out.WriteString(segment.text[start : start+n])
			offset += n
			i++
			start = 0
		}
		text = out.String()
	}
	// Canonical sources are valid UTF-8. An incomplete suffix can contain at
	// most three bytes; trim only that suffix, never scan the complete body.
	if end < field.bytes && len(text) > 0 {
		last := len(text) - 1
		for last > 0 && !utf8.RuneStart(text[last]) {
			last--
		}
		if !utf8.FullRuneInString(text[last:]) {
			text = text[:last]
		}
	}
	return text
}
