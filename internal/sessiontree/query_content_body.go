package sessiontree

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func directQueryField(rec *queryRecord, field string) bool {
	return (rec.row.Object == threadquery.Messages && field == "body") || (rec.row.Object == threadquery.ToolCalls && rec.row.ToolKind == "local" && (field == "arguments" || field == "result"))
}

// Visit immutable entries by metadata. Only read the blocks intersecting the
// requested page and the two boundary runes, never the enclosing transcript.
func readQueryBodyRange(ctx context.Context, tx spi.ReadTx, rec *queryRecord, field string, in threadquery.ContentRange) (threadquery.ContentPage, error) {
	var empty threadquery.ContentPage
	m := queryDocumentMetadata{Row: rec.row, FirstSource: rec.firstSource, PreviousSource: rec.previousSource, LastSource: rec.lastSource}
	var pos int64
	var text strings.Builder
	endedPage := false
	found := false
	err := visitCanonicalQuerySources(ctx, tx, m, func(e Entry, _ int) error {
		name := "content"
		switch field {
		case "arguments":
			if e.Type != EntryToolCall {
				return nil
			}
			name = "arguments"
		case "result":
			if e.Type != EntryToolResult || e.Message.ToolResult == nil {
				return nil
			}
		}
		found = true
		v, err := canonicalFieldValue(e, name)
		if err != nil {
			return err
		}
		size := int64(len(v))
		if ref, ok := e.bodyFields[name]; ok {
			size = int64(ref.Bytes)
		}
		start := pos
		pos += size
		read := func(off, limit int64) (string, error) {
			chunk, err := readCanonicalEntryBody(ctx, e, name, int(off), int(max(4, limit)), func(fn func(spi.ReadTx) error) error { return fn(tx) })
			return chunk.Text, err
		}
		for _, boundary := range []int64{in.StartByte, in.EndByte} {
			if start < boundary && boundary < pos {
				if _, err := read(boundary-start, 4); err != nil {
					return threadquery.ErrInvalidQuery
				}
			}
		}
		from, to := max(start, in.StartByte), min(pos, in.EndByte)
		if from >= to || endedPage {
			return nil
		}
		n := min(8000-int64(text.Len()), to-from)
		if n == 0 {
			endedPage = true
			return nil
		}
		s, err := read(from-start, n)
		if err != nil {
			return err
		}
		if int64(len(s)) > n {
			s = s[:n]
			for !utf8.ValidString(s) {
				s = s[:len(s)-1]
			}
		}
		text.WriteString(s)
		endedPage = int64(len(s)) < to-from || text.Len() == 8000
		return nil
	})
	if err != nil {
		return empty, err
	}
	if !found {
		return empty, threadquery.ErrRecordUnavailable
	}
	if in.EndByte > pos {
		return empty, threadquery.ErrInvalidQuery
	}
	page := threadquery.ContentPage{Text: text.String()}
	in.StartByte += int64(len(page.Text))
	if in.StartByte < in.EndByte {
		if page.Text == "" {
			return empty, ErrAuthorityCorrupt
		}
		page.NextRead = &in
	}
	return page, nil
}
