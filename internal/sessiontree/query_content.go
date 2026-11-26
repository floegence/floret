package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/threadquery"
)

type queryFieldAddress struct{ Key, Field, Digest string }

func (a queryFieldAddress) source() string {
	b, _ := json.Marshal(a)
	return "field:v1:" + base64.RawURLEncoding.EncodeToString(b)
}

func fieldAddress(record *queryRecord, field queryReadField) queryFieldAddress {
	if directQueryField(record, field.name) {
		return queryFieldAddress{record.key, field.name, record.firstSource + ":" + record.lastSource}
	}
	h := sha256.New()
	for _, s := range field.segments {
		h.Write([]byte(s.text))
	}
	return queryFieldAddress{record.key, field.name, hex.EncodeToString(h.Sum(nil))}
}

func describeQueryContent(record *queryRecord, parts []queryPart) *threadquery.ContentFields {
	var out threadquery.ContentFields
	for _, f := range queryReadFields(parts) {
		if f.bytes == 0 {
			continue
		}
		out = append(out, threadquery.ContentField{Field: f.name, Read: threadquery.ContentRange{Source: fieldAddress(record, f).source(), EndByte: int64(f.bytes)}})
	}
	if len(out) == 0 {
		return nil
	}
	return &out
}

func (f queryReadField) boundary(n int64) bool {
	if n == int64(f.bytes) {
		return true
	}
	i := sort.Search(len(f.segments), func(i int) bool { return int64(f.segments[i].end) > n })
	s := f.segments[i]
	return utf8.RuneStart(s.text[int(n)-s.end+len(s.text)])
}

func (q *querySession) readContent(ctx context.Context, scope threadquery.Scope, in threadquery.ContentRange) (threadquery.ContentPage, error) {
	var empty threadquery.ContentPage
	ids, err := ValidateQueryScope(scope)
	if err != nil {
		return empty, err
	}
	raw, ok := strings.CutPrefix(in.Source, "field:v1:")
	if !ok || len(raw) > 16384 || in.StartByte < 0 || in.EndByte < in.StartByte || in.EndByte > 1<<53-1 {
		return empty, threadquery.ErrInvalidQuery
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	var a queryFieldAddress
	if err != nil || json.Unmarshal(b, &a) != nil || a.source() != in.Source {
		return empty, threadquery.ErrInvalidQuery
	}
	thread, _, ok := strings.Cut(a.Key, "\x00")
	if !ok || !ids[thread] {
		return empty, threadquery.ErrRecordUnavailable
	}
	if q.lookupContent == nil {
		return empty, ErrUnsupportedStoreCapability
	}
	rec, err := q.lookupContent(thread, a.Key)
	if err != nil {
		return empty, err
	}
	if rec == nil || rec.row.ThreadID.String() != thread {
		return empty, threadquery.ErrRecordUnavailable
	}
	if q.readDirect != nil && directQueryField(rec, a.Field) {
		if fieldAddress(rec, queryReadField{name: a.Field}) != a {
			return empty, threadquery.ErrRecordUnavailable
		}
		return q.readDirect(rec, a.Field, in)
	}
	rec, err = q.queryRecordContent(rec)
	if err != nil {
		return empty, err
	}
	parts, err := q.readParts(rec)
	if err != nil {
		return empty, err
	}
	for _, f := range queryReadFields(parts) {
		if f.name != a.Field {
			continue
		}
		if fieldAddress(rec, f) != a {
			return empty, threadquery.ErrRecordUnavailable
		}
		if in.EndByte > int64(f.bytes) || !f.boundary(in.StartByte) || !f.boundary(in.EndByte) {
			return empty, threadquery.ErrInvalidQuery
		}
		if in.StartByte == in.EndByte {
			return empty, nil
		}
		page := threadquery.ContentPage{Text: strings.Clone(f.chunk(int(in.StartByte), int(min(8000, in.EndByte-in.StartByte))))}
		in.StartByte += int64(len(page.Text))
		if in.StartByte < in.EndByte {
			page.NextRead = &in
		}
		return page, nil
	}
	return empty, threadquery.ErrRecordUnavailable
}

func (repo *BackendRepo) ReadQueryContent(ctx context.Context, scope threadquery.Scope, in threadquery.ContentRange) (result threadquery.ContentPage, err error) {
	err = repo.queryView(ctx, scope, func(q *querySession) error { result, err = q.readContent(ctx, scope, in); return err })
	return
}
