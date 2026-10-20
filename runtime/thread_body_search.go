package runtime

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/threadquery"
)

const (
	threadBodySearchBytes      = 64 << 10
	threadBodySearchQueryBytes = 4 << 10
	threadBodySearchMatches    = 100
)

// ThreadBodySearchInput searches exact, case-sensitive UTF-8 source text. Body
// identifies the field and first candidate byte offset. Its LimitBytes must be
// zero: search owns the bounded read size. Query is at most 4 KiB.
type ThreadBodySearchInput struct {
	Body  ThreadBodyInput `json:"body"`
	Query string          `json:"query"`
	Limit int             `json:"limit"`
}

// ThreadBodyMatch locates an exact match in canonical source bytes. Matches may
// overlap; offsets always denote complete UTF-8 boundaries.
type ThreadBodyMatch struct {
	OffsetBytes int `json:"offset_bytes"`
	LengthBytes int `json:"length_bytes"`
}

// ThreadBodySearchPage scans at most 64 KiB source per call. NextOffsetBytes is
// the first candidate not yet examined; resume with this offset and SHA256. A
// page without matches can still have more candidates. Locators grant no access.
type ThreadBodySearchPage struct {
	SHA256          string            `json:"sha256"`
	Bytes           int               `json:"bytes"`
	Matches         []ThreadBodyMatch `json:"matches"`
	NextOffsetBytes int               `json:"next_offset_bytes"`
	HasMore         bool              `json:"has_more"`
}

// ThreadBodySearcher is optional, like ThreadBodyReader. Hosts freshly authorize
// the scope and private-fact policy on every page; a prior result is not a grant.
type ThreadBodySearcher interface {
	SearchBody(context.Context, threadquery.Scope, ThreadBodySearchInput) (ThreadBodySearchPage, error)
}

var _ ThreadBodySearcher = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SearchBody(ctx context.Context, scope threadquery.Scope, input ThreadBodySearchInput) (ThreadBodySearchPage, error) {
	if input.Query == "" || len(input.Query) > threadBodySearchQueryBytes || !utf8.ValidString(input.Query) || input.Body.LimitBytes != 0 || input.Limit < 0 || input.Limit > threadBodySearchMatches {
		return ThreadBodySearchPage{}, threadquery.ErrInvalidQuery
	}
	limit := input.Limit
	if limit == 0 {
		limit = threadBodySearchMatches
	}
	body := input.Body
	start := body.OffsetBytes
	first, err := service.ReadBody(ctx, scope, body)
	if err != nil {
		return ThreadBodySearchPage{}, err
	}
	// Pin canonical identity for every chunk used by the shared search kernel.
	body.SHA256 = first.SHA256
	page, err := searchBodyText(ctx, start, first.Bytes, first.Text, input.Query, limit, func(offset, size int) (string, error) {
		body.OffsetBytes, body.LimitBytes = offset, size
		part, err := service.ReadBody(ctx, scope, body)
		return part.Text, err
	})
	if err != nil {
		return ThreadBodySearchPage{}, err
	}
	page.SHA256 = first.SHA256
	return page, nil
}

// One kernel owns overlap, UTF-8 boundaries and progress for immutable canonical
// fields and frozen live prefixes. It never reads or retains a complete body.
func searchBodyText(ctx context.Context, start, size int, first, query string, limit int, read func(int, int) (string, error)) (ThreadBodySearchPage, error) {
	page := ThreadBodySearchPage{Bytes: size, Matches: []ThreadBodyMatch{}, NextOffsetBytes: start}
	var source strings.Builder
	source.Grow(min(threadBodySearchBytes, size-start))
	source.WriteString(first)
	for start+source.Len() < size && source.Len() < threadBodySearchBytes-utf8.UTFMax {
		part, err := read(start+source.Len(), min(16<<10, threadBodySearchBytes-source.Len()))
		if err != nil {
			return ThreadBodySearchPage{}, err
		}
		if part == "" {
			return ThreadBodySearchPage{}, ErrAuthorityCorrupt
		}
		source.WriteString(part)
	}
	text := source.String()
	end := len(text)
	if start+end < page.Bytes {
		// Retain only the possible cross-page candidates. Every byte read in
		// overlap is covered by the same 64 KiB budget on the following call.
		end = max(0, end-len(query)+1)
		for end < len(text) && !utf8.RuneStart(text[end]) {
			end++
		}
	}
	position := 0
	for position < end {
		if err := ctx.Err(); err != nil {
			return ThreadBodySearchPage{}, err
		}
		found := strings.Index(text[position:], query)
		if found < 0 || position+found >= end {
			break
		}
		position += found
		page.Matches = append(page.Matches, ThreadBodyMatch{OffsetBytes: start + position, LengthBytes: len(query)})
		_, width := utf8.DecodeRuneInString(text[position:])
		position += width
		if len(page.Matches) == limit {
			end = position
			break
		}
	}
	page.NextOffsetBytes = start + end
	page.HasMore = page.NextOffsetBytes < page.Bytes
	return page, nil
}
