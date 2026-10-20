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
	page := ThreadBodySearchPage{SHA256: first.SHA256, Bytes: first.Bytes, Matches: []ThreadBodyMatch{}, NextOffsetBytes: start}
	// The first read establishes the immutable source identity. Pin all later
	// chunks, including after a concurrent append, branch change or restart.
	body.SHA256 = first.SHA256
	var source strings.Builder
	source.Grow(min(threadBodySearchBytes, first.Bytes-start))
	source.WriteString(first.Text)
	for start+source.Len() < page.Bytes && source.Len() < threadBodySearchBytes-utf8.UTFMax {
		body.OffsetBytes = start + source.Len()
		body.LimitBytes = min(16<<10, threadBodySearchBytes-source.Len())
		part, err := service.ReadBody(ctx, scope, body)
		if err != nil {
			return ThreadBodySearchPage{}, err
		}
		if part.Text == "" {
			return ThreadBodySearchPage{}, ErrAuthorityCorrupt
		}
		source.WriteString(part.Text)
	}
	text := source.String()
	end := len(text)
	if start+end < page.Bytes {
		// Retain only the possible cross-page candidates. Every byte read in
		// overlap is covered by the same 64 KiB budget on the following call.
		end = max(0, end-len(input.Query)+1)
		for end < len(text) && !utf8.RuneStart(text[end]) {
			end++
		}
	}
	position := 0
	for position < end {
		if err := ctx.Err(); err != nil {
			return ThreadBodySearchPage{}, err
		}
		found := strings.Index(text[position:], input.Query)
		if found < 0 || position+found >= end {
			break
		}
		position += found
		page.Matches = append(page.Matches, ThreadBodyMatch{OffsetBytes: start + position, LengthBytes: len(input.Query)})
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
