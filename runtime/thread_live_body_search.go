package runtime

import (
	"context"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/threadquery"
	"unicode/utf8"
)

// ThreadLiveBodySearchInput searches an exact case-sensitive frozen live prefix.
// Body.LimitBytes must be zero; Query is valid UTF-8 and at most 4 KiB. Keep
// ThroughBytes unchanged when resuming; later appends never extend this search.
type ThreadLiveBodySearchInput struct {
	Body  ThreadLiveBodyInput `json:"body"`
	Query string              `json:"query"`
	Limit int                 `json:"limit"`
}

// ThreadLiveBodySearchPage scans at most 64 KiB source and returns overlapping
// matches at complete UTF-8 boundaries. Resume the same frozen source using
// NextOffsetBytes. Commit, retry, deletion or restart explicitly expires it.
type ThreadLiveBodySearchPage struct {
	ThreadID        identity.ThreadID `json:"thread_id"`
	RunID           identity.RunID    `json:"run_id"`
	SourceID        string            `json:"source_id"`
	ThroughBytes    int               `json:"through_bytes"`
	Matches         []ThreadBodyMatch `json:"matches"`
	NextOffsetBytes int               `json:"next_offset_bytes"`
	HasMore         bool              `json:"has_more"`
}

// ThreadLiveBodySearcher is optional. Every search and internal chunk read
// freshly authorizes private facts through the existing live-body owner.
type ThreadLiveBodySearcher interface {
	SearchLiveBody(context.Context, threadquery.Scope, ThreadLiveBodySearchInput) (ThreadLiveBodySearchPage, error)
}

var _ ThreadLiveBodySearcher = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SearchLiveBody(ctx context.Context, scope threadquery.Scope, input ThreadLiveBodySearchInput) (ThreadLiveBodySearchPage, error) {
	if input.Query == "" || len(input.Query) > threadBodySearchQueryBytes || !utf8.ValidString(input.Query) || input.Body.LimitBytes != 0 || input.Limit < 0 || input.Limit > threadBodySearchMatches {
		return ThreadLiveBodySearchPage{}, threadquery.ErrInvalidQuery
	}
	limit := input.Limit
	if limit == 0 {
		limit = threadBodySearchMatches
	}
	body := input.Body
	first, err := service.ReadLiveBody(ctx, scope, body)
	if err != nil {
		return ThreadLiveBodySearchPage{}, err
	}
	page, err := searchBodyText(ctx, body.OffsetBytes, body.ThroughBytes, first.Text, input.Query, limit, func(offset, size int) (string, error) {
		body.OffsetBytes, body.LimitBytes = offset, size
		part, err := service.ReadLiveBody(ctx, scope, body)
		return part.Text, err
	})
	if err != nil {
		return ThreadLiveBodySearchPage{}, err
	}
	return ThreadLiveBodySearchPage{ThreadID: body.ThreadID, RunID: body.RunID, SourceID: body.SourceID, ThroughBytes: body.ThroughBytes, Matches: page.Matches, NextOffsetBytes: page.NextOffsetBytes, HasMore: page.HasMore}, nil
}
