package runtime

import (
	"context"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadItemRangePageInput reads before or after an exclusive anchor at an exact leaf.
// Limit bounds the number of spans (1..32), not the number of covered members.
type ThreadItemRangePageInput struct {
	ThreadID  identity.ThreadID     `json:"thread_id"`
	LeafID    string                `json:"leaf_id"`
	AnchorID  string                `json:"anchor_id"`
	Direction ThreadWindowDirection `json:"direction"`
	Limit     int                   `json:"limit"`
	Predicate ThreadWindowRange     `json:"predicate"`
}

// ThreadItemSpan is either a matching contiguous range or one nonmatching member.
type ThreadItemSpan struct {
	ThreadItemRange
	Matched bool `json:"matched"`
}

// ThreadItemRangePageReader returns spans in ascending canonical order, with no
// bodies, execution or observation interest. Empty means no items in that direction.
type ThreadItemRangePageReader interface {
	ItemRangePage(context.Context, threadquery.Scope, ThreadItemRangePageInput) ([]ThreadItemSpan, error)
}

func (service *threadRuntimeService) ItemRangePage(ctx context.Context, scope threadquery.Scope, input ThreadItemRangePageInput) ([]ThreadItemSpan, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return nil, err
	}
	reader, ok := service.host.store.repo.(sessiontree.ItemRangePageReader)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	query := canonicalWindowRange(&input.Predicate)
	query.ThreadID, query.LeafID, query.AnchorID = input.ThreadID.String(), input.LeafID, input.AnchorID
	spans, err := reader.ItemRangePage(ctx, sessiontree.ItemRangePageOptions{ItemMembershipRangeOptions: *query, Direction: sessiontree.PathWindowDirection(input.Direction), Limit: input.Limit})
	if err != nil {
		return nil, activityReadError(err)
	}
	result := make([]ThreadItemSpan, len(spans))
	for index, span := range spans {
		result[index] = ThreadItemSpan{publicThreadItemRange(input.ThreadID, span.ItemMembershipRange), span.Matched}
	}
	return result, nil
}
