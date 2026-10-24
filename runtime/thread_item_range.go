package runtime

import (
	"context"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadItemRangeFact identifies a canonical fact that may terminate a range.
// Hosts choose presentation policy; these facts never encode UI groups.
type ThreadItemRangeFact uint32

const (
	ThreadItemRangeUncertainEffect ThreadItemRangeFact = 1 << iota
	ThreadItemRangeErrorPayload
	ThreadItemRangeErrorWithoutFailure
	ThreadItemRangeUnknownActivity
	ThreadItemRangeDeclined
	ThreadItemRangeCanceled
	ThreadItemRangeApproval
	ThreadItemRangeInputRequired
)

// ThreadItemRangeInput selects the maximal contiguous canonical range containing
// AnchorID, with the same turn/run, allowed kinds, and no excluded facts. The
// anchor must satisfy the predicate. Read authorization must be freshly supplied.
type ThreadItemRangeInput struct {
	ThreadID identity.ThreadID   `json:"thread_id"`
	AnchorID string              `json:"anchor_id"`
	Kinds    []ThreadItemKind    `json:"kinds"`
	Exclude  ThreadItemRangeFact `json:"exclude,omitempty"`
}

type ThreadItemKindCounts struct {
	User        int `json:"user"`
	Assistant   int `json:"assistant"`
	Thinking    int `json:"thinking"`
	Tool        int `json:"tool"`
	Interaction int `json:"interaction"`
	Goal        int `json:"goal"`
	Steering    int `json:"steering"`
}

type ThreadItemActivityCounts struct {
	Pending  int `json:"pending"`
	Running  int `json:"running"`
	Waiting  int `json:"waiting"`
	Success  int `json:"success"`
	Error    int `json:"error"`
	Declined int `json:"declined"`
	Canceled int `json:"canceled"`
	Unknown  int `json:"unknown"`
}

// ThreadItemRange contains source-bound facts at one canonical LeafID. Counts
// include every member, not only loaded rows. It contains no bodies or member
// arrays. Live drafts and subsequent changes are outside this read snapshot;
// it must never be labelled with an independently sampled actor ViewVersion.
type ThreadItemRange struct {
	ThreadID     identity.ThreadID        `json:"thread_id"`
	TurnID       identity.TurnID          `json:"turn_id"`
	RunID        identity.RunID           `json:"run_id"`
	LeafID       string                   `json:"leaf_id"`
	FirstItemID  string                   `json:"first_item_id"`
	LastItemID   string                   `json:"last_item_id"`
	FirstOrdinal uint64                   `json:"first_ordinal"`
	LastOrdinal  uint64                   `json:"last_ordinal"`
	Counts       ThreadItemKindCounts     `json:"counts"`
	Activity     ThreadItemActivityCounts `json:"activity"`
	Facts        ThreadItemRangeFact      `json:"facts"`
	HasBefore    bool                     `json:"has_before"`
	HasAfter     bool                     `json:"has_after"`
}

// ThreadItemRangeReader reads canonical range metadata without materializing
// tool bodies, subscribing, or preparing execution. Prepared SQLite work is
// logarithmic in membership. Missing historical metadata requires canonical
// owner preparation; this capability alone does not promise first-open latency.
// This additive capability does not expand ThreadService or change View.
type ThreadItemRangeReader interface {
	ItemRange(context.Context, threadquery.Scope, ThreadItemRangeInput) (ThreadItemRange, error)
}

var _ ThreadItemRangeReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ItemRange(ctx context.Context, scope threadquery.Scope, input ThreadItemRangeInput) (ThreadItemRange, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadItemRange{}, err
	}
	reader, ok := service.host.store.repo.(sessiontree.ItemMembershipRangeReader)
	if !ok {
		return ThreadItemRange{}, ErrUnsupportedStoreCapability
	}
	kinds := make([]string, len(input.Kinds))
	for i, kind := range input.Kinds {
		kinds[i] = string(kind)
	}
	value, err := reader.ItemMembershipRange(ctx, sessiontree.ItemMembershipRangeOptions{ThreadID: input.ThreadID.String(), AnchorID: input.AnchorID, Kinds: kinds, Exclude: sessiontree.ItemRangeFacts(input.Exclude)})
	if err != nil {
		return ThreadItemRange{}, activityReadError(err)
	}
	c, a := value.Counts, value.Activity
	return ThreadItemRange{ThreadID: input.ThreadID, TurnID: identity.TurnID(value.First.TurnID), RunID: identity.RunID(value.First.RunID), LeafID: value.LeafID, FirstItemID: value.First.ID, LastItemID: value.Last.ID, FirstOrdinal: value.FirstOrdinal, LastOrdinal: value.LastOrdinal, Counts: ThreadItemKindCounts{User: c.User, Assistant: c.Assistant, Thinking: c.Thinking, Tool: c.Tool, Interaction: c.Interaction, Goal: c.Goal, Steering: c.Steering}, Activity: ThreadItemActivityCounts{Pending: a.Pending, Running: a.Running, Waiting: a.Waiting, Success: a.Success, Error: a.Error, Declined: a.Declined, Canceled: a.Canceled, Unknown: a.Unknown}, Facts: ThreadItemRangeFact(value.Facts), HasBefore: value.HasBefore, HasAfter: value.HasAfter}, nil
}
