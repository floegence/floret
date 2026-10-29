package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadItemPageInput reads canonical items without installing an observation
// interest. AncestorLeafID validates path continuity while permitting append.
// Mutable facts are sampled at the returned leaf, not frozen at the ancestor.
// AnchorID is exclusive for before/after. Limit defaults to 40 (maximum 150).
type ThreadItemPageInput struct {
	ThreadID       identity.ThreadID     `json:"thread_id"`
	AncestorLeafID string                `json:"ancestor_leaf_id,omitempty"`
	AnchorID       string                `json:"anchor_id,omitempty"`
	Direction      ThreadWindowDirection `json:"direction,omitempty"`
	Limit          int                   `json:"limit,omitempty"`
	PreviewBytes   int                   `json:"preview_bytes,omitempty"`
}

// ThreadItemPage contains requested canonical items and their turn facts. It is
// not a live baseline. Pages have at most 150 items and 512 KiB of encoded JSON.
// Activity details remain source-backed summaries.
type ThreadItemPage struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	LeafID      string            `json:"leaf_id"`
	FirstItemID string            `json:"first_item_id,omitempty"`
	LastItemID  string            `json:"last_item_id,omitempty"`
	HasBefore   bool              `json:"has_before"`
	HasAfter    bool              `json:"has_after"`
	Items       []ThreadItem      `json:"items"`
	Turns       []ThreadTurn      `json:"turns,omitempty"`
}

// ThreadItemPageReader performs authorized demand-only canonical page reads.
// No execution preparation, subscriber or persistent cursor is installed.
// Prepared persistent stores read indexed membership and selected sources.
type ThreadItemPageReader interface {
	ItemPage(context.Context, threadquery.Scope, ThreadItemPageInput) (ThreadItemPage, error)
}

var _ ThreadItemPageReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ItemPage(ctx context.Context, scope threadquery.Scope, input ThreadItemPageInput) (ThreadItemPage, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadItemPage{}, err
	}
	if input.Limit == 0 {
		input.Limit = 40
	}
	if input.Direction == "" {
		input.Direction = ThreadWindowBefore
	}
	if input.PreviewBytes == 0 {
		input.PreviewBytes = 1024
	}
	if input.Limit < 1 || input.Limit > 150 || input.PreviewBytes < 128 || input.PreviewBytes > 65536 || (input.Direction != ThreadWindowBefore && input.Direction != ThreadWindowAfter && input.Direction != ThreadWindowAround) || input.Direction == ThreadWindowAround && input.AnchorID == "" {
		return ThreadItemPage{}, threadquery.ErrInvalidQuery
	}
	reader, ok := service.host.store.repo.(sessiontree.WindowProjectionReader)
	if !ok {
		return ThreadItemPage{}, ErrUnsupportedStoreCapability
	}
	window, err := reader.ReadWindowProjection(ctx, sessiontree.ItemMembershipWindowOptions{ThreadID: input.ThreadID.String(), AncestorLeafID: input.AncestorLeafID, ItemsOnly: true, AnchorID: input.AnchorID, Direction: sessiontree.PathWindowDirection(input.Direction), Limit: input.Limit, ActivityFields: true}, func(source sessiontree.PathMetadata, control bool) []string {
		return coldProjectionFields(source, false, control)
	})
	if err != nil {
		return ThreadItemPage{}, activityReadError(err)
	}
	in := ThreadWindowInput{ThreadID: input.ThreadID, AnchorID: input.AnchorID, Direction: input.Direction, Limit: input.Limit, PreviewBytes: input.PreviewBytes, ActivityFields: true}
	view, page, err := service.projectSourceWindow(ctx, in, window, true)
	if err != nil {
		return ThreadItemPage{}, err
	}
	// Source hydration helpers only; this value is never registered as an actor.
	source := threadRuntimeState{threadID: input.ThreadID.String(), service: service}
	items, err := source.cloneWindowItemsMode(ctx, view.Items, min(input.PreviewBytes, threadDisplayPreviewBytes/max(1, len(view.Items))-3), true)
	if err != nil {
		return ThreadItemPage{}, err
	}
	snapshot, err := boundWindowSnapshot(ThreadWindowSnapshot{ThreadID: input.ThreadID, LeafID: page.leaf, Items: items, HasBefore: page.hasBefore, HasAfter: page.hasAfter, resolvedAnchor: window.Membership.ResolvedAnchorID}, in)
	if err != nil {
		return ThreadItemPage{}, err
	}
	for {
		selected := map[string]bool{}
		selectedTurns := map[identity.TurnID]bool{}
		for _, item := range snapshot.Items {
			selected[item.ID] = true
			selectedTurns[item.TurnID] = true
		}
		turns := []ThreadTurn{}
		for _, turn := range cloneThreadTurns(view.Turns) {
			if !selectedTurns[turn.TurnID] {
				continue
			}
			turn.IntermediateItemIDs = selectedThreadIDs(turn.IntermediateItemIDs, selected)
			turn.FinalItemIDs = selectedThreadIDs(turn.FinalItemIDs, selected)
			turns = append(turns, turn)
		}
		result := ThreadItemPage{ThreadID: input.ThreadID, LeafID: page.leaf, Items: snapshot.Items, Turns: turns, HasBefore: snapshot.HasBefore, HasAfter: snapshot.HasAfter, FirstItemID: snapshot.FirstItemID, LastItemID: snapshot.LastItemID}
		raw, err := json.Marshal(result)
		if err != nil {
			return ThreadItemPage{}, err
		}
		if len(raw) <= threadDisplayPageBytes {
			return result, nil
		}
		if len(snapshot.Items) <= 1 {
			return ThreadItemPage{}, errors.New("thread item page metadata exceeds display budget")
		}
		removeFirst := input.Direction == ThreadWindowBefore
		if input.Direction == ThreadWindowAround {
			anchor := 0
			for i, item := range snapshot.Items {
				if item.ID == input.AnchorID {
					anchor = i
				}
			}
			removeFirst = anchor >= len(snapshot.Items)/2
		}
		if removeFirst {
			snapshot.Items = snapshot.Items[1:]
			snapshot.HasBefore = true
		} else {
			snapshot.Items = snapshot.Items[:len(snapshot.Items)-1]
			snapshot.HasAfter = true
		}
		setWindowBounds(&snapshot)
	}
}
