package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

func readNavigatedMembershipWindow(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, options ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	return navigateMembershipWindow(ctx, options,
		func(input ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
			return readItemMembershipWindow(ctx, tx, meta, input)
		},
		func(input ItemMembershipRangeOptions) (ItemMembershipRange, error) {
			kinds, err := validateItemRangeOptions(ctx, input)
			if err != nil {
				return ItemMembershipRange{}, err
			}
			return readItemMembershipRange(ctx, tx, meta, input, kinds)
		})
}

// Navigation resolves facts and selected membership at one leaf. The bounded
// union includes the range endpoints so all omitted detail remains reachable.
func navigateMembershipWindow(ctx context.Context, options ItemMembershipWindowOptions,
	read func(ItemMembershipWindowOptions) (ItemMembershipWindow, error),
	readRange func(ItemMembershipRangeOptions) (ItemMembershipRange, error)) (ItemMembershipWindow, error) {
	if options.ItemIDs != nil || len(options.RetainItemIDs) > 150 || options.SkipRange != nil && (options.AnchorID == "" || options.AnchorOffset != 0 || options.Direction != PathWindowBefore && options.Direction != PathWindowAfter) {
		return ItemMembershipWindow{}, spi.ErrInvalidArgument
	}
	input := options
	input.SkipRange, input.RetainItemIDs = nil, nil
	var skipped *ItemMembershipRange
	if options.SkipRange != nil {
		query := *options.SkipRange
		query.ThreadID, query.AnchorID, query.LeafID = options.ThreadID, options.AnchorID, options.LeafID
		if _, err := validateItemRangeOptions(ctx, query); err != nil {
			return ItemMembershipWindow{}, err
		}
		value, err := readRange(query)
		if err != nil && !errors.Is(err, spi.ErrInvalidArgument) {
			return ItemMembershipWindow{}, err
		}
		// A changed safety fact makes the range ineligible, so read the adjacent
		// ordinary page instead. Never hide an item using stale host classification.
		if err == nil {
			skipped = &value
			input.LeafID = value.LeafID
			if options.Direction == PathWindowBefore {
				input.AnchorID = value.First.ID
			} else {
				input.AnchorID = value.Last.ID
			}
		}
	}
	page, err := read(input)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	ids := make([]string, 0, len(page.Members)+len(options.RetainItemIDs)+2)
	seen := map[string]bool{}
	add := func(id string) {
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	for _, id := range options.RetainItemIDs {
		if id == "" {
			return ItemMembershipWindow{}, spi.ErrInvalidArgument
		}
		add(id)
	}
	for _, member := range page.Members {
		add(member.ID)
	}
	if skipped != nil {
		add(skipped.First.ID)
		add(skipped.Last.ID)
	}
	if len(ids) == 0 {
		return page, nil
	}
	if len(ids) > 150 {
		return ItemMembershipWindow{}, spi.ErrInvalidArgument
	}
	selected, err := read(ItemMembershipWindowOptions{ThreadID: options.ThreadID, LeafID: page.LeafID, ItemIDs: ids, Limit: len(ids), OmitMissing: options.OmitMissing})
	selected.SkippedRange = skipped
	return selected, err
}
