package sessiontree

import (
	"context"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type ItemRangePageOptions struct {
	ItemMembershipRangeOptions
	Direction PathWindowDirection
	Limit     int
}

type ItemRangeSpan struct {
	ItemMembershipRange
	Matched bool
}

type ItemRangePageReader interface {
	ItemRangePage(context.Context, ItemRangePageOptions) ([]ItemRangeSpan, error)
}

func rangePageKinds(ctx context.Context, input ItemRangePageOptions) (map[string]bool, error) {
	if input.LeafID == "" || input.Limit < 1 || input.Limit > 32 || input.Direction != PathWindowBefore && input.Direction != PathWindowAfter {
		return nil, spi.ErrInvalidArgument
	}
	return validateItemRangeOptions(ctx, input.ItemMembershipRangeOptions)
}

func collectRangePage(ctx context.Context, input ItemRangePageOptions, next func(string) (ItemMembershipWindow, error), read func(ItemMembershipRangeOptions) (ItemMembershipRange, error)) ([]ItemRangeSpan, error) {
	spans := make([]ItemRangeSpan, 0, input.Limit)
	before := input.Direction == PathWindowBefore
	anchor := input.AnchorID
	for len(spans) < input.Limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := next(anchor)
		if err != nil {
			return nil, err
		}
		if len(page.Members) == 0 {
			break
		}
		member, ordinal := page.Members[0], page.Ordinals[0]
		query := input.ItemMembershipRangeOptions
		query.AnchorID = member.ID
		if before {
			query.endSlot = member.Slot + 1
		} else {
			query.minSlot = member.Slot
		}
		span, err := read(query)
		matched := err == nil
		if errors.Is(err, errItemRangePredicate) {
			counts, countErr := itemKindCount(member.Kind)
			if countErr != nil {
				return nil, countErr
			}
			activity, countErr := itemActivityCount(member)
			if countErr != nil {
				return nil, countErr
			}
			span = ItemMembershipRange{LeafID: page.LeafID, First: member, Last: member, FirstOrdinal: ordinal, LastOrdinal: ordinal, Counts: counts, Activity: activity, Facts: member.RangeFacts, HasBefore: page.HasBefore, HasAfter: page.HasAfter}
		} else if err != nil {
			return nil, err
		}
		if span.LeafID != input.LeafID || before && span.LastOrdinal != ordinal || !before && span.FirstOrdinal != ordinal {
			return nil, ErrAuthorityCorrupt
		}
		spans = append(spans, ItemRangeSpan{span, matched})
		if before {
			if !span.HasBefore {
				break
			}
			anchor = span.First.ID
		} else {
			if !span.HasAfter {
				break
			}
			anchor = span.Last.ID
		}
	}
	if before {
		slices.Reverse(spans)
	}
	return spans, nil
}

func (repo *MemoryRepo) ItemRangePage(ctx context.Context, input ItemRangePageOptions) ([]ItemRangeSpan, error) {
	kinds, err := rangePageKinds(ctx, input)
	if err != nil {
		return nil, err
	}
	meta, err := repo.Thread(ctx, input.ThreadID)
	if err != nil {
		return nil, err
	}
	if meta.LeafID != input.LeafID {
		return nil, spi.ErrConflict
	}
	entries, err := repo.Path(ctx, input.ThreadID, meta.LeafID)
	if err != nil {
		return nil, err
	}
	members, err := memoryRangeMembers(ctx, entries)
	if err != nil {
		return nil, err
	}
	positions := make(map[string]int, len(members))
	for position, member := range members {
		positions[member.ID] = position
	}
	return collectRangePage(ctx, input, func(anchor string) (ItemMembershipWindow, error) {
		index, ok := positions[anchor]
		if !ok {
			return ItemMembershipWindow{}, ErrEntryNotFound
		}
		if input.Direction == PathWindowBefore {
			index--
		} else {
			index++
		}
		if index < 0 || index >= len(members) {
			return ItemMembershipWindow{}, nil
		}
		return ItemMembershipWindow{LeafID: meta.LeafID, Members: members[index : index+1], Ordinals: []uint64{uint64(index + 1)}, HasBefore: index > 0, HasAfter: index+1 < len(members)}, nil
	}, func(query ItemMembershipRangeOptions) (ItemMembershipRange, error) {
		return memoryMemberRange(meta, entries, query, kinds, members)
	})
}

func (repo *BackendRepo) ItemRangePage(ctx context.Context, input ItemRangePageOptions) ([]ItemRangeSpan, error) {
	kinds, err := rangePageKinds(ctx, input)
	if err != nil {
		return nil, err
	}
	if err = repo.prepareWindowReadSources(ctx, input.ThreadID, windowPreparationNeeds{membership: true, window: input.IntermediateAssistant, response: input.IntermediateAssistant}); err != nil {
		return nil, err
	}
	var spans []ItemRangeSpan
	err = repo.withActivitySummaryPreparation(ctx, input.ThreadID, func() error {
		return repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
			var err error
			tx = newProjectionSourceTx(tx)
			if member.From(tx) != nil {
				spans, err = collectRangePage(ctx, input, func(anchor string) (ItemMembershipWindow, error) {
					return readItemMembershipWindow(ctx, tx, meta, ItemMembershipWindowOptions{ThreadID: meta.ID, LeafID: input.LeafID, AnchorID: anchor, Direction: input.Direction, Limit: 1})
				}, func(query ItemMembershipRangeOptions) (ItemMembershipRange, error) {
					return memberItemRange(ctx, tx, meta, query)
				})
			} else {
				spans, err = readRangePageTree(ctx, tx, meta, input, kinds)
			}
			return err
		}, func(memory *MemoryRepo) error {
			var err error
			spans, err = memory.ItemRangePage(ctx, input)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return spans, nil
}
