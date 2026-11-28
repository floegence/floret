package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func memberItemRange(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, input ItemMembershipRangeOptions) (ItemMembershipRange, error) {
	journal := member.From(tx)
	result := ItemMembershipRange{LeafID: meta.LeafID}
	page, err := readItemMembershipWindow(ctx, tx, meta, ItemMembershipWindowOptions{ThreadID: meta.ID, LeafID: input.LeafID, Limit: 1, ItemIDs: []string{input.AnchorID}})
	if err != nil {
		return result, err
	}
	anchor := page.Members[0]
	slots, hidden, err := journal.WindowSlots()
	if err != nil {
		return result, err
	}
	filter := member.WindowRangeFilter{Kinds: input.Kinds, Turn: anchor.TurnID, Run: anchor.RunID, Anchor: anchor.Slot, Start: input.minSlot, End: slots, Exclude: uint32(input.Exclude), Settled: input.SettledActivities, Intermediate: input.IntermediateAssistant}
	if input.endSlot != 0 {
		filter.End = min(slots, input.endSlot)
	}
	if input.IntermediateAssistant && anchor.TurnID != "" {
		leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return result, err
		}
		segmented := false
		err = visitMemberSelection(ctx, tx, meta, leaf, member.SourceSelection{Turn: anchor.TurnID, Flag: member.WindowTurn}, func(event member.EntryEvent) (bool, error) {
			if event.Marker != string(TurnStarted) {
				return false, nil
			}
			e, err := memberPathEntry(journal, event)
			if err == nil {
				segmented = e.Metadata[ResponseSegmentsMetadataKey] == "1"
			}
			return true, err
		})
		if err != nil {
			return result, err
		}
		if segmented {
			err := visitMemberSelection(ctx, tx, meta, leaf, member.SourceSelection{Flag: member.WindowResponse, Turn: anchor.TurnID, Run: anchor.RunID, Reverse: true}, func(e member.EntryEvent) (bool, error) { filter.ResponseDepth = e.Depth; return true, nil })
			if err != nil {
				return result, err
			}
		}
	}
	first, last, err := journal.WindowRange(filter, func(role, status string, facts uint32, count int) error {
		kinds, err := countItemsOfKind(role, count)
		if err != nil {
			return err
		}
		activities, err := countActivities(role, status, count)
		if err != nil {
			return err
		}
		result.Counts.add(kinds)
		result.Activity.add(activities)
		result.Facts |= ItemRangeFacts(facts)
		return nil
	})
	if errors.Is(err, spi.ErrNotFound) {
		return result, errItemRangePredicate
	}
	if err != nil {
		return result, err
	}
	before, err := journal.HiddenBefore(first)
	if err != nil {
		return result, err
	}
	after, err := journal.HiddenBefore(last)
	if err != nil {
		return result, err
	}
	lo, hi := first-before, last-after+1
	if hi-lo != result.Counts.total() || hi <= lo {
		return result, ErrAuthorityCorrupt
	}
	result.FirstOrdinal, result.LastOrdinal = uint64(lo+1), uint64(hi)
	result.HasBefore, result.HasAfter = lo > 0, hi < slots-hidden
	for _, p := range []struct {
		slot int
		dest *ItemMember
	}{{first, &result.First}, {last, &result.Last}} {
		if err := journal.WindowItems(p.slot, 1, func(row member.WindowItem) error {
			var err error
			*p.dest, err = restoreWindowMember(ctx, tx, meta, row)
			return err
		}); err != nil {
			return result, err
		}
		if p.dest.ID == "" || p.dest.Slot != p.slot {
			return result, ErrAuthorityCorrupt
		}
	}
	return result, nil
}
