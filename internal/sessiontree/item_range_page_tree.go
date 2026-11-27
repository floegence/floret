package sessiontree

import (
	"context"
	"errors"
	"math/bits"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

func readRangePageTree(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, input ItemRangePageOptions, kinds map[string]bool) ([]ItemRangeSpan, error) {
	if meta.LeafID != input.LeafID {
		return nil, spi.ErrConflict
	}
	manifest, found, err := readItemManifest(tx, meta.ID)
	if err != nil || !found || manifest.Leaf != meta.LeafID {
		return nil, errors.Join(err, ErrAuthorityCorrupt)
	}
	if manifest.Slots == 0 {
		return nil, ErrEntryNotFound
	}
	leaf, err := activeLocatedEntry(ctx, tx, meta, manifest.Leaf)
	if err != nil || leaf.RawHash != manifest.Hash {
		return nil, errors.Join(err, ErrAuthorityCorrupt)
	}
	depth := bits.Len(uint(manifest.Slots - 1))
	root, err := readItemRangeSummary(tx, meta.ID, depth, 0)
	if err != nil || root.Counts.total() != manifest.Visible {
		return nil, errors.Join(err, ErrAuthorityCorrupt)
	}
	total, err := itemRankPrefix(tx, meta.ID, manifest.Slots)
	if err != nil || total != manifest.Visible {
		return nil, errors.Join(err, ErrAuthorityCorrupt)
	}
	resolve := func(slot int) (ItemMember, error) {
		if slot < 0 || slot >= manifest.Slots {
			return ItemMember{}, ErrAuthorityCorrupt
		}
		member, err := readItemSlot(tx, meta.ID, slot)
		if err != nil || member.Hidden {
			return ItemMember{}, errors.Join(err, ErrAuthorityCorrupt)
		}
		source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
		if err != nil || source.RawHash != member.RawHash || uint64(source.PathDepth) != member.CanonicalOrdinal || source.TurnID != member.TurnID || source.RunID != member.RunID {
			return ItemMember{}, errors.Join(err, ErrAuthorityCorrupt)
		}
		return member.ItemMember, validateItemActivitySource(ctx, tx, meta, member.ItemMember)
	}
	var cut int
	if err = readItemJSON(tx, itemIdentityKey(meta.ID, input.AnchorID), &cut); errors.Is(err, spi.ErrNotFound) {
		return nil, ErrEntryNotFound
	} else if err != nil {
		return nil, err
	}
	anchor, err := resolve(cut)
	if err != nil || anchor.ID != input.AnchorID {
		return nil, errors.Join(err, ErrAuthorityCorrupt)
	}
	type segment struct {
		summary     itemRangeSummary
		first, last int
		matched     bool
	}
	segments := make([]segment, 0, input.Limit)
	before, done := input.Direction == PathWindowBefore, false
	ends := map[[2]string]int{}
	var visit func(level, block, rank int, summary itemRangeSummary) error
	visit = func(level, block, rank int, summary itemRangeSummary) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		lo, hi := block<<level, min((block+1)<<level, manifest.Slots)
		size := summary.Counts.total()
		if done || size == 0 || lo >= hi || before && lo >= cut || !before && hi <= cut+1 {
			return nil
		}
		inside := before && hi <= cut || !before && lo > cut
		matched := inside && !summary.Mixed && summary.Pending == 0 && summary.Counts.selected(kinds) == size && summary.Facts&input.Exclude == 0 && (!input.SettledActivities || summary.Activity.Pending+summary.Activity.Running+summary.Activity.Waiting+summary.Activity.Unknown == 0)
		if matched && input.IntermediateAssistant && summary.Counts.Assistant > 0 {
			key := [2]string{summary.Turn, summary.Run}
			end, ok := ends[key]
			if !ok {
				ordinal, err := rangeResponseBoundary(ctx, tx, meta, key[0], key[1])
				if err != nil {
					return err
				}
				end, err = itemSlotBeforeDepth(ctx, tx, meta.ID, manifest.Slots, ordinal)
				if err != nil {
					return err
				}
				ends[key] = end
			}
			matched = summary.NonResponseAssistant == 0 && hi <= end
		}
		if !matched && level > 0 {
			left, err := readItemRangeSummary(tx, meta.ID, level-1, block*2)
			if err != nil {
				return err
			}
			rightRank := rank + left.Counts.total()
			visitRight := func() error {
				if done || before && ((block*2+1)<<(level-1)) >= cut || rightRank == rank+size {
					return nil
				}
				right, err := readItemRangeSummary(tx, meta.ID, level-1, block*2+1)
				if err != nil || size != left.Counts.total()+right.Counts.total() {
					return errors.Join(err, ErrAuthorityCorrupt)
				}
				return visit(level-1, block*2+1, rightRank, right)
			}
			if before {
				if err = visitRight(); err != nil {
					return err
				}
				return visit(level-1, block*2, rank, left)
			}
			if err = visit(level-1, block*2, rank, left); err != nil {
				return err
			}
			return visitRight()
		}
		if !inside {
			return nil
		}
		if summary.Pending != 0 {
			member, err := readItemSlot(tx, meta.ID, lo)
			if err != nil {
				return err
			}
			return &activitySummaryPreparationRequired{source: member.SourceID}
		}
		if size > hi-lo {
			return ErrAuthorityCorrupt
		}
		if count := len(segments); matched && count > 0 {
			prior := &segments[count-1]
			if prior.matched && prior.summary.Turn == summary.Turn && prior.summary.Run == summary.Run {
				prior.summary.add(summary)
				prior.first = min(prior.first, rank+1)
				prior.last = max(prior.last, rank+size)
				return nil
			}
		}
		if len(segments) == input.Limit {
			done = true
			return nil
		}
		segments = append(segments, segment{summary, rank + 1, rank + size, matched})
		return nil
	}
	if err = visit(depth, 0, 0, root); err != nil {
		return nil, err
	}
	if before {
		slices.Reverse(segments)
	}
	result := make([]ItemRangeSpan, 0, len(segments))
	endpoint := func(rank int) (ItemMember, error) {
		slot, err := selectItemRank(tx, meta.ID, manifest.Slots, rank)
		if err != nil {
			return ItemMember{}, err
		}
		return resolve(slot)
	}
	for _, span := range segments {
		first, err := endpoint(span.first)
		if err != nil {
			return nil, err
		}
		last := first
		if span.first != span.last {
			last, err = endpoint(span.last)
			if err != nil {
				return nil, err
			}
		}
		summary := span.summary
		if first.TurnID != summary.Turn || last.TurnID != summary.Turn || first.RunID != summary.Run || last.RunID != summary.Run || summary.Counts.total() != span.last-span.first+1 {
			return nil, ErrAuthorityCorrupt
		}
		result = append(result, ItemRangeSpan{ItemMembershipRange{LeafID: meta.LeafID, First: first, Last: last, FirstOrdinal: uint64(span.first), LastOrdinal: uint64(span.last), Counts: summary.Counts, Activity: summary.Activity, Facts: summary.Facts, HasBefore: span.first > 1, HasAfter: span.last < manifest.Visible}, span.matched})
	}
	return result, nil
}
