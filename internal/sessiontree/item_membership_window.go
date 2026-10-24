package sessiontree

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

type ItemMembershipWindowOptions struct {
	// Summary explicitly requests a bounded canonical preview. Source-only
	// consumers leave it false so dependency discovery never reads text bodies.
	Summary      bool
	AnchorOffset int
	// Companions selects additional bounded windows in the same read transaction.
	Companions []ItemMembershipWindowOptions
	// MissingAnchorTail is for automatic refresh of an accepted interest only.
	MissingAnchorTail bool
	// OmitMissing is reserved for refreshing an already accepted selection after
	// canonical path replacement. Explicit selection reads remain strict.
	OmitMissing                bool
	ItemIDs                    []string
	ActivityFields             bool
	ThreadID, LeafID, AnchorID string
	Direction                  PathWindowDirection
	Limit                      int
}
type ItemMembershipWindow struct {
	LeafID              string
	ResolvedAnchorID    string
	Members             []ItemMember
	Ordinals            []uint64
	HasBefore, HasAfter bool
	AnchorUnavailable   bool
}
type ItemMembershipReader interface {
	ItemMembershipWindow(context.Context, ItemMembershipWindowOptions) (ItemMembershipWindow, error)
}

func validateMembershipSeek(options ItemMembershipWindowOptions) error {
	if options.AnchorOffset != 0 && (options.AnchorID == "" || options.ItemIDs != nil || options.Direction != PathWindowAround) {
		return spi.ErrInvalidArgument
	}
	return nil
}

func membershipWindowBounds(count, anchor int, options ItemMembershipWindowOptions) (int, int, error) {
	if options.AnchorOffset != 0 {
		if options.AnchorID == "" || options.ItemIDs != nil || options.Direction != PathWindowAround {
			return 0, 0, spi.ErrInvalidArgument
		}
		if options.AnchorOffset < -anchor || options.AnchorOffset >= count-anchor {
			return 0, 0, ErrEntryNotFound
		}
		anchor += options.AnchorOffset
	}
	var first, last int
	switch options.Direction {
	case "", PathWindowBefore:
		last = anchor
		first = max(0, last-options.Limit)
	case PathWindowAfter:
		first = 0
		if options.AnchorID != "" {
			first = anchor + 1
		}
		last = min(count, first+options.Limit)
	case PathWindowAround:
		if options.AnchorID == "" {
			return 0, 0, ErrEntryNotFound
		}
		first = max(0, anchor-options.Limit/2)
		last = min(count, first+options.Limit)
		first = max(0, last-options.Limit)
	default:
		return 0, 0, spi.ErrInvalidArgument
	}
	return first, last, nil
}

func (repo *BackendRepo) ItemMembershipWindow(ctx context.Context, options ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	if ctx == nil || validateMembershipSeek(options) != nil || strings.TrimSpace(options.ThreadID) == "" || options.Limit < 1 || options.Limit > 256 {
		return ItemMembershipWindow{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareWindowReadSources(ctx, options.ThreadID, windowPreparationNeeds{membership: true}); err != nil {
		return ItemMembershipWindow{}, err
	}
	var result ItemMembershipWindow
	err := repo.canonicalThreadView(ctx, options.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		var err error
		result, err = readItemMembershipWindow(ctx, tx, meta, options)
		return err
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ItemMembershipWindow(ctx, options)
		return err
	})
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	return result, nil
}

func readItemMembershipWindow(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, options ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	if err := validateMembershipSeek(options); err != nil {
		return ItemMembershipWindow{}, err
	}
	tx = newProjectionSourceTx(tx)
	if options.LeafID != "" && options.LeafID != meta.LeafID {
		return ItemMembershipWindow{}, spi.ErrConflict
	}
	m, found, err := readItemManifest(tx, meta.ID)
	if err != nil || !found || m.Leaf != meta.LeafID {
		return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	if m.Leaf != "" {
		source, err := activeLocatedEntry(ctx, tx, meta, m.Leaf)
		if err != nil || source.RawHash != m.Hash {
			return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	total, err := itemRankPrefix(tx, meta.ID, m.Slots)
	if err != nil || total != m.Visible {
		return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	if options.ItemIDs != nil {
		if len(options.ItemIDs) < 1 || len(options.ItemIDs) > 256 {
			return ItemMembershipWindow{}, spi.ErrInvalidArgument
		}
		result := ItemMembershipWindow{LeafID: m.Leaf, Members: []ItemMember{}, Ordinals: []uint64{}}
		type ranked struct {
			member ItemMember
			rank   uint64
		}
		selected := make([]ranked, 0, len(options.ItemIDs))
		seen := map[string]bool{}
		for _, id := range options.ItemIDs {
			if id == "" || seen[id] {
				return ItemMembershipWindow{}, spi.ErrInvalidArgument
			}
			seen[id] = true
			var slot int
			if err := readItemJSON(tx, itemIdentityKey(meta.ID, id), &slot); err != nil {
				if errors.Is(err, spi.ErrNotFound) {
					if options.OmitMissing {
						continue
					}
					return ItemMembershipWindow{}, ErrEntryNotFound
				}
				return ItemMembershipWindow{}, err
			}
			member, err := readItemSlot(tx, meta.ID, slot)
			if err != nil || member.ID != id || member.Hidden || slot >= m.Slots {
				return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
			if err != nil || source.RawHash != member.RawHash || uint64(source.PathDepth) != member.CanonicalOrdinal || source.TurnID != member.TurnID || source.RunID != member.RunID {
				return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
			}
			rank, err := itemRankPrefix(tx, meta.ID, slot+1)
			if err != nil {
				return ItemMembershipWindow{}, err
			}
			selected = append(selected, ranked{member.ItemMember, uint64(rank)})
		}
		slices.SortFunc(selected, func(a, b ranked) int {
			if a.rank < b.rank {
				return -1
			}
			if a.rank > b.rank {
				return 1
			}
			return 0
		})
		for _, value := range selected {
			result.Members = append(result.Members, value.member)
			result.Ordinals = append(result.Ordinals, value.rank)
		}
		if len(selected) > 0 {
			result.HasBefore = selected[0].rank > 1
			result.HasAfter = selected[len(selected)-1].rank < uint64(m.Visible)
		}
		return result, nil
	}
	anchor := m.Visible
	if options.AnchorID != "" {
		var slot int
		if err := readItemJSON(tx, itemIdentityKey(meta.ID, options.AnchorID), &slot); errors.Is(err, spi.ErrNotFound) {
			return ItemMembershipWindow{}, ErrEntryNotFound
		} else if err != nil {
			return ItemMembershipWindow{}, err
		}
		member, err := readItemSlot(tx, meta.ID, slot)
		if err != nil || member.ID != options.AnchorID || member.Hidden || slot >= m.Slots {
			return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		anchor, err = itemRankPrefix(tx, meta.ID, slot+1)
		if err != nil {
			return ItemMembershipWindow{}, err
		}
		anchor--
	}
	first, last, err := membershipWindowBounds(m.Visible, anchor, options)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	result := ItemMembershipWindow{LeafID: m.Leaf, Members: []ItemMember{}, Ordinals: []uint64{}, HasBefore: first > 0, HasAfter: last < m.Visible}
	for rank := first + 1; rank <= last; rank++ {
		if err := ctx.Err(); err != nil {
			return ItemMembershipWindow{}, err
		}
		slot, err := selectItemRank(tx, meta.ID, m.Slots, rank)
		if err != nil {
			return ItemMembershipWindow{}, err
		}
		member, err := readItemSlot(tx, meta.ID, slot)
		if err != nil || member.Hidden {
			return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		source, err := activeLocatedEntry(ctx, tx, meta, member.SourceID)
		if err != nil || source.RawHash != member.RawHash || uint64(source.PathDepth) != member.CanonicalOrdinal || source.TurnID != member.TurnID || source.RunID != member.RunID {
			return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		if rank == anchor+options.AnchorOffset+1 && options.Direction == PathWindowAround {
			result.ResolvedAnchorID = member.ID
		}
		result.Members = append(result.Members, member.ItemMember)
		result.Ordinals = append(result.Ordinals, uint64(rank))
	}
	return result, nil
}

func (repo *MemoryRepo) ItemMembershipWindow(ctx context.Context, options ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	if ctx == nil || validateMembershipSeek(options) != nil || options.Limit < 1 || options.Limit > 256 {
		return ItemMembershipWindow{}, spi.ErrInvalidArgument
	}
	meta, err := repo.Thread(ctx, options.ThreadID)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	if options.LeafID != "" && options.LeafID != meta.LeafID {
		return ItemMembershipWindow{}, spi.ErrConflict
	}
	entries, err := repo.Path(ctx, options.ThreadID, meta.LeafID)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	var projection ItemMembership
	for _, entry := range entries {
		if _, err := projection.Observe(entry, uint64(entry.PathDepth)); err != nil {
			return ItemMembershipWindow{}, err
		}
	}
	members := []ItemMember{}
	anchor := -1
	for _, member := range projection.Members {
		if member.Hidden {
			continue
		}
		if member.ID == options.AnchorID {
			anchor = len(members)
		}
		members = append(members, member)
	}
	if options.ItemIDs != nil {
		return selectMemoryMembershipItems(meta.LeafID, members, options.ItemIDs, options.OmitMissing)
	}
	if options.AnchorID == "" {
		anchor = len(members)
	} else if anchor < 0 {
		return ItemMembershipWindow{}, ErrEntryNotFound
	}
	first, last, err := membershipWindowBounds(len(members), anchor, options)
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	result := ItemMembershipWindow{LeafID: meta.LeafID, Members: members[first:last], Ordinals: []uint64{}, HasBefore: first > 0, HasAfter: last < len(members)}
	if options.Direction == PathWindowAround {
		result.ResolvedAnchorID = members[anchor+options.AnchorOffset].ID
	}
	for i := first; i < last; i++ {
		result.Ordinals = append(result.Ordinals, uint64(i+1))
	}
	return result, nil
}

func selectMemoryMembershipItems(leaf string, members []ItemMember, ids []string, omitMissing bool) (ItemMembershipWindow, error) {
	if len(ids) < 1 || len(ids) > 256 {
		return ItemMembershipWindow{}, spi.ErrInvalidArgument
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || wanted[id] {
			return ItemMembershipWindow{}, spi.ErrInvalidArgument
		}
		wanted[id] = true
	}
	result := ItemMembershipWindow{LeafID: leaf, Members: []ItemMember{}, Ordinals: []uint64{}}
	for i, member := range members {
		if wanted[member.ID] {
			result.Members = append(result.Members, member)
			result.Ordinals = append(result.Ordinals, uint64(i+1))
			delete(wanted, member.ID)
		}
	}
	if len(wanted) != 0 && !omitMissing {
		return ItemMembershipWindow{}, ErrEntryNotFound
	}
	if len(result.Ordinals) > 0 {
		result.HasBefore = result.Ordinals[0] > 1
		result.HasAfter = result.Ordinals[len(result.Ordinals)-1] < uint64(len(members))
	}
	return result, nil
}
