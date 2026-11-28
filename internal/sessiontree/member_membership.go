package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"slices"
	"strings"
)

type memberWindowDetail struct {
	Hash        string              `json:"hash"`
	Builtin     session.BuiltinTool `json:"builtin,omitempty"`
	Facts       ItemRangeFacts      `json:"facts,omitempty"`
	Response    bool                `json:"response,omitempty"`
	Final       bool                `json:"final,omitempty"`
	Activity    *contextSourceRef   `json:"activity,omitempty"`
	Interaction *contextSourceRef   `json:"interaction,omitempty"`
}

func storeWindowMember(ctx context.Context, tx spi.WriteTx, meta ThreadMeta, item ItemMember, hash, last string, create bool) error {
	journal := member.From(tx)
	if item.Kind == "tool" {
		if strings.HasPrefix(item.ID, "hosted:") || item.ActivityStatus == "" || session.IsGoalTool(session.Message{ToolName: item.ToolName, BuiltinTool: item.BuiltinTool}) {
			item.RangeFacts |= ItemRangeUnknownActivity
		} else {
			frozen := meta
			frozen.LeafID = last
			fields, _, err := memberActivitySelection(ctx, tx, frozen, item)
			if err != nil {
				return err
			}
			item.RangeFacts = activityRangeFacts(activityFactsFromFields(fields), item.ActivityStatus) | item.RangeFacts&(ItemRangeApproval|ItemRangeInput)
		}
	}
	detail := memberWindowDetail{Hash: hash, Builtin: item.BuiltinTool, Facts: item.RangeFacts, Response: item.AssistantResponse, Final: item.HostWorkFinal}
	if item.ActivitySource.ID != "" {
		detail.Activity = &item.ActivitySource
	}
	if item.InteractionSource.ID != "" {
		detail.Interaction = &item.InteractionSource
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	return journal.PutWindow(member.WindowItem{ID: item.ID, First: item.SourceID, Last: last, Role: item.Kind, Tool: item.ToolName, Status: item.ActivityStatus, Hidden: item.Hidden, Summary: raw}, create)
}

func rebuildMemberWindow(ctx context.Context, tx spi.WriteTx, meta ThreadMeta) error {
	journal := member.From(tx)
	if err := refreshMemberModelSources(journal, meta.LeafID, nil); err != nil {
		return err
	}
	if err := journal.ClearWindow(); err != nil {
		return err
	}
	var membership ItemMembership
	hashes := map[string]string{}
	last := map[string]string{}
	if err := visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, func(entry Entry) error {
		step, err := membership.Observe(entry, uint64(entry.PathDepth))
		if err != nil {
			return err
		}
		for _, c := range []*ItemContribution{step.Reasoning, step.Content, step.Main, step.Input} {
			if c != nil {
				last[c.Member.ID] = entry.ID
				if c.New {
					hashes[c.Member.SourceID] = entry.RawHash
				}
			}
		}
		for _, c := range step.Revealed {
			last[c.ID] = entry.ID
		}
		return nil
	}); err != nil {
		if errors.Is(err, errItemMembershipSource) {
			return journal.MarkWindow(false)
		}
		return err
	}
	for _, item := range membership.Members {
		if err := storeWindowMember(ctx, tx, meta, item, hashes[item.SourceID], last[item.ID], true); err != nil {
			return err
		}
	}
	return journal.MarkWindow(true)
}

func appendMemberWindow(ctx context.Context, tx spi.WriteTx, meta ThreadMeta, entry Entry) error {
	journal := member.From(tx)
	if err := refreshMemberModelSources(journal, meta.LeafID, &entry); err != nil {
		return err
	}
	valid, err := journal.WindowValid()
	if err != nil || !valid {
		return err
	}
	if !projectionCandidate(entry) && !(entry.Type == EntryTurnMarker && entry.TurnStatus == TurnWaiting) {
		return nil
	}
	leaf, err := readEntryLocator(tx, meta.ID, entry.ID)
	if err != nil {
		return err
	}
	sources, err := projectionSourceClosureMode(ctx, []string{entry.ID}, func(id string) (Entry, error) { return memberSourceEntry(ctx, tx, meta, id) }, func(group projectionSourceGroup, visit func(Entry) error) error {
		return visitMemberGroup(ctx, tx, meta, leaf, group, visit)
	}, true)
	if err != nil {
		return err
	}
	var p ItemMembership
	var step ItemMembershipStep
	hashes := map[string]string{}
	selectFields := projectionBodyFields(sources, func(source PathMetadata, control bool) []string {
		var result []string
		for _, f := range source.Bodies {
			if f.Field == "control_output" || control && source.Entry.Type == EntryToolResult && f.Field == "content" {
				result = append(result, f.Field)
			}
		}
		return result
	})
	for _, source := range sources {
		value := source.Entry
		value.bodyFields = map[string]canonicalBodyRef{}
		selected := selectFields(source)
		for _, field := range source.Bodies {
			value.bodyFields[field.Field] = canonicalBodyRef{SHA256: field.SHA256, Bytes: field.Bytes}
			if slices.Contains(selected, field.Field) {
				text, err := readMemberProjectionField(journal, value.ID, field)
				if err != nil {
					return err
				}
				if err := restoreCanonicalField(&value, field.Field, text); err != nil {
					return err
				}
			}
		}
		hashes[value.ID] = value.RawHash
		observed, err := p.Observe(value, uint64(value.PathDepth))
		if err != nil {
			if errors.Is(err, errItemMembershipSource) {
				return journal.MarkWindow(false)
			}
			return err
		}
		if value.ID == entry.ID {
			step = observed
		}
	}
	for _, item := range step.Revealed {
		if err := storeWindowMember(ctx, tx, meta, item, hashes[item.SourceID], entry.ID, false); err != nil {
			return err
		}
	}
	for _, c := range []*ItemContribution{step.Reasoning, step.Content, step.Main, step.Input} {
		if c == nil {
			continue
		}
		if err := storeWindowMember(ctx, tx, meta, c.Member, hashes[c.Member.SourceID], entry.ID, c.New); err != nil {
			return err
		}
	}
	return nil
}

func restoreWindowMember(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, item member.WindowItem) (ItemMember, error) {
	var detail memberWindowDetail
	if err := storagecodec.DecodeJSON(item.Summary, &detail); err != nil {
		return ItemMember{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	source, err := memberSourceEntry(ctx, tx, meta, item.First)
	if err != nil || source.RawHash != detail.Hash || item.Depth != int(source.PathDepth) || source.TurnID != item.Turn || source.RunID != item.Run {
		return ItemMember{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	result := ItemMember{ID: item.ID, SourceID: item.First, Kind: item.Role, TurnID: item.Turn, RunID: item.Run, Slot: item.Position, CanonicalOrdinal: uint64(item.Depth), ToolName: item.Tool, BuiltinTool: detail.Builtin, RangeFacts: detail.Facts, ActivityStatus: item.Status, AssistantResponse: detail.Response, HostWorkFinal: detail.Final}
	if detail.Activity != nil {
		result.ActivitySource = *detail.Activity
	}
	if detail.Interaction != nil {
		result.InteractionSource = *detail.Interaction
	}
	return result, nil
}

func readMemberWindowPage(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, input ItemMembershipWindowOptions) (ItemMembershipWindow, error) {
	journal := member.From(tx)
	if ok, err := journal.WindowValid(); err != nil || !ok {
		return ItemMembershipWindow{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	slots, hidden, err := journal.WindowSlots()
	if err != nil {
		return ItemMembershipWindow{}, err
	}
	count := slots - hidden
	result := ItemMembershipWindow{LeafID: meta.LeafID, Members: []ItemMember{}, Ordinals: []uint64{}}
	pos := func(id string) (int, int, error) {
		slot, err := journal.WindowPosition(id)
		if errors.Is(err, spi.ErrNotFound) {
			return 0, 0, ErrEntryNotFound
		}
		if err != nil {
			return 0, 0, err
		}
		skipped := 0
		if hidden > 0 {
			skipped, err = journal.HiddenBefore(slot)
		}
		return slot, slot - skipped, err
	}
	load := func(slot, rank, total int) error {
		n := 0
		err := journal.WindowItems(slot, total, func(row member.WindowItem) error {
			item, err := restoreWindowMember(ctx, tx, meta, row)
			if err != nil {
				return err
			}
			result.Members = append(result.Members, item)
			result.Ordinals = append(result.Ordinals, uint64(rank+n+1))
			n++
			return nil
		})
		if err == nil && n != total {
			return ErrAuthorityCorrupt
		}
		return err
	}
	if input.ItemIDs != nil {
		if len(input.ItemIDs) < 1 || len(input.ItemIDs) > 256 {
			return result, spi.ErrInvalidArgument
		}
		type point struct{ slot, rank int }
		var points []point
		seen := map[string]bool{}
		for _, id := range input.ItemIDs {
			if id == "" || seen[id] {
				return result, spi.ErrInvalidArgument
			}
			seen[id] = true
			slot, rank, err := pos(id)
			if input.OmitMissing && errors.Is(err, ErrEntryNotFound) {
				continue
			}
			if err != nil {
				return result, err
			}
			points = append(points, point{slot, rank})
		}
		slices.SortFunc(points, func(a, b point) int { return a.slot - b.slot })
		for _, p := range points {
			if err := load(p.slot, p.rank, 1); err != nil {
				return result, err
			}
		}
		if len(points) > 0 {
			result.HasBefore = points[0].rank > 0
			result.HasAfter = points[len(points)-1].rank < count-1
		}
		return result, nil
	}
	anchor := count
	if input.AnchorID != "" {
		_, anchor, err = pos(input.AnchorID)
		if err != nil {
			return result, err
		}
	}
	first, last, err := membershipWindowBounds(count, anchor, input)
	if err != nil {
		return result, err
	}
	result.HasBefore, result.HasAfter = first > 0, last < count
	if first == last {
		return result, nil
	}
	slot := first
	if hidden > 0 {
		lo, hi := first, first+hidden
		for lo < hi {
			mid := lo + (hi-lo)/2
			n, err := journal.HiddenBefore(mid + 1)
			if err != nil {
				return result, err
			}
			if mid-n >= first {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		slot = lo
	}
	if err := load(slot, first, last-first); err != nil {
		return result, err
	}
	if input.Direction == PathWindowAround {
		result.ResolvedAnchorID = result.Members[anchor+input.AnchorOffset-first].ID
	}
	return result, nil
}
