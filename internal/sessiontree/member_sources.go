package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"slices"
)

func memberReadFacets(entry Entry) (string, int64, error) {
	var call string
	var flags int64
	groups, err := projectionSourceGroups(entry)
	if err != nil {
		if entry.Type != EntryInteractionAsked {
			return "", 0, err
		}
		flags |= member.WindowInteraction
	}
	for _, group := range groups {
		switch group.Kind {
		case "tool", "hosted":
			call = group.ID
		case "goal":
			flags |= member.WindowGoal
		case "window_turn":
			flags |= member.WindowTurn
		case "interaction", "parent_interaction", "input_batch":
			flags |= member.WindowInteraction
		}
	}
	if entry.Type == EntryMessageRevision {
		flags |= member.WindowGoal
	}
	kind := ThreadContextEntryKind(entry)
	if entry.Type == EntryMessageRevision || kind == ThreadContextPolicyEntryKind || kind == ThreadContextStatusEntryKind || kind == ThreadContextCompactionEntryKind || entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) || runlessToolObservation(entry) {
		flags |= member.WindowContext
	}
	if entry.Message.ModelContextSnapshot != nil || entry.Type == EntryCompaction || entry.Type == EntrySteeringApplied || entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted {
		flags |= member.WindowModel
	}
	if windowResponseBoundary(entry) {
		flags |= member.WindowResponse
	}
	if summarySourceCandidate(entry) {
		flags |= member.WindowSummary
	}
	return call, flags, nil
}

func visitMemberSelection(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, input member.SourceSelection, visit func(member.EntryEvent) (bool, error)) error {
	journal := member.From(tx)
	if journal == nil || journal.Thread() != meta.ID {
		return ErrInvalidThreadAuthority
	}
	if leaf.ID == "" {
		return nil
	}
	return journal.Sources(input, leaf.Ordinal, func(event member.EntryEvent) (bool, error) {
		if event.Depth < 1 || event.Number < event.Depth-1 {
			return false, ErrAuthorityCorrupt
		}
		if event.Depth > leaf.Depth {
			return false, nil
		}
		if leaf.Depth != int64(leaf.Ordinal)+1 {
			ancestor, err := entryAncestor(ctx, tx, meta.ID, leaf, event.Depth)
			if err != nil || ancestor.ID != event.ID {
				return false, err
			}
		}
		return visit(event)
	})
}

func visitMemberGroup(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, leaf entryLocator, group projectionSourceGroup, visit func(Entry) error) error {
	selection := member.SourceSelection{Turn: group.Turn, Run: group.Run}
	switch group.Kind {
	case "tool", "hosted", "invocation":
		selection.Call = group.ID
	case "output":
		selection.Output = group.ID
	case "window_turn", "turn_boundary":
		selection.Flag = member.WindowTurn
	case "input_batch", "interaction", "parent_interaction", "input_ready":
		selection.Flag = member.WindowInteraction
	case "goal", "goal_version", "goal_review", "goal_mutation":
		selection.Flag = member.WindowGoal
	case "steering":
		selection.Kinds = []string{string(EntrySteeringRequested), string(EntrySteeringApplied), string(EntrySteeringCancelled)}
	default:
		return ErrAuthorityCorrupt
	}
	return visitMemberSelection(ctx, tx, meta, leaf, selection, func(event member.EntryEvent) (bool, error) {
		entry, err := memberPathEntry(member.From(tx), event)
		if err != nil {
			return false, err
		}
		groups, err := projectionSourceGroups(entry)
		if err != nil {
			return false, err
		}
		if !slices.Contains(groups, group) {
			return false, nil
		}
		return false, visit(entry)
	})
}

func memberGoalRefs(ctx context.Context, tx spi.ReadTx, thread, id string) (goalSourceContext, error) {
	var refs goalSourceContext
	if id == "" {
		return refs, nil
	}
	leaf, err := readEntryLocator(tx, thread, id)
	if err != nil {
		return refs, err
	}
	err = visitMemberSelection(ctx, tx, ThreadMeta{ID: thread}, leaf, member.SourceSelection{Flag: member.WindowGoal}, func(event member.EntryEvent) (bool, error) {
		entry, err := materializeMemberEntry(member.From(tx), event)
		if err != nil {
			return false, err
		}
		if entry.ID != id && !windowGoalSource(entry) {
			return false, nil
		}
		refs, err = deriveGoalSourceContext(ctx, tx, entry, refs)
		return false, err
	})
	return refs, err
}

func memberSourceEntry(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, id string) (Entry, error) {
	node, err := activeEntryLocator(ctx, tx, meta, id)
	if err != nil {
		return Entry{}, err
	}
	journal := member.From(tx)
	event, err := journal.Entry(id)
	if err != nil || event.Depth != node.Depth {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return memberPathEntry(journal, event)
}
