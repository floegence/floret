package runtime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"
)

// Display limits apply only to disposable presentation. They never constrain
// canonical facts, provider context, accepted commands or explicit body reads.
const (
	threadDisplayPageItems = 150
	threadDisplayPageBytes = 512 << 10
	// Start with half the wire budget so ordinary UTF-8 Markdown remains
	// complete at the standard page size. windowSnapshotContext enforces the
	// exact encoded limit and reduces only pages containing unusually expensive
	// escaped content.
	threadDisplayPreviewBytes = threadDisplayPageBytes / 2
	threadDisplayTextBytes    = 64 << 10
	threadDisplayPages        = 3
)

type threadWindowDirection string

const (
	threadWindowBefore threadWindowDirection = "before"
	threadWindowAfter  threadWindowDirection = "after"
	threadWindowAround threadWindowDirection = "around"
)

type threadItemWindowInput struct {
	anchor    string
	direction threadWindowDirection
	limit     int
}

type threadItemWindow struct {
	first, last         int // half-open positions in the accepted actor presentation
	hasBefore, hasAfter bool
	items               []ThreadItem
}

var errThreadWindowAnchorUnavailable = errors.New("thread window anchor is unavailable")

// itemWindow selects by the actor's existing stable-identity index. A read of
// 150 items does not visit, clone or encode any item outside that interval.
// The caller owns the actor boundary and the returned detached item metadata.
func (actor *threadRuntimeState) itemWindow(input threadItemWindowInput) (threadItemWindow, error) {
	return actor.itemWindowContext(context.Background(), input)
}

func (actor *threadRuntimeState) itemWindowContext(ctx context.Context, input threadItemWindowInput) (threadItemWindow, error) {
	if input.limit < 1 || input.limit > threadDisplayPageItems {
		return threadItemWindow{}, errors.New("invalid thread window limit")
	}
	count := len(actor.state.view.Items)
	anchor := count
	if input.anchor != "" {
		anchor = actor.itemIndexByID(input.anchor)
		if anchor < 0 {
			return threadItemWindow{}, errThreadWindowAnchorUnavailable
		}
	}
	var first, last int
	switch input.direction {
	case threadWindowBefore:
		last = anchor
		first = max(0, last-input.limit)
	case threadWindowAfter:
		first = 0
		if input.anchor != "" {
			first = anchor + 1
		}
		last = min(count, first+input.limit)
	case threadWindowAround:
		if input.anchor == "" {
			return threadItemWindow{}, errThreadWindowAnchorUnavailable
		}
		first = max(0, anchor-input.limit/2)
		last = min(count, first+input.limit)
		first = max(0, last-input.limit)
	default:
		return threadItemWindow{}, errors.New("invalid thread window direction")
	}
	previewLimit := min(threadDisplayTextBytes, threadDisplayPreviewBytes/max(1, last-first)-(utf8.UTFMax-1))
	items := cloneWindowItemsLimit(actor.state.view.Items[first:last], previewLimit)
	for index, item := range items {
		detached, err := actor.windowItem(ctx, item, previewLimit)
		if err != nil {
			return threadItemWindow{}, err
		}
		items[index] = detached
	}
	return threadItemWindow{first: first, last: last, hasBefore: first > 0, hasAfter: last < count, items: items}, nil
}

// boundedThreadText returns an exact UTF-8 prefix and detaches it from a giant
// source. Partial Markdown is raw preview text, never an independent document.
func boundedThreadText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return strings.Clone(text), false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return strings.Clone(text[:end]), true
}

// Truncate the source before detaching it. In particular, private source-entry
// membership can be arbitrarily large for one segmented assistant response.
func cloneWindowItems(items []ThreadItem) []ThreadItem {
	return cloneWindowItemsLimit(items, threadDisplayTextBytes)
}

func cloneWindowItemsLimit(items []ThreadItem, limit int) []ThreadItem {
	out := make([]ThreadItem, len(items))
	for i, item := range items {
		out[i] = boundedWindowItem(item, limit)
	}
	return cloneThreadItems(out)
}

// windowTurns borrows canonical turn indexes and tests only the selected item
// identities against existing membership. A single enormous turn therefore
// does not copy its complete IntermediateItemIDs into every window update.
func (actor *threadRuntimeState) windowTurns(selected map[string]bool) []ThreadTurn {
	byTurn := make(map[string][]string)
	for id := range selected {
		if index := actor.itemIndexByID(id); index >= 0 {
			item := actor.state.view.Items[index]
			if item.TurnID != "" {
				byTurn[item.TurnID.String()] = append(byTurn[item.TurnID.String()], id)
			}
		}
	}
	out := make([]ThreadTurn, 0, len(byTurn))
	for id, ids := range byTurn {
		if actor.canonical == nil {
			// Only synthetic actors lack the canonical reducer. Normal runtime
			// initialization always installs it together with the accepted view.
			for _, turn := range actor.state.view.Turns {
				if turn.TurnID.String() != id {
					continue
				}
				turn.IntermediateItemIDs = selectedThreadIDs(turn.IntermediateItemIDs, selected)
				turn.FinalItemIDs = selectedThreadIDs(turn.FinalItemIDs, selected)
				out = append(out, turn)
			}
			continue
		}
		facts := actor.canonical.turns.turns[id]
		if facts == nil {
			continue
		}
		turn := facts.turn
		turn.IntermediateItemIDs, turn.FinalItemIDs = nil, []string{}
		run := facts.runs[turn.RunID.String()]
		for _, itemID := range ids {
			if run != nil && run.seen[itemID] {
				turn.IntermediateItemIDs = append(turn.IntermediateItemIDs, itemID)
			}
			if facts.finalItems[itemID] {
				turn.FinalItemIDs = append(turn.FinalItemIDs, itemID)
			}
		}
		less := func(ids []string) {
			slices.SortFunc(ids, func(a, b string) int { return actor.itemIndexByID(a) - actor.itemIndexByID(b) })
		}
		less(turn.IntermediateItemIDs)
		less(turn.FinalItemIDs)
		out = append(out, turn)
	}
	slices.SortFunc(out, func(a, b ThreadTurn) int {
		if actor.canonical != nil {
			return actor.canonical.turns.indices[a.TurnID.String()] - actor.canonical.turns.indices[b.TurnID.String()]
		}
		return a.StartedAt.Compare(b.StartedAt)
	})
	return cloneThreadTurns(out)
}

func selectedThreadIDs(ids []string, selected map[string]bool) []string {
	out := []string{}
	for _, id := range ids {
		if selected[id] {
			out = append(out, id)
		}
	}
	return out
}

// Pending controls remain reachable independently of a reading window. Resolved
// controls are included only when their canonical timeline item is selected.
func (actor *threadRuntimeState) windowInteractions(selected map[string]bool) []ThreadInteraction {
	actor.ensureInteractionIndex()
	ids := make(map[string]bool, len(actor.pendingInteractionIDs))
	for id := range actor.pendingInteractionIDs {
		ids[id] = true
	}
	for id := range selected {
		if index := actor.itemIndexByID(id); index >= 0 {
			if interaction := actor.state.view.Items[index].Interaction; interaction != nil {
				ids[interaction.ID] = true
			}
		}
	}
	positions := make([]int, 0, len(ids))
	for id := range ids {
		if position, ok := actor.interactionIndex[id]; ok {
			positions = append(positions, position)
		}
	}
	slices.Sort(positions)
	out := make([]ThreadInteraction, 0, len(positions))
	for _, position := range positions {
		out = append(out, cloneThreadInteraction(actor.state.view.Interactions[position]))
	}
	return out
}
