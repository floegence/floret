package runtime

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadTurn contains canonical facts about one admitted conversational turn.
// Durations settle only with a terminal marker, exclude queue time, and deduct
// the union of user-input and approval waits. An unavailable final boundary is
// distinct from a recorded final boundary with no assistant text.
type ThreadTurn struct {
	TurnID                identity.TurnID `json:"turn_id"`
	RunID                 identity.RunID  `json:"run_id"`
	Kind                  TurnKind        `json:"kind,omitempty"`
	StartedAt             time.Time       `json:"started_at"`
	EndedAt               *time.Time      `json:"ended_at,omitempty"`
	Outcome               *TurnOutcome    `json:"outcome,omitempty"`
	ElapsedMS             int64           `json:"elapsed_ms"`
	WaitingMS             int64           `json:"waiting_ms"`
	WorkDurationMS        int64           `json:"work_duration_ms"`
	FinalItemIDs          []string        `json:"final_item_ids"`
	FinalBoundaryComplete bool            `json:"final_boundary_complete"`
	segmented             bool
}

func cloneThreadTurns(turns []ThreadTurn) []ThreadTurn {
	if turns == nil {
		return nil
	}
	out := append([]ThreadTurn(nil), turns...)
	for i := range out {
		out[i].FinalItemIDs = append([]string{}, turns[i].FinalItemIDs...)
		if turns[i].Outcome != nil {
			value := *turns[i].Outcome
			out[i].Outcome = &value
		}
		if turns[i].EndedAt != nil {
			value := *turns[i].EndedAt
			out[i].EndedAt = &value
		}
	}
	return out
}

func threadRuntimeTurnsFromEntries(entries []sessiontree.Entry, items []ThreadItem) ([]ThreadTurn, error) {
	type turnEntries struct {
		start   sessiontree.Entry
		entries []sessiontree.Entry
	}
	byTurn := make(map[string]*turnEntries)
	order := make([]string, 0)
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			if _, found := byTurn[entry.TurnID]; !found {
				byTurn[entry.TurnID] = &turnEntries{start: entry}
				order = append(order, entry.TurnID)
			}
		}
		if turn := byTurn[entry.TurnID]; turn != nil {
			turn.entries = append(turn.entries, entry)
		}
	}
	itemByEntry := make(map[string]ThreadItem)
	for _, item := range items {
		for _, id := range item.sourceEntryIDs {
			itemByEntry[id] = item
		}
	}
	out := make([]ThreadTurn, 0, len(order))
	for _, id := range order {
		canonical := byTurn[id]
		started, err := sessiontree.EntryOccurrenceTime(canonical.start)
		if err != nil {
			return nil, err
		}
		_, runID, _, outcome, _ := threadRuntimeLifecycleFromEntries(sessiontree.ThreadMeta{}, canonical.entries)
		turn := ThreadTurn{TurnID: identity.TurnID(id), RunID: runID, StartedAt: started,
			Kind: TurnKind(canonical.start.Metadata[sessiontree.TurnKindMetadataKey]), FinalItemIDs: []string{},
			segmented: canonical.start.Metadata[sessiontree.ResponseSegmentsMetadataKey] == "1"}
		var terminal *sessiontree.Entry
		for i := range canonical.entries {
			entry := &canonical.entries[i]
			if entry.Type == sessiontree.EntryTurnMarker && (entry.TurnStatus == sessiontree.TurnCompleted || entry.TurnStatus == sessiontree.TurnFailed || entry.TurnStatus == sessiontree.TurnAborted) {
				terminal = entry
			}
		}
		if terminal != nil && outcome != nil {
			ended, err := sessiontree.EntryOccurrenceTime(*terminal)
			if err != nil {
				return nil, err
			}
			turn.EndedAt, turn.Outcome = &ended, outcome
			turn.ElapsedMS = max(0, ended.Sub(started).Milliseconds())
			turn.WaitingMS, err = turnWaitingDuration(canonical.entries, started, ended)
			if err != nil {
				return nil, err
			}
			turn.WorkDurationMS = turn.ElapsedMS - turn.WaitingMS
			refs, known, err := sessiontree.FinalResponseEntryIDs(*terminal)
			if err != nil {
				return nil, err
			}
			if known && turn.segmented && outcome != nil && *outcome == TurnOutcomeCompleted {
				seen := map[string]bool{}
				var lastOrdinal uint64
				for _, ref := range refs {
					item, found := itemByEntry[ref]
					if !found || item.TurnID != turn.TurnID || item.RunID != turn.RunID || item.Kind != ThreadItemAssistant || strings.TrimSpace(item.Text) == "" || item.Ordinal < lastOrdinal {
						return nil, fmt.Errorf("%w: final response is outside its canonical turn", ErrAuthorityCorrupt)
					}
					if !seen[item.ID] {
						turn.FinalItemIDs = append(turn.FinalItemIDs, item.ID)
						seen[item.ID] = true
					}
					lastOrdinal = item.Ordinal
				}
				turn.FinalBoundaryComplete = true
			}
		}
		out = append(out, turn)
	}
	return out, nil
}

func turnWaitingDuration(entries []sessiontree.Entry, start, end time.Time) (int64, error) {
	type interval struct{ start, end time.Time }
	pending := map[string]time.Time{}
	intervals := []interval{}
	for _, entry := range entries {
		at, err := sessiontree.EntryOccurrenceTime(entry)
		if err != nil {
			return 0, err
		}
		switch entry.Type {
		case sessiontree.EntryInteractionAsked:
			id := strings.TrimPrefix(entry.ID, "interaction-requested:")
			if id == "" {
				id = entry.RequestKey
			}
			if _, exists := pending[id]; !exists {
				pending[id] = at
			}
		case sessiontree.EntryToolCall:
			if controlstate.Classify(entry.Message.ControlSignal) == controlstate.WaitingInput {
				id := entry.Message.ControlSignal.CallID
				if _, exists := pending[id]; !exists {
					pending[id] = at
				}
			}
		case sessiontree.EntryToolResult:
			if entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
				pending[sessiontree.ToolInputInteractionID(entry.ID)] = at
			}
		case sessiontree.EntryInteractionDone:
			id := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			if id == "" {
				id = entry.RequestKey
			}
			if asked, found := pending[id]; found {
				intervals = append(intervals, interval{asked, at})
				delete(pending, id)
			}
		}
	}
	for _, asked := range pending {
		intervals = append(intervals, interval{asked, end})
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
	var duration time.Duration
	cursor := start
	for _, wait := range intervals {
		left, right := wait.start, wait.end
		if left.Before(cursor) {
			left = cursor
		}
		if right.After(end) {
			right = end
		}
		if right.After(left) {
			duration += right.Sub(left)
			cursor = right
		}
	}
	return min(max(0, end.Sub(start).Milliseconds()), duration.Milliseconds()), nil
}
