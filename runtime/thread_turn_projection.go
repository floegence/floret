package runtime

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessionlifecycle"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// threadTurnProjection retains presentation facts and open waits, never journal
// messages. One append visits only its turn and semantic boundary dependencies.
type threadTurnProjection struct {
	order   []string
	turns   map[string]*threadTurnFacts
	indices map[string]int
}
type threadTurnFacts struct {
	turn      ThreadTurn
	lifecycle sessionlifecycle.Projection
	terminal  *sessiontree.Entry
	runs      map[string]*threadTurnRunFacts
	waits     threadWaitProjection
	finalErr  error
}
type threadTurnRunFacts struct {
	pending      []string
	intermediate []string
	seen         map[string]bool
	err          error
}
type threadWaitInterval struct{ start, end time.Time }
type threadWaitProjection struct {
	pending   map[string]time.Time
	intervals []threadWaitInterval
}

func (projection *threadTurnProjection) observe(entry sessiontree.Entry, itemForEntry func(string) (ThreadItem, bool)) error {
	if projection.turns == nil {
		projection.turns = map[string]*threadTurnFacts{}
		projection.indices = map[string]int{}
	}
	facts := projection.turns[entry.TurnID]
	if facts == nil && entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
		at, err := sessiontree.EntryOccurrenceTime(entry)
		if err != nil {
			return runtimeHostError(err)
		}
		facts = &threadTurnFacts{turn: ThreadTurn{TurnID: identity.TurnID(entry.TurnID), Kind: TurnKind(entry.Metadata[sessiontree.TurnKindMetadataKey]), StartedAt: at, FinalItemIDs: []string{}, segmented: entry.Metadata[sessiontree.ResponseSegmentsMetadataKey] == "1"}, runs: map[string]*threadTurnRunFacts{}}
		projection.turns[entry.TurnID] = facts
		projection.indices[entry.TurnID] = len(projection.order)
		projection.order = append(projection.order, entry.TurnID)
	}
	if facts == nil {
		return nil
	}
	facts.lifecycle.Observe(entry)
	if entry.Type != sessiontree.EntryCustom && strings.TrimSpace(entry.RunID) != "" {
		facts.turn.RunID = identity.RunID(strings.TrimSpace(entry.RunID))
	}
	at, err := sessiontree.EntryOccurrenceTime(entry)
	if err != nil {
		return runtimeHostError(err)
	}
	facts.waits.observe(entry, at)
	if facts.turn.segmented {
		run := facts.runs[entry.RunID]
		if run == nil {
			run = &threadTurnRunFacts{seen: map[string]bool{}}
			facts.runs[entry.RunID] = run
		}
		if entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Kind != "control_signal" && strings.TrimSpace(entry.Message.Content) != "" {
			item, found := itemForEntry(entry.ID)
			if !found || item.TurnID != facts.turn.TurnID || item.RunID.String() != entry.RunID || item.Kind != ThreadItemAssistant {
				run.err = fmt.Errorf("%w: intermediate response is outside its canonical turn", ErrAuthorityCorrupt)
			} else {
				run.pending = append(run.pending, item.ID)
			}
		}
		boundary := entry.Type == sessiontree.EntryToolCall && entry.Message.Kind != "control_signal" && entry.Message.Kind != session.MessageKindToolValidationError || entry.Type == sessiontree.EntryCustom && entry.Metadata["kind"] == sessiontree.HostedToolEntryKind || entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnSavePoint && entry.Metadata["reason"] == "context_continue"
		if boundary {
			for _, id := range run.pending {
				if !run.seen[id] {
					run.intermediate = append(run.intermediate, id)
					run.seen[id] = true
				}
			}
			run.pending = nil
		}
	}
	if entry.Type == sessiontree.EntryTurnMarker && (entry.TurnStatus == sessiontree.TurnCompleted || entry.TurnStatus == sessiontree.TurnFailed || entry.TurnStatus == sessiontree.TurnAborted) {
		// Keep only the exact terminal facts used by presentation; no body or raw.
		terminal := sessiontree.Entry{Type: entry.Type, TurnID: entry.TurnID, RunID: entry.RunID, TurnStatus: entry.TurnStatus, CreatedAt: entry.CreatedAt, Metadata: map[string]string{}}
		for _, key := range []string{sessiontree.FinalResponseEntriesMetadataKey, sessiontree.OccurrenceTimeMetadataKey} {
			if value, ok := entry.Metadata[key]; ok {
				terminal.Metadata[key] = value
			}
		}
		facts.terminal = &terminal
	}
	return facts.refresh(itemForEntry)
}

func (facts *threadTurnFacts) refresh(itemForEntry func(string) (ThreadItem, bool)) error {
	turn := &facts.turn
	turn.EndedAt, turn.Outcome = nil, nil
	turn.ElapsedMS, turn.WaitingMS, turn.WorkDurationMS = 0, 0, 0
	turn.FinalItemIDs = []string{}
	turn.FinalBoundaryComplete = false
	facts.finalErr = nil
	switch facts.lifecycle.Snapshot(sessionlifecycle.PhaseTurn).Status() {
	case sessionlifecycle.StatusCompleted:
		value := TurnOutcomeCompleted
		turn.Outcome = &value
	case sessionlifecycle.StatusFailed:
		value := TurnOutcomeFailed
		turn.Outcome = &value
	case sessionlifecycle.StatusInterrupted:
		value := TurnOutcomeInterrupted
		turn.Outcome = &value
	case sessionlifecycle.StatusCancelled:
		value := TurnOutcomeCancelled
		turn.Outcome = &value
	}
	if facts.terminal != nil && turn.Outcome != nil {
		end, err := sessiontree.EntryOccurrenceTime(*facts.terminal)
		if err != nil {
			return runtimeHostError(err)
		}
		turn.EndedAt = &end
		turn.ElapsedMS = max(0, end.Sub(turn.StartedAt).Milliseconds())
		turn.WaitingMS = facts.waits.duration(turn.StartedAt, end)
		turn.WorkDurationMS = turn.ElapsedMS - turn.WaitingMS
		refs, known, err := sessiontree.FinalResponseEntryIDs(*facts.terminal)
		if err != nil {
			facts.finalErr = err
			return nil
		}
		if known && turn.segmented && *turn.Outcome == TurnOutcomeCompleted {
			seen := map[string]bool{}
			var lastOrdinal uint64
			for _, ref := range refs {
				item, found := itemForEntry(ref)
				if !found || item.TurnID != turn.TurnID || item.RunID != turn.RunID || item.Kind != ThreadItemAssistant || strings.TrimSpace(item.Text) == "" || item.Ordinal < lastOrdinal {
					facts.finalErr = fmt.Errorf("%w: final response is outside its canonical turn", ErrAuthorityCorrupt)
					break
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
	turn.IntermediateItemIDs = nil
	if run := facts.runs[turn.RunID.String()]; turn.segmented && run != nil {
		turn.IntermediateItemIDs = run.intermediate
	}
	return nil
}

func (projection *threadTurnProjection) turnSnapshot(id string) (ThreadTurn, bool, error) {
	facts, found := projection.turns[id]
	if !found {
		return ThreadTurn{}, false, nil
	}
	if facts.finalErr != nil {
		return ThreadTurn{}, true, facts.finalErr
	}
	turn := facts.turn
	if run := facts.runs[turn.RunID.String()]; turn.segmented && run != nil {
		if run.err != nil {
			return ThreadTurn{}, true, run.err
		}
		for _, id := range turn.FinalItemIDs {
			if run.seen[id] {
				return ThreadTurn{}, true, fmt.Errorf("%w: intermediate and final response overlap", ErrAuthorityCorrupt)
			}
		}
	}
	return cloneThreadTurns([]ThreadTurn{turn})[0], true, nil
}

func (projection *threadTurnProjection) snapshot() ([]ThreadTurn, error) {
	out := make([]ThreadTurn, 0, len(projection.order))
	for _, id := range projection.order {
		turn, _, err := projection.turnSnapshot(id)
		if err != nil {
			return nil, err
		}
		out = append(out, turn)
	}
	return out, nil
}

func (waits *threadWaitProjection) observe(entry sessiontree.Entry, at time.Time) {
	if waits.pending == nil {
		waits.pending = map[string]time.Time{}
	}
	switch entry.Type {
	case sessiontree.EntryInteractionAsked:
		id := strings.TrimPrefix(entry.ID, "interaction-requested:")
		if id == "" {
			id = entry.RequestKey
		}
		if _, exists := waits.pending[id]; !exists {
			waits.pending[id] = at
		}
	case sessiontree.EntryToolCall:
		if controlstate.Classify(entry.Message.ControlSignal) == controlstate.WaitingInput {
			id := entry.Message.ControlSignal.CallID
			if _, exists := waits.pending[id]; !exists {
				waits.pending[id] = at
			}
		}
	case sessiontree.EntryToolResult:
		if entry.Message.ToolResult != nil && entry.Message.ToolResult.InputRequired != nil {
			waits.pending[sessiontree.ToolInputInteractionID(entry.ID)] = at
		}
	case sessiontree.EntryInteractionDone:
		id := strings.TrimPrefix(entry.ID, "interaction-resolved:")
		if id == "" {
			id = entry.RequestKey
		}
		if asked, found := waits.pending[id]; found {
			waits.insert(threadWaitInterval{asked, at})
			delete(waits.pending, id)
		}
	}
}

func (waits *threadWaitProjection) insert(interval threadWaitInterval) {
	// Keep a disjoint ordered union. Reversed occurrence times are zero work,
	// matching the original clipped union without depending on wall-clock order.
	if !interval.end.After(interval.start) {
		return
	}
	first := sort.Search(len(waits.intervals), func(i int) bool { return !waits.intervals[i].end.Before(interval.start) })
	last := first
	for last < len(waits.intervals) && !waits.intervals[last].start.After(interval.end) {
		if waits.intervals[last].start.Before(interval.start) {
			interval.start = waits.intervals[last].start
		}
		if waits.intervals[last].end.After(interval.end) {
			interval.end = waits.intervals[last].end
		}
		last++
	}
	waits.intervals = append(waits.intervals, threadWaitInterval{})
	copy(waits.intervals[first+1:], waits.intervals[last:len(waits.intervals)-1])
	waits.intervals = waits.intervals[:len(waits.intervals)-(last-first)]
	waits.intervals[first] = interval
}
func (waits *threadWaitProjection) duration(start, end time.Time) int64 {
	var duration time.Duration
	for _, interval := range waits.intervals {
		left, right := interval.start, interval.end
		if left.Before(start) {
			left = start
		}
		if right.After(end) {
			right = end
		}
		if right.After(left) {
			duration += right.Sub(left)
		}
	}
	// Pending intervals all end at the terminal, so their union is one suffix.
	earliest := end
	for _, at := range waits.pending {
		if at.Before(earliest) {
			earliest = at
		}
	}
	if earliest.Before(start) {
		earliest = start
	}
	if earliest.Before(end) {
		duration += end.Sub(earliest)
		for _, interval := range waits.intervals {
			left, right := interval.start, interval.end
			if left.Before(earliest) {
				left = earliest
			}
			if right.After(end) {
				right = end
			}
			if right.After(left) {
				duration -= right.Sub(left)
			}
		}
	}
	return min(max(0, end.Sub(start).Milliseconds()), duration.Milliseconds())
}
