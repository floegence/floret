package runtime

import (
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadTurn contains canonical facts about one admitted conversational turn.
// Durations settle only with a terminal marker, exclude queue time, and deduct
// the union of user-input and approval waits. An unavailable final boundary is
// distinct from a recorded final boundary with no assistant text.
type ThreadTurn struct {
	TurnID         identity.TurnID `json:"turn_id"`
	RunID          identity.RunID  `json:"run_id"`
	Kind           TurnKind        `json:"kind,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	EndedAt        *time.Time      `json:"ended_at,omitempty"`
	Outcome        *TurnOutcome    `json:"outcome,omitempty"`
	ElapsedMS      int64           `json:"elapsed_ms"`
	WaitingMS      int64           `json:"waiting_ms"`
	WorkDurationMS int64           `json:"work_duration_ms"`
	// IntermediateItemIDs identifies segmented assistant text superseded by
	// canonical tool activity or a semantic continuation. Trailing partial text
	// and unsegmented legacy text remain unclassified.
	IntermediateItemIDs   []string `json:"intermediate_item_ids,omitempty"`
	FinalItemIDs          []string `json:"final_item_ids"`
	FinalBoundaryComplete bool     `json:"final_boundary_complete"`
	segmented             bool
}

func cloneThreadTurns(turns []ThreadTurn) []ThreadTurn {
	if turns == nil {
		return nil
	}
	out := append([]ThreadTurn(nil), turns...)
	for i := range out {
		out[i].IntermediateItemIDs = append([]string(nil), turns[i].IntermediateItemIDs...)
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
	itemByEntry := make(map[string]ThreadItem)
	for _, item := range items {
		for _, id := range item.sourceEntryIDs {
			itemByEntry[id] = item
		}
	}
	projection := threadTurnProjection{}
	for _, entry := range entries {
		if err := projection.observe(entry, func(id string) (ThreadItem, bool) { item, found := itemByEntry[id]; return item, found }); err != nil {
			return nil, err
		}
	}
	return projection.snapshot()
}
