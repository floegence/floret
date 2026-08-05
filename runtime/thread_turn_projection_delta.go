package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v3/identity"
)

// ThreadTurnProjectionDelta is one validated incremental replacement over a
// previously observed turn projection.
type ThreadTurnProjectionDelta struct {
	ThreadID           identity.ThreadID                   `json:"thread_id"`
	TurnID             identity.TurnID                     `json:"turn_id"`
	RunID              identity.RunID                      `json:"run_id"`
	TraceID            identity.TraceID                    `json:"trace_id,omitempty"`
	BaseThroughOrdinal int64                               `json:"base_through_ordinal"`
	ThroughOrdinal     int64                               `json:"through_ordinal"`
	Status             TurnStatus                          `json:"status"`
	SegmentCount       int                                 `json:"segment_count"`
	Changes            []ThreadTurnProjectionSegmentChange `json:"changes,omitempty"`
	ProjectedAt        time.Time                           `json:"projected_at,omitempty"`
}

// ThreadTurnProjectionSegmentChange replaces one segment at its stable index.
type ThreadTurnProjectionSegmentChange struct {
	Index   int                         `json:"index"`
	Segment ThreadTurnProjectionSegment `json:"segment"`
}

// Validate checks the standalone shape of one projection delta. Apply also
// verifies its base identity and ordinal against the previous projection.
func (delta ThreadTurnProjectionDelta) Validate() error {
	if strings.TrimSpace(string(delta.ThreadID)) == "" || strings.TrimSpace(string(delta.TurnID)) == "" || strings.TrimSpace(string(delta.RunID)) == "" {
		return errors.New("turn projection delta identity is incomplete")
	}
	if delta.BaseThroughOrdinal < 0 {
		return fmt.Errorf("turn projection delta base ordinal must not be negative, got %d", delta.BaseThroughOrdinal)
	}
	if delta.ThroughOrdinal <= delta.BaseThroughOrdinal {
		return fmt.Errorf("turn projection delta ordinal %d must advance beyond base %d", delta.ThroughOrdinal, delta.BaseThroughOrdinal)
	}
	if !delta.Status.Valid() {
		return fmt.Errorf("unsupported turn projection delta status %q", delta.Status)
	}
	if delta.SegmentCount < 0 {
		return fmt.Errorf("turn projection delta segment count must not be negative, got %d", delta.SegmentCount)
	}
	lastIndex := -1
	for changeIndex, change := range delta.Changes {
		if change.Index < 0 || change.Index >= delta.SegmentCount {
			return fmt.Errorf("turn projection delta change %d index %d is outside segment count %d", changeIndex, change.Index, delta.SegmentCount)
		}
		if change.Index <= lastIndex {
			return fmt.Errorf("turn projection delta changes must use strictly increasing indexes, got %d after %d", change.Index, lastIndex)
		}
		candidate := ThreadTurnProjection{
			ThreadID: delta.ThreadID, TurnID: delta.TurnID, RunID: delta.RunID, TraceID: delta.TraceID,
			Status: delta.Status, ThroughOrdinal: delta.ThroughOrdinal,
			Segments: []ThreadTurnProjectionSegment{cloneThreadTurnProjectionSegment(change.Segment)},
		}
		if err := candidate.Validate(); err != nil {
			return fmt.Errorf("turn projection delta change %d: %w", changeIndex, err)
		}
		lastIndex = change.Index
	}
	return nil
}

// DiffThreadTurnProjections returns the minimal segment replacement delta from
// previous to current. A nil previous projection denotes the initial frame.
func DiffThreadTurnProjections(previous *ThreadTurnProjection, current ThreadTurnProjection) (ThreadTurnProjectionDelta, error) {
	if err := current.Validate(); err != nil {
		return ThreadTurnProjectionDelta{}, fmt.Errorf("current turn projection: %w", err)
	}
	baseOrdinal := int64(0)
	if previous != nil {
		if err := previous.Validate(); err != nil {
			return ThreadTurnProjectionDelta{}, fmt.Errorf("previous turn projection: %w", err)
		}
		if !threadTurnProjectionIdentityEqual(*previous, current) {
			return ThreadTurnProjectionDelta{}, errors.New("turn projection delta identity mismatch")
		}
		baseOrdinal = previous.ThroughOrdinal
	}
	delta := ThreadTurnProjectionDelta{
		ThreadID: current.ThreadID, TurnID: current.TurnID, RunID: current.RunID, TraceID: current.TraceID,
		BaseThroughOrdinal: baseOrdinal, ThroughOrdinal: current.ThroughOrdinal,
		Status: current.Status, SegmentCount: len(current.Segments), ProjectedAt: current.ProjectedAt,
	}
	for index, segment := range current.Segments {
		if previous != nil && index < len(previous.Segments) && reflect.DeepEqual(previous.Segments[index], segment) {
			continue
		}
		delta.Changes = append(delta.Changes, ThreadTurnProjectionSegmentChange{
			Index: index, Segment: cloneThreadTurnProjectionSegment(segment),
		})
	}
	if err := delta.Validate(); err != nil {
		return ThreadTurnProjectionDelta{}, err
	}
	return delta, nil
}

// ApplyThreadTurnProjectionDelta validates and applies one incremental
// replacement. A nil previous projection accepts only an initial base ordinal.
func ApplyThreadTurnProjectionDelta(previous *ThreadTurnProjection, delta ThreadTurnProjectionDelta) (ThreadTurnProjection, error) {
	if err := delta.Validate(); err != nil {
		return ThreadTurnProjection{}, err
	}
	if previous == nil {
		if delta.BaseThroughOrdinal != 0 {
			return ThreadTurnProjection{}, fmt.Errorf("initial turn projection delta base ordinal must be zero, got %d", delta.BaseThroughOrdinal)
		}
	} else {
		if err := previous.Validate(); err != nil {
			return ThreadTurnProjection{}, fmt.Errorf("previous turn projection: %w", err)
		}
		if !threadTurnProjectionDeltaMatchesPrevious(delta, *previous) {
			return ThreadTurnProjection{}, errors.New("turn projection delta identity mismatch")
		}
		if delta.BaseThroughOrdinal != previous.ThroughOrdinal {
			return ThreadTurnProjection{}, fmt.Errorf("turn projection delta base ordinal %d does not match previous ordinal %d", delta.BaseThroughOrdinal, previous.ThroughOrdinal)
		}
	}

	var segments []ThreadTurnProjectionSegment
	if delta.SegmentCount > 0 {
		segments = make([]ThreadTurnProjectionSegment, delta.SegmentCount)
	}
	if previous != nil {
		for index := 0; index < len(previous.Segments) && index < len(segments); index++ {
			segments[index] = cloneThreadTurnProjectionSegment(previous.Segments[index])
		}
	}
	for _, change := range delta.Changes {
		segments[change.Index] = cloneThreadTurnProjectionSegment(change.Segment)
	}
	out := ThreadTurnProjection{
		ThreadID: delta.ThreadID, TurnID: delta.TurnID, RunID: delta.RunID, TraceID: delta.TraceID,
		Status: delta.Status, Segments: segments, ThroughOrdinal: delta.ThroughOrdinal, ProjectedAt: delta.ProjectedAt,
	}
	if err := out.Validate(); err != nil {
		return ThreadTurnProjection{}, fmt.Errorf("applied turn projection delta: %w", err)
	}
	return out, nil
}

func threadTurnProjectionIdentityEqual(left, right ThreadTurnProjection) bool {
	return left.ThreadID == right.ThreadID && left.TurnID == right.TurnID && left.RunID == right.RunID && left.TraceID == right.TraceID
}

func threadTurnProjectionDeltaMatchesPrevious(delta ThreadTurnProjectionDelta, previous ThreadTurnProjection) bool {
	return delta.ThreadID == previous.ThreadID && delta.TurnID == previous.TurnID && delta.RunID == previous.RunID && delta.TraceID == previous.TraceID
}
