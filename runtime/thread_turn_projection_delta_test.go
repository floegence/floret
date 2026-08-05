package runtime

import (
	"reflect"
	"testing"
	"time"
)

func TestThreadTurnProjectionDeltaRoundTrip(t *testing.T) {
	t.Parallel()
	previous := ThreadTurnProjection{
		ThreadID: "thread-delta", TurnID: "turn-delta", RunID: "run-delta", TraceID: "trace-delta",
		Status: TurnStatusRunning, ThroughOrdinal: 3, ProjectedAt: time.UnixMilli(3000).UTC(),
		Segments: []ThreadTurnProjectionSegment{
			{Kind: ThreadTurnProjectionSegmentAssistantText, Text: "before", EventIDs: []string{"event-1"}},
			{Kind: ThreadTurnProjectionSegmentAssistantText, Text: "working", EventIDs: []string{"event-2"}},
		},
	}
	current := ThreadTurnProjection{
		ThreadID: previous.ThreadID, TurnID: previous.TurnID, RunID: previous.RunID, TraceID: previous.TraceID,
		Status: TurnStatusCompleted, ThroughOrdinal: 7, ProjectedAt: time.UnixMilli(7000).UTC(),
		Segments: []ThreadTurnProjectionSegment{
			previous.Segments[0],
			{Kind: ThreadTurnProjectionSegmentAssistantText, Text: "done", EventIDs: []string{"event-2", "event-3"}},
			{Kind: ThreadTurnProjectionSegmentControlSignal, Text: "complete", EventIDs: []string{"event-4"}},
		},
	}

	delta, err := DiffThreadTurnProjections(&previous, current)
	if err != nil {
		t.Fatalf("diff projection: %v", err)
	}
	if delta.BaseThroughOrdinal != previous.ThroughOrdinal || delta.ThroughOrdinal != current.ThroughOrdinal || len(delta.Changes) != 2 {
		t.Fatalf("delta=%#v", delta)
	}
	applied, err := ApplyThreadTurnProjectionDelta(&previous, delta)
	if err != nil {
		t.Fatalf("apply projection delta: %v", err)
	}
	if !reflect.DeepEqual(applied, current) {
		t.Fatalf("applied projection mismatch\n got: %#v\nwant: %#v", applied, current)
	}

	delta.Changes[0].Segment.Text = "mutated"
	if previous.Segments[1].Text != "working" || current.Segments[1].Text != "done" || applied.Segments[1].Text != "done" {
		t.Fatal("projection delta aliases caller-owned segments")
	}
}

func TestThreadTurnProjectionDeltaInitialAndTruncatedRoundTrips(t *testing.T) {
	t.Parallel()
	initial := ThreadTurnProjection{
		ThreadID: "thread-initial", TurnID: "turn-initial", RunID: "run-initial",
		Status: TurnStatusRunning, ThroughOrdinal: 1, ProjectedAt: time.UnixMilli(1000).UTC(),
		Segments: []ThreadTurnProjectionSegment{{Kind: ThreadTurnProjectionSegmentAssistantText, Text: "hello"}},
	}
	delta, err := DiffThreadTurnProjections(nil, initial)
	if err != nil {
		t.Fatalf("initial diff: %v", err)
	}
	if delta.BaseThroughOrdinal != 0 || len(delta.Changes) != 1 {
		t.Fatalf("initial delta=%#v", delta)
	}
	applied, err := ApplyThreadTurnProjectionDelta(nil, delta)
	if err != nil || !reflect.DeepEqual(applied, initial) {
		t.Fatalf("initial apply=(%#v, %v), want %#v", applied, err, initial)
	}

	truncated := initial
	truncated.Status = TurnStatusCompleted
	truncated.ThroughOrdinal = 2
	truncated.ProjectedAt = time.UnixMilli(2000).UTC()
	truncated.Segments = nil
	delta, err = DiffThreadTurnProjections(&initial, truncated)
	if err != nil {
		t.Fatalf("truncated diff: %v", err)
	}
	if delta.SegmentCount != 0 || len(delta.Changes) != 0 {
		t.Fatalf("truncated delta=%#v", delta)
	}
	applied, err = ApplyThreadTurnProjectionDelta(&initial, delta)
	if err != nil || !reflect.DeepEqual(applied, truncated) {
		t.Fatalf("truncated apply=(%#v, %v), want %#v", applied, err, truncated)
	}
}

func TestThreadTurnProjectionDeltaRejectsInvalidContracts(t *testing.T) {
	t.Parallel()
	previous := ThreadTurnProjection{
		ThreadID: "thread-invalid", TurnID: "turn-invalid", RunID: "run-invalid",
		Status: TurnStatusRunning, ThroughOrdinal: 4,
		Segments: []ThreadTurnProjectionSegment{{Kind: ThreadTurnProjectionSegmentAssistantText, Text: "hello"}},
	}
	valid := ThreadTurnProjectionDelta{
		ThreadID: previous.ThreadID, TurnID: previous.TurnID, RunID: previous.RunID,
		BaseThroughOrdinal: 4, ThroughOrdinal: 5, Status: TurnStatusRunning, SegmentCount: 1,
		Changes: []ThreadTurnProjectionSegmentChange{{Index: 0, Segment: previous.Segments[0]}},
	}
	tests := []struct {
		name  string
		prior *ThreadTurnProjection
		delta ThreadTurnProjectionDelta
	}{
		{name: "initial nonzero base", delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.BaseThroughOrdinal = 1 })},
		{name: "base mismatch", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.BaseThroughOrdinal = 3 })},
		{name: "identity mismatch", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.ThreadID = "other" })},
		{name: "non advancing ordinal", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.ThroughOrdinal = 4 })},
		{name: "negative segment count", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.SegmentCount = -1 })},
		{name: "out of range change", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.Changes[0].Index = 1 })},
		{name: "duplicate change", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) { delta.Changes = append(delta.Changes, delta.Changes[0]) })},
		{name: "unordered change", prior: &previous, delta: withProjectionDelta(valid, func(delta *ThreadTurnProjectionDelta) {
			delta.SegmentCount = 2
			delta.Changes = []ThreadTurnProjectionSegmentChange{{Index: 1, Segment: previous.Segments[0]}, {Index: 0, Segment: previous.Segments[0]}}
		})},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ApplyThreadTurnProjectionDelta(test.prior, test.delta); err == nil {
				t.Fatal("ApplyThreadTurnProjectionDelta unexpectedly succeeded")
			}
		})
	}
}

func withProjectionDelta(delta ThreadTurnProjectionDelta, mutate func(*ThreadTurnProjectionDelta)) ThreadTurnProjectionDelta {
	delta.Changes = append([]ThreadTurnProjectionSegmentChange(nil), delta.Changes...)
	mutate(&delta)
	return delta
}
