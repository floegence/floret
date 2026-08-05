package runtime

import (
	"strings"
	"time"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
)

const runtimeLiveProjectionMaxOpenEvents = 32

type runtimeLiveProjectionOpenKind uint8

const (
	runtimeLiveProjectionOpenNone runtimeLiveProjectionOpenKind = iota
	runtimeLiveProjectionOpenText
	runtimeLiveProjectionOpenActivity
	runtimeLiveProjectionOpenStandalone
)

type runtimeLiveTurnProjectionState struct {
	threadID identity.ThreadID
	turnID   identity.TurnID
	runID    identity.RunID
	traceID  identity.TraceID

	stableSegments []ThreadTurnProjectionSegment
	openEvents     []ThreadDetailEvent
	openKind       runtimeLiveProjectionOpenKind
	settlements    []ThreadDetailEvent
	joinStable     bool
	status         TurnStatus
	throughOrdinal int64
	lastProjection *ThreadTurnProjection
}

func newRuntimeLiveTurnProjectionState(threadID, turnID, runID string) *runtimeLiveTurnProjectionState {
	return &runtimeLiveTurnProjectionState{
		threadID: identity.ThreadID(threadID),
		turnID:   identity.TurnID(turnID),
		runID:    identity.RunID(runID),
		traceID:  identity.TraceID(runID),
	}
}

func (state *runtimeLiveTurnProjectionState) append(event ThreadDetailEvent) ThreadTurnProjection {
	state.noteOrdinalAndStatus(event)
	switch runtimeLiveProjectionEventKind(event) {
	case runtimeLiveProjectionOpenText:
		state.prepareOpen(runtimeLiveProjectionOpenText)
		state.openEvents = append(state.openEvents, event)
	case runtimeLiveProjectionOpenActivity:
		state.prepareOpen(runtimeLiveProjectionOpenActivity)
		state.openEvents = append(state.openEvents, event)
	case runtimeLiveProjectionOpenStandalone:
		state.freeze(true)
		state.openKind = runtimeLiveProjectionOpenStandalone
		state.openEvents = append(state.openEvents, event)
	default:
		state.applyNonRenderingEvent(event)
	}
	projection := state.project()
	state.lastProjection = &projection
	return projection
}

func (state *runtimeLiveTurnProjectionState) noteOrdinalAndStatus(event ThreadDetailEvent) {
	if event.Ordinal > state.throughOrdinal {
		state.throughOrdinal = event.Ordinal
	}
	if event.Kind == ThreadDetailEventError {
		state.status = TurnStatusFailed
	}
	if event.Kind != ThreadDetailEventTurnMarker || event.TurnMarker == nil {
		return
	}
	switch strings.TrimSpace(event.TurnMarker.Status) {
	case "started":
		state.status = TurnStatusRunning
	case "completed":
		state.status = TurnStatusCompleted
	case "waiting":
		state.status = TurnStatusWaiting
	case "failed":
		state.status = TurnStatusFailed
	case "aborted":
		state.status = TurnStatusCancelled
	}
}

func runtimeLiveProjectionEventKind(event ThreadDetailEvent) runtimeLiveProjectionOpenKind {
	switch event.Kind {
	case ThreadDetailEventAssistantMessage:
		if event.Message == nil {
			return runtimeLiveProjectionOpenNone
		}
		if strings.TrimSpace(event.Message.Kind) == "control_signal" {
			return runtimeLiveProjectionOpenStandalone
		}
		if strings.TrimSpace(threadTurnProjectionMessageText(event.Message)) != "" {
			return runtimeLiveProjectionOpenText
		}
	case ThreadDetailEventToolCall:
		if event.ToolCall != nil {
			return runtimeLiveProjectionOpenActivity
		}
	case ThreadDetailEventToolDispatch, ThreadDetailEventToolActivity, ThreadDetailEventApproval, ThreadDetailEventError:
		return runtimeLiveProjectionOpenActivity
	case ThreadDetailEventToolResult:
		if event.ToolResult != nil && event.Type != threadTurnProjectionPendingToolSettlementType {
			return runtimeLiveProjectionOpenActivity
		}
	}
	return runtimeLiveProjectionOpenNone
}

func (state *runtimeLiveTurnProjectionState) prepareOpen(kind runtimeLiveProjectionOpenKind) {
	if state.openKind == runtimeLiveProjectionOpenStandalone ||
		(state.openKind != runtimeLiveProjectionOpenNone && state.openKind != kind) {
		state.freeze(true)
	}
	if state.openKind == kind && len(state.openEvents) >= runtimeLiveProjectionMaxOpenEvents {
		state.freeze(false)
	}
	state.openKind = kind
}

func (state *runtimeLiveTurnProjectionState) applyNonRenderingEvent(event ThreadDetailEvent) {
	if event.Kind == ThreadDetailEventToolResult && event.Type == threadTurnProjectionPendingToolSettlementType {
		if state.openKind == runtimeLiveProjectionOpenText || state.openKind == runtimeLiveProjectionOpenStandalone {
			state.freeze(true)
		}
		state.settlements = append(state.settlements, event)
		return
	}
	if event.Kind != ThreadDetailEventTurnMarker || event.TurnMarker == nil {
		return
	}
	if state.openKind == runtimeLiveProjectionOpenText || state.openKind == runtimeLiveProjectionOpenStandalone {
		state.freeze(true)
	}
	if threadTurnProjectionIsToolResultBatchSavePoint(event) {
		state.freeze(true)
		return
	}
	if _, _, ok := threadTurnProjectionTerminalSettlementStatus(event); ok {
		state.settlements = append(state.settlements, event)
	}
}

func (state *runtimeLiveTurnProjectionState) freeze(realBoundary bool) {
	if state.lastProjection != nil {
		state.stableSegments = state.lastProjection.Segments
	}
	state.openEvents = nil
	state.openKind = runtimeLiveProjectionOpenNone
	state.joinStable = !realBoundary
}

func (state *runtimeLiveTurnProjectionState) project() ThreadTurnProjection {
	projection := ThreadTurnProjection{
		ThreadID: state.threadID, TurnID: state.turnID, RunID: state.runID, TraceID: state.traceID,
		Status: state.status, ThroughOrdinal: state.throughOrdinal, ProjectedAt: time.Now().UTC(),
		Segments: cloneThreadTurnProjectionSegments(state.stableSegments),
	}
	if len(state.openEvents) > 0 {
		open := ProjectThreadTurn(ProjectThreadTurnRequest{
			ThreadID: state.threadID, TurnID: state.turnID, RunID: state.runID, TraceID: state.traceID,
			Events: cloneThreadDetailEvents(state.openEvents),
		})
		openSegments := cloneThreadTurnProjectionSegments(open.Segments)
		if state.joinStable {
			projection.Segments, openSegments = runtimeLiveJoinProjectionBoundary(projection.Segments, openSegments)
		}
		projection.Segments = append(projection.Segments, openSegments...)
	}
	var pending []ThreadDetailEvent
	var terminal []ThreadDetailEvent
	for _, settlement := range state.settlements {
		if settlement.Kind == ThreadDetailEventToolResult && settlement.Type == threadTurnProjectionPendingToolSettlementType {
			pending = append(pending, settlement)
		}
		if _, _, ok := threadTurnProjectionTerminalSettlementStatus(settlement); ok {
			terminal = append(terminal, settlement)
		}
	}
	threadTurnProjectionApplyPendingSettlements(&projection, pending)
	threadTurnProjectionApplyTerminalSettlements(&projection, terminal)
	threadTurnProjectionMergeDuplicateActivityItems(&projection)
	if len(projection.Segments) == 0 {
		projection.Segments = nil
	}
	if projection.Status == "" {
		projection.Status = TurnStatusRunning
	}
	return projection
}

func runtimeLiveJoinProjectionBoundary(stable, open []ThreadTurnProjectionSegment) ([]ThreadTurnProjectionSegment, []ThreadTurnProjectionSegment) {
	if len(stable) == 0 || len(open) == 0 {
		return stable, open
	}
	left := &stable[len(stable)-1]
	right := open[0]
	if left.Kind != right.Kind {
		return stable, open
	}
	switch left.Kind {
	case ThreadTurnProjectionSegmentAssistantText:
		left.Text += right.Text
		left.EventIDs = threadTurnProjectionMergeEventIDs(left.EventIDs, right.EventIDs)
		return stable, open[1:]
	case ThreadTurnProjectionSegmentActivityTimeline:
		if left.ActivityTimeline == nil || right.ActivityTimeline == nil {
			return stable, open
		}
		rightTimeline := observation.CloneActivityTimeline(right.ActivityTimeline)
		left.ActivityTimeline.Items = append(left.ActivityTimeline.Items, rightTimeline.Items...)
		left.ActivityTimeline.Summary = threadTurnProjectionActivitySummary(left.ActivityTimeline.Items)
		left.EventIDs = threadTurnProjectionMergeEventIDs(left.EventIDs, right.EventIDs)
		return stable, open[1:]
	default:
		return stable, open
	}
}

func cloneThreadTurnProjectionSegments(segments []ThreadTurnProjectionSegment) []ThreadTurnProjectionSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]ThreadTurnProjectionSegment, 0, len(segments))
	for _, segment := range segments {
		out = append(out, cloneThreadTurnProjectionSegment(segment))
	}
	return out
}
