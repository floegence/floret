package runtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
)

// threadRuntimeState is the only in-memory lifecycle owner for one thread.
// Provider and tool I/O must never run while its mutex is held.
type threadRuntimeState struct {
	threadID string
	mu       sync.Mutex
	closed   bool
	deleting bool
	deleted  bool
	state    threadRuntimeData
}

type threadRuntimeData struct {
	turnID              identity.TurnID
	runID               identity.RunID
	logicalRequestID    identity.LogicalRequestID
	attemptID           string
	attemptEpoch        int
	openTextSegmentID   string
	openTextKind        ThreadItemKind
	view                ThreadView
	cancel              context.CancelFunc
	cancelOwner         string
	executionDone       <-chan struct{}
	activeEffects       int
	effectsDone         chan struct{}
	requestKeys         map[string]threadRuntimeRequest
	agent               *Agent
	pendingInteractions map[string]*pendingThreadInteraction
	hydrationStarted    bool
}

type beginThreadRunInput struct {
	turnID           identity.TurnID
	runID            identity.RunID
	logicalRequestID identity.LogicalRequestID
	cancel           context.CancelFunc
	executionDone    <-chan struct{}
	clearOutcome     bool
}

// beginRun is the only in-memory transition that installs a new provider Run
// on an existing thread actor. Attempt and live-segment identities are scoped
// to one Run and must never leak into its successor.
func (runtime *threadRuntimeState) beginRun(input beginThreadRunInput) (<-chan struct{}, error) {
	if runtime == nil || input.turnID == "" || input.runID == "" || input.logicalRequestID == "" || input.cancel == nil || input.executionDone == nil {
		return nil, errors.New("new thread run identity and ownership are required")
	}
	runtime.finishLiveTextSegment()
	previousExecution := runtime.state.executionDone
	runtime.state.turnID = input.turnID
	runtime.state.runID = input.runID
	runtime.state.logicalRequestID = input.logicalRequestID
	runtime.state.attemptID = ""
	runtime.state.attemptEpoch = 0
	runtime.state.openTextSegmentID = ""
	runtime.state.openTextKind = ""
	runtime.state.cancel = input.cancel
	runtime.state.cancelOwner = "run:" + input.runID.String()
	runtime.state.executionDone = input.executionDone
	runtime.state.view.Activity = ThreadActivityActive
	runtime.state.view.TurnID = input.turnID
	runtime.state.view.RunID = input.runID
	runtime.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	if input.clearOutcome {
		runtime.state.view.LastOutcome = nil
		runtime.state.view.Failure = nil
	}
	runtime.state.view.ViewVersion++
	return previousExecution, nil
}

type pendingThreadInteraction struct {
	resolution chan InteractionResolution
}

func (runtime *threadRuntimeState) apply(ctx context.Context, mutate func() error) error {
	if runtime == nil || mutate == nil {
		return errors.New("thread runtime and mutation are required")
	}
	if ctx == nil {
		return errors.New("thread actor context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return ErrHostClosed
	}
	if runtime.deleting || runtime.deleted {
		return ErrThreadDeleted
	}
	return mutate()
}

// acceptLiveEvent applies the current provider-attempt fence and records the
// latest memory-only projection. It is called only from the actor mailbox.
func (runtime *threadRuntimeState) acceptLiveEvent(event Event) bool {
	if runtime == nil || runtime.deleting || runtime.deleted {
		return false
	}
	if runtime.state.view.Activity == ThreadActivityIdle && runtime.state.view.LastOutcome != nil {
		return false
	}
	if event.TurnID != "" && runtime.state.turnID != "" && event.TurnID != runtime.state.turnID {
		return false
	}
	if event.RunID != "" && runtime.state.runID != "" && event.RunID != runtime.state.runID {
		return false
	}
	if event.TurnID != "" {
		runtime.state.turnID = event.TurnID
	}
	if event.RunID != "" {
		runtime.state.runID = event.RunID
		runtime.state.view.RunID = event.RunID
	}
	logicalRequestID, attemptID, attemptEpoch, hasAttempt := liveEventAttemptIdentity(event)
	if hasAttempt {
		if logicalRequestID == "" || logicalRequestID != runtime.state.logicalRequestID {
			return false
		}
		switch {
		case runtime.state.attemptEpoch == 0 && attemptEpoch != 1:
			return false
		case attemptEpoch < runtime.state.attemptEpoch:
			return false
		case attemptEpoch == runtime.state.attemptEpoch && runtime.state.attemptID != "" && attemptID != runtime.state.attemptID:
			return false
		case attemptEpoch > runtime.state.attemptEpoch:
			runtime.finishLiveTextSegment()
		}
		runtime.state.attemptID = attemptID
		runtime.state.attemptEpoch = attemptEpoch
	}
	if event.Stream != nil {
		switch event.Stream.Type {
		case StreamObservationAssistantDelta:
			runtime.appendLiveTextSegment(ThreadItemAssistant, event.Stream.Text)
		case StreamObservationReasoningDelta:
			runtime.appendLiveTextSegment(ThreadItemThinking, event.Stream.Text)
		case StreamObservationToolCallStart, StreamObservationToolCallDelta:
			runtime.finishLiveTextSegment()
		case StreamObservationToolCallEnd:
			runtime.finishLiveTextSegment()
			runtime.appendLiveToolSegment(event.Stream.ToolCallStream)
		case StreamObservationModelRetry:
			runtime.finishLiveTextSegment()
		}
	}
	return true
}

func (runtime *threadRuntimeState) claimEffectDispatch() error {
	if runtime == nil {
		return ErrThreadDeleted
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return ErrHostClosed
	}
	if runtime.deleting || runtime.deleted {
		return ErrThreadDeleted
	}
	if runtime.state.activeEffects == 0 {
		runtime.state.effectsDone = make(chan struct{})
	}
	runtime.state.activeEffects++
	return nil
}

func (runtime *threadRuntimeState) releaseEffectDispatch() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.state.activeEffects <= 0 {
		return
	}
	runtime.state.activeEffects--
	if runtime.state.activeEffects == 0 && runtime.state.effectsDone != nil {
		close(runtime.state.effectsDone)
		runtime.state.effectsDone = nil
	}
}

func (runtime *threadRuntimeState) appendLiveToolSegment(stream *ToolCallStream) {
	if runtime == nil || stream == nil || strings.TrimSpace(stream.ID) == "" {
		return
	}
	name := strings.TrimSpace(stream.Name)
	if name == CoreControlAskUser {
		return
	}
	id := threadToolSegmentID(runtime.state.turnID, stream.ID)
	if threadItemIndexByID(runtime.state.view.Items, id) >= 0 {
		return
	}
	activity := observation.ActivityItem{
		ItemID: id, ToolID: strings.TrimSpace(stream.ID), ToolName: name,
		Kind: observation.ActivityKindTool, Status: observation.ActivityStatusRunning,
	}
	runtime.state.view.Items = appendThreadItem(runtime.state.view.Items, ThreadItem{
		ID: id, TurnID: runtime.state.turnID, RunID: runtime.state.runID, Kind: ThreadItemTool, Live: true, Activity: &activity,
	})
}

func (runtime *threadRuntimeState) appendLiveTextSegment(kind ThreadItemKind, text string) {
	if runtime == nil || text == "" {
		return
	}
	if runtime.state.openTextSegmentID != "" && runtime.state.openTextKind != kind {
		runtime.finishLiveTextSegment()
	}
	if runtime.state.openTextSegmentID == "" {
		runtime.state.openTextSegmentID = nextThreadTextSegmentID(runtime.state.view.Items, runtime.state.turnID, kind)
		runtime.state.openTextKind = kind
		runtime.state.view.Items = appendThreadItem(runtime.state.view.Items, ThreadItem{
			ID: runtime.state.openTextSegmentID, TurnID: runtime.state.turnID, RunID: runtime.state.runID, Kind: kind, Live: true,
		})
	}
	if index := threadItemIndexByID(runtime.state.view.Items, runtime.state.openTextSegmentID); index >= 0 {
		runtime.state.view.Items[index].Text += text
		runtime.state.view.Items[index].Live = true
	}
}

func (runtime *threadRuntimeState) finishLiveTextSegment() {
	if runtime == nil {
		return
	}
	if index := threadItemIndexByID(runtime.state.view.Items, runtime.state.openTextSegmentID); index >= 0 {
		runtime.state.view.Items[index].Live = false
	}
	runtime.state.openTextSegmentID = ""
	runtime.state.openTextKind = ""
}

func settleTerminalToolSegments(items []ThreadItem, turnID identity.TurnID, timeline observation.ActivityTimeline) []ThreadItem {
	terminal := make(map[string]observation.ActivityItem, len(timeline.Items))
	for _, item := range timeline.Items {
		if toolID := strings.TrimSpace(item.ToolID); toolID != "" {
			terminal[toolID] = item
		}
	}
	out := items[:0]
	for _, item := range items {
		if item.TurnID == turnID && item.Kind == ThreadItemTool && item.Activity != nil {
			activity, found := terminal[strings.TrimSpace(item.Activity.ToolID)]
			if found {
				item.Activity = &activity
				item.Live = false
			} else if item.Live {
				// Provider-streamed schema corrections never become canonical tool facts.
				continue
			}
		}
		item.Ordinal = uint64(len(out) + 1)
		out = append(out, item)
	}
	return out
}

func nextThreadTextSegmentID(items []ThreadItem, turnID identity.TurnID, kind ThreadItemKind) string {
	prefix := string(kind) + ":" + turnID.String() + ":"
	next := 1
	for _, item := range items {
		if item.TurnID == turnID && item.Kind == kind && strings.HasPrefix(item.ID, prefix) {
			next++
		}
	}
	return prefix + strconv.Itoa(next)
}

func liveEventAttemptIdentity(event Event) (identity.LogicalRequestID, string, int, bool) {
	if event.attemptEpoch > 0 && strings.TrimSpace(event.attemptID) != "" {
		return event.attemptLogicalID, strings.TrimSpace(event.attemptID), event.attemptEpoch, true
	}
	if event.Stream != nil && event.Stream.AttemptEpoch > 0 && strings.TrimSpace(event.Stream.AttemptID) != "" {
		return event.Stream.LogicalRequestID, strings.TrimSpace(event.Stream.AttemptID), event.Stream.AttemptEpoch, true
	}
	if event.Type != observation.EventTypeProviderRequest {
		return "", "", 0, false
	}
	metadata := event.Metadata
	if metadata == nil {
		return "", "", 0, false
	}
	logicalRequestID := identity.LogicalRequestID(stringFromMetadata(metadata, "logical_request_id"))
	attemptID := strings.TrimSpace(stringFromMetadata(metadata, "attempt_id"))
	attemptEpoch := intFromMetadata(metadata, "attempt_epoch")
	return logicalRequestID, attemptID, attemptEpoch, attemptEpoch > 0 && attemptID != ""
}
