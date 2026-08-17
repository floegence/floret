package runtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/observation"
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
	effectRetryCancels  map[uint64]context.CancelFunc
	effectRetryEpoch    uint64
	effectRetrySources  map[string]struct{}
	requestKeys         map[string]threadRuntimeRequest
	agent               *Agent
	pendingInteractions map[string]*pendingThreadInteraction
	hydrationStarted    bool
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
	if event.TurnID != "" && runtime.state.turnID != "" && event.TurnID != runtime.state.turnID {
		return false
	}
	if event.TurnID != "" {
		runtime.state.turnID = event.TurnID
	}
	if event.RunID != "" {
		runtime.state.runID = event.RunID
	}
	logicalRequestID, attemptID, attemptEpoch, hasAttempt := liveEventAttemptIdentity(event)
	if hasAttempt {
		switch {
		case attemptEpoch < runtime.state.attemptEpoch:
			return false
		case attemptEpoch == runtime.state.attemptEpoch && runtime.state.attemptID != "" && attemptID != runtime.state.attemptID:
			return false
		case attemptEpoch > runtime.state.attemptEpoch:
			runtime.finishLiveTextSegment()
		}
		runtime.state.logicalRequestID = logicalRequestID
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

// claimEffectRetrySource fences one in-process dispatcher for a persisted
// source attempt. The journal claim is the durable authority; this marker
// distinguishes an active local dispatch from a stale claim recovered after a
// process restart.
func (runtime *threadRuntimeState) claimEffectRetrySource(sourceID string) (bool, error) {
	if runtime == nil || strings.TrimSpace(sourceID) == "" {
		return false, ErrThreadDeleted
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return false, ErrHostClosed
	}
	if runtime.deleting || runtime.deleted {
		return false, ErrThreadDeleted
	}
	if runtime.state.effectRetrySources == nil {
		runtime.state.effectRetrySources = make(map[string]struct{})
	}
	if _, exists := runtime.state.effectRetrySources[strings.TrimSpace(sourceID)]; exists {
		return false, &RequestConflictError{Operation: "retry effect", RequestID: strings.TrimSpace(sourceID), Err: ErrRequestConflict}
	}
	runtime.state.effectRetrySources[strings.TrimSpace(sourceID)] = struct{}{}
	return true, nil
}

func (runtime *threadRuntimeState) releaseEffectRetrySource(sourceID string) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	delete(runtime.state.effectRetrySources, strings.TrimSpace(sourceID))
	runtime.mu.Unlock()
}

func (runtime *threadRuntimeState) appendLiveToolSegment(stream *ToolCallStream) {
	if runtime == nil || stream == nil || strings.TrimSpace(stream.ID) == "" {
		return
	}
	name := strings.TrimSpace(stream.Name)
	if name == CoreControlAskUser || name == CoreControlTaskComplete {
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
		ID: id, TurnID: runtime.state.turnID, Kind: ThreadItemTool, Live: true, Activity: &activity,
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
			ID: runtime.state.openTextSegmentID, TurnID: runtime.state.turnID, Kind: kind, Live: true,
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
