package runtime

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/observation"
)

// threadRuntimeState is the only in-memory lifecycle owner for one thread.
// Provider and tool I/O must never run while its mutex is held.
type threadRuntimeState struct {
	canonical             *threadCanonicalProjection
	observedItemIDs       map[string]bool
	summaryIndexed        bool
	summaryPreviewIndex   int
	summaryTimeIndex      int
	threadID              string
	mu                    sync.Mutex
	closed                bool
	deleting              bool
	deleted               bool
	lookupErr             error
	state                 threadRuntimeData
	itemIndex             map[string]int
	indexedItems          int
	interactionIndex      map[string]int
	indexedInteractions   int
	interactionAttention  AttentionSummary
	pendingInteractionIDs map[string]bool
	interactionItems      map[string][]string
	textBuffer            strings.Builder
	textBufferID          string
	outputOrder           []string
	service               *threadRuntimeService
	mutations             threadMutations
	pendingUpdate         *ThreadUpdate
	pendingText           strings.Builder
	updateTimer           *time.Timer
}

type threadRuntimeData struct {
	contextErr          error
	steering            []string
	fragmentCancel      context.CancelCauseFunc
	executionSealed     bool
	turnID              identity.TurnID
	runID               identity.RunID
	logicalRequestID    identity.LogicalRequestID
	attemptID           string
	attemptEpoch        int
	retiredAttempt      bool
	attemptDraftIDs     map[string]bool
	openTextSegmentID   string
	openTextKind        ThreadItemKind
	responseSegmented   bool
	view                ThreadView
	cancel              context.CancelCauseFunc
	cancelOwner         string
	executionDone       <-chan struct{}
	activeEffects       int
	effectsDone         chan struct{}
	requestKeys         map[string]threadRuntimeRequest
	agent               *Agent
	pendingInteractions map[string]*pendingThreadInteraction
	hydrationStarted    bool
}

// releaseDeletedState runs under the actor lock only after execution and effects
// have joined and canonical deletion has committed. Keep the actor identity and
// permanent fence so captured pointers and late dispatches cannot revive it.
func (runtime *threadRuntimeState) releaseDeletedState() {
	runtime.stopUpdateTimer()
	runtime.state = threadRuntimeData{}
	runtime.canonical = nil
	runtime.observedItemIDs = nil
	runtime.summaryIndexed = false
	runtime.summaryPreviewIndex = 0
	runtime.summaryTimeIndex = 0
	runtime.itemIndex = nil
	runtime.indexedItems = 0
	runtime.interactionIndex = nil
	runtime.indexedInteractions = 0
	runtime.interactionAttention = AttentionSummary{}
	runtime.pendingInteractionIDs = nil
	runtime.interactionItems = nil
	runtime.textBuffer.Reset()
	runtime.textBufferID = ""
	runtime.outputOrder = nil
	runtime.mutations = threadMutations{}
}

type beginThreadRunInput struct {
	turnKind         TurnKind
	turnID           identity.TurnID
	runID            identity.RunID
	logicalRequestID identity.LogicalRequestID
	cancel           context.CancelCauseFunc
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
	runtime.state.responseSegmented = input.clearOutcome
	for _, turn := range runtime.state.view.Turns {
		if turn.TurnID == input.turnID {
			runtime.state.responseSegmented = turn.segmented
		}
	}
	runtime.state.executionSealed = false
	runtime.state.fragmentCancel = nil
	runtime.state.steering = nil
	runtime.state.attemptID = ""
	runtime.state.attemptEpoch = 0
	runtime.state.retiredAttempt = false
	runtime.state.attemptDraftIDs = nil
	runtime.state.openTextSegmentID = ""
	runtime.state.openTextKind = ""
	runtime.state.cancel = input.cancel
	runtime.state.cancelOwner = "run:" + input.runID.String()
	runtime.state.executionDone = input.executionDone
	runtime.state.view.Activity = ThreadActivityActive
	runtime.state.view.TurnKind = input.turnKind
	runtime.state.view.TurnID = input.turnID
	runtime.state.view.RunID = input.runID
	runtime.state.view.ToolCallPreviews = nil
	runtime.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	if input.clearOutcome {
		runtime.state.view.LastOutcome = nil
		runtime.state.view.Failure = nil
		runtime.state.view.Cancellation = nil
	}
	runtime.markComponents(threadPreviewsChanged)
	runtime.outputOrder = nil
	runtime.mutations.boundary = true
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
	if runtime.lookupErr != nil {
		return runtime.lookupErr
	}
	if runtime.deleting || runtime.deleted {
		return ErrThreadDeleted
	}
	base := runtime.state.view.ViewVersion
	runtime.mutations = threadMutations{}
	err := mutate()
	runtime.refreshSummaryPositions()
	runtime.refreshAttention()
	if runtime.service != nil {
		switch {
		case runtime.state.contextErr != nil:
			runtime.stopUpdateTimer()
			runtime.service.invalidateUpdates()
		case runtime.state.view.ViewVersion > base:
			if runtime.service.hasObservers() {
				update := runtime.takeUpdate(base)
				runtime.service.publishUpdateLocked(runtime, update, runtime.mutations.growth())
			} else {
				runtime.stopUpdateTimer()
			}
		}
	}
	runtime.mutations = threadMutations{}
	return err
}

// acceptLiveEvent applies the current provider-attempt fence and records the
// latest memory-only projection. It is called only from the actor mailbox.
func (runtime *threadRuntimeState) acceptLiveEvent(event Event) bool {
	if runtime == nil || runtime.deleting || runtime.deleted {
		return false
	}
	if event.ThreadID != "" && runtime.state.view.ThreadID != "" && event.ThreadID != runtime.state.view.ThreadID {
		return false
	}
	if timeline := event.ActivityTimeline; timeline != nil && (timeline.ThreadID != event.ThreadID || timeline.TurnID != event.TurnID || timeline.RunID != event.RunID) {
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
			runtime.discardAttemptDraft()
			runtime.state.retiredAttempt = false
			runtime.state.attemptDraftIDs = nil
			runtime.finishLiveTextSegment()
		}
		runtime.state.attemptID = attemptID
		runtime.state.attemptEpoch = attemptEpoch
	}
	if hasAttempt && runtime.state.retiredAttempt {
		return false
	}
	if event.Retry != nil {
		runtime.discardAttemptDraft()
		runtime.state.retiredAttempt = true
	}
	if runtime.state.responseSegmented && event.Type == observation.EventTypeStepEnd {
		runtime.finishLiveTextSegment()
	}
	if event.Type == observation.EventTypeHostedToolCall || event.Type == observation.EventTypeHostedToolResult {
		runtime.finishLiveTextSegment()
	}
	if event.Stream != nil {
		if event.outputBodyErr != nil {
			runtime.state.contextErr = event.outputBodyErr
			return false
		}
		switch event.Stream.Type {
		case StreamObservationAssistantDelta:
			runtime.appendLiveOutput(ThreadItemAssistant, event)
		case StreamObservationReasoningDelta:
			runtime.appendLiveOutput(ThreadItemThinking, event)
		case StreamObservationToolCallStart, StreamObservationToolCallDelta:
			runtime.finishLiveTextSegment()
		case StreamObservationToolCallEnd:
			runtime.finishLiveTextSegment()
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
	if runtime.lookupErr != nil {
		return runtime.lookupErr
	}
	if runtime.deleting || runtime.deleted {
		return ErrThreadDeleted
	}
	if runtime.state.view.Cancellation != nil && runtime.state.view.Cancellation.TurnID == runtime.state.turnID {
		return context.Canceled
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

// The Engine's Activity projection already owns validation, presentation merging,
// and tool status. Apply that snapshot in the actor before dispatch can proceed;
// provider argument streams are not admitted tool executions.
func (runtime *threadRuntimeState) applyToolActivity(timeline *observation.ActivityTimeline, observedAt time.Time) bool {
	if timeline == nil {
		return false
	}
	return runtime.applyActivityItems(timeline.ThreadID, timeline.TurnID, timeline.RunID, timeline.Items, observedAt)
}

func (runtime *threadRuntimeState) applyActivityItems(threadID identity.ThreadID, turnID identity.TurnID, runID identity.RunID, items []observation.ActivityItem, observedAt time.Time) bool {
	if threadID != runtime.state.view.ThreadID || turnID != runtime.state.turnID || runID != runtime.state.runID {
		return false
	}
	changed := false
	for _, activity := range items {
		if (activity.Kind != observation.ActivityKindTool && activity.Kind != observation.ActivityKindHosted) || activity.ToolID == "" {
			continue
		}
		id := threadToolSegmentID(turnID, activity.ToolID)
		if activity.Kind == observation.ActivityKindHosted {
			id = "hosted:" + threadToolKey(turnID, runID, activity.ToolID)
			if runtime.itemIndexByID(id) < 0 {
				if runtime.state.attemptDraftIDs == nil {
					runtime.state.attemptDraftIDs = make(map[string]bool)
				}
				runtime.state.attemptDraftIDs[id] = true
			}
		}
		index := runtime.itemIndexByID(id)
		live := !threadToolActivityTerminal(activity.Status)
		activity = observation.CloneActivityTimeline(&observation.ActivityTimeline{Items: []observation.ActivityItem{activity}}).Items[0]
		if index >= 0 {
			item := &runtime.state.view.Items[index]
			if item.TurnID != turnID || item.RunID != runID {
				continue
			}
			if reflect.DeepEqual(item.Activity, &activity) && item.Live == live {
				continue
			}
			item.Activity, item.Live = &activity, live
			item.activityOmitted = false
			// An observation is not a canonical source body. Terminal installation
			// restores the journal association from the same reducer.
			item.activityEntryIDs = nil
			runtime.markItem(id, ThreadItemReplaced)
		} else {
			runtime.finishLiveTextSegment()
			createdAt := observedAt
			if activity.StartedAtUnixMS > 0 {
				createdAt = time.UnixMilli(activity.StartedAtUnixMS).UTC()
			}
			if createdAt.IsZero() {
				createdAt = time.Now().UTC()
			}
			runtime.state.view.Items = appendThreadItem(runtime.state.view.Items, ThreadItem{
				ID: id, TurnID: turnID, RunID: runID, Kind: ThreadItemTool, Live: live, Activity: &activity, CreatedAt: createdAt,
			})
		}
		if index < 0 {
			runtime.markItem(id, ThreadItemAdded)
		}
		if runtime.observedItemIDs == nil {
			runtime.observedItemIDs = map[string]bool{}
		}
		runtime.observedItemIDs[id] = true
		changed = true
	}
	return changed
}

func threadToolActivityTerminal(status observation.ActivityStatus) bool {
	switch status {
	case observation.ActivityStatusSuccess, observation.ActivityStatusError, observation.ActivityStatusCanceled, observation.ActivityStatusDeclined:
		return true
	default:
		return false
	}
}

func (runtime *threadRuntimeState) appendLiveOutput(kind ThreadItemKind, event Event) {
	runtime.appendLiveTextSegment(kind, event.Stream.Text, event.Timestamp, event.outputSourceID, event.outputBody)
}

func (runtime *threadRuntimeState) appendLiveTextSegment(kind ThreadItemKind, text string, createdAt time.Time, source string, bodies ...outputbody.Reader) {
	if runtime == nil || text == "" || source == "" {
		return
	}
	if runtime.state.openTextSegmentID != source {
		runtime.finishLiveTextSegment()
	}
	index := runtime.itemIndexByID(source)
	if index < 0 {
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		runtime.state.view.Items = appendThreadItem(runtime.state.view.Items, ThreadItem{ID: source, contentSourceID: source, TurnID: runtime.state.turnID, RunID: runtime.state.runID, Kind: kind, Live: true, CreatedAt: createdAt})
		index = len(runtime.state.view.Items) - 1
		runtime.markItem(source, ThreadItemAdded)
		if runtime.state.attemptDraftIDs == nil {
			runtime.state.attemptDraftIDs = make(map[string]bool)
		}
		runtime.state.attemptDraftIDs[source] = true
	}
	item := &runtime.state.view.Items[index]
	if !item.Live {
		runtime.markItem(source, ThreadItemReplaced)
	}
	if item.CanonicalOrdinal != 0 || item.contentSourceID != source || item.Kind != kind {
		return
	}
	if runtime.observedItemIDs == nil {
		runtime.observedItemIDs = map[string]bool{}
	}
	runtime.observedItemIDs[source] = true
	if len(bodies) > 0 && bodies[0] != nil {
		offset := item.textBytes
		if !item.textOmitted {
			offset = len(item.Text)
		}
		runtime.markAppend(source, offset, text)
		item.liveBody = bodies[0]
		item.textBytes = offset + len(text)
		item.textSummary, item.textSummaryTail = canonicalTextSummary(item.textSummary, text, item.textSummaryTail)
		item.textNonSpace = item.textNonSpace || strings.TrimSpace(text) != ""
		item.Text, item.textOmitted = "", true
		item.Live = true
		runtime.state.openTextSegmentID, runtime.state.openTextKind = source, kind
		return
	}
	if runtime.textBufferID != source {
		runtime.textBuffer.Reset()
		runtime.textBuffer.WriteString(item.Text)
		runtime.textBufferID = source
	}
	runtime.markAppend(source, len(item.Text), text)
	runtime.textBuffer.WriteString(text)
	item.Text = runtime.textBuffer.String()
	item.Live = true
	runtime.state.openTextSegmentID = source
	runtime.state.openTextKind = kind
}

func (runtime *threadRuntimeState) finishLiveTextSegment() {
	if runtime == nil {
		return
	}
	if index := runtime.itemIndexByID(runtime.state.openTextSegmentID); index >= 0 {
		if runtime.state.view.Items[index].Live {
			runtime.markItem(runtime.state.openTextSegmentID, ThreadItemReplaced)
		}
		runtime.state.view.Items[index].Live = false
	}
	runtime.state.openTextSegmentID = ""
	runtime.state.openTextKind = ""
	runtime.textBuffer.Reset()
	runtime.textBufferID = ""
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

func (runtime *threadRuntimeState) discardAttemptDraft() {
	runtime.mutations.order = true
	runtime.mutations.boundary = true
	runtime.outputOrder = nil
	runtime.state.openTextSegmentID = ""
	runtime.state.openTextKind = ""
	items := runtime.state.view.Items[:0]
	for _, item := range runtime.state.view.Items {
		if runtime.state.attemptDraftIDs[item.ID] && item.CanonicalOrdinal == 0 {
			runtime.markItem(item.ID, ThreadItemRemoved)
			continue
		}
		item.Ordinal = uint64(len(items) + 1)
		items = append(items, item)
	}
	clear(runtime.state.view.Items[len(items):])
	runtime.state.view.Items = items
	runtime.invalidateItemIndex()
	runtime.textBuffer.Reset()
	runtime.textBufferID = ""
	runtime.state.attemptDraftIDs = nil
}

// The Engine publishes its ordered fragment sources. The actor applies that
// order only to this uncommitted attempt, preserving all canonical positions.
func (runtime *threadRuntimeState) orderAttemptOutput(order []string) {
	if slices.Equal(runtime.outputOrder, order) {
		return
	}
	runtime.outputOrder = slices.Clone(order)
	if len(order) < 2 {
		return
	}
	ranks := make(map[string]int, len(order))
	for i, source := range order {
		ranks[source] = i
	}
	var positions []int
	var drafts []ThreadItem
	for i, item := range runtime.state.view.Items {
		if !runtime.state.attemptDraftIDs[item.ID] || item.CanonicalOrdinal != 0 {
			continue
		}
		source := item.ID
		if item.Activity != nil && item.Activity.Kind == observation.ActivityKindHosted {
			source = "hosted:" + item.Activity.ToolID
		}
		if _, ok := ranks[source]; !ok {
			continue
		}
		positions = append(positions, i)
		drafts = append(drafts, item)
	}
	rank := func(item ThreadItem) int {
		if item.Activity != nil && item.Activity.Kind == observation.ActivityKindHosted {
			return ranks["hosted:"+item.Activity.ToolID]
		}
		return ranks[item.ID]
	}
	sort.SliceStable(drafts, func(i, j int) bool { return rank(drafts[i]) < rank(drafts[j]) })
	for i, position := range positions {
		drafts[i].Ordinal = uint64(position + 1)
		runtime.state.view.Items[position] = drafts[i]
	}
	runtime.invalidateItemIndex()
	runtime.mutations.order = true
}

// itemIndexByID extends the index for appended rows. Reordering and removals
// explicitly invalidate it, so new output never scans unchanged history.
func (runtime *threadRuntimeState) itemIndexByID(id string) int {
	if id == "" {
		return -1
	}
	runtime.ensureItemIndex()
	if i, ok := runtime.itemIndex[id]; ok {
		return i
	}
	return -1
}

func (runtime *threadRuntimeState) ensureItemIndex() {
	if runtime.itemIndex == nil || runtime.indexedItems > len(runtime.state.view.Items) {
		runtime.itemIndex = make(map[string]int, len(runtime.state.view.Items))
		runtime.interactionItems = make(map[string][]string)
		runtime.indexedItems = 0
	}
	for i := runtime.indexedItems; i < len(runtime.state.view.Items); i++ {
		item := runtime.state.view.Items[i]
		runtime.itemIndex[item.ID] = i
		if item.Interaction != nil {
			runtime.interactionItems[item.Interaction.ID] = append(runtime.interactionItems[item.Interaction.ID], item.ID)
		}
	}
	runtime.indexedItems = len(runtime.state.view.Items)
}
func (runtime *threadRuntimeState) invalidateItemIndex() {
	runtime.itemIndex = nil
	runtime.interactionItems = nil
	runtime.indexedItems = 0
}
