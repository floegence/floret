package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v7/identity"
)

// ThreadUpdateSource observes the same actor and versions as ThreadService.View.
// Subscribe before reading a baseline; discard updates at or below that view's
// version. Any later base-version mismatch requires a new subscription and view.
// The interface is separate to preserve external ThreadService implementations.
type ThreadUpdateSource interface {
	SubscribeUpdates(context.Context) (*ThreadUpdateSubscription, error)
}

// ErrThreadUpdateOverflow means a slow observer must establish a new baseline.
// Providers never wait for an observer and semantic boundaries are never dropped.
var ErrThreadUpdateOverflow = errors.New("thread update subscription overflow; read a new baseline")

// ErrThreadUpdateInvalidated requires a new subscription and authoritative
// workspace baseline after deletion or a failed canonical presentation refresh.
var ErrThreadUpdateInvalidated = errors.New("thread update baseline invalidated; read a new baseline")

// ThreadComponent distinguishes no change (nil component) from a clear (a
// component whose Value is nil or empty).
type ThreadComponent[T any] struct {
	Value T `json:"value"`
}

// ThreadTextAppend appends exactly Text at a UTF-8 byte offset. Consumers whose
// strings use another encoding must validate the byte offset before applying it.
type ThreadTextAppend struct {
	OffsetBytes int    `json:"offset_bytes"`
	Text        string `json:"text"`
}

type ThreadItemChangeKind string

const (
	ThreadItemAdded    ThreadItemChangeKind = "add"
	ThreadItemReplaced ThreadItemChangeKind = "replace"
	ThreadItemRemoved  ThreadItemChangeKind = "remove"
	ThreadItemAppended ThreadItemChangeKind = "append"
)

// ThreadItemChange is an explicit mutation, never an instruction to infer a diff.
type ThreadItemChange struct {
	Kind   ThreadItemChangeKind `json:"kind"`
	ItemID string               `json:"item_id"`
	Item   *ThreadItem          `json:"item,omitempty"`
	Append *ThreadTextAppend    `json:"append,omitempty"`
}

// ThreadRuntimeFacts contains the small execution facts needed by host cleanup
// and summary consumers, independently of presentation history.
type ThreadRuntimeFacts struct {
	Activity     ThreadActivity      `json:"activity"`
	Attention    AttentionSummary    `json:"attention"`
	TurnKind     TurnKind            `json:"turn_kind,omitempty"`
	LastOutcome  *TurnOutcome        `json:"last_outcome,omitempty"`
	Failure      *ThreadTurnFailure  `json:"failure,omitempty"`
	Cancellation *ThreadCancellation `json:"cancellation,omitempty"`
	RunProgress  *ThreadRunProgress  `json:"run_progress,omitempty"`
	QueueCount   int                 `json:"queue_count"`
}

// ThreadUpdate contains only mutations recorded by the authoritative actor.
// ItemOrder is present only when existing items move. Additions carry Ordinal.
// A batch may span several actor versions when ordinary growth is coalesced.
type ThreadUpdate struct {
	ThreadID         identity.ThreadID                         `json:"thread_id"`
	TurnID           identity.TurnID                           `json:"turn_id,omitempty"`
	RunID            identity.RunID                            `json:"run_id,omitempty"`
	BaseVersion      uint64                                    `json:"base_version"`
	ViewVersion      uint64                                    `json:"view_version"`
	Facts            ThreadRuntimeFacts                        `json:"facts"`
	Items            []ThreadItemChange                        `json:"items,omitempty"`
	ItemOrder        *ThreadComponent[[]string]                `json:"item_order,omitempty"`
	Context          *ThreadComponent[*ThreadContextSnapshot]  `json:"context,omitempty"`
	Goal             *ThreadComponent[*ThreadGoal]             `json:"goal,omitempty"`
	ParentEvents     *ThreadComponent[*ParentEventPage]        `json:"parent_events,omitempty"`
	Turns            *ThreadComponent[[]ThreadTurn]            `json:"turns,omitempty"`
	Queue            *ThreadComponent[[]QueuedInput]           `json:"queue,omitempty"`
	Interactions     *ThreadComponent[[]ThreadInteraction]     `json:"interactions,omitempty"`
	ToolCallPreviews *ThreadComponent[[]ThreadToolCallPreview] `json:"tool_call_previews,omitempty"`
	RestoredInputs   *ThreadComponent[[]RestoredInput]         `json:"restored_inputs,omitempty"`
}

type ThreadUpdateSubscription struct {
	mu          sync.Mutex
	updates     chan queuedThreadUpdate
	queuedBytes int
	done        chan struct{}
	closed      bool
	closeErr    error
	service     *threadRuntimeService
}

func (subscription *ThreadUpdateSubscription) Next(ctx context.Context) (ThreadUpdate, error) {
	if subscription == nil || ctx == nil {
		return ThreadUpdate{}, errors.New("thread update subscription and context are required")
	}
	// Prefer terminal errors to queued mutations after continuity was lost.
	subscription.mu.Lock()
	if subscription.closed {
		err := subscription.closeErr
		subscription.mu.Unlock()
		return ThreadUpdate{}, err
	}
	subscription.mu.Unlock()
	select {
	case <-ctx.Done():
		return ThreadUpdate{}, ctx.Err()
	case <-subscription.done:
		subscription.mu.Lock()
		err := subscription.closeErr
		subscription.mu.Unlock()
		return ThreadUpdate{}, err
	case queued := <-subscription.updates:
		subscription.mu.Lock()
		if subscription.closed {
			err := subscription.closeErr
			subscription.mu.Unlock()
			return ThreadUpdate{}, err
		}
		subscription.queuedBytes -= queued.bytes
		subscription.mu.Unlock()
		return cloneThreadUpdate(queued.update), nil
	}
}

func (subscription *ThreadUpdateSubscription) Close() { subscription.closeWithError(ErrHostClosed) }
func (subscription *ThreadUpdateSubscription) closeWithError(err error) {
	if subscription == nil {
		return
	}
	subscription.mu.Lock()
	if !subscription.closed {
		subscription.finishLocked(err)
	}
	service := subscription.service
	subscription.mu.Unlock()
	if service != nil {
		service.mu.Lock()
		delete(service.updateSubscribers, subscription)
		service.mu.Unlock()
	}
}

// queuedThreadUpdate shares one immutable batch across subscribers. The byte
// count is computed once per publication and includes its complete JSON shape.
type queuedThreadUpdate struct {
	update ThreadUpdate
	bytes  int
}

func (subscription *ThreadUpdateSubscription) finishLocked(err error) {
	subscription.closed = true
	subscription.closeErr = err
	close(subscription.done)
	for {
		select {
		case <-subscription.updates:
		default:
			subscription.queuedBytes = 0
			return
		}
	}
}

func (service *threadRuntimeService) hasObservers() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	return !service.subscriptionsClosed && (len(service.updateSubscribers) > 0 || len(service.subscribers) > 0)
}

func (service *threadRuntimeService) invalidateUpdates() {
	service.mu.Lock()
	defer service.mu.Unlock()
	for subscription := range service.updateSubscribers {
		subscription.mu.Lock()
		if !subscription.closed {
			subscription.finishLocked(ErrThreadUpdateInvalidated)
		}
		subscription.mu.Unlock()
		delete(service.updateSubscribers, subscription)
	}
}

var _ ThreadUpdateSource = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SubscribeUpdates(ctx context.Context) (*ThreadUpdateSubscription, error) {
	if err := service.host.available(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("thread update subscription context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscription := &ThreadUpdateSubscription{updates: make(chan queuedThreadUpdate, threadUpdateQueueCount), done: make(chan struct{}), service: service}
	service.mu.Lock()
	if service.subscriptionsClosed {
		service.mu.Unlock()
		return nil, ErrHostClosed
	}
	if service.updateSubscribers == nil {
		service.updateSubscribers = make(map[*ThreadUpdateSubscription]struct{})
	}
	service.updateSubscribers[subscription] = struct{}{}
	service.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			subscription.closeWithError(ctx.Err())
		case <-subscription.done:
		}
	}()
	return subscription, nil
}

func cloneThreadUpdate(update ThreadUpdate) ThreadUpdate {
	// The queued batch is immutable. Copy at the public read boundary only.
	update.Items = append([]ThreadItemChange(nil), update.Items...)
	for i := range update.Items {
		change := &update.Items[i]
		if change.Item != nil {
			item := cloneThreadItems([]ThreadItem{*change.Item})[0]
			change.Item = &item
		}
		if change.Append != nil {
			value := *change.Append
			change.Append = &value
		}
	}
	view := cloneThreadRuntimeFields(ThreadView{Context: componentValue(update.Context), Goal: componentValue(update.Goal), ParentEvents: componentValue(update.ParentEvents), Turns: componentValue(update.Turns), Queue: componentValue(update.Queue), Interactions: componentValue(update.Interactions), ToolCallPreviews: componentValue(update.ToolCallPreviews), RestoredInputs: componentValue(update.RestoredInputs), Failure: update.Facts.Failure, Cancellation: update.Facts.Cancellation, RunProgress: update.Facts.RunProgress})
	update.Facts.Failure, update.Facts.Cancellation, update.Facts.RunProgress = view.Failure, view.Cancellation, view.RunProgress
	if update.Facts.LastOutcome != nil {
		value := *update.Facts.LastOutcome
		update.Facts.LastOutcome = &value
	}
	if update.Context != nil {
		update.Context = &ThreadComponent[*ThreadContextSnapshot]{view.Context}
	}
	if update.Goal != nil {
		update.Goal = &ThreadComponent[*ThreadGoal]{view.Goal}
	}
	if update.ParentEvents != nil {
		update.ParentEvents = &ThreadComponent[*ParentEventPage]{view.ParentEvents}
	}
	if update.Turns != nil {
		update.Turns = &ThreadComponent[[]ThreadTurn]{view.Turns}
	}
	if update.Queue != nil {
		update.Queue = &ThreadComponent[[]QueuedInput]{view.Queue}
	}
	if update.Interactions != nil {
		update.Interactions = &ThreadComponent[[]ThreadInteraction]{view.Interactions}
	}
	if update.ToolCallPreviews != nil {
		update.ToolCallPreviews = &ThreadComponent[[]ThreadToolCallPreview]{view.ToolCallPreviews}
	}
	if update.RestoredInputs != nil {
		update.RestoredInputs = &ThreadComponent[[]RestoredInput]{view.RestoredInputs}
	}
	if update.ItemOrder != nil {
		update.ItemOrder = &ThreadComponent[[]string]{append([]string(nil), update.ItemOrder.Value...)}
	}
	return update
}

func componentValue[T any](value *ThreadComponent[T]) (out T) {
	if value != nil {
		return value.Value
	}
	return
}

type threadComponentMask uint16

const (
	threadContextChanged threadComponentMask = 1 << iota
	threadGoalChanged
	threadParentEventsChanged
	threadTurnsChanged
	threadQueueChanged
	threadInteractionsChanged
	threadPreviewsChanged
	threadRestoredInputsChanged
)

type threadMutations struct {
	items      []ThreadItemChange
	components threadComponentMask
	order      bool
	boundary   bool
}

func (mutations threadMutations) growth() bool {
	return !mutations.boundary && mutations.components == 0 && !mutations.order && len(mutations.items) == 1 && mutations.items[0].Kind == ThreadItemAppended
}
func (actor *threadRuntimeState) markComponents(mask threadComponentMask) {
	actor.mutations.components |= mask
}
func (actor *threadRuntimeState) markItem(id string, kind ThreadItemChangeKind) {
	if id == "" {
		return
	}
	for i := range actor.mutations.items {
		change := &actor.mutations.items[i]
		if change.ItemID != id {
			continue
		}
		if change.Kind == ThreadItemAdded && kind != ThreadItemRemoved {
			return
		}
		change.Kind, change.Append = kind, nil
		return
	}
	actor.mutations.items = append(actor.mutations.items, ThreadItemChange{ItemID: id, Kind: kind})
}
func (actor *threadRuntimeState) markAppend(id string, offset int, text string) {
	for i := range actor.mutations.items {
		if actor.mutations.items[i].ItemID == id {
			return
		}
	}
	actor.mutations.items = append(actor.mutations.items, ThreadItemChange{ItemID: id, Kind: ThreadItemAppended, Append: &ThreadTextAppend{OffsetBytes: offset, Text: text}})
}

// replaceItems records a canonical/path boundary as mutations while installing
// its authoritative order. It is never called for ordinary text growth.
func (actor *threadRuntimeState) replaceItems(items []ThreadItem) {
	next := make(map[string]bool, len(items))
	for _, item := range items {
		next[item.ID] = true
	}
	for _, item := range actor.state.view.Items {
		if !next[item.ID] {
			actor.markItem(item.ID, ThreadItemRemoved)
		}
	}
	old := actor.itemIndex
	if old == nil || actor.indexedItems != len(actor.state.view.Items) {
		old = make(map[string]int, len(actor.state.view.Items))
		for i, item := range actor.state.view.Items {
			old[item.ID] = i
		}
	}
	for _, item := range items {
		kind := ThreadItemAdded
		if _, ok := old[item.ID]; ok {
			kind = ThreadItemReplaced
		}
		actor.markItem(item.ID, kind)
	}
	actor.state.view.Items = items
	actor.invalidateItemIndex()
	actor.textBuffer.Reset()
	actor.textBufferID = ""
	actor.mutations.order = true
	actor.mutations.boundary = true
}
func (actor *threadRuntimeState) refreshAttention() {
	if actor.mutations.components&threadInteractionsChanged != 0 {
		attention := AttentionSummary{}
		for _, interaction := range actor.state.view.Interactions {
			if interaction.Resolved {
				continue
			}
			if interaction.Kind == ThreadInteractionApproval {
				attention.ApprovalCount++
			}
			if interaction.Kind == ThreadInteractionInput {
				attention.InputCount++
			}
		}
		actor.state.view.Attention = attention
	}
}

func (actor *threadRuntimeState) takeUpdate(base uint64) ThreadUpdate {
	view := actor.state.view
	update := ThreadUpdate{ThreadID: view.ThreadID, TurnID: view.TurnID, RunID: view.RunID, BaseVersion: base, ViewVersion: view.ViewVersion, Facts: ThreadRuntimeFacts{Activity: view.Activity, Attention: view.Attention, TurnKind: view.TurnKind, LastOutcome: view.LastOutcome, Failure: view.Failure, Cancellation: view.Cancellation, RunProgress: view.RunProgress, QueueCount: len(view.Queue)}, Items: actor.mutations.items}
	for i := range update.Items {
		change := &update.Items[i]
		if change.Kind == ThreadItemAdded || change.Kind == ThreadItemReplaced {
			if index := actor.itemIndexByID(change.ItemID); index >= 0 {
				item := view.Items[index]
				change.Item = &item
			}
		}
	}
	if actor.mutations.order {
		order := make([]string, len(view.Items))
		for i, item := range view.Items {
			order[i] = item.ID
		}
		update.ItemOrder = &ThreadComponent[[]string]{order}
	}
	mask := actor.mutations.components
	if mask&threadContextChanged != 0 {
		update.Context = &ThreadComponent[*ThreadContextSnapshot]{view.Context}
	}
	if mask&threadGoalChanged != 0 {
		update.Goal = &ThreadComponent[*ThreadGoal]{view.Goal}
	}
	if mask&threadParentEventsChanged != 0 {
		update.ParentEvents = &ThreadComponent[*ParentEventPage]{view.ParentEvents}
	}
	if mask&threadTurnsChanged != 0 {
		update.Turns = &ThreadComponent[[]ThreadTurn]{view.Turns}
	}
	if mask&threadQueueChanged != 0 {
		update.Queue = &ThreadComponent[[]QueuedInput]{view.Queue}
	}
	if mask&threadInteractionsChanged != 0 {
		update.Interactions = &ThreadComponent[[]ThreadInteraction]{view.Interactions}
	}
	if mask&threadPreviewsChanged != 0 {
		update.ToolCallPreviews = &ThreadComponent[[]ThreadToolCallPreview]{view.ToolCallPreviews}
	}
	if mask&threadRestoredInputsChanged != 0 {
		update.RestoredInputs = &ThreadComponent[[]RestoredInput]{view.RestoredInputs}
	}
	return update
}

const (
	threadGrowthInterval = 50 * time.Millisecond
	threadGrowthBytes    = 64 << 10
)

// publishUpdateLocked is called with the actor lock held. It owns ordering,
// coalescing and legacy materialization for the one publication kernel.
func (service *threadRuntimeService) publishUpdateLocked(actor *threadRuntimeState, update ThreadUpdate, growth bool) {
	if growth {
		if pending := actor.pendingUpdate; pending != nil && pending.ViewVersion == update.BaseVersion && len(pending.Items) == 1 && len(update.Items) == 1 && pending.Items[0].ItemID == update.Items[0].ItemID && pending.Items[0].Append != nil && update.Items[0].Append != nil {
			actor.pendingText.WriteString(update.Items[0].Append.Text)
			pending.ViewVersion, pending.Facts = update.ViewVersion, update.Facts
			if actor.pendingText.Len() >= threadGrowthBytes {
				service.flushUpdateLocked(actor)
			}
			return
		}
		service.flushUpdateLocked(actor)
		actor.pendingUpdate = &update
		actor.pendingText.WriteString(update.Items[0].Append.Text)
		if actor.pendingText.Len() >= threadGrowthBytes {
			service.flushUpdateLocked(actor)
			return
		}
		scheduled := actor.pendingUpdate
		actor.updateTimer = time.AfterFunc(threadGrowthInterval, func() {
			actor.mu.Lock()
			defer actor.mu.Unlock()
			if actor.pendingUpdate != scheduled {
				return
			}
			if actor.closed || actor.deleted {
				actor.pendingUpdate = nil
				actor.pendingText.Reset()
				return
			}
			service.flushUpdateLocked(actor)
		})
		return
	}
	service.flushUpdateLocked(actor)
	service.deliverUpdateLocked(actor, update)
}

func (service *threadRuntimeService) flushUpdateLocked(actor *threadRuntimeState) {
	if actor.updateTimer != nil {
		actor.updateTimer.Stop()
		actor.updateTimer = nil
	}
	if actor.pendingUpdate == nil {
		return
	}
	update := *actor.pendingUpdate
	update.Items[0].Append.Text = actor.pendingText.String()
	actor.pendingUpdate = nil
	actor.pendingText.Reset()
	service.deliverUpdateLocked(actor, update)
}

func (service *threadRuntimeService) deliverUpdateLocked(actor *threadRuntimeState, update ThreadUpdate) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.subscriptionsClosed || actor.state.contextErr != nil {
		return
	}
	if update.ViewVersion <= service.published[update.ThreadID.String()] {
		return
	}
	service.published[update.ThreadID.String()] = update.ViewVersion
	var queued queuedThreadUpdate
	if len(service.updateSubscribers) > 0 {
		raw, err := json.Marshal(update)
		if err != nil {
			for subscription := range service.updateSubscribers {
				subscription.mu.Lock()
				if !subscription.closed {
					subscription.finishLocked(ErrThreadUpdateInvalidated)
				}
				subscription.mu.Unlock()
				delete(service.updateSubscribers, subscription)
			}
		} else {
			queued.bytes = len(raw)
			// Oversized batches invalidate observers without retaining a copy.
			if queued.bytes <= threadUpdateQueueBytes {
				queued.update = cloneThreadUpdate(update)
			}
		}
	}
	for subscription := range service.updateSubscribers {
		subscription.mu.Lock()
		if !subscription.closed {
			overflow := queued.bytes > threadUpdateQueueBytes-subscription.queuedBytes
			if !overflow {
				select {
				case subscription.updates <- queued:
					subscription.queuedBytes += queued.bytes
				default:
					overflow = true
				}
			}
			if overflow {
				subscription.finishLocked(ErrThreadUpdateOverflow)
				delete(service.updateSubscribers, subscription)
			}
		}
		subscription.mu.Unlock()
	}
	// Legacy view queues are replaceable. They never close their data channel,
	// so observing done is sufficient and delivery never acquires observer locks.
	if update.ViewVersion != actor.state.view.ViewVersion {
		return
	}
	for subscription := range service.subscribers {
		select {
		case <-subscription.done:
			continue
		default:
		}
		if subscription.wakeOnly {
			select {
			case subscription.changed <- struct{}{}:
			default:
			}
			continue
		}
		view := cloneThreadRuntimeView(actor.state.view)
		select {
		case subscription.views <- view:
		default:
			select {
			case <-subscription.views:
			default:
			}
			select {
			case subscription.views <- view:
			default:
			}
		}
	}
}

func (actor *threadRuntimeState) resolveInteraction(interactionID string, resolution InteractionResolution) {
	resolveThreadInteractionCanonical(&actor.state.view, interactionID, resolution)
	actor.markComponents(threadInteractionsChanged)
	for _, item := range actor.state.view.Items {
		if item.Interaction != nil && item.Interaction.ID == interactionID {
			actor.markItem(item.ID, ThreadItemReplaced)
		}
	}
}

func (actor *threadRuntimeState) stopUpdateTimer() {
	if actor.updateTimer != nil {
		actor.updateTimer.Stop()
		actor.updateTimer = nil
	}
	actor.pendingUpdate = nil
	actor.pendingText.Reset()
}
