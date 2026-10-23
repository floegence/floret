package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadWindowDirection selects an interval relative to a stable item identity.
type ThreadWindowDirection string

const (
	ThreadWindowBefore ThreadWindowDirection = "before"
	ThreadWindowAfter  ThreadWindowDirection = "after"
	ThreadWindowAround ThreadWindowDirection = "around"
)

// ThreadWindowInput moves the one reading interest for a thread on this
// subscription. Append preserves existing history anchors. A missing anchor
// fails explicitly; callers can request the available tail with no AnchorID.
type ThreadWindowInput struct {
	ThreadID  identity.ThreadID     `json:"thread_id"`
	AnchorID  string                `json:"anchor_id,omitempty"`
	Direction ThreadWindowDirection `json:"direction"`
	Limit     int                   `json:"limit,omitempty"`
}

// ThreadItemBody identifies complete text omitted from a bounded preview.
// A source is resolved through BodyManifest; a single field through ReadBody.
// Live text uses ReadLiveBody with its RunID and frozen Bytes extent.
// Control output Bytes measures the raw source; its preview retains the full
// view's whitespace trimming and empty-label presentation.
// Locators are associations, never grants. Full views leave this field nil.
type ThreadItemBody struct {
	RunID        identity.RunID  `json:"run_id,omitempty"`
	EntryID      string          `json:"entry_id,omitempty"`
	SourceID     string          `json:"source_id,omitempty"`
	Field        ThreadBodyField `json:"field"`
	Bytes        int             `json:"bytes"`
	PreviewBytes int             `json:"preview_bytes"`
}

// ThreadWindowSnapshot is a replaceable bounded item interval at ViewVersion.
// It is sent on the same subscription before any later delta. It introduces no
// second version, journal, lifecycle or independently advancing projection.
// Window replacement also replaces Turns with selected membership. Interactions
// contains all pending controls plus resolved controls in the accepted interval.
type ThreadWindowSnapshot struct {
	ThreadID          identity.ThreadID `json:"thread_id"`
	ViewVersion       uint64            `json:"view_version"`
	LeafID            string            `json:"leaf_id,omitempty"`
	AnchorUnavailable bool              `json:"anchor_unavailable,omitempty"`
	FirstItemID       string            `json:"first_item_id,omitempty"`
	LastItemID        string            `json:"last_item_id,omitempty"`
	HasBefore         bool              `json:"has_before"`
	HasAfter          bool              `json:"has_after"`
	Items             []ThreadItem      `json:"items"`
}

// ThreadWindowSource observes authorized lightweight facts for every scoped
// thread. Item details require SetWindow reading interest. Pending controls
// remain available regardless of the selected historical interval.
// Hosts must refresh authorization and close the subscription on revocation.
// This optional capability does not widen ThreadService or ThreadUpdateSource.
type ThreadWindowSource interface {
	SubscribeWindowUpdates(context.Context, threadquery.Scope) (*ThreadUpdateSubscription, error)
}

var _ ThreadWindowSource = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) SubscribeWindowUpdates(ctx context.Context, scope threadquery.Scope) (*ThreadUpdateSubscription, error) {
	authorized, err := sessiontree.ValidateQueryScope(scope)
	if err != nil {
		return nil, err
	}
	return service.subscribeUpdates(ctx, authorized)
}

// SetWindow atomically flushes earlier growth, queues the baseline, and installs
// the reading interest under the existing actor lock. Failed reads retain the
// prior accepted interest. The caller must consume the same Next channel.
func (subscription *ThreadUpdateSubscription) SetWindow(ctx context.Context, scope threadquery.Scope, input ThreadWindowInput) error {
	if subscription == nil || subscription.service == nil || !subscription.windowed || subscription.factsOnly {
		return errors.New("bounded window subscription required")
	}
	service := subscription.service
	if owner, ok := service.host.store.repo.(sessiontree.ForegroundReadOwner); ok {
		release, err := owner.BeginForegroundRead(ctx)
		if err != nil {
			return runtimeHostError(err)
		}
		defer release()
	}
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return err
	}
	subscription.mu.Lock()
	allowed := subscription.authorized[input.ThreadID.String()]
	closed := subscription.closed
	subscription.mu.Unlock()
	if closed {
		return ErrThreadUpdateInvalidated
	}
	if !allowed {
		return threadquery.ErrRecordUnavailable
	}
	if _, err := service.viewResult(ctx, input.ThreadID, threadAckResult); err != nil {
		return err
	}
	return subscription.setActorWindow(ctx, service.runtime(input.ThreadID), input)
}

func (subscription *ThreadUpdateSubscription) setActorWindow(ctx context.Context, actor *threadRuntimeState, input ThreadWindowInput) error {
	service := subscription.service
	limit := input.Limit
	if limit == 0 {
		limit = threadDisplayPageItems
	}
	if input.Direction == "" {
		input.Direction = ThreadWindowBefore
	}
	return actor.apply(ctx, func() error {
		if actor.state.contextErr != nil {
			return actor.state.contextErr
		}
		snapshot, err := actor.windowSnapshotContext(ctx, input)
		if err != nil {
			return err
		}
		service.flushUpdateLocked(actor)
		baseline := actor.windowBaseline()
		baseline.Items = nil
		baseline.Window = &ThreadComponent[*ThreadWindowSnapshot]{Value: &snapshot}
		selected := make(map[string]bool, len(snapshot.Items))
		for _, item := range snapshot.Items {
			selected[item.ID] = true
		}
		baseline.Turns = &ThreadComponent[[]ThreadTurn]{Value: actor.windowTurns(selected)}
		baseline.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: actor.windowInteractions(selected)}
		raw, err := json.Marshal(baseline)
		if err != nil {
			return err
		}
		subscription.mu.Lock()
		defer subscription.mu.Unlock()
		if subscription.closed {
			return subscription.closeErr
		}
		if !subscription.enqueueLocked(cloneThreadUpdate(baseline), len(raw)) {
			return ErrThreadUpdateOverflow
		}
		ids := make(map[string]bool, len(snapshot.Items))
		for _, item := range snapshot.Items {
			ids[item.ID] = true
		}
		if subscription.windows == nil {
			subscription.windows = map[string]threadWindowInterest{}
		}
		subscription.windows[input.ThreadID.String()] = threadWindowInterest{items: ids, bodies: windowBodyLocators(snapshot.Items), tail: input.AnchorID == "" && input.Direction == ThreadWindowBefore, version: snapshot.ViewVersion, anchor: snapshot.FirstItemID, limit: limit}
		return nil
	})
}

// ClearWindow releases detail interest without reconnecting workspace facts or
// cancelling the Agent. Existing queued detail is discarded to release memory.
func (subscription *ThreadUpdateSubscription) ClearWindow(threadID identity.ThreadID) {
	if subscription == nil {
		return
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	delete(subscription.windows, threadID.String())
	// A queued event may contain detail for the cleared window. Continuity is
	// retained by replacing only its detail fields, never semantic facts.
	count := len(subscription.updates)
	for range count {
		queued := <-subscription.updates
		if queued.update.ThreadID == threadID {
			queued.update.Items = nil
			queued.update.Window = nil
			queued.update.Turns = nil
			raw, _ := json.Marshal(queued.update)
			subscription.queuedBytes -= queued.bytes - len(raw)
			queued.bytes = len(raw)
		}
		subscription.updates <- queued
	}
}

type threadWindowInterest struct {
	items   map[string]bool
	bodies  map[string]ThreadItemBody
	tail    bool
	version uint64
	anchor  string
	limit   int
}

func setWindowBounds(snapshot *ThreadWindowSnapshot) {
	snapshot.FirstItemID, snapshot.LastItemID = "", ""
	if len(snapshot.Items) > 0 {
		snapshot.FirstItemID = snapshot.Items[0].ID
		snapshot.LastItemID = snapshot.Items[len(snapshot.Items)-1].ID
	}
}

func boundedWindowItem(item ThreadItem, limit int) ThreadItem {
	bytes := len(item.Text)
	if item.textOmitted {
		bytes = item.textBytes
	}
	if item.Body != nil {
		bytes = item.Body.Bytes
	}
	if item.controlTextBytes > 0 {
		bytes = item.controlTextBytes
	}
	text, partial := boundedThreadText(item.Text, limit)
	item.Text = text
	if partial || item.Body != nil || item.textOmitted || item.controlTextBytes > limit {
		field := ThreadBodyContent
		if item.Kind == ThreadItemThinking {
			field = ThreadBodyReasoning
		}
		body := ThreadItemBody{Field: field, Bytes: bytes, PreviewBytes: len(text)}
		if item.Body != nil {
			body = *item.Body
			body.PreviewBytes = len(text)
		}
		if item.CanonicalOrdinal != 0 {
			if item.Kind == ThreadItemAssistant || item.Kind == ThreadItemThinking {
				body.SourceID = item.ID
			} else {
				body.EntryID = item.ID
			}
		} else if item.liveBody != nil || body.RunID != "" {
			body.SourceID, body.RunID = item.ID, item.RunID
		}
		item.Body = &body
	}
	// Private entry-assembly locators never belong to a window clone.
	item.sourceEntryIDs = nil
	return item
}

// projectWindowUpdate is called with subscription.mu and actor.mu held. Its
// work is proportional to the mutation and accepted interest, never history.
func (subscription *ThreadUpdateSubscription) projectWindowUpdate(actor *threadRuntimeState, update ThreadUpdate) (ThreadUpdate, bool) {
	if !subscription.authorized[update.ThreadID.String()] {
		return ThreadUpdate{}, false
	}
	interest, reading := subscription.windows[update.ThreadID.String()]
	items := update.Items
	reordered := update.ItemOrder != nil
	update.Items = nil
	update.ItemOrder = nil
	if !reading {
		if update.Interactions != nil {
			update.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: actor.windowInteractions(nil)}
		}
		update.Turns = nil
		return update, true
	}
	if update.ViewVersion <= interest.version {
		return ThreadUpdate{}, false
	}
	if reordered {
		return subscription.replaceWindow(actor, update, interest)
	}
	for _, change := range items {
		wanted := interest.items[change.ItemID]
		if !wanted && !(interest.tail && change.Kind == ThreadItemAdded) {
			continue
		}
		if change.Item != nil {
			item, err := actor.windowItem(context.Background(), *change.Item, threadDisplayTextBytes)
			if err != nil {
				subscription.finishLocked(ErrThreadUpdateInvalidated)
				return ThreadUpdate{}, false
			}
			change.Item = &item
		}
		if change.Kind == ThreadItemAppended && change.Append != nil {
			if body, truncated := interest.bodies[change.ItemID]; truncated {
				// The accepted preview is immutable. Advance its frozen source extent
				// without reading, cloning or encoding the same prefix again.
				body.Bytes = change.Append.OffsetBytes + len(change.Append.Text)
				change.Kind, change.Body, change.Append = ThreadItemBodyUpdated, &body, nil
			} else if index := actor.itemIndexByID(change.ItemID); index >= 0 && max(len(actor.state.view.Items[index].Text), actor.state.view.Items[index].textBytes) > threadDisplayTextBytes {
				current := actor.state.view.Items[index]
				// A preceding coalesced batch can flush while the actor already
				// contains the next append. Freeze this batch's accepted extent.
				through := change.Append.OffsetBytes + len(change.Append.Text)
				if current.textOmitted {
					current.textBytes = through
				} else {
					current.Text = current.Text[:through]
				}
				item, err := actor.windowItem(context.Background(), current, threadDisplayTextBytes)
				if err != nil {
					subscription.finishLocked(ErrThreadUpdateInvalidated)
					return ThreadUpdate{}, false
				}
				change.Kind, change.Item, change.Append = ThreadItemReplaced, &item, nil
			}
		}
		if interest.bodies == nil {
			interest.bodies = make(map[string]ThreadItemBody)
		}
		if change.Item != nil {
			delete(interest.bodies, change.ItemID)
			if change.Item.Body != nil {
				interest.bodies[change.ItemID] = *change.Item.Body
			}
		} else if change.Body != nil {
			interest.bodies[change.ItemID] = *change.Body
		} else if change.Kind == ThreadItemRemoved {
			delete(interest.bodies, change.ItemID)
		}
		update.Items = append(update.Items, change)
		if change.Kind == ThreadItemRemoved {
			delete(interest.items, change.ItemID)
		} else {
			interest.items[change.ItemID] = true
		}
	}
	if len(interest.items) > threadDisplayPageItems*threadDisplayPages {
		return subscription.replaceWindow(actor, update, interest)
	}
	if update.Turns != nil {
		update.Turns = &ThreadComponent[[]ThreadTurn]{Value: actor.windowTurns(interest.items)}
	}
	if update.Interactions != nil {
		update.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: actor.windowInteractions(interest.items)}
	}
	interest.version = update.ViewVersion
	subscription.windows[update.ThreadID.String()] = interest
	return update, true
}

// windowBaseline explicitly snapshots control components. takeUpdate only
// contains this mutation's mask and cannot serve as an initial baseline.
func (actor *threadRuntimeState) windowBaseline() ThreadUpdate {
	update := actor.runtimeFactsUpdate(actor.state.view.ViewVersion)
	view := actor.state.view
	update.Items, update.ItemOrder = nil, nil
	update.Context = &ThreadComponent[*ThreadContextSnapshot]{Value: view.Context}
	update.Goal = &ThreadComponent[*ThreadGoal]{Value: view.Goal}
	update.ParentEvents = &ThreadComponent[*ParentEventPage]{Value: view.ParentEvents}
	update.Queue = &ThreadComponent[[]QueuedInput]{Value: view.Queue}
	update.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: actor.windowInteractions(nil)}
	update.ToolCallPreviews = &ThreadComponent[[]ThreadToolCallPreview]{Value: view.ToolCallPreviews}
	update.RestoredInputs = &ThreadComponent[[]RestoredInput]{Value: view.RestoredInputs}
	return update
}

func (actor *threadRuntimeState) windowSnapshot(input ThreadWindowInput) (ThreadWindowSnapshot, error) {
	return actor.windowSnapshotContext(context.Background(), input)
}

func (actor *threadRuntimeState) windowSnapshotContext(ctx context.Context, input ThreadWindowInput) (ThreadWindowSnapshot, error) {
	limit := input.Limit
	if limit == 0 {
		limit = threadDisplayPageItems
	}
	direction := input.Direction
	if direction == "" {
		direction = ThreadWindowBefore
	}
	page, err := actor.itemWindowContext(ctx, threadItemWindowInput{anchor: input.AnchorID, direction: threadWindowDirection(direction), limit: limit})
	if err != nil {
		return ThreadWindowSnapshot{}, err
	}
	snapshot := ThreadWindowSnapshot{ThreadID: actor.state.view.ThreadID, ViewVersion: actor.state.view.ViewVersion, HasBefore: page.hasBefore, HasAfter: page.hasAfter, Items: page.items}
	if actor.canonical != nil {
		snapshot.LeafID = actor.canonical.leafID
	}
	for {
		setWindowBounds(&snapshot)
		raw, err := json.Marshal(snapshot)
		if err != nil {
			return ThreadWindowSnapshot{}, err
		}
		if len(raw) <= threadDisplayPageBytes {
			return snapshot, nil
		}
		changed := false
		for i, item := range snapshot.Items {
			if len(item.Text) > 128 {
				snapshot.Items[i] = boundedWindowItem(item, max(128, len(item.Text)/2))
				changed = true
			}
		}
		if changed {
			continue
		}
		if len(snapshot.Items) <= 1 {
			return ThreadWindowSnapshot{}, errors.New("thread window metadata exceeds display budget")
		}
		// Keep an around target even when its neighbors have large metadata.
		removeFirst := direction == ThreadWindowBefore
		if direction == ThreadWindowAround {
			anchor := 0
			for i, item := range snapshot.Items {
				if item.ID == input.AnchorID {
					anchor = i
					break
				}
			}
			removeFirst = anchor >= len(snapshot.Items)/2
		}
		if removeFirst {
			snapshot.Items = snapshot.Items[1:]
			snapshot.HasBefore = true
		} else {
			snapshot.Items = snapshot.Items[:len(snapshot.Items)-1]
			snapshot.HasAfter = true
		}
	}
}

func (subscription *ThreadUpdateSubscription) replaceWindow(actor *threadRuntimeState, update ThreadUpdate, interest threadWindowInterest) (ThreadUpdate, bool) {
	input := ThreadWindowInput{ThreadID: update.ThreadID, AnchorID: interest.anchor, Direction: ThreadWindowAround, Limit: interest.limit}
	if interest.tail {
		input.AnchorID, input.Direction = "", ThreadWindowBefore
	}
	snapshot, err := actor.windowSnapshot(input)
	if errors.Is(err, errThreadWindowAnchorUnavailable) {
		snapshot, err = actor.windowSnapshot(ThreadWindowInput{ThreadID: update.ThreadID, Limit: interest.limit})
		snapshot.AnchorUnavailable = true
	}
	if err != nil {
		subscription.finishLocked(ErrThreadUpdateInvalidated)
		return ThreadUpdate{}, false
	}
	interest.items = make(map[string]bool, len(snapshot.Items))
	for _, item := range snapshot.Items {
		interest.items[item.ID] = true
	}
	// A flushed coalesced batch may precede the actor's current mutation. The
	// replacement freezes the current complete baseline, and later publication
	// at or below it is ignored. Never label current geometry with an old version.
	base := update.BaseVersion
	update = actor.windowBaseline()
	update.BaseVersion = base
	interest.bodies = windowBodyLocators(snapshot.Items)
	interest.anchor, interest.version = snapshot.FirstItemID, snapshot.ViewVersion
	subscription.windows[update.ThreadID.String()] = interest
	update.Items, update.ItemOrder = nil, nil
	update.Window = &ThreadComponent[*ThreadWindowSnapshot]{Value: &snapshot}
	update.Turns = &ThreadComponent[[]ThreadTurn]{Value: actor.windowTurns(interest.items)}
	update.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: actor.windowInteractions(interest.items)}
	return update, true
}

// Window-local locators describe accepted truncated previews only. They never
// retain source text and have the same budget as the accepted window.
func windowBodyLocators(items []ThreadItem) map[string]ThreadItemBody {
	bodies := make(map[string]ThreadItemBody)
	for _, item := range items {
		if item.Body != nil {
			bodies[item.ID] = *item.Body
		}
	}
	return bodies
}
