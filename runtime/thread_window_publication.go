package runtime

import (
	"context"
	"encoding/json"
	"unicode/utf8"
)

// A reading interest owns only cancellable observation work. It never owns
// execution or canonical writes. Publication order stays in the existing queue.
type windowInterestLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newWindowInterestLifetime() *windowInterestLifetime {
	ctx, cancel := context.WithCancel(context.Background())
	return &windowInterestLifetime{ctx: ctx, cancel: cancel}
}

type windowPublication struct {
	actor    *threadRuntimeState
	interest *windowInterestLifetime
	limit    int
	fields   bool
}

func withoutWindowDetail(update ThreadUpdate) ThreadUpdate {
	update.Items, update.Window, update.Turns = nil, nil, nil
	update.ItemOrder = nil
	return update
}

// retireWindowLocked withdraws one interest's entire undelivered detail chain.
// Resident and deferred batches have the same ownership. Explicit SetWindow
// baselines are independent boundaries and do not belong to this chain.
func (subscription *ThreadUpdateSubscription) retireWindowLocked(interest *windowInterestLifetime) {
	if interest == nil {
		return
	}
	interest.cancel()
	count := len(subscription.updates)
	for range count {
		queued := <-subscription.updates
		if queued.interest == interest {
			queued.update = withoutWindowDetail(queued.update)
			queued.hydration = nil
			raw, _ := json.Marshal(queued.update) // A subset of an already encoded batch.
			subscription.queuedBytes -= queued.bytes - len(raw)
			queued.bytes = len(raw)
		}
		subscription.updates <- queued
	}
	// In-flight storage still owns its reservation until Next unwinds. Its
	// final ownership check withdraws detail even if hydration already finished.
}

func (work *windowPublication) read(ctx context.Context, update ThreadUpdate) (ThreadUpdate, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(work.interest.ctx, cancel)
	defer stop()
	defer cancel()
	if work.interest.ctx.Err() != nil {
		return ThreadUpdate{}, context.Canceled
	}
	hydrate := func(item ThreadItem) (ThreadItem, error) {
		return work.actor.windowItemMode(ctx, item, work.limit, work.fields)
	}
	for i := range update.Items {
		if update.Items[i].Item == nil {
			continue
		}
		item, err := hydrate(*update.Items[i].Item)
		if err != nil {
			return ThreadUpdate{}, err
		}
		update.Items[i].Item = &item
	}
	if update.Window != nil && update.Window.Value != nil {
		for i, source := range update.Window.Value.Items {
			item, err := hydrate(source)
			if err != nil {
				return ThreadUpdate{}, err
			}
			update.Window.Value.Items[i] = item
		}
		// Retain selected identities: hydration may shrink previews, never evict
		// an item after its interest and turn membership have been installed.
		for {
			raw, err := json.Marshal(update.Window.Value)
			if err != nil {
				return ThreadUpdate{}, err
			}
			if len(raw) <= threadDisplayPageBytes {
				break
			}
			changed := false
			for i, item := range update.Window.Value.Items {
				if len(item.Text) > 128 {
					update.Window.Value.Items[i] = boundedWindowItem(item, max(128, len(item.Text)/2))
					changed = true
				}
			}
			if !changed {
				return ThreadUpdate{}, ErrThreadUpdateOverflow
			}
		}
	}
	raw, err := json.Marshal(update)
	if err != nil {
		return ThreadUpdate{}, err
	}
	if len(raw) > threadUpdateQueueBytes {
		return ThreadUpdate{}, ErrThreadUpdateOverflow
	}
	return update, nil
}

func (subscription *ThreadUpdateSubscription) enqueueWindowLocked(actor *threadRuntimeState, update ThreadUpdate) bool {
	interest := subscription.windows[update.ThreadID.String()]
	var work *windowPublication
	limit := windowTextLimit(interest.sourceInput)
	if update.Window != nil && update.Window.Value != nil {
		limit = min(limit, threadDisplayPreviewBytes/max(1, len(update.Window.Value.Items))-(utf8.UTFMax-1))
	}
	reserve := 0
	inspect := func(item ThreadItem) {
		if !interest.activityFields || !item.textOmitted && !item.activityOmitted {
			return
		}
		if interest.lifetime == nil {
			interest.lifetime = newWindowInterestLifetime()
			subscription.windows[update.ThreadID.String()] = interest
		}
		if work == nil {
			work = &windowPublication{actor: actor, interest: interest.lifetime, limit: limit, fields: interest.activityFields}
		}
		// Charge bounded decoded previews and retained source locators while
		// queued or reading. Encoded response limits are checked after hydration.
		if item.textOmitted {
			reserve += min(limit, item.textBytes)
		}
		if item.activityOmitted {
			reserve += 8 << 10
		}
		for _, id := range item.activityEntryIDs {
			reserve += len(id) + 16
		}
	}
	for _, change := range update.Items {
		if change.Item != nil {
			inspect(*change.Item)
		}
	}
	if update.Window != nil && update.Window.Value != nil {
		for _, item := range update.Window.Value.Items {
			inspect(item)
		}
	}
	raw, err := json.Marshal(update)
	if err != nil {
		subscription.finishLocked(ErrThreadUpdateInvalidated)
		return false
	}
	if update.Window != nil && update.Window.Value != nil {
		reserve = min(reserve, threadDisplayPageBytes)
	}
	return subscription.enqueuePreparedLocked(queuedThreadUpdate{update: cloneThreadUpdate(update), bytes: len(raw) + reserve, hydration: work, interest: interest.lifetime})
}

// Field-mode publication selects the current resident identities without any
// storage access. Next hydrates its frozen sources outside all actor locks.
func (actor *threadRuntimeState) publicationWindowSnapshot(input ThreadWindowInput) (ThreadWindowSnapshot, error) {
	if !input.ActivityFields {
		return actor.windowSnapshot(input)
	}
	input = normalizeWindowInput(input)
	items, before, after, err := actor.selectWindowItems(input)
	if err != nil {
		return ThreadWindowSnapshot{}, err
	}
	limit := min(threadDisplayTextBytes, threadDisplayPreviewBytes/max(1, len(items))-(utf8.UTFMax-1), windowTextLimit(input))
	snapshot := ThreadWindowSnapshot{resolvedAnchor: actor.resolvedWindowAnchor(input), ThreadID: actor.state.view.ThreadID, ViewVersion: actor.state.view.ViewVersion, ItemSelection: input.ItemIDs != nil, HasBefore: before, HasAfter: after, Items: make([]ThreadItem, len(items))}
	if actor.canonical != nil {
		snapshot.LeafID = actor.canonical.leafID
	}
	for i, item := range items {
		snapshot.Items[i] = actor.freezeWindowItem(item, limit, true)
	}
	return boundWindowSnapshot(snapshot, input)
}
