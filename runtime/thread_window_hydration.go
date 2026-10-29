package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// Prepared bodies are disposable request-local data. The actor still selects
// identities, owns versions and installs baseline plus interest atomically.
// A changing resident draft is taken at commit, never waited on or re-read.
type preparedWindowItem struct{ source, value ThreadItem }
type windowReadRequest struct{ cancel context.CancelCauseFunc }

func (subscription *ThreadUpdateSubscription) withWindowRead(ctx context.Context, thread string, read func(context.Context, *windowReadRequest) error) error {
	if ctx == nil {
		return errors.New("thread window context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	token := &windowReadRequest{cancel: cancel}
	subscription.mu.Lock()
	if subscription.closed {
		err := subscription.closeErr
		subscription.mu.Unlock()
		return err
	}
	if subscription.windowReads == nil {
		subscription.windowReads = make(map[string]*windowReadRequest)
	}
	if prior := subscription.windowReads[thread]; prior != nil {
		prior.cancel(ErrThreadUpdateInvalidated)
	}
	subscription.windowReads[thread] = token
	subscription.mu.Unlock()
	defer func() {
		subscription.mu.Lock()
		if subscription.windowReads[thread] == token {
			delete(subscription.windowReads, thread)
		}
		subscription.mu.Unlock()
	}()
	return read(ctx, token)
}

func (subscription *ThreadUpdateSubscription) prepareActorWindow(ctx context.Context, actor *threadRuntimeState, input ThreadWindowInput, token *windowReadRequest) error {
	if input.Limit < 1 || input.Limit > threadDisplayPageItems {
		return errors.New("invalid thread window limit")
	}
	if input.Direction != ThreadWindowBefore && input.Direction != ThreadWindowAfter && input.Direction != ThreadWindowAround {
		return errors.New("invalid thread window direction")
	}
	var summaryMeta sessiontree.ThreadMeta
	if input.IncludeSummary {
		var err error
		summaryMeta, err = actor.service.ensureThread(ctx, input.ThreadID)
		if err != nil {
			return err
		}
	}
	cache := map[string]preparedWindowItem{}
	var cold *threadSourceProjection
	var coldView *canonicalThreadView
	var coldBefore, coldAfter bool
	var coldAnchor string
	var skippedRange *ThreadItemRange
	var warmLeaf string
	var warmRangeReady bool
	var warmInput ThreadWindowInput
	warmAttempts := 0
	for {
		var missing []ThreadItem
		var loadCold bool
		var done bool
		var pending <-chan struct{}
		limit := 0
		err := actor.apply(ctx, func() error {
			if actor.state.contextErr != nil {
				return actor.state.contextErr
			}
			subscription.mu.Lock()
			current := !subscription.closed && subscription.windowReads[input.ThreadID.String()] == token
			subscription.mu.Unlock()
			if !current {
				return ErrThreadUpdateInvalidated
			}
			// A source refresh freezes all accepted readers before publication. A
			// concurrently opened reader joins that publication before installing
			// another interest, so no reader is absent from its frozen sources.
			// Complete execution recovery promotes the same actor atomically and
			// does not freeze display interests. Its slow or failed read must not
			// delay an already accepted source window.
			if preparation := actor.windowPreparation; preparation != nil {
				select {
				case <-preparation.done:
				default:
					pending = preparation.done
					return nil
				}
			}
			var items []ThreadItem
			var snapshot ThreadWindowSnapshot
			var turns []ThreadTurn
			var interactions []ThreadInteraction
			if actor.sourceWindow != nil {
				if cold != actor.sourceWindow {
					cold, coldView, cache = actor.sourceWindow, nil, map[string]preparedWindowItem{}
					coldBefore, coldAfter = cold.hasBefore, cold.hasAfter
					coldAnchor = cold.resolvedAnchor
					skippedRange = cold.skippedRange
				}
				if coldView == nil {
					if sameWindowInterval(input, cold.input) {
						value := canonicalThreadView{ThreadView: actor.state.view}
						value.Items = cloneThreadItems(value.Items)
						value.Turns = cloneThreadTurns(value.Turns)
						value.Interactions = cloneThreadInteractions(value.Interactions)
						coldView = &value
					} else {
						for _, frozen := range cold.companions {
							if sameWindowInterval(input, frozen.page.input) {
								value := frozen.view
								coldView = &value
								coldBefore, coldAfter = frozen.page.hasBefore, frozen.page.hasAfter
								coldAnchor = frozen.page.resolvedAnchor
								skippedRange = frozen.page.skippedRange
								break
							}
						}
						if coldView == nil {
							loadCold = true
							return nil
						}
					}
				}
				items = coldView.Items
				snapshot = ThreadWindowSnapshot{resolvedAnchor: coldAnchor, ThreadID: input.ThreadID, LeafID: cold.leaf, HasBefore: coldBefore, HasAfter: coldAfter}
				turns, interactions = coldView.Turns, coldView.Interactions
			} else {
				if input.SkipRange != nil {
					if actor.canonical == nil {
						return ErrUnsupportedStoreCapability
					}
					if !warmRangeReady || warmLeaf != actor.canonical.leafID {
						warmLeaf = actor.canonical.leafID
						warmRangeReady = false
						loadCold = true
						return nil
					}
				}
				selectionInput := input
				if input.SkipRange != nil {
					selectionInput = warmInput
				}
				selected, before, after, err := actor.selectWindowItems(selectionInput)
				if err != nil {
					return err
				}
				items = selected
				snapshot = ThreadWindowSnapshot{resolvedAnchor: actor.resolvedWindowAnchor(input), ThreadID: input.ThreadID, HasBefore: before, HasAfter: after}
				if actor.canonical != nil {
					snapshot.LeafID = actor.canonical.leafID
				}
			}
			snapshot.SkippedRange = skippedRange
			snapshot.RequestID = input.RequestID
			snapshot.ItemSelection = input.ItemIDs != nil || input.SkipRange != nil || len(input.RetainItemIDs) > 0
			limit = min(threadDisplayTextBytes, threadDisplayPreviewBytes/max(1, len(items))-(utf8.UTFMax-1), windowTextLimit(input))
			snapshot.Items = make([]ThreadItem, len(items))
			nextCache := make(map[string]preparedWindowItem, len(items))
			for i, item := range items {
				detached := actor.freezeWindowItem(item, limit, input.ActivityFields)
				if detached.textOmitted || detached.activityOmitted && detached.ActivitySummary == nil {
					prepared, ok := cache[item.ID]
					value, reusable := prepared.reuse(detached)
					if !ok || !reusable {
						missing = append(missing, detached)
						continue
					}
					snapshot.Items[i] = value
					nextCache[item.ID] = prepared
				} else {
					snapshot.Items[i] = detached
				}
			}
			cache = nextCache
			if len(missing) > 0 {
				return nil
			}
			snapshot.ViewVersion = actor.state.view.ViewVersion
			if input.IncludeSummary {
				if !actor.hasCompleteSummaryPreview() {
					return ErrAuthorityCorrupt
				}
				meta := summaryMeta
				if actor.sourceWindow != nil {
					// Metadata and preview share the accepted source snapshot. A
					// separately sampled UpdatedAt could acknowledge unseen work.
					meta = actor.sourceWindow.meta
				}
				summary := actor.threadSummary(meta)
				snapshot.Summary = &summary
			}
			var err error
			snapshot, err = boundWindowSnapshot(snapshot, input)
			if err != nil {
				return err
			}
			selected := make(map[string]bool, len(snapshot.Items))
			for _, item := range snapshot.Items {
				selected[item.ID] = true
			}
			if actor.sourceWindow == nil {
				turns, interactions = actor.windowTurns(selected), actor.windowInteractions(selected)
			} else {
				turns = cloneThreadTurns(turns)
				for i := range turns {
					turns[i].IntermediateItemIDs = selectedThreadIDs(turns[i].IntermediateItemIDs, selected)
					turns[i].FinalItemIDs = selectedThreadIDs(turns[i].FinalItemIDs, selected)
				}
				interactions = selectedWindowInteractions(snapshot.Items, interactions)
			}
			actor.service.flushUpdateLocked(actor)
			baseline := actor.windowBaseline()
			baseline.Items = nil
			baseline.Window = &ThreadComponent[*ThreadWindowSnapshot]{Value: &snapshot}
			baseline.Turns = &ThreadComponent[[]ThreadTurn]{Value: turns}
			baseline.Interactions = &ThreadComponent[[]ThreadInteraction]{Value: interactions}
			raw, err := json.Marshal(baseline)
			if err != nil {
				return err
			}
			subscription.mu.Lock()
			defer subscription.mu.Unlock()
			if subscription.closed {
				return subscription.closeErr
			}
			if subscription.windowReads[input.ThreadID.String()] != token {
				return ErrThreadUpdateInvalidated
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !subscription.enqueueLocked(cloneThreadUpdate(baseline), len(raw)) {
				return ErrThreadUpdateOverflow
			}
			if subscription.windows == nil {
				subscription.windows = map[string]threadWindowInterest{}
			}
			if previous := subscription.windows[input.ThreadID.String()]; previous.lifetime != nil {
				previous.lifetime.cancel()
			}
			retainedInput := normalizeWindowInput(input)
			if input.AnchorOffset != 0 || input.SkipRange != nil || len(input.RetainItemIDs) > 0 {
				retainedInput.SkipRange, retainedInput.RetainItemIDs = nil, nil
				retainedInput.AnchorID, retainedInput.AnchorOffset, retainedInput.Direction = "", 0, ""
				retainedInput.ItemIDs = make([]string, len(snapshot.Items))
				for i, item := range snapshot.Items {
					retainedInput.ItemIDs[i] = item.ID
				}
				retainedInput.Limit = 0
			}
			subscription.windows[input.ThreadID.String()] = threadWindowInterest{sourceInput: normalizeWindowInput(retainedInput), selection: slices.Clone(retainedInput.ItemIDs), lifetime: newWindowInterestLifetime(), items: selected, bodies: windowBodyLocators(snapshot.Items), tail: input.ItemIDs == nil && input.AnchorID == "" && input.Direction == ThreadWindowBefore, version: snapshot.ViewVersion, anchor: snapshot.FirstItemID, limit: input.Limit, activityFields: input.ActivityFields}
			done = true
			return nil
		})
		if err != nil || done {
			return err
		}
		if pending != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pending:
			}
			continue
		}
		if loadCold && !warmRangeReady && input.SkipRange != nil && cold == nil {
			reader, ok := actor.service.host.store.repo.(sessiontree.ItemMembershipRangeReader)
			if !ok {
				return ErrUnsupportedStoreCapability
			}
			warmAttempts++
			if warmAttempts > 8 {
				return spi.ErrConflict
			}
			query := canonicalWindowRange(input.SkipRange)
			query.ThreadID, query.LeafID, query.AnchorID = input.ThreadID.String(), warmLeaf, input.AnchorID
			selected, err := reader.ItemMembershipRange(ctx, *query)
			value := publicThreadItemRange(input.ThreadID, selected)
			if errors.Is(err, spi.ErrConflict) {
				continue
			}
			err = activityReadError(err)
			if err != nil && !errors.Is(err, threadquery.ErrInvalidQuery) {
				return err
			}
			warmInput = input
			warmInput.SkipRange = nil
			skippedRange = nil
			if err == nil {
				skippedRange = &value
				if input.Direction == ThreadWindowBefore {
					warmInput.AnchorID = value.FirstItemID
				} else {
					warmInput.AnchorID = value.LastItemID
				}
				warmInput.RetainItemIDs = append(slices.Clone(input.RetainItemIDs), value.FirstItemID, value.LastItemID)
			}
			warmRangeReady = true
			continue
		}
		if loadCold {
			// Read another interval at the already accepted leaf without replacing
			// actor facts. The next actor turn discards it if publication advanced.
			// A slow complete recovery must not serialize ordinary browsing.
			value, page, err := actor.service.loadSourceWindowAt(ctx, input, cold.leaf)
			if errors.Is(err, spi.ErrConflict) {
				// Membership changed after the accepted leaf was sampled. Refresh
				// facts and every accepted interest through the existing owner;
				// never attach a newer interval to an older actor version.
				if err := actor.service.prepareThreadRead(ctx, input.ThreadID, actor, true, input); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			coldView = &value
			coldBefore, coldAfter, coldAnchor = page.hasBefore, page.hasAfter, page.resolvedAnchor
			skippedRange = page.skippedRange
			continue
		}
		for _, item := range missing {
			value, err := actor.windowItemMode(ctx, item, limit, input.ActivityFields)
			if err != nil {
				return err
			}
			cache[item.ID] = preparedWindowItem{source: item, value: value}
		}
	}
}

func (actor *threadRuntimeState) freezeWindowItem(item ThreadItem, limit int, fields bool) ThreadItem {
	if fields && item.Activity != nil && item.activityOmitted && item.activitySummary != nil {
		item = activitySummaryWindowItem(item, *item.activitySummary, limit)
	}
	if fields && item.Activity != nil && !item.activityOmitted && item.Activity.Presentation != nil {
		item = activitySummaryWindowItem(item, residentItemActivitySummary(item), limit)
	}
	if fields && item.liveBody != nil && item.livePreview != "" {
		item = boundedWindowItem(item, limit)
		item.Text, _ = boundedThreadText(item.livePreview, limit)
		item.textOmitted, item.liveBody = false, nil
		item.livePreview = ""
		if item.Body != nil {
			body := *item.Body
			body.PreviewBytes = len(item.Text)
			item.Body = &body
		}
	}
	return cloneWindowItemsLimit([]ThreadItem{item}, limit)[0]
}

func selectedWindowInteractions(items []ThreadItem, interactions []ThreadInteraction) []ThreadInteraction {
	ids := map[string]bool{}
	for _, item := range items {
		if item.Interaction != nil {
			ids[item.Interaction.ID] = true
		}
	}
	out := []ThreadInteraction{}
	for _, interaction := range interactions {
		if !interaction.Resolved || ids[interaction.ID] {
			out = append(out, cloneThreadInteraction(interaction))
		}
	}
	return out
}

func normalizeWindowInput(input ThreadWindowInput) ThreadWindowInput {
	input.RetainItemIDs = slices.Clone(input.RetainItemIDs)
	if input.SkipRange != nil {
		value := *input.SkipRange
		value.Kinds = slices.Clone(value.Kinds)
		input.SkipRange = &value
	}

	if input.ItemIDs != nil {
		input.ItemIDs = slices.Clone(input.ItemIDs)
		input.Limit = len(input.ItemIDs)
	} else if input.Limit == 0 {
		input.Limit = threadDisplayPageItems
	}
	if input.Direction == "" {
		input.Direction = ThreadWindowBefore
	}
	return input
}
func sameWindowInterval(a, b ThreadWindowInput) bool {
	a, b = normalizeWindowInput(a), normalizeWindowInput(b)
	return reflect.DeepEqual(a.SkipRange, b.SkipRange) && slices.Equal(a.RetainItemIDs, b.RetainItemIDs) && slices.Equal(a.ItemIDs, b.ItemIDs) && (a.ItemIDs == nil) == (b.ItemIDs == nil) && a.ActivityFields == b.ActivityFields && windowTextLimit(a) == windowTextLimit(b) && a.AnchorID == b.AnchorID && a.AnchorOffset == b.AnchorOffset && a.Direction == b.Direction && a.Limit == b.Limit
}

// A live body is append-only. A bounded prefix already read from this exact
// source remains valid as its extent grows; the baseline advertises unread
// suffix bytes without chasing them. Other source changes require a new read.
func (prepared preparedWindowItem) reuse(current ThreadItem) (ThreadItem, bool) {
	if reflect.DeepEqual(prepared.source, current) {
		return prepared.value, true
	}
	source := prepared.source
	if source.liveBody == nil || current.liveBody != source.liveBody || current.textBytes < source.textBytes || current.Body == nil || source.Body == nil {
		return ThreadItem{}, false
	}
	normalized := current
	normalized.textBytes = source.textBytes
	normalized.textSummary, normalized.textSummaryTail = source.textSummary, source.textSummaryTail
	normalized.textNonSpace = source.textNonSpace
	body := *current.Body
	body.Bytes = source.Body.Bytes
	normalized.Body = &body
	if !reflect.DeepEqual(source, normalized) {
		return ThreadItem{}, false
	}
	value := current
	value.Text = prepared.value.Text
	value.textOmitted, value.liveBody = false, nil
	body = *current.Body
	body.PreviewBytes = len(value.Text)
	value.Body = &body
	return value, true
}

func windowTextLimit(input ThreadWindowInput) int {
	if input.PreviewBytes > 0 {
		return input.PreviewBytes
	}
	return threadDisplayTextBytes
}

func validateWindowSelection(input ThreadWindowInput) error {
	if input.SkipRange != nil || len(input.RetainItemIDs) > 0 {
		if input.ItemIDs != nil || input.Limit < 1 || len(input.RetainItemIDs)+input.Limit+2 > threadDisplayPageItems {
			return errors.New("invalid navigation window size")
		}
		seen := map[string]bool{}
		for _, id := range input.RetainItemIDs {
			if id == "" || seen[id] {
				return errors.New("invalid retained identity")
			}
			seen[id] = true
		}
		if input.SkipRange != nil {
			if input.AnchorID == "" || input.AnchorOffset != 0 || input.Direction != ThreadWindowBefore && input.Direction != ThreadWindowAfter || len(input.SkipRange.Kinds) == 0 || len(input.SkipRange.Kinds) > 7 {
				return errors.New("invalid range navigation")
			}
			allowed := map[ThreadItemKind]bool{ThreadItemUser: true, ThreadItemAssistant: true, ThreadItemThinking: true, ThreadItemTool: true, ThreadItemInteraction: true, ThreadItemGoal: true, ThreadItemSteering: true}
			for _, kind := range input.SkipRange.Kinds {
				if !allowed[kind] {
					return errors.New("invalid range kind")
				}
			}
			if input.SkipRange.Exclude & ^ThreadItemRangeFact(255) != 0 {
				return errors.New("invalid range facts")
			}
		}
	}

	if input.PreviewBytes != 0 && (input.PreviewBytes < 128 || input.PreviewBytes > threadDisplayTextBytes) {
		return errors.New("invalid thread window preview budget")
	}
	if input.AnchorOffset != 0 && (input.AnchorID == "" || input.ItemIDs != nil || input.Direction != ThreadWindowAround) {
		return errors.New("relative window anchor requires an around identity")
	}
	if len(input.RequestID) > 128 || !utf8.ValidString(input.RequestID) {
		return errors.New("invalid thread window request correlation")
	}
	if input.ItemIDs == nil {
		return nil
	}
	if len(input.ItemIDs) < 1 || len(input.ItemIDs) > threadDisplayPageItems || input.AnchorID != "" || input.Direction != "" || input.Limit != 0 {
		return errors.New("invalid explicit thread item selection")
	}
	seen := map[string]bool{}
	for _, id := range input.ItemIDs {
		if id == "" || seen[id] {
			return errors.New("invalid explicit thread item identity")
		}
		seen[id] = true
	}
	return nil
}

func (actor *threadRuntimeState) selectWindowItems(input ThreadWindowInput) ([]ThreadItem, bool, bool, error) {
	if input.ItemIDs == nil {
		page, err := actor.selectItemWindow(threadItemWindowInput{anchor: input.AnchorID, anchorOffset: input.AnchorOffset, direction: threadWindowDirection(input.Direction), limit: input.Limit})
		if err != nil {
			return nil, false, false, err
		}
		if len(input.RetainItemIDs) == 0 {
			return actor.state.view.Items[page.first:page.last], page.hasBefore, page.hasAfter, nil
		}
		ids := make([]string, 0, len(input.RetainItemIDs)+page.last-page.first)
		seen := map[string]bool{}
		for _, id := range input.RetainItemIDs {
			if !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		for _, item := range actor.state.view.Items[page.first:page.last] {
			if !seen[item.ID] {
				ids = append(ids, item.ID)
				seen[item.ID] = true
			}
		}
		if len(ids) > threadDisplayPageItems {
			return nil, false, false, errors.New("navigation exceeds window size")
		}
		return actor.selectWindowItems(ThreadWindowInput{ItemIDs: ids})
	}
	positions := make([]int, 0, len(input.ItemIDs))
	for _, id := range input.ItemIDs {
		i := actor.itemIndexByID(id)
		if i < 0 {
			return nil, false, false, errThreadWindowAnchorUnavailable
		}
		positions = append(positions, i)
	}
	slices.Sort(positions)
	items := make([]ThreadItem, len(positions))
	for i, index := range positions {
		items[i] = actor.state.view.Items[index]
	}
	return items, positions[0] > 0, positions[len(positions)-1] < len(actor.state.view.Items)-1, nil
}

// Resolve once from the same resident membership used to select the page. A
// wire-budget reduction must preserve the actual target, including edge seeks.
func (actor *threadRuntimeState) resolvedWindowAnchor(input ThreadWindowInput) string {
	if input.Direction != ThreadWindowAround || input.AnchorID == "" {
		return ""
	}
	anchor := actor.itemIndexByID(input.AnchorID)
	if anchor < 0 || input.AnchorOffset < -anchor || input.AnchorOffset >= len(actor.state.view.Items)-anchor {
		return ""
	}
	return actor.state.view.Items[anchor+input.AnchorOffset].ID
}
