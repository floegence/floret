package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadWindowBaselinePrecedesContinuousDetailsAndRetainsFacts(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	// The fixture owns an accepted actor; SetWindow must not initialize storage.
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	emitText(sink, "first", "before baseline")
	facts := nextUpdate(t, sub)
	if len(facts.Items) != 0 || facts.Window != nil {
		t.Fatal("unread thread sent body detail")
	}
	// Test the exact actor boundary without this lightweight fixture's absent
	// canonical store; public SetWindow uses the same installation helper.
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", Direction: ThreadWindowBefore}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if baseline.Window == nil || len(baseline.Window.Value.Items) != 1 || baseline.Window.Value.Items[0].Text != "before baseline" {
		t.Fatal("missing bounded baseline")
	}
	emitText(sink, "first", "中文🌸")
	update := nextUpdate(t, sub)
	if update.BaseVersion != baseline.ViewVersion || update.Items[0].Append == nil || update.Items[0].Append.OffsetBytes != len("before baseline") {
		t.Fatal("baseline/delta gap")
	}
	// A failed target does not consume or replace the accepted reading interest.
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", AnchorID: "absent", Direction: ThreadWindowAround}); err == nil {
		t.Fatal("missing anchor accepted")
	}
	emitText(sink, "first", " retained")
	if update := nextUpdate(t, sub); len(update.Items) != 1 {
		t.Fatal("failed page destroyed accepted interest")
	}
	sub.ClearWindow("thread-live")
	emitText(sink, "first", " background")
	if update := nextUpdate(t, sub); len(update.Items) != 0 {
		t.Fatal("cleared interest retained bodies")
	}
}

func TestThreadWindowPreviewBoundedAndQueueRelease(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := range 5 {
		emitText(sink, fmt.Sprint(i), strings.Repeat("中文🌸", 10000))
	}
	for len(sub.updates) > 0 {
		nextUpdate(t, sub)
	}
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", Direction: ThreadWindowBefore}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	for _, item := range baseline.Window.Value.Items {
		if len(item.Text) > threadDisplayTextBytes || item.Body == nil || item.Body.Bytes != 100000 {
			t.Fatal("giant preview was not explicitly bounded")
		}
	}
	emitText(sink, "4", "tail")
	update := nextUpdate(t, sub)
	if len(update.Items) != 1 || update.Items[0].Kind != ThreadItemBodyUpdated || update.Items[0].Body.Bytes != 100004 || update.Items[0].Item != nil || update.Items[0].Append != nil {
		t.Fatal("preview overflow did not replace metadata")
	}
	previewBytes := update.Items[0].Body.PreviewBytes
	update.Items[0].Body.Bytes = -1
	for index := range 20 {
		emitText(sink, "4", "中🌸\r\n")
		update = nextUpdate(t, sub)
		raw, err := json.Marshal(update)
		if err != nil || len(raw) > 2048 || len(update.Items) != 1 {
			t.Fatal("exhausted preview copied source", err)
		}
		change := update.Items[0]
		if change.Kind != ThreadItemBodyUpdated || change.Item != nil || change.Append != nil || change.Body.Bytes != 100004+(index+1)*len("中🌸\r\n") || change.Body.PreviewBytes != previewBytes {
			t.Fatal("metadata delta changed accepted preview or source extent")
		}
	}
	// Establishing another baseline remains explicit and refreshes its extent.
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live"}); err != nil {
		t.Fatal(err)
	}
	baseline = nextUpdate(t, sub)
	if last := baseline.Window.Value.Items[4]; last.Body.Bytes != 100184 || last.Body.PreviewBytes != previewBytes {
		t.Fatal("new baseline did not retain exact prefix")
	}
	sub.Close()
	if sub.queuedBytes != 0 || sub.windows != nil || sub.authorized != nil {
		t.Fatal("closed subscriber retained budget or interest")
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrHostClosed) {
		t.Fatal(err)
	}
}

func TestThreadWindowOverflowUsesPublishedExtentBeforeNextMutation(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	emitText(sink, "source", strings.Repeat("x", threadDisplayTextBytes-1))
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live"}); err != nil {
		t.Fatal(err)
	}
	nextUpdate(t, sub)
	// Freeze two mutations under one actor lock, as publication does when a
	// pending growth batch flushes before a later semantic boundary.
	actor.mu.Lock()
	actor.state.view.Items[0].Text += "firstsecond"
	version := actor.state.view.ViewVersion
	update := ThreadUpdate{ThreadID: "thread-live", BaseVersion: version, ViewVersion: version + 1, Items: []ThreadItemChange{{Kind: ThreadItemAppended, ItemID: "source", Append: &ThreadTextAppend{OffsetBytes: threadDisplayTextBytes - 1, Text: "first"}}}}
	sub.mu.Lock()
	projected, ok := sub.projectWindowUpdate(actor, update)
	sub.mu.Unlock()
	actor.mu.Unlock()
	if !ok || len(projected.Items) != 1 || projected.Items[0].Item == nil || projected.Items[0].Item.Body.Bytes != threadDisplayTextBytes+4 {
		t.Fatal("first batch exposed later canonical extent")
	}
	actor.mu.Lock()
	sub.mu.Lock()
	update.BaseVersion, update.ViewVersion = version+1, version+2
	update.Items = []ThreadItemChange{{Kind: ThreadItemAppended, ItemID: "source", Append: &ThreadTextAppend{OffsetBytes: threadDisplayTextBytes + 4, Text: "second"}}}
	projected, ok = sub.projectWindowUpdate(actor, update)
	sub.mu.Unlock()
	actor.mu.Unlock()
	if !ok || len(projected.Items) != 1 || projected.Items[0].Body == nil || projected.Items[0].Body.Bytes != threadDisplayTextBytes+10 || projected.Items[0].Item != nil {
		t.Fatal("second batch did not advance exact frozen extent")
	}
}

func TestThreadWindowClearAndNextShareQueueOwnership(t *testing.T) {
	for range 100 {
		sub := &ThreadUpdateSubscription{updates: make(chan queuedThreadUpdate, 64), done: make(chan struct{})}
		for i := range 32 {
			sub.mu.Lock()
			sub.enqueueLocked(ThreadUpdate{ThreadID: "thread-live", ViewVersion: uint64(i + 1), Window: &ThreadComponent[*ThreadWindowSnapshot]{Value: &ThreadWindowSnapshot{Items: []ThreadItem{{ID: "body", Text: strings.Repeat("x", 100)}}}}}, 512)
			sub.mu.Unlock()
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			for range 32 {
				if _, err := sub.Next(ctx); err != nil {
					return
				}
			}
		}()
		go func() {
			defer workers.Done()
			for range 32 {
				sub.ClearWindow("thread-live")
			}
		}()
		workers.Wait()
		cancel()
		sub.Close()
		if sub.queuedBytes != 0 {
			t.Fatal("queue accounting retained bytes")
		}
	}
}

func TestThreadWindowRetainsInitialAndClearedControlComponents(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	sub, err := service.SubscribeWindowUpdates(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	approval := ThreadInteraction{ID: "approval", TurnID: "turn-live", RunID: "run-live", Kind: ThreadInteractionApproval, Approval: &ApprovalPresentation{Label: "Approve", ToolName: "shell"}}
	if err := actor.apply(t.Context(), func() error {
		actor.installInteraction(approval, 0)
		actor.state.view.Goal = &ThreadGoal{Text: "objective"}
		actor.state.view.Queue = []QueuedInput{{ID: "queue", RequestKey: "request"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live"}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if baseline.Interactions == nil || len(baseline.Interactions.Value) != 1 || baseline.Queue == nil || len(baseline.Queue.Value) != 1 || baseline.Goal == nil || baseline.Goal.Value.Text != "objective" || baseline.Context == nil || baseline.ToolCallPreviews == nil || baseline.RestoredInputs == nil || baseline.ParentEvents == nil {
		t.Fatal("baseline omitted initial control state")
	}
	if err := actor.apply(t.Context(), func() error {
		actor.state.view.Goal, actor.state.view.Queue = nil, nil
		actor.markComponents(threadGoalChanged | threadQueueChanged)
		actor.resolveInteraction("approval", InteractionResolution{Accepted: true})
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	update := nextUpdate(t, sub)
	if update.Goal == nil || update.Goal.Value != nil || update.Queue == nil || len(update.Queue.Value) != 0 || update.Interactions == nil || len(update.Interactions.Value) != 0 {
		t.Fatal("explicit control clearing was lost")
	}
	if baseline.Interactions.Value[0].Resolved {
		t.Fatal("baseline aliases actor controls")
	}
}

func TestThreadWindowTailEvictsWithoutClosingWorkspaceSubscription(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	sub, err := service.SubscribeWindowUpdates(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	nextUpdate(t, sub)
	resets := 0
	for i := range 1000 {
		emitText(sink, fmt.Sprint(i), "small body")
		update := nextUpdate(t, sub)
		if update.Window != nil {
			resets++
			if len(update.Window.Value.Items) > 10 || update.Window.Value.ViewVersion != update.ViewVersion {
				t.Fatal("invalid replacement baseline")
			}
		}
		if len(sub.windows["thread-live"].items) > threadDisplayPageItems*threadDisplayPages {
			t.Fatal("unbounded tail interest")
		}
	}
	if resets == 0 || sub.closed {
		t.Fatal("tail did not evict in place")
	}
}

func TestThreadWindowAroundRetainsAnchorAfterWireBudgetReduction(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread-live"}}}
	for i := range 150 {
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprint(i), Context: []MessageContextItem{{Title: "metadata", Text: strings.Repeat("x", 16000)}}})
	}
	snapshot, err := actor.windowSnapshot(ThreadWindowInput{AnchorID: "100", Direction: ThreadWindowAround, Limit: 150})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snapshot.Items, func(item ThreadItem) bool { return item.ID == "100" }) {
		t.Fatal("wire reduction removed requested anchor")
	}
	raw, _ := json.Marshal(snapshot)
	if len(raw) > threadDisplayPageBytes {
		t.Fatal("page exceeds wire budget")
	}
}

func TestThreadWindowEmptyScopeNeverReceivesUnauthorizedUpdates(t *testing.T) {
	service, _, sink, full := nativeUpdatesFixture(t)
	full.Close()
	sub, err := service.SubscribeWindowUpdates(t.Context(), threadquery.Scope{Authority: "reader"})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	emitText(sink, "source", "secret")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty scope received update: %v", err)
	}
}

func TestThreadWindowPublicSQLitePagingAuthorizationAndRestart(t *testing.T) {
	path := t.TempDir() + "/window.sqlite"
	open := func() (*Host, *threadRuntimeService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		agent, err := testAgent(newBlockingThreadGateway())
		if err != nil {
			t.Fatal(err)
		}
		full, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, full.(*threadRuntimeService)
	}
	host, service := open()
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread-window"}); err != nil {
		t.Fatal(err)
	}
	for i := range 170 {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: "thread-window", TurnID: "turn-window", RunID: "run-window", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprintf("%d中文🌸", i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-window"}}
	for restart := range 2 {
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: "thread-window", Limit: 10}); err != nil {
			t.Fatal(err)
		}
		// Initialization may precede the explicit baseline on the same channel.
		var update ThreadUpdate
		for update.Window == nil {
			update = nextUpdate(t, sub)
		}
		window := update.Window.Value
		if window.LeafID != "169" || window.FirstItemID != "160" || len(window.Items) != 10 || !window.HasBefore || window.HasAfter {
			t.Fatalf("wrong tail: %+v", window)
		}
		if err := sub.SetWindow(t.Context(), threadquery.Scope{Authority: "reader"}, ThreadWindowInput{ThreadID: "thread-window"}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("revoked scope accepted: %v", err)
		}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: "thread-window", AnchorID: window.FirstItemID, Direction: ThreadWindowBefore, Limit: 10}); err != nil {
			t.Fatal(err)
		}
		page := nextUpdate(t, sub).Window.Value
		if page.FirstItemID != "150" || page.LastItemID != "159" || !page.HasAfter || page.LeafID != window.LeafID {
			t.Fatal("stable paging changed path")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := sub.SetWindow(ctx, scope, ThreadWindowInput{ThreadID: "thread-window"}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		sub.Close()
		if restart == 0 {
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
		}
	}
}

func TestThreadWindowReorderReplacesGeometryAndReportsUnavailableAnchor(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	sub, err := service.SubscribeWindowUpdates(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := range 5 {
		emitText(sink, fmt.Sprint(i), "text")
		nextUpdate(t, sub)
	}
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", AnchorID: "2", Direction: ThreadWindowAround, Limit: 3}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if err := actor.apply(t.Context(), func() error {
		items := cloneThreadItems(actor.state.view.Items)
		slices.Reverse(items)
		for i := range items {
			items[i].Ordinal = uint64(i + 1)
		}
		actor.replaceItems(items)
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	update := nextUpdate(t, sub)
	if update.Window == nil || update.ItemOrder != nil || update.BaseVersion != baseline.ViewVersion || update.Window.Value.AnchorUnavailable {
		t.Fatal("reorder did not replace bounded geometry")
	}
	anchor := sub.windows["thread-live"].anchor
	if err := actor.apply(t.Context(), func() error {
		items := slices.DeleteFunc(cloneThreadItems(actor.state.view.Items), func(item ThreadItem) bool { return item.ID == anchor })
		for i := range items {
			items[i].Ordinal = uint64(i + 1)
		}
		actor.replaceItems(items)
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	update = nextUpdate(t, sub)
	if update.Window == nil || !update.Window.Value.AnchorUnavailable || update.ViewVersion != actor.state.view.ViewVersion {
		t.Fatal("missing anchor did not provide recoverable replacement")
	}
}

func TestThreadWindowTurnsMatchCanonicalSelectedMembership(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	entries[0].Metadata = map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}
	for i := 1; i < len(entries); i++ {
		entries[i].ParentID = entries[i-1].ID
	}
	projection := newThreadCanonicalProjection()
	if err := projection.append(entries); err != nil {
		t.Fatal(err)
	}
	items, _ := projection.items.snapshot()
	turns, err := projection.turns.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	actor := &threadRuntimeState{canonical: projection, state: threadRuntimeData{view: ThreadView{Items: items, Turns: turns}}}
	for start := range len(items) {
		selected := map[string]bool{}
		for _, item := range items[start:min(len(items), start+3)] {
			selected[item.ID] = true
		}
		got := actor.windowTurns(selected)
		for _, turn := range got {
			var expected ThreadTurn
			for _, candidate := range turns {
				if candidate.TurnID == turn.TurnID {
					expected = candidate
					break
				}
			}
			expected.IntermediateItemIDs = selectedThreadIDs(expected.IntermediateItemIDs, selected)
			expected.FinalItemIDs = selectedThreadIDs(expected.FinalItemIDs, selected)
			left, _ := json.Marshal(turn)
			right, _ := json.Marshal(expected)
			if string(left) != string(right) {
				t.Fatalf("selected turn mismatch: %s / %s", left, right)
			}
		}
	}
}

func BenchmarkThreadWindowNativeAppendFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			actor := textGrowthActor(count)
			actor.state.view.ThreadID = "thread"
			actor.state.view.TurnID, actor.state.view.RunID = "turn", "run"
			actor.state.view.ViewVersion = 1
			actor.appendLiveTextSegment(ThreadItemAssistant, "start", time.Time{}, "source")
			sub := &ThreadUpdateSubscription{updates: make(chan queuedThreadUpdate, 64), done: make(chan struct{}), windowed: true, authorized: map[string]bool{"thread": true}}
			service := &threadRuntimeService{published: map[string]uint64{}, updateSubscribers: map[*ThreadUpdateSubscription]struct{}{sub: {}}}
			actor.service, sub.service = service, service
			if err := sub.setActorWindow(b.Context(), actor, ThreadWindowInput{ThreadID: "thread"}); err != nil {
				b.Fatal(err)
			}
			if _, err := sub.Next(b.Context()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := actor.apply(b.Context(), func() error {
					actor.appendLiveTextSegment(ThreadItemAssistant, "x", time.Time{}, "source")
					actor.state.view.ViewVersion++
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				actor.mu.Lock()
				service.flushUpdateLocked(actor)
				actor.mu.Unlock()
				if _, err := sub.Next(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			actor.stopUpdateTimer()
		})
	}
}

func TestEmptyCanonicalProjectionStartsWithAbsentGoal(t *testing.T) {
	projection := newThreadCanonicalProjection()
	view, err := projection.retainedSnapshot("empty-thread", nil)
	if err != nil {
		t.Fatal(err)
	}
	if view.Goal == nil || view.Goal.Status != "absent" || view.Goal.Version != 0 || view.Goal.Actor != "" || view.Goal.Text != "" {
		t.Fatalf("empty canonical Goal = %#v", view.Goal)
	}
	// Public full and bounded reads share this reducer, including after reopen.
	state, err := sessiontree.GoalFromPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	if *view.Goal != *publicThreadGoal(state) {
		t.Fatal("empty chronological projections disagree")
	}
}
