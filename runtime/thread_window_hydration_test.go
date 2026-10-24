package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/threadquery"
)

type blockedWindowBodyRepo struct {
	*internalstorage.BackendKernel
	entered, release chan struct{}
	once             sync.Once
}

func (repo *blockedWindowBodyRepo) ReadCanonicalBody(ctx context.Context, thread, entry, field string, offset, limit int) (sessiontree.CanonicalBodyChunk, error) {
	repo.once.Do(func() { close(repo.entered) })
	select {
	case <-ctx.Done():
		return sessiontree.CanonicalBodyChunk{}, ctx.Err()
	case <-repo.release:
		return repo.BackendKernel.ReadCanonicalBody(ctx, thread, entry, field, offset, limit)
	}
}

func TestWindowHydrationDoesNotHoldExecutionActor(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "window-read-concurrent"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "persisted message"}}, sessiontree.AppendOptions{ID: "message"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(id)
	repo := &blockedWindowBodyRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	var release sync.Once
	defer release.Do(func() { close(repo.release) })
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	read := make(chan error, 1)
	go func() { read <- sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id}) }()
	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("window did not reach body read")
	}
	mutation := make(chan error, 1)
	go func() {
		mutation <- actor.apply(t.Context(), func() error {
			actor.state.turnID, actor.state.runID = "turn", "run"
			actor.appendLiveTextSegment(ThreadItemAssistant, "arrived during read", time.Time{}, "live")
			actor.state.view.ViewVersion++
			return nil
		})
	}()
	select {
	case err := <-mutation:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("window body read blocked the execution actor")
	}
	release.Do(func() { close(repo.release) })
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	var baseline ThreadUpdate
	for baseline.Window == nil {
		baseline = nextUpdate(t, sub)
	}
	if len(baseline.Window.Value.Items) != 2 || baseline.Window.Value.Items[0].Text != "persisted message" || baseline.Window.Value.Items[1].Text != "arrived during read" {
		t.Fatalf("baseline lost concurrent state: %#v", baseline.Window.Value.Items)
	}
	if baseline.ViewVersion != actor.state.view.ViewVersion {
		t.Fatal("baseline has stale version")
	}
}

func TestWindowPublicationDoesNotHoldExecutionActor(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "window-publication-concurrent"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "persisted message"}}, sessiontree.AppendOptions{ID: "message"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.View(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(id)
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id, ActivityFields: true}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	repo := &blockedWindowBodyRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	var release sync.Once
	defer release.Do(func() { close(repo.release) })
	mutation := make(chan error, 1)
	go func() {
		mutation <- actor.apply(t.Context(), func() error {
			actor.markItem("message", ThreadItemReplaced)
			actor.state.view.ViewVersion++
			return nil
		})
	}()
	select {
	case err := <-mutation:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("window publication body read blocked the execution actor")
	}
	type result struct {
		update ThreadUpdate
		err    error
	}
	read := make(chan result, 1)
	go func() { u, err := sub.Next(t.Context()); read <- result{u, err} }()
	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("publication did not hydrate its selected body")
	}
	if err := actor.apply(t.Context(), func() error {
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(repo.release) })
	value := <-read
	if value.err != nil {
		t.Fatal(value.err)
	}
	if value.update.BaseVersion != baseline.ViewVersion || len(value.update.Items) != 1 || value.update.Items[0].Item.Text != "persisted message" {
		t.Fatalf("deferred publication lost its exact baseline: %+v", value)
	}
}

func TestWindowPublicationCancellationKeepsVersionOrder(t *testing.T) {
	for _, mode := range []string{"clear", "replace", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			host, public := testThreadService(t, newBlockingThreadGateway())
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "window-publication-concurrent"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "persisted message"}}, sessiontree.AppendOptions{ID: "message"}); err != nil {
				t.Fatal(err)
			}
			if _, err := service.View(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			actor := service.runtime(id)
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id, ActivityFields: true}); err != nil {
				t.Fatal(err)
			}
			baseline := nextUpdate(t, sub)
			repo := &blockedWindowBodyRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
			host.store.repo = repo
			var release sync.Once
			defer release.Do(func() { close(repo.release) })

			if err := actor.apply(t.Context(), func() error {
				actor.markItem("message", ThreadItemReplaced)
				actor.state.view.ViewVersion++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			type result struct {
				update ThreadUpdate
				err    error
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			read := make(chan result, 1)
			go func() { u, err := sub.Next(ctx); read <- result{u, err} }()
			select {
			case <-repo.entered:
			case <-time.After(time.Second):
				t.Fatal("read never started")
			}
			switch mode {
			case "clear":
				sub.ClearWindow(id)
			case "close":
				sub.Close()
			case "cancel":
				cancel()
			case "replace":
				if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id, AnchorID: "message", Direction: ThreadWindowAfter, Limit: 1, ActivityFields: true}); err != nil {
					t.Fatal(err)
				}
			}
			var value result
			select {
			case value = <-read:
			case <-time.After(time.Second):
				t.Fatal("cancel retained storage read")
			}
			if mode == "cancel" {
				if !errors.Is(value.err, context.Canceled) {
					t.Fatalf("cancel = %v", value.err)
				}
				sub.mu.Lock()
				retained := sub.inflight != nil && sub.queuedBytes > 0
				sub.mu.Unlock()
				if !retained {
					t.Fatal("cancel discarded pending publication")
				}
				release.Do(func() { close(repo.release) })
				value.update, value.err = sub.Next(t.Context())
				if value.err != nil || value.update.BaseVersion != baseline.ViewVersion || len(value.update.Items) != 1 || value.update.Items[0].Item.Text != "persisted message" {
					t.Fatalf("retry skipped accepted mutation: %+v", value)
				}
			} else if mode == "close" {
				if !errors.Is(value.err, ErrHostClosed) {
					t.Fatalf("closed = %v", value.err)
				}
			} else {
				if value.err != nil || value.update.Items != nil || value.update.Window != nil || value.update.BaseVersion != baseline.ViewVersion {
					t.Fatalf("stale detail returned: %+v", value)
				}
				if mode == "replace" {
					update := nextUpdate(t, sub)
					if update.Window == nil || len(update.Window.Value.Items) != 0 {
						t.Fatal("replacement baseline lost")
					}
				}
			}
			sub.mu.Lock()
			defer sub.mu.Unlock()
			if sub.inflight != nil || sub.queuedBytes != 0 {
				t.Fatal("read budget was not released")
			}
		})
	}
}

func TestWindowHydrationClearAndCloseCancelRead(t *testing.T) {
	for _, mode := range []string{"clear", "close", "cancel", "supersede"} {
		t.Run(mode, func(t *testing.T) {
			host, public := testThreadService(t, newBlockingThreadGateway())
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "window-cancel-read"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			for _, entryID := range []string{"older", "newer"} {
				if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: entryID}}, sessiontree.AppendOptions{ID: entryID}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.View(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			actor := service.runtime(id)
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id, Limit: 1}); err != nil {
				t.Fatal(err)
			}
			baseline := nextUpdate(t, sub)
			repo := &blockedWindowBodyRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
			host.store.repo = repo
			defer close(repo.release)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			read := make(chan error, 1)
			go func() {
				read <- sub.setActorWindow(ctx, actor, ThreadWindowInput{ThreadID: id, AnchorID: "newer", Direction: ThreadWindowBefore, Limit: 1})
			}()
			select {
			case <-repo.entered:
			case <-time.After(time.Second):
				t.Fatal("body read did not start")
			}
			switch mode {
			case "clear":
				sub.ClearWindow(id)
			case "close":
				sub.Close()
			case "cancel":
				cancel()
			case "supersede":
				// This valid empty window performs no source read and must win immediately.
				if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: id, AnchorID: "newer", Direction: ThreadWindowAfter, Limit: 1}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-read:
				if err == nil {
					t.Fatal("cancelled window was accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled read retained storage work")
			}
			sub.mu.Lock()
			defer sub.mu.Unlock()
			if len(sub.windowReads) != 0 {
				t.Fatal("pending read retained")
			}
			switch mode {
			case "clear", "close":
				if len(sub.windows) != 0 {
					t.Fatal("cancelled interest returned")
				}
			case "cancel":
				if sub.windows[id.String()].anchor != baseline.Window.Value.FirstItemID {
					t.Fatal("cancel replaced accepted interest")
				}
			case "supersede":
				if len(sub.windows[id.String()].items) != 0 {
					t.Fatal("late read overwrote newer interest")
				}
			}
		})
	}
}

type observedWindowProjectionRepo struct {
	*internalstorage.BackendKernel
	mu     sync.Mutex
	inputs []sessiontree.ItemMembershipWindowOptions
}

func (repo *observedWindowProjectionRepo) ReadWindowProjection(ctx context.Context, input sessiontree.ItemMembershipWindowOptions, fields func(sessiontree.PathMetadata, bool) []string) (sessiontree.WindowProjection, error) {
	repo.mu.Lock()
	repo.inputs = append(repo.inputs, input)
	repo.mu.Unlock()
	return repo.BackendKernel.ReadWindowProjection(ctx, input, fields)
}
func TestColdWindowPreparesOnlyRequestedInterval(t *testing.T) {
	for _, limit := range []int{1, 40} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			host, public := testThreadService(t, newBlockingThreadGateway())
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "requested-window"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			for i := range 160 {
				if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					t.Fatal(err)
				}
			}
			repo := &observedWindowProjectionRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
			host.store.repo = repo
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, AnchorID: "100", Direction: ThreadWindowBefore, Limit: limit}); err != nil {
				t.Fatal(err)
			}
			var baseline ThreadUpdate
			for baseline.Window == nil {
				baseline = nextUpdate(t, sub)
			}
			if len(baseline.Window.Value.Items) != limit || baseline.Window.Value.LastItemID != "99" {
				t.Fatal("wrong selected interval")
			}
			repo.mu.Lock()
			defer repo.mu.Unlock()
			if len(repo.inputs) != 1 || repo.inputs[0].Limit != limit || repo.inputs[0].AnchorID != "100" {
				t.Fatalf("cold read prepared more than requested: %+v", repo.inputs)
			}
		})
	}
}

type growingWindowBody struct {
	calls  int
	append func()
}

func (body *growingWindowBody) Read(ctx context.Context, offset, limit, through int) (string, error) {
	body.calls++
	body.append()
	if body.calls > 8 {
		return "", errors.New("window repeatedly chased the growing extent")
	}
	return strings.Repeat("x", min(limit, through-offset)), nil
}
func TestWindowHydrationAcceptsFrozenGrowingPrefix(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	actor.threadID = "thread-live"
	body := &growingWindowBody{}
	actor.state.view.Items = []ThreadItem{{ID: "growing", TurnID: "turn", RunID: "run", Kind: ThreadItemAssistant, textOmitted: true, textBytes: 100, liveBody: body}}
	actor.invalidateItemIndex()
	body.append = func() {
		if err := actor.apply(t.Context(), func() error {
			actor.state.view.Items[0].textBytes++
			actor.state.view.ViewVersion++
			return nil
		}); err != nil {
			t.Error(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", Limit: 1}); err != nil {
		t.Fatal(err)
	}
	var baseline ThreadUpdate
	for baseline.Window == nil {
		baseline = nextUpdate(t, sub)
	}
	item := baseline.Window.Value.Items[0]
	if body.calls != 1 || len(item.Text) != 100 || item.Body == nil || item.Body.Bytes != 101 || item.Body.PreviewBytes != 100 {
		t.Fatalf("frozen prefix mismatch: calls=%d item=%+v", body.calls, item)
	}
}

func TestColdHistoricalWindowDoesNotReplaceLatestSummary(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "historical-summary"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"older", "newer"} {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: name}}, sessiontree.AppendOptions{ID: name}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, AnchorID: "newer", Direction: ThreadWindowBefore, Limit: 1}); err != nil {
		t.Fatal(err)
	}
	summary, err := service.Summary(t.Context(), id)
	if err != nil || summary.LastItemPreview != "newer" {
		t.Fatalf("historical window changed latest summary: %q %v", summary.LastItemPreview, err)
	}
}

func TestFieldPublicationFreezesLivePrefixBeforeSourceClose(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", ActivityFields: true}); err != nil {
		t.Fatal(err)
	}
	nextUpdate(t, sub)
	owner := outputbody.NewOwner()
	body := owner.NewBody()
	first := strings.Repeat("x", threadDisplayTextBytes-1) + "🌸"
	if err := body.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error {
		actor.state.turnID, actor.state.runID = "turn", "run"
		actor.appendLiveTextSegment(ThreadItemAssistant, first, time.Time{}, "live", body)
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := body.Append("later"); err != nil {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error {
		actor.appendLiveTextSegment(ThreadItemAssistant, "later", time.Time{}, "live", body)
		actor.state.view.ViewVersion++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	update := nextUpdate(t, sub)
	if len(update.Items) != 1 || update.Items[0].Item == nil {
		t.Fatal("missing frozen live item")
	}
	item := update.Items[0].Item
	if !strings.HasPrefix(first, item.Text) || len(item.Text) != threadDisplayTextBytes-1 || item.Body.PreviewBytes != len(item.Text) || item.Body.Bytes != len(first) {
		t.Fatalf("live prefix changed or split a rune: %+v", item.Body)
	}
}
