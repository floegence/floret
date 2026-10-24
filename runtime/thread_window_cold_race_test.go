package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
	"sync"
	"testing"
)

type windowReadMutationRepo struct {
	*internalstorage.BackendKernel
	once   sync.Once
	mutate func() error
	every  func(int) error
	reads  int
}

func (r *windowReadMutationRepo) ReadWindowProjection(ctx context.Context, input sessiontree.ItemMembershipWindowOptions, fields func(sessiontree.PathMetadata, bool) []string) (sessiontree.WindowProjection, error) {
	r.reads++
	page, err := r.BackendKernel.ReadWindowProjection(ctx, input, fields)
	if err != nil {
		return page, err
	}
	r.once.Do(func() { err = r.mutate() })
	if err == nil && r.every != nil {
		err = r.every(r.reads)
	}
	return page, err
}

func TestColdWindowRepeatedAppendDoesNotFreezeSubsequentReads(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/cold.sqlite"), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-repeated-append"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendText := func(name string) error {
		_, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: name, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: name}}, sessiontree.AppendOptions{})
		return err
	}
	if err := appendText("before"); err != nil {
		t.Fatal(err)
	}
	repo := &windowReadMutationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), mutate: func() error { return nil }, every: func(read int) error {
		if read <= 2 {
			return appendText(fmt.Sprint("during-", read))
		}
		return nil
	}}
	host.store.repo = repo
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	read := func() ThreadUpdate {
		t.Helper()
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true}); err != nil {
			t.Fatal(err)
		}
		for {
			update := nextUpdate(t, sub)
			if update.Window != nil {
				return update
			}
		}
	}
	_ = read()
	baseline := read()
	if len(baseline.Window.Value.Items) != 3 || baseline.Window.Value.Items[2].Text != "during-2" {
		t.Fatalf("subsequent read froze an obsolete baseline: %+v", baseline.Window.Value.Items)
	}
	if repo.reads > 4 {
		t.Fatalf("unbounded rereads: %d", repo.reads)
	}
}
func TestColdWindowAppendDuringSnapshotDoesNotFailOpen(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/cold.sqlite"), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "cold-append"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendText := func(name string) error {
		_, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: name, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: name}}, sessiontree.AppendOptions{})
		return err
	}
	if err := appendText("before"); err != nil {
		t.Fatal(err)
	}
	repo := &windowReadMutationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), mutate: func() error { return appendText("during") }}
	host.store.repo = repo
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 2, ActivityFields: true}); err != nil {
		t.Fatalf("append broke cold open: %v", err)
	}
	update := nextUpdate(t, sub)
	for update.Window == nil {
		update = nextUpdate(t, sub)
	}
	if update.Window == nil || len(update.Window.Value.Items) != 2 || update.Window.Value.Items[1].Text != "during" {
		t.Fatalf("baseline missed append: %+v", update.Window)
	}
	if repo.reads > 2 {
		t.Fatalf("cold open restarted %d times", repo.reads)
	}
}

func TestSparseWindowRefreshPreservesOtherReadingInterests(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "independent-window-refresh"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendText := func(name string) {
		t.Helper()
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: name, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: name}}, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		appendText(fmt.Sprintf("item-%02d", i))
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	newSubscription := func() *ThreadUpdateSubscription {
		t.Helper()
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sub.Close)
		return sub
	}
	read := func(sub *ThreadUpdateSubscription, input ThreadWindowInput) ThreadUpdate {
		t.Helper()
		if err := sub.SetWindow(t.Context(), scope, input); err != nil {
			t.Fatal(err)
		}
		for {
			update := nextUpdate(t, sub)
			if update.Window != nil {
				return update
			}
		}
	}
	history, tail := newSubscription(), newSubscription()
	initial := read(history, ThreadWindowInput{ThreadID: id, AnchorID: "item-02", Direction: ThreadWindowAround, Limit: 3, ActivityFields: true})
	_ = read(tail, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true})
	appendText("item-20")
	latest := read(tail, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true})
	if latest.Window.Value.LastItemID != "item-20" {
		t.Fatal("explicit tail read did not advance")
	}
	update := nextUpdate(t, history)
	if update.Window == nil || update.Window.Value.FirstItemID != initial.Window.Value.FirstItemID || update.Window.Value.LastItemID != initial.Window.Value.LastItemID || update.BaseVersion != initial.ViewVersion || update.ViewVersion != latest.ViewVersion {
		t.Fatalf("refresh replaced another reader's interval: %+v", update)
	}
	actor := service.runtime(id)
	version, accepted := actor.state.view.ViewVersion, actor.sourceWindow
	appendText("item-21")
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := tail.SetWindow(cancelled, scope, ThreadWindowInput{ThreadID: id, Limit: 3}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled refresh: %v", err)
	}
	if actor.sourceWindow != accepted || actor.state.view.ViewVersion != version {
		t.Fatal("cancelled caller changed the accepted baseline")
	}
	if err := host.store.repo.MoveLeaf(t.Context(), id.String(), "item-05"); err != nil {
		t.Fatal(err)
	}
	latest = read(tail, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true})
	if latest.Window.Value.LastItemID != "item-05" {
		t.Fatal("branch change retained discarded items")
	}
	update = nextUpdate(t, history)
	if update.Window == nil || update.Window.Value.LastItemID != initial.Window.Value.LastItemID || update.ViewVersion != latest.ViewVersion {
		t.Fatal("branch refresh lost the retained reading interval")
	}
	if _, err := service.View(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error {
		service.flushUpdateLocked(actor)
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: "live", Kind: ThreadItemAssistant, TurnID: "turn", RunID: "run", Ordinal: 7, Text: "live", Live: true})
		actor.invalidateItemIndex()
		actor.state.view.ViewVersion++
		actor.mutations.order = true
		service.flushUpdateLocked(actor)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	update = nextUpdate(t, history)
	if update.Window == nil || update.Window.Value.FirstItemID != initial.Window.Value.FirstItemID || update.Window.Value.LastItemID != initial.Window.Value.LastItemID {
		t.Fatalf("promotion shifted the reading interval: %+v", update.Window)
	}
}

func TestSparseQueuedRefreshPreservesFrozenVersion(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			source := storage.Memory()
			if persistent {
				source = storage.SQLite(t.TempDir() + "/queued.sqlite")
			}
			host, err := Open(t.Context(), Options{Storage: source, DeferExecution: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
			if err != nil {
				t.Fatal(err)
			}
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "queued-source-refresh"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			appendText := func(name string) {
				t.Helper()
				if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: name, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: name}}, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 6 {
				appendText(fmt.Sprint("item-", i))
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			newSub := func() *ThreadUpdateSubscription {
				t.Helper()
				sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(sub.Close)
				return sub
			}
			read := func(sub *ThreadUpdateSubscription, input ThreadWindowInput) ThreadUpdate {
				t.Helper()
				if err := sub.SetWindow(t.Context(), scope, input); err != nil {
					t.Fatal(err)
				}
				for {
					u := nextUpdate(t, sub)
					if u.Window != nil {
						return u
					}
				}
			}
			history, tail := newSub(), newSub()
			first := read(history, ThreadWindowInput{ThreadID: id, AnchorID: "item-2", Direction: ThreadWindowAround, Limit: 3, ActivityFields: true})
			read(tail, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true})
			var versions []uint64
			for i := 6; i < 9; i++ {
				appendText(fmt.Sprint("item-", i))
				current := read(tail, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true})
				versions = append(versions, current.ViewVersion)
			}
			// Promote the actor before consuming queued source publications. Interest
			// membership must already have been installed at publication time.
			if _, err := service.View(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			// A further canonical append must not change any already queued snapshot.
			appendText("not-yet-observed")
			previous := first.ViewVersion
			for i, version := range versions {
				update := nextUpdate(t, history)
				if update.BaseVersion != previous || update.ViewVersion != version || update.Window == nil {
					t.Fatalf("lost queued version: %+v", update)
				}
				window := update.Window.Value
				if window.LeafID != fmt.Sprint("item-", i+6) || window.ViewVersion != version || window.FirstItemID != first.Window.Value.FirstItemID || window.LastItemID != first.Window.Value.LastItemID {
					t.Fatalf("mixed queued source/version: %+v", window)
				}
				previous = version
			}
		})
	}
}

func TestSparseWindowBranchReplacementRetainsValidSelections(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			host, public := testThreadService(t, newBlockingThreadGateway())
			service := public.(*threadRuntimeService)
			const id identity.ThreadID = "branch-selection"
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			for i := range 10 {
				if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: fmt.Sprint("item-", i), ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			input := ThreadWindowInput{ThreadID: id, AnchorID: "item-8", Direction: ThreadWindowAround, Limit: 3, ActivityFields: true}
			if explicit {
				input = ThreadWindowInput{ThreadID: id, ItemIDs: []string{"item-1", "item-8"}, ActivityFields: true}
			}
			if err := sub.SetWindow(t.Context(), scope, input); err != nil {
				t.Fatal(err)
			}
			for update := nextUpdate(t, sub); update.Window == nil; update = nextUpdate(t, sub) {
			}
			if err := host.store.repo.MoveLeaf(t.Context(), id.String(), "item-4"); err != nil {
				t.Fatal(err)
			}
			other, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if err := other.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 3, ActivityFields: true}); err != nil {
				t.Fatal(err)
			}
			update := nextUpdate(t, sub)
			if update.Window == nil {
				t.Fatal("missing replacement")
			}
			page := update.Window.Value
			if explicit {
				if !page.ItemSelection || len(page.Items) != 1 || page.Items[0].ID != "item-1" {
					t.Fatalf("lost retained selection: %+v", page)
				}
			} else if !page.AnchorUnavailable || page.LastItemID != "item-4" {
				t.Fatalf("missing anchor did not resolve at canonical tail: %+v", page)
			}
		})
	}
}

func TestDeferredWindowPublicationPreservesLegacyActivity(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "legacy-deferred-window"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(id)
	presentation := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "retained detail", Format: tools.StructuredActivityRowFormatText}}}}
	item := ThreadItem{ID: "tool:turn:tool", TurnID: "turn", RunID: "run", Kind: ThreadItemTool, Activity: &observation.ActivityItem{ToolID: "tool", ToolName: "inspect", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusSuccess, Presentation: presentation}}
	work := windowPublication{actor: actor, interest: newWindowInterestLifetime(), limit: 4096}
	defer work.interest.cancel()
	update, err := work.read(t.Context(), ThreadUpdate{ThreadID: id, Window: &ThreadComponent[*ThreadWindowSnapshot]{Value: &ThreadWindowSnapshot{ThreadID: id, Items: []ThreadItem{item}}}})
	if err != nil {
		t.Fatal(err)
	}
	got := update.Window.Value.Items[0]
	if got.Activity.Presentation == nil || got.ActivitySummary != nil {
		t.Fatal("legacy complete presentation was replaced by field summary")
	}
}
