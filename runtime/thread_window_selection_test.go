package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestWindowSelectionSkipsHiddenMiddleAndKeepsOneInterest(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	actor.state.view.Items = nil
	for i := range 10002 {
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: fmt.Sprint("item-", i), TurnID: "turn", RunID: "run", Kind: ThreadItemAssistant, Text: fmt.Sprint(i), Ordinal: uint64(i + 1)})
	}
	actor.invalidateItemIndex()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	input := ThreadWindowInput{ThreadID: "thread-live", ActivityFields: true, ItemIDs: []string{"item-10001", "item-0"}}
	if err := sub.setActorWindow(t.Context(), actor, input); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if baseline.Window == nil || !baseline.Window.Value.ItemSelection || len(baseline.Window.Value.Items) != 2 || baseline.Window.Value.Items[0].ID != "item-0" || baseline.Window.Value.Items[1].ID != "item-10001" {
		t.Fatalf("not an exact ordered selection: %+v", baseline.Window)
	}
	actor.mu.Lock()
	sub.mu.Lock()
	visible := actor.state.view.Items[0]
	visible.Text = "updated"
	hidden := actor.state.view.Items[5000]
	hidden.Text = "hidden"
	update, ok := sub.projectWindowUpdate(actor, ThreadUpdate{ThreadID: "thread-live", BaseVersion: baseline.ViewVersion, ViewVersion: baseline.ViewVersion + 1, Items: []ThreadItemChange{{Kind: ThreadItemReplaced, ItemID: visible.ID, Item: &visible}, {Kind: ThreadItemReplaced, ItemID: hidden.ID, Item: &hidden}}})
	sub.mu.Unlock()
	actor.mu.Unlock()
	if !ok || len(update.Items) != 1 || update.Items[0].ItemID != "item-0" {
		t.Fatalf("hidden membership escaped: %+v", update)
	}
	if len(sub.windows) != 1 || len(sub.windows["thread-live"].items) != 2 {
		t.Fatal("selection widened its observation")
	}
	input.ItemIDs = []string{"item-0", "item-0"}
	if err := sub.setActorWindow(t.Context(), actor, input); err == nil {
		t.Fatal("accepted duplicate selection")
	}
}

func TestColdWindowSelectionPreservesCanonicalOrderAndFailedInterest(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/selection.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "selection-cold"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: fmt.Sprint("message-", i), ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, ids := range [][]string{{"message-99", "message-0"}, {"message-70", "message-20"}} {
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, ActivityFields: true, ItemIDs: ids}); err != nil {
			t.Fatal(err)
		}
		baseline := nextUpdate(t, sub)
		for baseline.Window == nil {
			baseline = nextUpdate(t, sub)
		}
		items := baseline.Window.Value.Items
		if !baseline.Window.Value.ItemSelection || len(items) != 2 || items[0].ID != ids[1] || items[1].ID != ids[0] {
			t.Fatalf("selection differs: %+v", baseline.Window.Value)
		}
	}
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, ItemIDs: []string{"missing"}}); err == nil {
		t.Fatal("missing selection silently changed")
	}
	if len(sub.windows[id.String()].items) != 2 || !sub.windows[id.String()].items["message-20"] {
		t.Fatal("failed selection replaced the accepted interest")
	}
}

func TestLegacyWindowSnapshotHydratesOmittedText(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "legacy-selection-hydration"
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
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if !actor.state.view.Items[0].textOmitted {
		t.Fatal("fixture must exercise omitted canonical text")
	}
	for _, input := range []ThreadWindowInput{{ThreadID: id}, {ThreadID: id, ItemIDs: []string{"message"}}} {
		snapshot, err := actor.windowSnapshotContext(t.Context(), input)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Items) != 1 || snapshot.Items[0].Text != "persisted message" {
			t.Fatalf("legacy window lost visible body: %+v", snapshot.Items)
		}
	}
}

func TestWindowSelectionRetainsPendingControlsOutsideSelection(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	actor.state.view.Items = []ThreadItem{{ID: "visible", TurnID: "turn", RunID: "run", Kind: ThreadItemUser, Text: "history", Ordinal: 1}}
	actor.state.view.Interactions = []ThreadInteraction{{ID: "pending", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput}, {ID: "resolved", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput, Resolved: true}}
	actor.invalidateItemIndex()
	actor.interactionIndex = nil
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.setActorWindow(t.Context(), actor, ThreadWindowInput{ThreadID: "thread-live", ActivityFields: true, ItemIDs: []string{"visible"}}); err != nil {
		t.Fatal(err)
	}
	baseline := nextUpdate(t, sub)
	if baseline.Interactions == nil || len(baseline.Interactions.Value) != 1 || baseline.Interactions.Value[0].ID != "pending" {
		t.Fatalf("selection hid pending control or leaked resolved history: %+v", baseline.Interactions)
	}
}

func TestRelativeWindowSeekInstallsStableSelection(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	actor.state.view.Items = nil
	for i := range 10002 {
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: fmt.Sprint("item-", i), TurnID: "turn", RunID: "run", Kind: ThreadItemAssistant, Text: fmt.Sprint(i), Ordinal: uint64(i + 1)})
	}
	actor.invalidateItemIndex()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, input := range []ThreadWindowInput{
		{ThreadID: "thread-live", AnchorID: "item-1", AnchorOffset: 5000, Direction: ThreadWindowAround, Limit: 1},
		{ThreadID: "thread-live", AnchorID: "item-10001", AnchorOffset: -5000, Direction: ThreadWindowAround, Limit: 1},
	} {
		if err := sub.setActorWindow(t.Context(), actor, input); err != nil {
			t.Fatal(err)
		}
		baseline := nextUpdate(t, sub)
		if baseline.Window == nil || len(baseline.Window.Value.Items) != 1 || baseline.Window.Value.Items[0].ID != "item-5001" {
			t.Fatalf("unexpected seek: %+v", baseline.Window)
		}
		interest := sub.windows["thread-live"]
		if interest.sourceInput.AnchorOffset != 0 || len(interest.selection) != 1 || interest.selection[0] != "item-5001" {
			t.Fatalf("seek did not bind identities: %+v", interest.sourceInput)
		}
	}
	for _, input := range []ThreadWindowInput{
		{ThreadID: "thread-live", AnchorOffset: 1, Direction: ThreadWindowAround},
		{ThreadID: "thread-live", AnchorID: "item-1", AnchorOffset: 1, Direction: ThreadWindowBefore},
		{ThreadID: "thread-live", AnchorID: "item-1", AnchorOffset: -100, Direction: ThreadWindowAround},
	} {
		if err := sub.setActorWindow(t.Context(), actor, input); err == nil {
			t.Fatalf("accepted invalid seek: %+v", input)
		}
	}
}

func TestRelativeWindowSeekKeepsResolvedTargetUnderWireBudget(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	actor.state.view.Items = nil
	for i := range 100 {
		actor.state.view.Items = append(actor.state.view.Items, ThreadItem{ID: fmt.Sprint("item-", i), TurnID: "turn", RunID: "run", Kind: ThreadItemAssistant, Text: "visible", Ordinal: uint64(i + 1), Context: []MessageContextItem{{Kind: "file", Title: "visible context", Text: strings.Repeat("metadata", 4096)}}})
	}
	actor.invalidateItemIndex()
	for _, offset := range []int{-50, 49} {
		target := fmt.Sprint("item-", 50+offset)
		snapshot, err := actor.windowSnapshot(ThreadWindowInput{ThreadID: "thread-live", AnchorID: "item-50", AnchorOffset: offset, Direction: ThreadWindowAround, Limit: 40})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range snapshot.Items {
			found = found || item.ID == target
		}
		if !found {
			t.Fatalf("wire bounding dropped resolved target %s: %+v", target, snapshot)
		}
	}
}

func TestColdRelativeWindowSeekSurvivesRestartAndAppend(t *testing.T) {
	path := t.TempDir() + "/relative.sqlite"
	const id identity.ThreadID = "relative-cold"
	var host *Host
	open := func() *threadRuntimeService {
		t.Helper()
		var err error
		host, err = Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
		if err != nil {
			t.Fatal(err)
		}
		return service.(*threadRuntimeService)
	}
	_ = open()
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendItem := func(i int) {
		t.Helper()
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: fmt.Sprint("message-", i), ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 100 {
		appendItem(i)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	for restart := range 2 {
		service := open()
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		read := func(input ThreadWindowInput) *ThreadWindowSnapshot {
			t.Helper()
			if err := sub.SetWindow(t.Context(), scope, input); err != nil {
				t.Fatal(err)
			}
			for {
				update := nextUpdate(t, sub)
				if update.Window != nil {
					return update.Window.Value
				}
			}
		}
		for _, offset := range []int{-50, 49} {
			snapshot := read(ThreadWindowInput{ThreadID: id, AnchorID: "message-50", AnchorOffset: offset, Direction: ThreadWindowAround, Limit: 3, ActivityFields: true})
			want := fmt.Sprint("message-", 50+offset)
			if len(snapshot.Items) != 3 || snapshot.resolvedAnchor != want {
				t.Fatalf("target %s: %+v", want, snapshot)
			}
			interest := sub.windows[id.String()]
			if interest.sourceInput.AnchorOffset != 0 || len(interest.selection) != 3 {
				t.Fatalf("seek was not normalized: %+v", interest.sourceInput)
			}
		}
		snapshot := read(ThreadWindowInput{ThreadID: id, AnchorID: "message-50", AnchorOffset: -30, Direction: ThreadWindowAround, Limit: 3, ActivityFields: true})
		before := snapshot.Items[1].ID
		appendItem(100 + restart)
		tail, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := tail.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, Limit: 2, ActivityFields: true}); err != nil {
			t.Fatal(err)
		}
		for {
			update := nextUpdate(t, sub)
			if update.Window == nil {
				continue
			}
			if len(update.Window.Value.Items) != 3 || update.Window.Value.Items[1].ID != before {
				t.Fatalf("append reapplied seek: %+v", update.Window.Value)
			}
			break
		}
		tail.Close()
		sub.Close()
		if err := host.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
