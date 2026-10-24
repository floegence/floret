package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"strings"
	"sync"
	"time"
)

type failingActiveWindowPreparation struct {
	*internalstorage.BackendKernel
	entered, release chan struct{}
	once             sync.Once
	err              error
	appendOnRead     func(context.Context) error
	appendOnce       sync.Once
}

func (repo *failingActiveWindowPreparation) ReadWindowProjection(ctx context.Context, options sessiontree.ItemMembershipWindowOptions, fields func(sessiontree.PathMetadata, bool) []string) (sessiontree.WindowProjection, error) {
	var err error
	if options.LeafID != "" && repo.appendOnRead != nil {
		repo.appendOnce.Do(func() { err = repo.appendOnRead(ctx) })
	}
	if err != nil {
		return sessiontree.WindowProjection{}, err
	}
	return repo.BackendKernel.ReadWindowProjection(ctx, options, fields)
}

func (repo *failingActiveWindowPreparation) ScanPathProjection(ctx context.Context, _, _ string, _ func([]sessiontree.PathMetadata) error, _ func(sessiontree.PathMetadata) []string, _ func(sessiontree.PathProjectionPage) error) error {
	repo.once.Do(func() { close(repo.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-repo.release:
		return repo.err
	}
}

func TestActiveSourceWindowPreparationFailureReachesObservers(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "active-window-failure"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted}, sessiontree.AppendOptions{ID: "started"}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected active recovery read failure")
	repo := &failingActiveWindowPreparation{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{}), err: injected}
	host.store.repo = repo
	sub, baseline := coldWindowBaseline(t, public, id)
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("active recovery did not start")
	}
	close(repo.release)
	wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := sub.Next(wait); !errors.Is(err, injected) || !errors.Is(err, ErrThreadUpdateInvalidated) {
		t.Fatalf("background failure was lost: %v", err)
	}
	actor := service.runtime(id)
	if err := actor.apply(t.Context(), func() error {
		if actor.sourceWindow == nil || actor.state.view.ViewVersion != baseline.ViewVersion || actor.state.contextErr != nil {
			t.Error("failed recovery replaced accepted state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	host.store.lifetimeMu.Lock()
	reported := host.store.backgroundErr
	host.store.lifetimeMu.Unlock()
	if !errors.Is(reported, injected) {
		t.Fatalf("Host lost background failure: %v", reported)
	}
}

func TestSourceWindowFailedPromotionRetainsAcceptedState(t *testing.T) {
	actor := &threadRuntimeState{sourceWindow: &threadSourceProjection{leaf: "accepted"}, state: threadRuntimeData{view: ThreadView{ThreadID: "thread", ViewVersion: 7, Items: []ThreadItem{{ID: "retained", Kind: ThreadItemUser, Ordinal: 151, CanonicalOrdinal: 170}}}}}
	before, _ := json.Marshal(actor.state.view)
	window := actor.sourceWindow
	err := actor.installCanonicalView(canonicalThreadView{ThreadView: ThreadView{Items: []ThreadItem{{ID: "other", Ordinal: 1, Kind: ThreadItemAssistant}}}})
	after, _ := json.Marshal(actor.state.view)
	if !errors.Is(err, errCanonicalRefreshDiscarded) || actor.sourceWindow != window || string(before) != string(after) {
		t.Fatalf("failed promotion changed accepted window: %v", err)
	}
}

func TestSourceWindowBrowsingDoesNotWaitForExecutionRecovery(t *testing.T) {
	for _, appendDuringRead := range []bool{false, true} {
		t.Run(fmt.Sprint("append=", appendDuringRead), func(t *testing.T) {
			testSourceWindowBrowsingDuringRecovery(t, appendDuringRead)
		})
	}
}

func testSourceWindowBrowsingDuringRecovery(t *testing.T, appendDuringRead bool) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "recovery-browsing"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	entries := []sessiontree.Entry{{ID: "started", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted}}
	for i := range 170 {
		entries = append(entries, sessiontree.Entry{ID: fmt.Sprint("message-", i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}})
	}
	for _, entry := range entries {
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	repo := &failingActiveWindowPreparation{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{}), err: errors.New("injected recovery failure")}
	if appendDuringRead {
		repo.appendOnRead = func(ctx context.Context) error {
			_, err := repo.BackendKernel.Append(ctx, sessiontree.Entry{ID: "late-message", ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "concurrent append"}}, sessiontree.AppendOptions{})
			return err
		}
	}
	host.store.repo = repo
	defer close(repo.release)
	if err := service.prepareThreadRead(t.Context(), id, service.runtime(id), true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("execution recovery did not enter its blocked source read")
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, anchor := range []string{"", "message-40", "message-120", "message-40"} {
		read, cancel := context.WithTimeout(t.Context(), time.Second)
		input := ThreadWindowInput{ThreadID: id, AnchorID: anchor, Limit: 20, RequestID: "navigation:" + anchor}
		if anchor != "" {
			input.Direction = ThreadWindowAround
		}
		err := sub.SetWindow(read, scope, input)
		if err != nil {
			cancel()
			t.Fatalf("browsing %q waited for execution recovery: %v", anchor, err)
		}
		update, err := sub.Next(read)
		for err == nil && (update.Window == nil || update.Window.Value.RequestID != input.RequestID) {
			update, err = sub.Next(read)
		}
		cancel()
		if err != nil || update.Window == nil || len(update.Window.Value.Items) != 20 {
			t.Fatalf("browsing %q did not publish a complete window: %+v, %v", anchor, update, err)
		}
		if anchor != "" {
			found := false
			for _, item := range update.Window.Value.Items {
				found = found || item.ID == anchor
			}
			if !found {
				t.Fatalf("window omitted requested anchor %q", anchor)
			}
		}
	}
}

func TestSourceWindowSummaryRetainsPreviewBeforeWindow(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "window-summary"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "preview", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "earlier user message stays visible in sidebar"}})
	for i := range 170 {
		call := fmt.Sprint("call-", i)
		appendEntry(sessiontree.Entry{ID: call, Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: call, ToolArgs: "{}"}})
		appendEntry(sessiontree.Entry{ID: call + ":result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: call, Content: "ok", ToolResult: &session.ToolResultView{Status: "success"}}})
	}
	want, err := service.Summary(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = coldWindowBaseline(t, public, id)
	got, err := service.Summary(t.Context(), id)
	if err != nil || got.LastItemPreview != want.LastItemPreview || !got.LastItemAt.Equal(want.LastItemAt) {
		t.Fatalf("opening a window changed sidebar preview: got %q at %s want %q at %s: %v", got.LastItemPreview, got.LastItemAt, want.LastItemPreview, want.LastItemAt, err)
	}
}

type failingPromotionTailRepo struct {
	*internalstorage.BackendKernel
	err error
}

func (repo *failingPromotionTailRepo) PathEntriesWindow(ctx context.Context, input sessiontree.PathWindowOptions) (sessiontree.PathEntriesWindow, error) {
	if repo.err != nil {
		return sessiontree.PathEntriesWindow{}, repo.err
	}
	return repo.BackendKernel.PathEntriesWindow(ctx, input)
}

func TestSourceWindowTailFailureRetainsAcceptedState(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "window-tail-failure"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendMessage := func(entryID string) {
		t.Helper()
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: entryID}}, sessiontree.AppendOptions{ID: entryID}); err != nil {
			t.Fatal(err)
		}
	}
	appendMessage("accepted")
	sub, baseline := coldWindowBaseline(t, public, id)
	actor := service.runtime(id)
	window := actor.sourceWindow
	before, err := json.Marshal(actor.state.view)
	if err != nil {
		t.Fatal(err)
	}
	appendMessage("late")
	injected := errors.New("injected tail read failure")
	repo := &failingPromotionTailRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), err: injected}
	host.store.repo = repo
	if _, err := service.View(t.Context(), id); !errors.Is(err, injected) {
		t.Fatalf("promotion error = %v", err)
	}
	after, err := json.Marshal(actor.state.view)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || actor.sourceWindow != window || actor.canonical != nil || actor.state.contextErr != nil {
		t.Fatal("failed tail read changed the accepted window")
	}
	wait, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if update, err := sub.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed promotion published an update: %+v, %v", update, err)
	}
	repo.err = nil
	view, err := service.View(t.Context(), id)
	if err != nil || len(view.Items) != 2 || view.Items[1].ID != "late" || view.ViewVersion != baseline.ViewVersion+1 {
		t.Fatalf("retry did not install the complete tail: %+v, %v", view, err)
	}
	if update := nextUpdate(t, sub); update.BaseVersion != baseline.ViewVersion || update.ViewVersion != view.ViewVersion {
		t.Fatalf("promotion lost subscription continuity: %+v", update)
	}
}

func TestSourceWindowPromotionCatchesAppendedCanonicalTail(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "window-tail"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for i := range 170 {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
		t.Fatal(err)
	}
	var baseline ThreadUpdate
	for baseline.Window == nil {
		baseline = nextUpdate(t, sub)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "arrived after opening"}}, sessiontree.AppendOptions{ID: "late"}); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), id)
	if err != nil || len(view.Items) != 171 || view.Items[170].ID != "late" || view.ViewVersion <= baseline.ViewVersion {
		t.Fatalf("promotion lost tail: items=%d version=%d err=%v", len(view.Items), view.ViewVersion, err)
	}
	actor := service.runtime(id)
	if actor.sourceWindow != nil || actor.canonical == nil {
		t.Fatal("complete reader retained sparse state")
	}
	update := nextUpdate(t, sub)
	if update.ViewVersion != view.ViewVersion {
		t.Fatal("tail update did not advance accepted window")
	}
	actor.state.contextErr = errors.New("injected context invalidation")
	if err := sub.SetWindow(context.Background(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
		t.Fatalf("explicit read did not recover context: %v", err)
	}
}

func coldWindowBaseline(t *testing.T, svc ThreadService, id identity.ThreadID) (*ThreadUpdateSubscription, ThreadUpdate) {
	t.Helper()
	service := svc.(*threadRuntimeService)
	scope := threadquery.Scope{Authority: "cold-window-test", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
		t.Fatal(err)
	}
	for {
		update := nextUpdate(t, sub)
		if update.Window != nil {
			if err := service.runtime(id).apply(t.Context(), func() error {
				if service.runtime(id).sourceWindow == nil {
					t.Error("fixture completed hydration before cold command")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return sub, update
		}
	}
}

func TestColdWindowSendAndForkPreserveHistoryAndLiveUpdates(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retained answer"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "new answer"}, {Type: provider.EventDone, Reason: "stop"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "fork answer"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/cold-commands.db"
	open := func() (*Host, ThreadService) {
		h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
		return h, svc
	}
	host, svc := open()
	root, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: root.ThreadID, RequestKey: "initial", Input: UserInput{Text: "retain this fact"}}); err != nil {
		t.Fatal(err)
	}
	initial := waitThreadView(t, svc, root.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	anchor := ""
	for _, item := range initial.Items {
		if item.Kind == ThreadItemAssistant {
			anchor = item.ID
		}
	}
	if anchor == "" {
		t.Fatal("missing fork anchor")
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	sub, baseline := coldWindowBaseline(t, svc, root.ThreadID)
	if _, err := svc.(ThreadCommandService).SendAck(t.Context(), SendInput{ThreadID: root.ThreadID, RequestKey: "next", Input: UserInput{Text: "continue from retained fact"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	version := baseline.ViewVersion
	seenAnswer := false
	for {
		update, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if update.BaseVersion != version || update.ViewVersion <= version {
			t.Fatalf("version gap: %d -> %d -> %d", version, update.BaseVersion, update.ViewVersion)
		}
		if update.Window != nil {
			t.Fatalf("send replaced accepted window with another baseline: base=%d version=%d facts=%+v items=%+v", update.BaseVersion, update.ViewVersion, update.Facts, update.Window.Value.Items)
		}
		version = update.ViewVersion
		for _, change := range update.Items {
			if change.Item != nil && strings.Contains(change.Item.Text, "new answer") {
				seenAnswer = true
			}
		}
		if update.Facts.Activity == ThreadActivityIdle && update.Facts.LastOutcome != nil {
			break
		}
	}
	if !seenAnswer {
		t.Fatal("live subscription never received new answer")
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("provider requests=%d", len(requests))
	}
	retained := false
	for _, message := range requests[1].Messages {
		retained = retained || strings.Contains(message.Text, "retain this fact")
	}
	if !retained {
		t.Fatal("cold send lost earlier canonical context")
	}
	sub.Close()
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	_, _ = coldWindowBaseline(t, svc, root.ThreadID)
	fork, err := svc.Fork(t.Context(), ForkThreadInput{SourceThreadID: root.ThreadID, SourceItemID: anchor, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	history, err := svc.History(t.Context(), fork.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var text string
	for _, item := range history.Items {
		text += item.Text + "\n"
	}
	if !strings.Contains(text, "retained answer") || strings.Contains(text, "new answer") {
		t.Fatalf("wrong fork boundary: %s", text)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, svc = open()
	_, _ = coldWindowBaseline(t, svc, fork.ThreadID)
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: fork.ThreadID, RequestKey: "fork-send", Input: UserInput{Text: "continue fork"}}); err != nil {
		t.Fatal(err)
	}
	completed := waitThreadView(t, svc, fork.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if completed.Failure != nil {
		t.Fatalf("fork continuation failed: %+v", completed.Failure)
	}
}

func TestSourceWindowRefreshPromotesInFlightCompleteRead(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "promotion-browsing"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendMessage := func(entryID string) {
		t.Helper()
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: entryID, ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: entryID}}, sessiontree.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	appendMessage("first")
	sub, baseline := coldWindowBaseline(t, public, id)
	repo := &blockedPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	var once sync.Once
	release := func() { once.Do(func() { close(repo.release) }) }
	defer release()
	complete := make(chan error, 1)
	go func() { complete <- service.prepareThreadView(t.Context(), id, service.runtime(id)) }()
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("complete read did not start")
	}
	appendMessage("second")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	if err := sub.SetWindow(ctx, scope, ThreadWindowInput{ThreadID: id, RequestID: "refreshed", Limit: 20}); err != nil {
		t.Fatal(err)
	}
	var refreshed uint64
	for {
		update, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if update.Window != nil && update.Window.Value.RequestID == "refreshed" {
			if len(update.Window.Value.Items) != 2 || update.ViewVersion != baseline.ViewVersion+1 {
				t.Fatalf("wrong refreshed baseline: %+v", update)
			}
			refreshed = update.ViewVersion
			break
		}
	}
	release()
	select {
	case err := <-complete:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("complete read failed to promote after display refresh")
	}
	view, err := service.View(t.Context(), id)
	if err != nil || len(view.Items) != 2 || view.ViewVersion != refreshed {
		t.Fatalf("promotion changed accepted version or omitted append: %+v %v", view, err)
	}
}

type blockedWindowAndCompleteRepo struct {
	*blockedPreparationRepo
	windowEntered chan struct{}
	windowOnce    sync.Once
}

func (repo *blockedWindowAndCompleteRepo) ReadWindowProjection(ctx context.Context, _ sessiontree.ItemMembershipWindowOptions, _ func(sessiontree.PathMetadata, bool) []string) (sessiontree.WindowProjection, error) {
	repo.windowOnce.Do(func() { close(repo.windowEntered) })
	<-ctx.Done()
	return sessiontree.WindowProjection{}, ctx.Err()
}

func TestShutdownJoinsDisplayAndCompletePreparation(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "parallel-preparation-shutdown"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	repo := &blockedWindowAndCompleteRepo{blockedPreparationRepo: &blockedPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}, windowEntered: make(chan struct{})}
	host.store.repo = repo
	done := make(chan error, 2)
	go func() { done <- service.prepareThreadView(t.Context(), id, service.runtime(id)) }()
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("complete read never entered")
	}
	go func() { done <- service.prepareThreadRead(t.Context(), id, service.runtime(id), true) }()
	select {
	case <-repo.windowEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("display read waited for complete history")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrHostClosed) {
				t.Fatalf("wrong shutdown result: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("shutdown left an owned preparation running")
		}
	}
}
