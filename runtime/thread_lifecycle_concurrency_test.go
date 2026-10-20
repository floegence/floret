package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type cancellationTrackingGateway struct {
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
}

func newCancellationTrackingGateway() *cancellationTrackingGateway {
	return &cancellationTrackingGateway{started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (*cancellationTrackingGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "cancellation-tracking", StateCompatibilityKey: "test:cancellation-tracking:v1"}
}

func (*cancellationTrackingGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (gateway *cancellationTrackingGateway) Stream(ctx context.Context, _ provider.Request) (<-chan provider.Event, error) {
	gateway.once.Do(func() { close(gateway.started) })
	events := make(chan provider.Event)
	go func() {
		defer close(events)
		<-ctx.Done()
		close(gateway.cancelled)
	}()
	return events, nil
}

func TestThreadServiceDeleteCancelsAndJoinsActiveSubtree(t *testing.T) {
	rootGateway := newCancellationTrackingGateway()
	childGateway := newCancellationTrackingGateway()
	rootAgent, err := testAgent(rootGateway)
	if err != nil {
		t.Fatal(err)
	}
	childAgent, err := testAgent(childGateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	var rootID identity.ThreadID
	service, err := host.ThreadService(AgentFactoryFunc(func(_ context.Context, request AgentRequest) (*Agent, error) {
		if request.ThreadID == rootID {
			return rootAgent, nil
		}
		return childAgent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	root, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-delete-root"})
	if err != nil {
		t.Fatal(err)
	}
	rootID = root.ThreadID
	child, err := service.Create(t.Context(), CreateThreadInput{
		ParentThreadID: root.ThreadID,
		TaskName:       "child",
		HostProfileRef: "test",
		RequestKey:     "create-delete-child",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: root.ThreadID, Input: UserInput{Text: "root"}, RequestKey: "send-delete-root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, Input: UserInput{Text: "child"}, RequestKey: "send-delete-child"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, rootGateway.started, "root provider did not start")
	waitClosed(t, childGateway.started, "child provider did not start")

	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: root.ThreadID, RequestKey: "delete-active-tree"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, rootGateway.cancelled, "root provider was not joined before delete returned")
	waitClosed(t, childGateway.cancelled, "child provider was not joined before delete returned")
	for _, threadID := range []identity.ThreadID{root.ThreadID, child.ThreadID} {
		if _, err := service.View(t.Context(), threadID); !errors.Is(err, ErrThreadDeleted) {
			t.Fatalf("View(%s) error = %v, want ErrThreadDeleted", threadID, err)
		}
	}
}

func TestThreadServiceDeleteReleasesActorPresentation(t *testing.T) {
	_, typed := testThreadService(t, newBlockingThreadGateway())
	service := typed.(*threadRuntimeService)
	root, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "release-root"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: root.ThreadID, TaskName: "child", HostProfileRef: "test", RequestKey: "release-child"})
	if err != nil {
		t.Fatal(err)
	}
	actors := []*threadRuntimeState{service.runtime(root.ThreadID), service.runtime(child.ThreadID)}
	for _, actor := range actors {
		actor.mu.Lock()
		actor.state.view.Items = []ThreadItem{{ID: "large", Text: strings.Repeat("retained presentation", 4096)}}
		actor.state.requestKeys = map[string]threadRuntimeRequest{"request": {fingerprint: "retained"}}
		actor.itemIndex = map[string]int{"large": 0}
		actor.indexedItems = 1
		actor.textBuffer.WriteString(strings.Repeat("draft", 4096))
		actor.textBufferID = "large"
		actor.outputOrder = []string{"large"}
		actor.mu.Unlock()
	}
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: root.ThreadID, RequestKey: "release-delete"}); err != nil {
		t.Fatal(err)
	}
	service.runtimesMu.Lock()
	remaining := len(service.runtimes)
	service.runtimesMu.Unlock()
	if remaining != 0 {
		t.Errorf("deleted subtree retains %d actors in the runtime registry", remaining)
	}
	for _, actor := range actors {
		actor.mu.Lock()
		released := reflect.DeepEqual(actor.state, threadRuntimeData{}) && actor.itemIndex == nil && actor.indexedItems == 0 && actor.textBuffer.Len() == 0 && actor.textBufferID == "" && actor.outputOrder == nil && actor.pendingUpdate == nil && actor.pendingText.Len() == 0 && actor.updateTimer == nil
		fenced := actor.deleted && !actor.deleting
		actor.mu.Unlock()
		if !released {
			t.Errorf("deleted actor %s retains execution or presentation state", actor.threadID)
		}
		if !fenced {
			t.Errorf("deleted actor %s lost its permanent fence", actor.threadID)
		}
		if err := actor.apply(t.Context(), func() error { t.Error("late mutation ran"); return nil }); !errors.Is(err, ErrThreadDeleted) {
			t.Errorf("late mutation error = %v", err)
		}
		if err := actor.claimEffectDispatch(); !errors.Is(err, ErrThreadDeleted) {
			t.Errorf("late effect error = %v", err)
		}
		if err := service.runtime(identity.ThreadID(actor.threadID)).claimEffectDispatch(); !errors.Is(err, ErrThreadDeleted) {
			t.Errorf("late lookup effect error = %v", err)
		}
		if _, err := service.View(t.Context(), identity.ThreadID(actor.threadID)); !errors.Is(err, ErrThreadDeleted) {
			t.Errorf("deleted view error = %v", err)
		}
	}
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: root.ThreadID, RequestKey: "release-delete"}); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
}

type failingThreadDeleteRepo struct {
	sessiontree.Repo
	sessiontree.ThreadListRepo
	err error
}

func (repo failingThreadDeleteRepo) DeleteRootTreeWithRequest(context.Context, string, string, string) (sessiontree.DeleteRootTreeResult, error) {
	return sessiontree.DeleteRootTreeResult{}, repo.err
}

func TestThreadServiceFailedDeleteRetainsActorPresentation(t *testing.T) {
	_, typed := testThreadService(t, newBlockingThreadGateway())
	service := typed.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "failed-release"})
	if err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	actor.mu.Lock()
	actor.state.view.Items = []ThreadItem{{ID: "retained", Text: "keep on rollback"}}
	actor.itemIndex = map[string]int{"retained": 0}
	before := actor.state.view
	actor.mu.Unlock()
	original := service.host.store.repo
	failure := errors.New("delete transaction failed")
	service.host.store.repo = failingThreadDeleteRepo{Repo: original, ThreadListRepo: original.(sessiontree.ThreadListRepo), err: failure}
	err = service.Delete(t.Context(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "failed-release-delete"})
	service.host.store.repo = original
	if !errors.Is(err, failure) {
		t.Fatalf("delete error = %v", err)
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.deleted || actor.deleting || !reflect.DeepEqual(actor.state.view, before) || actor.itemIndex["retained"] != 0 || len(actor.itemIndex) != 1 {
		t.Fatal("failed deletion discarded presentation or retained a delete fence")
	}
}

func TestThreadServicePreparationFailureSettlesCanonicalTurn(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		return nil, errors.New("agent preparation failed")
	}))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-preparation-failure"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "must settle"}, RequestKey: "send-preparation-failure"}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.Activity == ThreadActivityIdle && view.LastOutcome != nil && *view.LastOutcome == TurnOutcomeFailed
	})
	if view.Failure == nil || view.Failure.Message != "agent preparation failed" {
		t.Fatalf("preparation failure = %#v", view.Failure)
	}
	entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	meta, err := host.store.repo.Thread(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	_, _, activity, outcome, _ := hydrateThreadRuntimeLifecycle(t.Context(), host.store.repo, meta)
	if activity != ThreadActivityIdle || outcome == nil || *outcome != TurnOutcomeFailed {
		t.Fatalf("preparation failure canonical lifecycle = activity=%q outcome=%v entries=%#v", activity, outcome, entries)
	}
}

func TestThreadServiceDeleteFencesEffectDispatchUntilInFlightEffectJoins(t *testing.T) {
	_, typed := testThreadService(t, newBlockingThreadGateway())
	service := typed.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-effect-delete"})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var dispatches atomic.Int32
	gate := threadRuntimeEffectGate{
		service: service,
		downstream: EffectAuthorizationGateFunc(func(context.Context, EffectAuthorizationRequest, AuthorizedEffect) (EffectDispatchResult, error) {
			dispatches.Add(1)
			close(started)
			<-release
			return EffectDispatchResult{}, nil
		}),
	}
	request := EffectAuthorizationRequest{ThreadID: created.ThreadID, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}
	dispatchDone := make(chan error, 1)
	go func() {
		_, dispatchErr := gate.Dispatch(context.Background(), request, func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error) {
			return EffectDispatchResult{}, nil
		})
		dispatchDone <- dispatchErr
	}()
	waitClosed(t, started, "effect dispatch did not start")
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- service.Delete(context.Background(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete-effect-thread"})
	}()
	waitForDeletingThread(t, service.runtime(created.ThreadID))

	if _, err := gate.Dispatch(t.Context(), request, func(context.Context, EffectAuthorizationProof) (EffectDispatchResult, error) {
		return EffectDispatchResult{}, nil
	}); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("late effect dispatch error = %v, want ErrThreadDeleted", err)
	}
	select {
	case err := <-deleteDone:
		t.Fatalf("Delete returned before in-flight effect joined: %v", err)
	default:
	}
	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if dispatches.Load() != 1 {
		t.Fatalf("effect dispatches = %d, want 1", dispatches.Load())
	}
}

func TestThreadRuntimePublishRejectsRegressingViewVersion(t *testing.T) {
	_, typed := testThreadService(t, newBlockingThreadGateway())
	service := typed.(*threadRuntimeService)
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	threadID := identity.ThreadID("thread-publish-fence")
	service.publish(ThreadView{ThreadID: threadID, ViewVersion: 2})
	service.publish(ThreadView{ThreadID: threadID, ViewVersion: 1})
	service.publish(ThreadView{ThreadID: threadID, ViewVersion: 3})
	for _, want := range []uint64{2, 3} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		view, nextErr := subscription.Next(ctx)
		cancel()
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if view.ViewVersion != want {
			t.Fatalf("published version = %d, want %d", view.ViewVersion, want)
		}
	}
}

func TestThreadServiceSetTitleUsesTrimmedRequestKeyForIdempotency(t *testing.T) {
	_, typed := testThreadService(t, newBlockingThreadGateway())
	service := typed.(*threadRuntimeService)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-title-key"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.SetTitle(t.Context(), SetTitleInput{ThreadID: created.ThreadID, Title: "A title", RequestKey: "  title-key  "})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := service.SetTitle(t.Context(), SetTitleInput{ThreadID: created.ThreadID, Title: "A title", RequestKey: "title-key"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ThreadID != replayed.ThreadID {
		t.Fatalf("title replay thread = first=%q replay=%q", first.ThreadID, replayed.ThreadID)
	}
	meta, err := service.host.store.repo.Thread(t.Context(), created.ThreadID.String())
	if err != nil || meta.Title != "A title" {
		t.Fatalf("canonical title = %q err=%v", meta.Title, err)
	}
	if _, err := service.SetTitle(t.Context(), SetTitleInput{ThreadID: created.ThreadID, Title: "different", RequestKey: "title-key"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reused title request key error = %v, want ErrRequestConflict", err)
	}
}

func TestHostThreadServiceShutdownDoesNotRaceOwnership(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	factory := AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		return nil, errors.New("test factory must not execute")
	})
	started := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		close(started)
		shutdownDone <- host.Shutdown(context.Background())
	}()
	<-started
	for index := 0; index < 200; index++ {
		service, serviceErr := host.ThreadService(factory)
		if serviceErr == nil && service == nil {
			t.Fatal("ThreadService returned a nil service without an error")
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestHostShutdownWaitsForDeleteCanonicalCommit(t *testing.T) {
	path := t.TempDir() + "/delete-shutdown.db"
	gateway := newCancellationTrackingGateway()
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-delete-shutdown"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "delete"}, RequestKey: "send-delete-shutdown"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, gateway.started, "provider did not start")
	actor := service.(*threadRuntimeService).runtime(created.ThreadID)
	heldDrain := make(chan struct{})
	actor.mu.Lock()
	actor.state.effectsDone = heldDrain
	actor.mu.Unlock()
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- service.Delete(context.Background(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete-shutdown"})
	}()
	waitForDeletingThread(t, actor)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- host.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before delete drain completed: %v", err)
	default:
	}
	close(heldDrain)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	reopenedService, err := reopened.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedService.View(t.Context(), created.ThreadID); !errors.Is(err, ErrThreadDeleted) {
		t.Fatalf("reopened deleted thread error = %v, want ErrThreadDeleted", err)
	}
}

func TestHydrateThreadRuntimeLifecyclePreservesForkInterruption(t *testing.T) {
	repo := sessiontree.NewMemoryRepo()
	now := time.Now().UTC()
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "source", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{
		ThreadID: "source", TurnID: "turn-source", RunID: "run-source", LogicalRequestID: "send-source",
		RequestFingerprint: "accept-source", InputRequestFingerprint: "input-source",
		Input: session.Message{Role: session.User, Content: "unfinished"}, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.UserMessage.ID == "" {
		t.Fatal("source turn was not accepted")
	}
	forked, err := repo.Fork(t.Context(), sessiontree.ForkOptions{
		SourceThreadID: "source", NewThreadID: "forked", OriginRequestKey: "fork-request", OriginFingerprint: "fork-fingerprint",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, activity, outcome, failure := hydrateThreadRuntimeLifecycle(t.Context(), repo, forked)
	if activity != ThreadActivityIdle || outcome == nil || *outcome != TurnOutcomeInterrupted ||
		failure == nil || failure.Code != ThreadTurnFailureInterrupted || failure.Message != sessiontree.BranchBoundaryTurnFailureMessage {
		t.Fatalf("hydrated fork lifecycle = activity=%q outcome=%v failure=%#v", activity, outcome, failure)
	}
}

func waitClosed(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}

func waitForDeletingThread(t *testing.T, actor *threadRuntimeState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		actor.mu.Lock()
		deleting := actor.deleting
		actor.mu.Unlock()
		if deleting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("thread runtime did not enter delete fence")
}
