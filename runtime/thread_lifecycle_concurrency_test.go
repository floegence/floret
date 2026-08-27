package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/provider"
	"github.com/floegence/floret/v5/storage"
	"github.com/floegence/floret/v5/tools"
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
	if view.Error != "agent preparation failed" {
		t.Fatalf("preparation failure view error = %q", view.Error)
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

func TestThreadRuntimeRetrySourceFenceAllowsOnlyOneLocalDispatcher(t *testing.T) {
	actor := &threadRuntimeState{threadID: "thread"}
	claimed, err := actor.claimEffectRetrySource("effect-source")
	if err != nil || !claimed {
		t.Fatalf("first source claim=(%v,%v), want true", claimed, err)
	}
	claimed, err = actor.claimEffectRetrySource("effect-source")
	if !errors.Is(err, ErrRequestConflict) || claimed {
		t.Fatalf("duplicate source claim=(%v,%v), want conflict", claimed, err)
	}
	actor.releaseEffectRetrySource("effect-source")
	claimed, err = actor.claimEffectRetrySource("effect-source")
	if err != nil || !claimed {
		t.Fatalf("released source claim=(%v,%v), want true", claimed, err)
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
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- service.Delete(context.Background(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete-shutdown"})
	}()
	waitForDeletingThread(t, service.(*threadRuntimeService).runtime(created.ThreadID))
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- host.Shutdown(context.Background()) }()
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
