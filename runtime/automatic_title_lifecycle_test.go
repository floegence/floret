package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestStoreCloseReturnsAutomaticTitleSettlementFailure(t *testing.T) {
	ctx := context.Background()
	completeErr := errors.New("injected runtime title completion failure")
	failErr := errors.New("injected runtime title failure settlement failure")
	store := NewMemoryStore()
	baseRepo := store.repo.(*sessiontree.MemoryRepo)
	faultRepo := &runtimeAutomaticTitleFaultRepo{
		MemoryRepo:        baseRepo,
		completeErr:       completeErr,
		failErr:           failErr,
		completeAttempted: make(chan struct{}),
	}
	store.repo = faultRepo
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.HasSuffix(string(req.RunID), ":thread-title") {
			return runtimeGatewayEvents("Generated title"), nil
		}
		return runtimeGatewayEvents("main turn completed"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ThreadTitleMode:      ThreadTitleModeProvider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-faultRepo.completeAttempted:
	case <-time.After(time.Second):
		t.Fatal("automatic title completion was not attempted")
	}

	closeErr := store.Close()
	if !errors.Is(closeErr, completeErr) || !errors.Is(closeErr, failErr) {
		t.Fatalf("Store.Close err = %v, want completion and failure settlement errors", closeErr)
	}
	store.lifetimeMu.Lock()
	activeOperations := store.activeOperations
	lifetimeState := store.lifetimeState
	store.lifetimeMu.Unlock()
	if activeOperations != 0 || lifetimeState != storeLifetimeClosed {
		t.Fatalf("store lifetime after Close = active:%d state:%q", activeOperations, lifetimeState)
	}
	meta, err := faultRepo.Thread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitlePending {
		t.Fatalf("title state = %#v, want pending for startup recovery", meta)
	}
}

func TestStoreCloseJoinsBackgroundAndStorageErrors(t *testing.T) {
	backgroundErr := errors.New("injected background operation failure")
	storageErr := errors.New("injected storage close failure")
	store := NewMemoryStore()
	store.close = func() error { return storageErr }
	store.reportBackgroundError(backgroundErr)

	err := store.Close()
	if !errors.Is(err, backgroundErr) || !errors.Is(err, storageErr) {
		t.Fatalf("Store.Close err = %v, want background and storage errors", err)
	}
	store.lifetimeMu.Lock()
	state := store.lifetimeState
	store.lifetimeMu.Unlock()
	if state != storeLifetimeClosing {
		t.Fatalf("store state = %q, want retryable closing state after storage close failure", state)
	}
}

func TestAutomaticTitleRecoveryRetriesAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	recoveryErr := errors.New("injected automatic title recovery failure")
	store := NewMemoryStore()
	baseRepo := store.repo.(*sessiontree.MemoryRepo)
	faultRepo := &runtimeAutomaticTitleRecoveryFaultRepo{
		MemoryRepo: baseRepo,
		failAtCall: 2,
		failErr:    recoveryErr,
	}
	store.repo = faultRepo
	for _, threadID := range []string{"thread-a", "thread-b", "thread-c"} {
		if _, err := faultRepo.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
		if _, err := faultRepo.BeginAutomaticThreadTitle(ctx, sessiontree.BeginAutomaticThreadTitleRequest{
			ThreadID: threadID, Token: "title-" + threadID, Now: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	newHost := func() error {
		_, err := newTestHost(t, providerHostOptions{
			Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "unused"},
			Store:  store,
		})
		return err
	}
	if err := newHost(); !errors.Is(err, recoveryErr) {
		t.Fatalf("first host recovery err = %v, want %v", err, recoveryErr)
	}
	pending, err := faultRepo.PendingAutomaticThreadTitles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending titles after partial recovery = %#v, want 2", pending)
	}
	if err := newHost(); err != nil {
		t.Fatalf("second host did not retry title recovery: %v", err)
	}
	pending, err = faultRepo.PendingAutomaticThreadTitles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending titles after recovery retry = %#v", pending)
	}
	if calls := faultRepo.failureCalls(); calls != 4 {
		t.Fatalf("automatic title recovery failure calls = %d, want 4", calls)
	}
}

func TestProviderHostOpensAfterReopenedClosedChildTitle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "closed-child-title.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child",
	}); err != nil {
		t.Fatal(err)
	}
	titleAuthority := store.repo.(sessiontree.ThreadTitleAuthorityRepo)
	pending, err := titleAuthority.BeginAutomaticThreadTitle(ctx, sessiontree.BeginAutomaticThreadTitleRequest{
		ThreadID: "child", Token: "title-worker", Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := titleAuthority.FailAutomaticThreadTitle(ctx, sessiontree.FailAutomaticThreadTitleRequest{
		ThreadID: "child", Generation: pending.Thread.TitleGeneration, Token: pending.Thread.TitleToken,
		Error: "title stopped", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	closeAuthority := store.repo.(sessiontree.SubAgentCloseAuthorityRepo)
	if _, err := closeAuthority.PrepareSubAgentClose(ctx, sessiontree.PrepareSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := closeAuthority.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := newTestHost(t, providerHostOptions{
		Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "unused"},
		Store:  reopened,
	}); err != nil {
		t.Fatalf("provider host open after closed child title: %v", err)
	}
}

func TestAutomaticTitleDeletionDoesNotReportBackgroundFailure(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	titleStarted := make(chan struct{})
	releaseTitle := make(chan struct{})
	var titleOnce sync.Once
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.HasSuffix(string(req.RunID), ":thread-title") {
			titleOnce.Do(func() { close(titleStarted) })
			<-releaseTitle
			return runtimeGatewayEvents("Deleted title"), nil
		}
		return runtimeGatewayEvents("main turn completed"), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ThreadTitleMode:      ThreadTitleModeProvider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-titleStarted:
	case <-time.After(time.Second):
		t.Fatal("automatic title worker did not start")
	}
	deleteHost, err := host.delete.NewHost(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteHost.DeleteThread(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	close(releaseTitle)
	if err := store.Close(); err != nil {
		t.Fatalf("Store.Close reported deleted automatic title as a background failure: %v", err)
	}
}

func TestCancelledRunTurnJoinsAutomaticTitleSettlementBeforeSQLiteRead(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "cancelled-title.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	mainStarted := make(chan struct{})
	titleStarted := make(chan struct{})
	titleCancelled := make(chan struct{})
	releaseTitle := make(chan struct{})
	var mainOnce sync.Once
	var titleStartOnce sync.Once
	var titleCancelOnce sync.Once
	gateway := runtimeModelGateway(func(ctx context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.HasSuffix(string(req.RunID), ":thread-title") {
			titleStartOnce.Do(func() { close(titleStarted) })
			<-ctx.Done()
			titleCancelOnce.Do(func() { close(titleCancelled) })
			<-releaseTitle
			return nil, ctx.Err()
		}
		mainOnce.Do(func() { close(mainStarted) })
		events := make(chan ModelEvent)
		go func() {
			<-ctx.Done()
			close(events)
		}()
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ThreadTitleMode:      ThreadTitleModeProvider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	type runOutcome struct {
		result TurnResult
		err    error
	}
	runDone := make(chan runOutcome, 1)
	go func() {
		result, runErr := host.RunTurn(runCtx, RunTurnRequest{
			RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "generate a title"},
		})
		runDone <- runOutcome{result: result, err: runErr}
	}()
	for name, started := range map[string]<-chan struct{}{"main provider": mainStarted, "title provider": titleStarted} {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not start", name)
		}
	}
	cancelRun()
	select {
	case <-titleCancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic title provider did not observe main cancellation")
	}
	select {
	case outcome := <-runDone:
		t.Fatalf("RunTurn returned before automatic title settlement barrier: result=%#v err=%v", outcome.result, outcome.err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseTitle)
	var outcome runOutcome
	select {
	case outcome = <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("RunTurn did not return after automatic title settlement barrier")
	}
	if !errors.Is(outcome.err, context.Canceled) || outcome.result.Status != TurnStatusCancelled {
		t.Fatalf("RunTurn result=%#v err=%v, want cancelled", outcome.result, outcome.err)
	}
	store.lifetimeMu.Lock()
	activeOperations := store.activeOperations
	store.lifetimeMu.Unlock()
	if activeOperations != 0 {
		t.Fatalf("Store active operations at RunTurn return = %d, want 0", activeOperations)
	}
	overview, err := host.ReadThreadOverview(context.Background(), "thread")
	if err != nil {
		t.Fatalf("ReadThreadOverview immediately after RunTurn: %v", err)
	}
	if overview.Thread.TitleStatus != string(sessiontree.ThreadTitleFailed) || overview.Thread.TitleError != automaticTitleInterruptedForRuntimeTest {
		t.Fatalf("title at RunTurn return = %#v, want failed", overview.Thread)
	}
}

func TestModelGatewayProviderCancelsNeverClosingUpstreamStream(t *testing.T) {
	upstream := make(chan ModelEvent)
	started := make(chan struct{})
	var once sync.Once
	adapter := modelGatewayProvider{
		gateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			once.Do(func() { close(started) })
			return upstream, nil
		}),
		identity: runtimeGatewayIdentity("fake-model"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := adapter.Stream(ctx, provider.Request{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", PromptScopeID: "thread",
		Messages: []session.Message{{Role: session.User, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("model gateway did not start")
	}
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("model gateway adapter emitted an event after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("model gateway adapter did not close after cancellation")
	}
}

func TestCancelledRunTurnSettlesNeverClosingTitleAndModelStreams(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "never-closing-streams.db"))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close()
		}
	})
	mainStarted := make(chan struct{})
	titleStarted := make(chan struct{})
	var mainOnce sync.Once
	var titleOnce sync.Once
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		if strings.HasSuffix(string(req.RunID), ":thread-title") {
			titleOnce.Do(func() { close(titleStarted) })
		} else {
			mainOnce.Do(func() { close(mainStarted) })
		}
		return make(chan ModelEvent), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("gateway system"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
		ThreadTitleMode:      ThreadTitleModeProvider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	type runOutcome struct {
		result TurnResult
		err    error
	}
	runDone := make(chan runOutcome, 1)
	go func() {
		result, runErr := host.RunTurn(runCtx, RunTurnRequest{
			RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", Input: TurnInput{Text: "generate a title"},
		})
		runDone <- runOutcome{result: result, err: runErr}
	}()
	for name, started := range map[string]<-chan struct{}{"main provider": mainStarted, "title provider": titleStarted} {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not start", name)
		}
	}
	cancelRun()
	var outcome runOutcome
	select {
	case outcome = <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("RunTurn did not join never-closing model streams")
	}
	if !errors.Is(outcome.err, context.Canceled) || outcome.result.Status != TurnStatusCancelled {
		t.Fatalf("RunTurn result=%#v err=%v, want cancellation", outcome.result, outcome.err)
	}
	overview, err := host.ReadThreadOverview(context.Background(), "thread")
	if err != nil {
		t.Fatalf("ReadThreadOverview after cancellation: %v", err)
	}
	if overview.LatestTurn == nil || overview.LatestTurn.Status != TurnStatusCancelled {
		t.Fatalf("canonical latest turn = %#v, want cancelled", overview.LatestTurn)
	}
	if overview.Thread.TitleStatus != string(sessiontree.ThreadTitleFailed) || overview.Thread.TitleError != automaticTitleInterruptedForRuntimeTest {
		t.Fatalf("canonical title = %#v, want settled failure", overview.Thread)
	}
	store.lifetimeMu.Lock()
	activeOperations := store.activeOperations
	store.lifetimeMu.Unlock()
	if activeOperations != 0 {
		t.Fatalf("Store active operations after cancellation = %d, want 0", activeOperations)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case closeErr := <-closeDone:
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		closed = true
	case <-time.After(3 * time.Second):
		t.Fatal("Store.Close blocked on never-closing model stream")
	}
}

type runtimeAutomaticTitleFaultRepo struct {
	*sessiontree.MemoryRepo

	completeErr       error
	failErr           error
	completeAttempted chan struct{}
	completeOnce      sync.Once
}

type runtimeAutomaticTitleRecoveryFaultRepo struct {
	*sessiontree.MemoryRepo

	mu         sync.Mutex
	failAtCall int
	failErr    error
	failCalls  int
}

func (r *runtimeAutomaticTitleRecoveryFaultRepo) FailAutomaticThreadTitle(ctx context.Context, req sessiontree.FailAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.mu.Lock()
	r.failCalls++
	call := r.failCalls
	r.mu.Unlock()
	if call == r.failAtCall {
		return sessiontree.ThreadTitleMutationResult{}, r.failErr
	}
	return r.MemoryRepo.FailAutomaticThreadTitle(ctx, req)
}

func (r *runtimeAutomaticTitleRecoveryFaultRepo) failureCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failCalls
}

func (r *runtimeAutomaticTitleFaultRepo) CompleteAutomaticThreadTitle(context.Context, sessiontree.CompleteAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.completeOnce.Do(func() { close(r.completeAttempted) })
	return sessiontree.ThreadTitleMutationResult{}, r.completeErr
}

func (r *runtimeAutomaticTitleFaultRepo) FailAutomaticThreadTitle(context.Context, sessiontree.FailAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	return sessiontree.ThreadTitleMutationResult{}, r.failErr
}

var _ sessiontree.ThreadTitleAuthorityRepo = (*runtimeAutomaticTitleFaultRepo)(nil)
var _ sessiontree.ThreadTitleAuthorityRepo = (*runtimeAutomaticTitleRecoveryFaultRepo)(nil)
