package agentharness

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
)

func TestAutomaticTitleBeginFailureTerminalizesTurnBeforeProviderExecution(t *testing.T) {
	ctx := context.Background()
	beginErr := errors.New("injected automatic title begin failure")
	repo := &automaticTitleFaultRepo{
		MemoryRepo: sessiontree.NewMemoryRepo(),
		beginErr:   beginErr,
	}
	mainProvider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done()),
	)
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
	if !errors.Is(err, beginErr) {
		t.Fatalf("Run err = %v, want %v", err, beginErr)
	}
	if result.Status != engine.Failed {
		t.Fatalf("Run result = %#v, want failed", result)
	}
	if len(mainProvider.Requests) != 0 {
		t.Fatalf("main provider requests = %d, want 0", len(mainProvider.Requests))
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := unfinishedTurns(journal.Path); len(got) != 0 {
		t.Fatalf("automatic title begin failure left unfinished turns %v: %#v", got, journal.Path)
	}
	if !hasEntry(journal.Path, sessiontree.EntryTurnMarker, sessiontree.TurnFailed) {
		t.Fatalf("automatic title begin failure did not persist a failed terminal marker: %#v", journal.Path)
	}
}

func TestAutomaticTitlePreflightDoesNotHideConcurrentClaimConflict(t *testing.T) {
	ctx := context.Background()
	repo := &automaticTitleBeginRaceRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	mainProvider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("must not run"), scriptharness.Done()),
	)
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1", RunID: "run-1"})
	if !errors.Is(err, sessiontree.ErrRequestConflict) || result.Status != engine.Failed {
		t.Fatalf("Run result = %#v, err = %v, want fail-closed request conflict", result, err)
	}
	if len(mainProvider.Requests) != 0 {
		t.Fatalf("main provider requests = %d, want 0", len(mainProvider.Requests))
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil || meta.TitleStatus != sessiontree.ThreadTitlePending || meta.TitleToken != "concurrent-title-token" {
		t.Fatalf("concurrent title claim = %#v, err = %v", meta, err)
	}
}

func TestAutomaticTitleCompleteFailureSettlesClaimAsFailed(t *testing.T) {
	ctx := context.Background()
	completeErr := errors.New("injected automatic title completion failure")
	repo := &automaticTitleFaultRepo{
		MemoryRepo:        sessiontree.NewMemoryRepo(),
		completeErr:       completeErr,
		completeRemaining: 1,
	}
	backgroundErrors := make(chan error, 1)
	mainProvider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("main turn completed"), scriptharness.Done()),
	)
	h := newAutomaticTitleTestHarness(mainProvider, repo, func(err error) {
		backgroundErrors <- err
	})
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("Run result = %#v, want completed", result)
	}
	meta := waitForAutomaticTitleStatus(t, repo, "thread", sessiontree.ThreadTitleFailed)
	if !strings.Contains(meta.TitleError, completeErr.Error()) || !strings.Contains(meta.TitleError, "completion") {
		t.Fatalf("title failure = %q, want completion persistence error", meta.TitleError)
	}
	completeCalls, failCalls := repo.titleSettlementCalls()
	if completeCalls != 1 || failCalls != 1 {
		t.Fatalf("title settlement calls = complete:%d fail:%d, want 1 and 1", completeCalls, failCalls)
	}
	select {
	case err := <-backgroundErrors:
		t.Fatalf("settled title failure reported as background infrastructure error: %v", err)
	default:
	}
}

func TestAutomaticTitlePersistentSettlementFailureIsReported(t *testing.T) {
	ctx := context.Background()
	completeErr := errors.New("injected persistent completion failure")
	failErr := errors.New("injected persistent failure settlement failure")
	repo := &automaticTitleFaultRepo{
		MemoryRepo:        sessiontree.NewMemoryRepo(),
		completeErr:       completeErr,
		completeRemaining: -1,
		failErr:           failErr,
		failRemaining:     -1,
	}
	backgroundErrors := make(chan error, 1)
	mainProvider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("main turn completed"), scriptharness.Done()),
	)
	h := newAutomaticTitleTestHarness(mainProvider, repo, func(err error) {
		backgroundErrors <- err
	})
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("Run result = %#v, want completed", result)
	}
	select {
	case backgroundErr := <-backgroundErrors:
		if !errors.Is(backgroundErr, completeErr) || !errors.Is(backgroundErr, failErr) {
			t.Fatalf("background error = %v, want completion and failure settlement errors", backgroundErr)
		}
	case <-time.After(time.Second):
		t.Fatal("persistent automatic title settlement failure was not reported")
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitlePending {
		t.Fatalf("title state = %#v, want pending for startup recovery", meta)
	}
}

func TestAutomaticTitleDetachesFromSuccessfulMainTurnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := sessiontree.NewMemoryRepo()
	titleGenerator := newBlockingAutomaticTitleGenerator()
	mainProvider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("main turn completed"), scriptharness.Done()),
	)
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	h.options.TitleGenerator = titleGenerator
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("Run result = %#v, want completed", result)
	}
	select {
	case <-titleGenerator.started:
	case <-time.After(time.Second):
		t.Fatal("automatic title generator did not start")
	}
	cancel()
	close(titleGenerator.release)
	meta := waitForAutomaticTitleStatus(t, repo, "thread", sessiontree.ThreadTitleReady)
	if meta.Title != "Detached" {
		t.Fatalf("detached title = %#v", meta)
	}
}

func TestAutomaticTitlePendingClaimDoesNotFailFollowUpTurn(t *testing.T) {
	for _, test := range []struct {
		name       string
		titleErr   error
		wantStatus sessiontree.ThreadTitleStatus
	}{
		{name: "worker succeeds", wantStatus: sessiontree.ThreadTitleReady},
		{name: "worker fails", titleErr: errors.New("title provider failed"), wantStatus: sessiontree.ThreadTitleFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo := sessiontree.NewMemoryRepo()
			titleGenerator := newBlockingAutomaticTitleGenerator()
			titleGenerator.err = test.titleErr
			mainProvider := scriptharness.NewScriptedProvider(
				scriptharness.Step(scriptharness.Text("first completed"), scriptharness.Done()),
				scriptharness.Step(scriptharness.Text("follow-up completed"), scriptharness.Done()),
			)
			h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
			h.options.TitleGenerator = titleGenerator
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			first, err := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1", RunID: "run-1"})
			if err != nil || first.Status != engine.Completed {
				t.Fatalf("first turn = %#v, err = %v", first, err)
			}
			select {
			case <-titleGenerator.started:
			case <-time.After(time.Second):
				t.Fatal("automatic title generator did not start")
			}
			pending, err := repo.Thread(ctx, "thread")
			if err != nil || pending.TitleStatus != sessiontree.ThreadTitlePending || pending.TitleGeneration != 1 {
				t.Fatalf("pending title = %#v, err = %v", pending, err)
			}
			second, err := thread.Run(ctx, "continue while title is pending", RunOptions{TurnID: "turn-2", RunID: "run-2"})
			if err != nil || second.Status != engine.Completed || second.ID != "turn-2" || len(mainProvider.Requests) != 2 {
				t.Fatalf("follow-up turn = %#v, requests = %d, err = %v", second, len(mainProvider.Requests), err)
			}
			stillPending, err := repo.Thread(ctx, "thread")
			if err != nil || stillPending.TitleStatus != sessiontree.ThreadTitlePending || stillPending.TitleGeneration != pending.TitleGeneration || stillPending.TitleToken != pending.TitleToken {
				t.Fatalf("follow-up replaced pending title claim: before=%#v after=%#v err=%v", pending, stillPending, err)
			}
			close(titleGenerator.release)
			settled := waitForAutomaticTitleStatus(t, repo, "thread", test.wantStatus)
			if settled.TitleGeneration != pending.TitleGeneration || settled.TitleToken != pending.TitleToken {
				t.Fatalf("settled title replaced claim = %#v", settled)
			}
			if test.wantStatus == sessiontree.ThreadTitleReady && settled.Title != "Detached" {
				t.Fatalf("settled title = %#v", settled)
			}
			if test.wantStatus == sessiontree.ThreadTitleFailed && !strings.Contains(settled.TitleError, test.titleErr.Error()) {
				t.Fatalf("failed title = %#v", settled)
			}
			if got := titleGenerator.calls.Load(); got != 1 {
				t.Fatalf("automatic title generator calls = %d, want 1", got)
			}
		})
	}
}

func TestAutomaticTitleCancelsWithCancelledMainTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := sessiontree.NewMemoryRepo()
	titleGenerator := newCancellationBarrierAutomaticTitleGenerator()
	mainProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Hang()))
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	h.options.TitleGenerator = titleGenerator
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, runErr := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
		runDone <- runErr
	}()
	select {
	case <-titleGenerator.started:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic title generator did not start")
	}
	cancel()
	select {
	case <-titleGenerator.cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled main turn did not cancel automatic title worker")
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("Run returned before cancelled automatic title worker settled: %v", runErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(titleGenerator.release)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("Run err = %v, want context cancellation", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled main turn did not return after automatic title settlement")
	}
	meta, err := repo.Thread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitleFailed {
		t.Fatalf("title status at Run return = %#v, want failed", meta)
	}
	if meta.TitleError != automaticTitleInterrupted {
		t.Fatalf("cancelled title = %#v", meta)
	}
}

func TestAutomaticTitleJoinsWhenCancellationIsMaskedByProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := sessiontree.NewMemoryRepo()
	titleGenerator := newCancellationBarrierAutomaticTitleGenerator()
	maskedErr := errors.New("provider masked context cancellation")
	mainProvider := &cancellationMaskedAutomaticTitleProvider{
		started: make(chan struct{}), err: maskedErr,
	}
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	h.options.TitleGenerator = titleGenerator
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, runErr := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
		runDone <- runErr
	}()
	for name, started := range map[string]<-chan struct{}{
		"main provider": mainProvider.started, "title provider": titleGenerator.started,
	} {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not start", name)
		}
	}
	cancel()
	select {
	case <-titleGenerator.cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("masked cancellation did not cancel automatic title worker")
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("Run returned before cancelled automatic title worker settled: %v", runErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(titleGenerator.release)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, maskedErr) {
			t.Fatalf("Run err = %v, want masked provider error", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after automatic title settlement")
	}
	meta, err := repo.Thread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitleFailed || meta.TitleError != automaticTitleInterrupted {
		t.Fatalf("title at Run return = %#v, want interrupted failure", meta)
	}
}

func TestAutomaticTitleCancelsAndJoinsOnProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := sessiontree.NewMemoryRepo()
	titleGenerator := newCancellationBarrierAutomaticTitleGenerator()
	providerErr := errors.New("provider failed")
	mainProvider := &failureAfterSignalAutomaticTitleProvider{
		proceed: titleGenerator.started,
		err:     providerErr,
	}
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	h.options.TitleGenerator = titleGenerator
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, runErr := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1"})
		runDone <- runErr
	}()
	select {
	case <-titleGenerator.cancelled:
	case runErr := <-runDone:
		close(titleGenerator.release)
		t.Fatalf("Run returned before cancelling automatic title worker: %v", runErr)
	case <-time.After(3 * time.Second):
		close(titleGenerator.release)
		t.Fatal("provider failure did not cancel automatic title worker")
	}
	select {
	case runErr := <-runDone:
		close(titleGenerator.release)
		t.Fatalf("Run returned before cancelled automatic title worker settled: %v", runErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(titleGenerator.release)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, providerErr) {
			t.Fatalf("Run err = %v, want provider failure", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after automatic title settlement")
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitleFailed || meta.TitleError != automaticTitleInterrupted {
		t.Fatalf("title at Run return = %#v, want interrupted failure", meta)
	}
}

func TestAutomaticTitleCancelsAndJoinsOnLeaseRenewalFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := sessiontree.NewMemoryRepo()
	renewalErr := errors.New("lease renewal failed")
	repo := &automaticTitleRenewalFailureRepo{
		MemoryRepo:   base,
		renewStarted: make(chan struct{}),
		releaseRenew: make(chan struct{}),
		err:          renewalErr,
	}
	titleGenerator := newCancellationBarrierAutomaticTitleGenerator()
	mainProvider := &successAfterSignalAutomaticTitleProvider{proceed: repo.renewStarted}
	h := newAutomaticTitleTestHarness(mainProvider, repo, nil)
	h.options.TitleGenerator = titleGenerator
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, runErr := thread.Run(ctx, "generate a title", RunOptions{TurnID: "turn-1", RunID: "run-1"})
		runDone <- runErr
	}()
	select {
	case <-repo.renewStarted:
	case <-time.After(3 * time.Second):
		close(repo.releaseRenew)
		close(titleGenerator.release)
		t.Fatal("lease renewal did not start")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		admission, ok, readErr := repo.ReadTurnAdmission(ctx, "thread", "turn-1", "run-1")
		if readErr != nil {
			close(repo.releaseRenew)
			close(titleGenerator.release)
			t.Fatal(readErr)
		}
		if ok && admission.Terminal != nil {
			break
		}
		if time.Now().After(deadline) {
			close(repo.releaseRenew)
			close(titleGenerator.release)
			t.Fatal("main turn did not finish while lease renewal was blocked")
		}
		time.Sleep(time.Millisecond)
	}
	close(repo.releaseRenew)
	select {
	case <-titleGenerator.cancelled:
	case runErr := <-runDone:
		close(titleGenerator.release)
		t.Fatalf("Run returned before cancelling automatic title worker: %v", runErr)
	case <-time.After(3 * time.Second):
		close(titleGenerator.release)
		t.Fatal("lease renewal failure did not cancel automatic title worker")
	}
	select {
	case runErr := <-runDone:
		close(titleGenerator.release)
		t.Fatalf("Run returned before cancelled automatic title worker settled: %v", runErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(titleGenerator.release)
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, renewalErr) {
			t.Fatalf("Run err = %v, want renewal failure", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after automatic title settlement")
	}
	meta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TitleStatus != sessiontree.ThreadTitleFailed || meta.TitleError != automaticTitleInterrupted {
		t.Fatalf("title at Run return = %#v, want interrupted failure", meta)
	}
}

func TestAutomaticTitleSettlementSerializesWithSubAgentClose(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T) (automaticTitleCloseTestRepo, storage.ForkOperationStore, cache.Store)
	}{
		{
			name: "memory",
			open: func(*testing.T) (automaticTitleCloseTestRepo, storage.ForkOperationStore, cache.Store) {
				repo := sessiontree.NewMemoryRepo()
				return repo, storage.NewMemoryForkOperationStore(repo), cache.NewMemoryStore()
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T) (automaticTitleCloseTestRepo, storage.ForkOperationStore, cache.Store) {
				repo, err := sqlite.Open(filepath.Join(t.TempDir(), "automatic-title-close.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = repo.Close() })
				return repo, repo, repo
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base, forkOperations, promptStore := test.open(t)
			repo := &automaticTitleSettlementBarrierRepo{
				automaticTitleCloseTestRepo: base,
				completeEntered:             make(chan struct{}),
				releaseComplete:             make(chan struct{}),
			}
			backgroundDone := make(chan struct{})
			backgroundErrors := make(chan error, 1)
			h := newTestHarness(scriptharness.NewScriptedProvider(), repo, promptStore)
			h.options.ForkOperations = forkOperations
			h.options.TitleGenerator = fixedTitleGenerator{title: "Child title"}
			h.options.BeginBackgroundExecution = func() (context.Context, func(), error) {
				executionCtx, cancel := context.WithCancel(context.Background())
				var once sync.Once
				return executionCtx, func() {
					once.Do(func() {
						cancel()
						close(backgroundDone)
					})
				}, nil
			}
			h.options.ReportBackgroundError = func(err error) { backgroundErrors <- err }
			otherHarness := newTestHarness(scriptharness.NewScriptedProvider(), repo, promptStore)
			otherHarness.options.ForkOperations = forkOperations

			if _, err := base.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
				t.Fatal(err)
			}
			if _, err := base.CreateThread(ctx, sessiontree.ThreadMeta{
				ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn",
				TaskName: "child", AgentPath: "/root/child",
			}); err != nil {
				t.Fatal(err)
			}
			child := h.cacheThread("child")
			execution, err := child.startAutomaticTitle(ctx, "turn-title", "run-title", "entry-title", session.Message{
				Role: session.User, Content: "generate a child title",
			})
			if err != nil {
				t.Fatal(err)
			}
			if execution == nil {
				t.Fatal("automatic title execution did not start")
			}
			select {
			case <-repo.completeEntered:
			case <-time.After(time.Second):
				t.Fatal("automatic title completion did not reach the settlement barrier")
			}
			var releaseOnce sync.Once
			releaseSettlement := func() { releaseOnce.Do(func() { close(repo.releaseComplete) }) }
			defer releaseSettlement()
			authorityReleasedBeforeSettlement := child.authorityMu.TryLock()
			if authorityReleasedBeforeSettlement {
				child.authorityMu.Unlock()
			}
			_, crossHarnessCloseErr := otherHarness.CloseSubAgent(ctx, CloseSubAgentOptions{
				CloseOperationID: "cross-harness-close", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test_close",
			})
			pendingMeta, err := base.Thread(ctx, "child")
			if err != nil {
				t.Fatal(err)
			}
			if pendingMeta.IsClosing() || pendingMeta.IsClosed() || pendingMeta.TitleStatus != sessiontree.ThreadTitlePending {
				t.Fatalf("cross-harness close crossed pending title authority: %#v", pendingMeta)
			}

			type closeResult struct {
				snapshot SubAgentSnapshot
				err      error
			}
			closeDone := make(chan closeResult, 1)
			go func() {
				snapshot, closeErr := h.CloseSubAgent(ctx, CloseSubAgentOptions{
					CloseOperationID: "close-child", ParentThreadID: "parent", ChildThreadID: "child", Reason: "test_close",
				})
				closeDone <- closeResult{snapshot: snapshot, err: closeErr}
			}()
			releaseSettlement()
			select {
			case <-backgroundDone:
			case <-time.After(time.Second):
				t.Fatal("automatic title worker did not finish")
			}
			var closed closeResult
			select {
			case closed = <-closeDone:
			case <-time.After(time.Second):
				t.Fatal("subagent close did not finish after title settlement")
			}
			if authorityReleasedBeforeSettlement {
				t.Fatal("automatic title released thread authority before the matching claim settled")
			}
			if !errors.Is(crossHarnessCloseErr, sessiontree.ErrThreadAuthorityBusy) {
				t.Fatalf("cross-harness CloseSubAgent err = %v, want ErrThreadAuthorityBusy", crossHarnessCloseErr)
			}
			if closed.err != nil || !closed.snapshot.Closed {
				t.Fatalf("CloseSubAgent result=%#v err=%v", closed.snapshot, closed.err)
			}
			meta, err := base.Thread(ctx, "child")
			if err != nil {
				t.Fatal(err)
			}
			if !meta.IsClosed() || meta.TitleStatus != sessiontree.ThreadTitleReady || meta.Title != "Child title" {
				t.Fatalf("closed child title state = %#v", meta)
			}
			pending, err := base.PendingAutomaticThreadTitles(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) != 0 {
				t.Fatalf("closed child left pending automatic titles: %#v", pending)
			}
			select {
			case backgroundErr := <-backgroundErrors:
				t.Fatalf("close/title serialization reported a background error: %v", backgroundErr)
			default:
			}
		})
	}
}

type automaticTitleFaultRepo struct {
	*sessiontree.MemoryRepo

	mu                sync.Mutex
	beginErr          error
	completeErr       error
	completeRemaining int
	failErr           error
	failRemaining     int
	completeCalls     int
	failCalls         int
}

type automaticTitleBeginRaceRepo struct {
	*sessiontree.MemoryRepo
	once sync.Once
	err  error
}

func (r *automaticTitleBeginRaceRepo) BeginAutomaticThreadTitle(ctx context.Context, req sessiontree.BeginAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.once.Do(func() {
		_, r.err = r.MemoryRepo.BeginAutomaticThreadTitle(ctx, sessiontree.BeginAutomaticThreadTitleRequest{
			ThreadID: req.ThreadID, Token: "concurrent-title-token", Now: req.Now,
		})
	})
	if r.err != nil {
		return sessiontree.ThreadTitleMutationResult{}, r.err
	}
	return r.MemoryRepo.BeginAutomaticThreadTitle(ctx, req)
}

type automaticTitleCloseTestRepo interface {
	sessiontree.Repo
	sessiontree.ThreadTitleAuthorityRepo
	sessiontree.SubAgentCloseAuthorityRepo
	sessiontree.ThreadAuthorityInspectionRepo
	sessiontree.ThreadListRepo
	sessiontree.SubAgentInputAuthorityRepo
}

type automaticTitleSettlementBarrierRepo struct {
	automaticTitleCloseTestRepo

	completeEntered chan struct{}
	releaseComplete chan struct{}
	completeOnce    sync.Once
}

func (r *automaticTitleSettlementBarrierRepo) CompleteAutomaticThreadTitle(ctx context.Context, req sessiontree.CompleteAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.completeOnce.Do(func() { close(r.completeEntered) })
	select {
	case <-r.releaseComplete:
	case <-ctx.Done():
		return sessiontree.ThreadTitleMutationResult{}, ctx.Err()
	}
	return r.automaticTitleCloseTestRepo.CompleteAutomaticThreadTitle(ctx, req)
}

func (r *automaticTitleFaultRepo) BeginAutomaticThreadTitle(ctx context.Context, req sessiontree.BeginAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	if r.beginErr != nil {
		return sessiontree.ThreadTitleMutationResult{}, r.beginErr
	}
	return r.MemoryRepo.BeginAutomaticThreadTitle(ctx, req)
}

func (r *automaticTitleFaultRepo) CompleteAutomaticThreadTitle(ctx context.Context, req sessiontree.CompleteAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.mu.Lock()
	r.completeCalls++
	err := r.completeErr
	shouldFail := err != nil && r.completeRemaining != 0
	if r.completeRemaining > 0 {
		r.completeRemaining--
	}
	r.mu.Unlock()
	if shouldFail {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	return r.MemoryRepo.CompleteAutomaticThreadTitle(ctx, req)
}

func (r *automaticTitleFaultRepo) FailAutomaticThreadTitle(ctx context.Context, req sessiontree.FailAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error) {
	r.mu.Lock()
	r.failCalls++
	err := r.failErr
	shouldFail := err != nil && r.failRemaining != 0
	if r.failRemaining > 0 {
		r.failRemaining--
	}
	r.mu.Unlock()
	if shouldFail {
		return sessiontree.ThreadTitleMutationResult{}, err
	}
	return r.MemoryRepo.FailAutomaticThreadTitle(ctx, req)
}

func (r *automaticTitleFaultRepo) titleSettlementCalls() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completeCalls, r.failCalls
}

func newAutomaticTitleTestHarness(p provider.Provider, repo sessiontree.Repo, report func(error)) *AgentHarness {
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	if faultRepo, ok := repo.(*automaticTitleFaultRepo); ok {
		h.options.ForkOperations = storage.NewMemoryForkOperationStore(faultRepo.MemoryRepo)
	}
	h.options.ReportBackgroundError = report
	return h
}

func waitForAutomaticTitleStatus(t *testing.T, repo sessiontree.JournalRepo, threadID string, status sessiontree.ThreadTitleStatus) sessiontree.ThreadMeta {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		meta, err := repo.Thread(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if meta.TitleStatus == status {
			return meta
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic title status = %#v, want %q", meta, status)
		}
		time.Sleep(time.Millisecond)
	}
}

type blockingAutomaticTitleGenerator struct {
	started    chan struct{}
	release    chan struct{}
	cancelled  chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
	calls      atomic.Int64
	err        error
}

type cancellationBarrierAutomaticTitleGenerator struct {
	started    chan struct{}
	cancelled  chan struct{}
	release    chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
}

type cancellationMaskedAutomaticTitleProvider struct {
	started chan struct{}
	err     error
	once    sync.Once
}

type failureAfterSignalAutomaticTitleProvider struct {
	proceed <-chan struct{}
	err     error
}

type successAfterSignalAutomaticTitleProvider struct {
	proceed <-chan struct{}
}

type automaticTitleRenewalFailureRepo struct {
	*sessiontree.MemoryRepo
	renewStarted chan struct{}
	releaseRenew chan struct{}
	err          error
	once         sync.Once
}

func newBlockingAutomaticTitleGenerator() *blockingAutomaticTitleGenerator {
	return &blockingAutomaticTitleGenerator{
		started: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{}),
	}
}

func newCancellationBarrierAutomaticTitleGenerator() *cancellationBarrierAutomaticTitleGenerator {
	return &cancellationBarrierAutomaticTitleGenerator{
		started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}),
	}
}

func (g *blockingAutomaticTitleGenerator) GenerateTitle(ctx context.Context, _ TitleRequest) (TitleResult, error) {
	g.calls.Add(1)
	g.startOnce.Do(func() { close(g.started) })
	select {
	case <-ctx.Done():
		g.cancelOnce.Do(func() { close(g.cancelled) })
		return TitleResult{}, ctx.Err()
	case <-g.release:
		if g.err != nil {
			return TitleResult{}, g.err
		}
		return TitleResult{Title: "Detached", Source: sessiontree.ThreadTitleSourceProvider}, nil
	}
}

func (g *cancellationBarrierAutomaticTitleGenerator) GenerateTitle(ctx context.Context, _ TitleRequest) (TitleResult, error) {
	g.startOnce.Do(func() { close(g.started) })
	<-ctx.Done()
	g.cancelOnce.Do(func() { close(g.cancelled) })
	<-g.release
	return TitleResult{}, ctx.Err()
}

func (p *cancellationMaskedAutomaticTitleProvider) Stream(ctx context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return nil, p.err
}

func (p *failureAfterSignalAutomaticTitleProvider) Stream(ctx context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
	select {
	case <-p.proceed:
		return nil, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *successAfterSignalAutomaticTitleProvider) Stream(ctx context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
	select {
	case <-p.proceed:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	stream := make(chan provider.StreamEvent, 2)
	stream <- scriptharness.Text("main turn completed")
	stream <- scriptharness.Done()
	close(stream)
	return stream, nil
}

func (r *automaticTitleRenewalFailureRepo) AuthorityLeasePolicy() sessiontree.LeasePolicy {
	return sessiontree.LeasePolicy{TTL: 30 * time.Millisecond, RenewInterval: time.Millisecond}
}

func (r *automaticTitleRenewalFailureRepo) RenewTurnLease(context.Context, sessiontree.TurnLease) (sessiontree.TurnLease, error) {
	r.once.Do(func() { close(r.renewStarted) })
	<-r.releaseRenew
	return sessiontree.TurnLease{}, r.err
}

var _ sessiontree.ThreadTitleAuthorityRepo = (*automaticTitleFaultRepo)(nil)
var _ sessiontree.ThreadTitleAuthorityRepo = (*automaticTitleBeginRaceRepo)(nil)
var _ TitleGenerator = (*blockingAutomaticTitleGenerator)(nil)
var _ TitleGenerator = (*cancellationBarrierAutomaticTitleGenerator)(nil)
var _ provider.Provider = (*cancellationMaskedAutomaticTitleProvider)(nil)
var _ provider.Provider = (*failureAfterSignalAutomaticTitleProvider)(nil)
var _ provider.Provider = (*successAfterSignalAutomaticTitleProvider)(nil)
var _ sessiontree.TurnLeaseRepo = (*automaticTitleRenewalFailureRepo)(nil)
