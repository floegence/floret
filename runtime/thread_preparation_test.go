package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
)

type blockedPreparationRepo struct {
	*internalstorage.BackendKernel
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	pages   atomic.Int32
}

func (r *blockedPreparationRepo) ScanPathProjection(ctx context.Context, id, leaf string, prepare func([]sessiontree.PathMetadata) error, fields func(sessiontree.PathMetadata) []string, visit func(sessiontree.PathProjectionPage) error) error {
	r.pages.Add(1)
	r.once.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
	}
	return r.BackendKernel.ScanPathProjection(ctx, id, leaf, prepare, fields, visit)
}
func TestColdPreparationSurvivesWaiterCancellationAndSharesWork(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "shared-preparation"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, sessiontree.AppendOptions{ID: "message"}); err != nil {
		t.Fatal(err)
	}
	repo := &blockedPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() { _, err := service.viewResult(ctx, id, threadAckResult); first <- err }()
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation never entered")
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	const readers = 8
	done := make(chan error, readers)
	for range readers {
		go func() { _, err := service.viewResult(t.Context(), id, threadAckResult); done <- err }()
	}
	close(repo.release)
	for range readers {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if got := repo.pages.Load(); got != 1 {
		t.Fatalf("one canceled waiter caused %d preparations", got)
	}
	view, err := service.View(t.Context(), id)
	if err != nil || len(view.Items) != 1 || view.Items[0].Text != "retained fact" {
		t.Fatalf("view=%+v err=%v", view, err)
	}
}

func TestShutdownCancelsAndJoinsColdPreparation(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "shutdown-preparation"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	repo := &blockedPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	done := make(chan error, 1)
	go func() { _, err := service.viewResult(t.Context(), id, threadAckResult); done <- err }()
	select {
	case <-repo.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation never entered")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrHostClosed) {
		t.Fatal(err)
	}
}

func TestColdPreparationConcurrencyIsBoundedAndShutdownReleasesQueue(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	for i := range 5 {
		if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: fmt.Sprint("limited-", i)}); err != nil {
			t.Fatal(err)
		}
	}
	repo := &blockedPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	done := make(chan error, 5)
	for i := range 5 {
		go func() {
			_, err := service.viewResult(t.Context(), identity.ThreadID(fmt.Sprint("limited-", i)), threadAckResult)
			done <- err
		}()
	}
	deadline := time.After(5 * time.Second)
	for repo.pages.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("workers did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := repo.pages.Load(); got != 2 {
		t.Fatalf("unbounded preparations: %d", got)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrHostClosed) {
			t.Fatal(err)
		}
	}
}

type panicPreparationRepo struct {
	*internalstorage.BackendKernel
	fail atomic.Bool
}

func (r *panicPreparationRepo) ScanPathProjection(ctx context.Context, id, leaf string, prepare func([]sessiontree.PathMetadata) error, fields func(sessiontree.PathMetadata) []string, visit func(sessiontree.PathProjectionPage) error) error {
	if r.fail.Swap(false) {
		panic("injected source failure")
	}
	return r.BackendKernel.ScanPathProjection(ctx, id, leaf, prepare, fields, visit)
}
func TestColdPreparationFailureCanRetryWithoutPublishingPartialView(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id identity.ThreadID = "failed-preparation"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	repo := &panicPreparationRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
	repo.fail.Store(true)
	host.store.repo = repo
	if _, err := service.viewResult(t.Context(), id, threadAckResult); err == nil {
		t.Fatal("panic did not reach waiter")
	}
	actor := service.runtime(id)
	actor.mu.Lock()
	version := actor.state.view.ViewVersion
	actor.mu.Unlock()
	if version != 0 {
		t.Fatal("failed preparation published a view")
	}
	if _, err := service.viewResult(t.Context(), id, threadAckResult); err != nil {
		t.Fatal(err)
	}
}

// Err is first checked before the actor lock. Cancel while that check returns,
// so the request reaches the mutex with an already-expired waiting context.
type cancelAtActorAdmissionContext struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (ctx *cancelAtActorAdmissionContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(ctx.cancel)
	return err
}

func TestActorDoesNotApplyCancelledWaiterAfterLockAdmission(t *testing.T) {
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelAtActorAdmissionContext{Context: base, cancel: cancel}
	actor := &threadRuntimeState{}
	called := false
	err := actor.apply(ctx, func() error { called = true; return nil })
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("cancelled actor waiter ran: called=%v err=%v", called, err)
	}
}
