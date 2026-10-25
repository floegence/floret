package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/sessiontree"
	publicstorage "github.com/floegence/floret/v7/storage"
)

type displayFailureRepo struct {
	sessiontree.Repo
	err error
}

func (repo displayFailureRepo) ThreadPrepared(context.Context, string) (bool, error) {
	return true, nil
}
func (repo displayFailureRepo) DisplayPrepared(context.Context, string) (bool, error) {
	return false, repo.err
}

func TestDisplayFailureDoesNotChangeRuntimeReadiness(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: publicstorage.Memory(), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	host.store.repo = displayFailureRepo{Repo: host.store.repo, err: sessiontree.ErrAuthorityCorrupt}
	ready, err := host.Readiness(t.Context(), "thread-display-readiness")
	if err != nil || !ready.Ready {
		t.Fatalf("optional display failure changed canonical readiness: %+v, %v", ready, err)
	}
	if ready.DisplayReady || ready.DisplayError == "" {
		t.Fatalf("display error was lost: %+v", ready)
	}
}

func TestPrepareDisplayThreadReadinessAndCancellation(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(t.TempDir() + "/display.sqlite"), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	const thread = "thread-display-ready"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: thread}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := host.PrepareDisplayThread(t.Context(), thread); err != nil {
			t.Fatal(err)
		}
		ready, err := host.Readiness(t.Context(), thread)
		if err != nil || !ready.Ready || !ready.DisplayReady || ready.DisplayError != "" {
			t.Fatalf("prepared readiness: %+v, %v", ready, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := host.PrepareDisplayThread(ctx, thread); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation: %v", err)
	}
}

type blockingDisplayRepo struct {
	sessiontree.Repo
	entered, canceled, settle chan struct{}
}

func (repo blockingDisplayRepo) PrepareDisplayThread(ctx context.Context, _ string) error {
	close(repo.entered)
	<-ctx.Done()
	close(repo.canceled)
	<-repo.settle
	return ctx.Err()
}

func TestShutdownCancelsAndJoinsExplicitDisplayPreparation(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: publicstorage.Memory(), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	const thread = "thread-display-shutdown"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: thread}); err != nil {
		t.Fatal(err)
	}
	repo := blockingDisplayRepo{Repo: host.store.repo, entered: make(chan struct{}), canceled: make(chan struct{}), settle: make(chan struct{})}
	host.store.repo = repo
	prepared, stopped := make(chan error, 1), make(chan error, 1)
	go func() { prepared <- host.PrepareDisplayThread(t.Context(), thread) }()
	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("display preparation did not start")
	}
	go func() { stopped <- host.Shutdown(t.Context()) }()
	select {
	case <-repo.canceled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel display preparation")
	}
	select {
	case err := <-stopped:
		t.Fatalf("shutdown returned before storage settled: %v", err)
	default:
	}
	close(repo.settle)
	if err := <-prepared; !errors.Is(err, context.Canceled) {
		t.Fatalf("preparation: %v", err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}
