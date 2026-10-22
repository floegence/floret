package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type readinessWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (ctx *readinessWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestStorageReadinessPublishesUsabilityToExistingWaiters(t *testing.T) {
	readiness := newStorageReadiness()
	done, started := readiness.begin()
	if !started {
		t.Fatal("initialization did not start")
	}
	ctx := &readinessWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- readiness.waitUsable(ctx) }()
	<-ctx.waiting
	readiness.markUsable()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("existing operation waited for unrelated background migration")
	}
	select {
	case <-done:
		t.Fatal("publishing usability completed background migration")
	default:
	}
	failure := errors.New("historical thread is corrupt")
	readiness.finish(failure)
	if err := readiness.waitUsable(t.Context()); err != nil {
		t.Fatalf("background failure disabled published owner: %v", err)
	}
	if err := readiness.wait(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("background failure was lost: %v", err)
	}
}

func TestStorageReadinessRetriesFailedOwnerInstallation(t *testing.T) {
	readiness := newStorageReadiness()
	readiness.begin()
	failure := errors.New("owner installation failed")
	readiness.finish(failure)
	if err := readiness.waitUsable(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("installation failure = %v", err)
	}
	readiness.begin()
	readiness.markUsable()
	if err := readiness.waitUsable(t.Context()); err != nil {
		t.Fatal(err)
	}
	readiness.finish(nil)
	if err := readiness.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}
