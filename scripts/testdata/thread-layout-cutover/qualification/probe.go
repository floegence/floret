package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func inspect(ctx context.Context, path string, thread identity.ThreadID) (err error) {
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true, EnableParentEvents: true})
	if err != nil {
		return errors.New("open failed")
	}
	var dispatched atomic.Bool
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if host.Shutdown(shutdown) != nil || dispatched.Load() {
			err = errors.New("shutdown or deferred-execution invariant failed")
		}
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, statusErr := host.StorageMigrationStatus(ctx)
		if statusErr != nil || status.State == runtime.StorageMigrationFailed || status.State == runtime.StorageMigrationCancelled {
			return errors.New("storage migration did not complete")
		}
		if status.State == runtime.StorageMigrationReady {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	if err := host.PrepareDisplayThread(ctx, thread); err != nil {
		return errors.New("display preparation failed")
	}
	state, err := host.Readiness(ctx, thread)
	if err != nil || !state.Ready || !state.DisplayReady {
		return errors.New("thread not ready")
	}
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		dispatched.Store(true)
		return nil, errors.New("qualification forbids execution")
	}))
	if err != nil {
		return errors.New("thread service unavailable")
	}
	scope := threadquery.Scope{Authority: "offline-qualification", ThreadIDs: []identity.ThreadID{thread}}
	page, err := service.(runtime.ThreadItemPageReader).ItemPage(ctx, scope, runtime.ThreadItemPageInput{ThreadID: thread, Limit: 20})
	if err != nil || page.ThreadID != thread {
		return errors.New("authorized page read failed")
	}
	scheduling, err := service.(runtime.ThreadSchedulingReader).SchedulingState(ctx, thread)
	if err != nil {
		return errors.New("scheduling state read failed")
	}
	encoded, err := json.Marshal([]any{page, scheduling.TurnID.String(), scheduling.Activity, scheduling.LastOutcome, scheduling.QueuedRequestKeys})
	if err != nil {
		return errors.New("fingerprint failed")
	}
	fmt.Printf("ready=true page_items=%d queued=%d sha256=%x\n", len(page.Items), len(scheduling.QueuedRequestKeys), sha256.Sum256(encoded))
	return nil
}

func main() {
	if len(os.Args) != 3 || os.Getenv("FLOEGENCE_TEST_RUN_DIR") == "" {
		os.Exit(2)
	}
	root, err := filepath.EvalSymlinks(os.Getenv("FLOEGENCE_TEST_RUN_DIR"))
	path, pathErr := filepath.EvalSymlinks(os.Args[1])
	thread, threadErr := identity.ParseThreadID(os.Args[2])
	if err != nil || pathErr != nil || threadErr != nil || !filepath.IsAbs(root) || !strings.HasPrefix(path, root+string(os.PathSeparator)) {
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := inspect(ctx, path, thread); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
