package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/storage"
)

type waitReadCounter struct {
	*internalstorage.BackendKernel
	lists atomic.Int64
	pages atomic.Int64
}

func (counter *waitReadCounter) ListThreads(ctx context.Context, options sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	counter.lists.Add(1)
	return counter.BackendKernel.ListThreads(ctx, options)
}

func (counter *waitReadCounter) ParentEvents(ctx context.Context, query sessiontree.ParentEventQuery) (sessiontree.ParentEventPage, error) {
	counter.pages.Add(1)
	return counter.BackendKernel.ParentEvents(ctx, query)
}

func TestConcurrentChildWaitsIgnoreTextAndUnrelatedLifecycles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		host, threads, _ := parentEventHost(t, true, storage.Memory())
		service := threads.(*threadRuntimeService)
		var parents []identity.ThreadID
		var groups [][]*threadRuntimeState
		for group, size := range []int{5, 3} {
			parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: RequestKey(fmt.Sprintf("parent-%d", group))})
			if err != nil {
				t.Fatal(err)
			}
			parents = append(parents, parent.ThreadID)
			var children []*threadRuntimeState
			for index := range size {
				name := fmt.Sprintf("child-%d-%d", group, index)
				child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: parent.ThreadID, RequestKey: RequestKey(name), TaskName: name, HostProfileRef: "default"})
				if err != nil {
					t.Fatal(err)
				}
				actor := service.runtime(child.ThreadID)
				actor.state.view.Activity = ThreadActivityActive
				children = append(children, actor)
			}
			groups = append(groups, children)
		}
		synctest.Wait()
		counter := &waitReadCounter{BackendKernel: host.store.repo.(*internalstorage.BackendKernel)}
		host.store.repo = counter
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		results := make(chan error, len(parents))
		for _, parent := range parents {
			go func() {
				_, err := service.WaitThreads(ctx, WaitThreadsInput{Scope: ThreadScope{ParentID: &parent}, PendingParentEvents: true, Timeout: time.Hour})
				results <- err
			}()
		}
		synctest.Wait()
		checkReads := func(want int64) {
			t.Helper()
			if lists, pages := counter.lists.Load(), counter.pages.Load(); lists != want || pages != want {
				t.Fatalf("inventory/receipt reads = %d/%d, want %d/%d", lists, pages, want, want)
			}
		}
		checkReads(2)
		for batch := range 40 {
			for _, children := range groups {
				for _, actor := range children {
					if err := actor.apply(ctx, func() error {
						actor.markAppend("text", batch, "x")
						actor.state.view.ViewVersion++
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			time.Sleep(2 * threadGrowthInterval)
			synctest.Wait()
		}
		checkReads(2)
		child := groups[0][0]
		if err := child.apply(ctx, func() error {
			child.state.view.Activity = ThreadActivityIdle
			child.state.view.ViewVersion++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		checkReads(3)
		cancel()
		for range parents {
			if err := <-results; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
	})
}
